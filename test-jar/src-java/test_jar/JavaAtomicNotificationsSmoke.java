package test_jar;

import datalevin.Connection;
import datalevin.Datalevin;
import datalevin.KV;
import datalevin.KVTransaction;
import datalevin.KVType;
import datalevin.Schema;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public final class JavaAtomicNotificationsSmoke {
    private static final String INCREMENT = "(fn [old amount] (+ (long (or old 0)) (long amount)))";

    private record Change(Map<?, ?> event, List<String> names) { }

    private static void check(boolean condition, String message) {
        if (!condition) throw new IllegalStateException(message);
    }

    public static void main(String[] args) throws Exception {
        Path dir = Files.createTempDirectory("datalevin-java-update-");
        try {
            try (KV kv = Datalevin.openKV(dir.toString())) {
                kv.openDbi("counts");
                check(Datalevin.kw("transacted").equals(Datalevin.updateKv(kv, "counts", "key",
                        old -> old == null ? 2L : 0L, KVType.STRING, KVType.LONG)), "missing key update");
                kv.update("counts", "key", values -> ((Number) values.get(0)).longValue()
                        + ((Number) values.get(1)).longValue(), KVType.STRING, KVType.LONG, List.of(3L));
                check(kv.getValue("counts", "key", KVType.STRING, KVType.LONG, true).equals(5L), "callback args");
                kv.update("counts", "key", INCREMENT, KVType.STRING, KVType.LONG, 4L);
                try (KVTransaction tx = kv.beginTransaction()) {
                    tx.update("counts", "key", old -> ((Number) old).longValue() + 10,
                              KVType.STRING, KVType.LONG);
                    check(tx.getValue("counts", "key", KVType.STRING, KVType.LONG, true).equals(19L), "staged update");
                    tx.abort();
                }
                try {
                    kv.update("counts", "key", old -> { throw new IllegalArgumentException("update failed"); },
                              KVType.STRING, KVType.LONG);
                    throw new IllegalStateException("failed callback was accepted");
                } catch (IllegalArgumentException expected) {
                    check(expected.getMessage().contains("update failed"), "callback exception");
                }
                check(kv.getValue("counts", "key", KVType.STRING, KVType.LONG, true).equals(9L), "rollback");
                kv.openDbi("data");
                Datalevin.updateKv(kv, "data", "key", old -> Map.of("count", 1L));
                check(((Map<?, ?>) kv.getValue("data", "key")).get("count").equals(1L), "default types");
            }
        } finally {
            try (var files = Files.walk(dir)) {
                for (Path file : files.sorted(Comparator.reverseOrder()).toList()) Files.delete(file);
            }
        }

        String name = "java-notify-" + UUID.randomUUID();
        String uri = args[0] + "/" + name;
        Map<?, ?> opts = Map.of(":pool-size", 1L, ":time-out", 5000L);
        try (Connection reader = Datalevin.createConn(uri,
                Datalevin.schema().attr("name", Schema.attribute().valueType(Schema.ValueType.STRING)), opts);
             Connection writer = Datalevin.createConn(uri, (Map<?, ?>) null, opts)) {
            KV kv = writer.datalogKV();
            kv.openDbi("counts");
            LinkedBlockingQueue<Change> changes = new LinkedBlockingQueue<>();
            List<Map<?, ?>> oldEvents = new ArrayList<>();
            reader.listenDb("watch", oldEvents::add);
            check("watch".equals(Datalevin.listenDb(reader, "watch", event -> changes.offer(new Change(event,
                    reader.queryCollection(Datalevin.query().findAll("?name")
                            .whereDatom(Datalevin.var("e"), "name", Datalevin.var("name")), String.class))))), "subscription key");
            writer.transact(List.of(Map.of(":db/id", -1L, ":name", "first")));
            Change change = changes.poll(5, TimeUnit.SECONDS);
            check(change != null && change.names().contains("first"), "cross-client refresh");
            check(change.event().get(Datalevin.kw("type")).equals(Datalevin.kw("db-changed"))
                    && change.event().get(Datalevin.kw("db-name")).equals(name), "notification payload");
            check(oldEvents.isEmpty(), "listener replacement");
            Datalevin.updateKv(kv, "counts", "key", INCREMENT, KVType.STRING, KVType.LONG, 4L);
            check(changes.poll(5, TimeUnit.SECONDS) != null, "KV update notification");
            try (KVTransaction tx = kv.beginTransaction()) {
                tx.update("counts", "key", INCREMENT, KVType.STRING, KVType.LONG, 5L);
                tx.abort();
            }
            check(changes.poll(200, TimeUnit.MILLISECONDS) == null, "abort notification");
            check(kv.getValue("counts", "key", KVType.STRING, KVType.LONG, true).equals(4L), "remote rollback");
            Datalevin.unlistenDb(reader, "watch");
            writer.transact(List.of(Map.of(":db/id", -2L, ":name", "second")));
            check(changes.poll(200, TimeUnit.MILLISECONDS) == null, "unsubscribe");
            check(reader.listenDb(event -> changes.offer(new Change(event, List.of()))) != null, "generated key");
            reader.close();
            writer.transact(List.of(Map.of(":db/id", -3L, ":name", "third")));
            check(changes.poll(200, TimeUnit.MILLISECONDS) == null, "close cleanup");
        }
        System.out.println("Java atomic updates and database subscriptions passed");
    }
}
