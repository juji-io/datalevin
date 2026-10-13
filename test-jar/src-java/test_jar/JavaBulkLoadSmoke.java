package test_jar;

import datalevin.Connection;
import datalevin.Datalevin;
import datalevin.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public final class JavaBulkLoadSmoke {
    private static void check(boolean condition, String message) {
        if (!condition) throw new IllegalStateException(message);
    }

    public static void main(String[] args) {
        String uri = args[0] + "/java-bulk-" + UUID.randomUUID();
        String query = "[:find (count ?e) . :where [?e :value]]";
        List<Object> datoms = new ArrayList<>();
        for (long i = 1; i <= 1003; i++) datoms.add(List.of(i, ":value", i));
        try (Connection conn = Datalevin.initDb(datoms, uri,
                Datalevin.schema().attr("value", Schema.attribute().valueType(Schema.ValueType.LONG)));
             Connection observer = Datalevin.createConn(uri)) {
            check(((Number) conn.query(query)).longValue() == 1003, "copy-in bulk load");
            check(((Number) observer.query(query)).longValue() == 1003, "observer initial query");
            check(Datalevin.fillDb(conn, List.of(List.of(2000L, ":value", 2000L))) == conn,
                  "static fillDb returns connection");
            check(conn.fillDb(List.of(Datalevin.datom(2001, ":value", 2001L))) == conn,
                  "instance fillDb returns connection");
            check(((Number) observer.query(query)).longValue() == 1005, "observer cache refresh");
            conn.transact(List.of(Map.of(":db/id", -1L, ":value", 9999L)));
            check(conn.query("[:find ?e . :where [?e :value 9999]]").equals(2002L),
                  "transaction allocates ID after loaded entities");
        }
        try (Connection reopened = Datalevin.createConn(uri)) {
            check(((Number) reopened.query(query)).longValue() == 1006, "reopened data");
        }
        System.out.println("Java remote bulk load passed");
    }
}
