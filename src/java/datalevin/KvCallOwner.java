package datalevin;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

/** Keeps a bridge's explicit KV transaction on one JVM thread across async calls. */
final class KvCallOwner {
    private static final Map<Object, KvCallOwner> OWNERS = new IdentityHashMap<>();
    private static volatile boolean hasOwners;
    private static final Runnable STOP = () -> {};

    private final BlockingQueue<Runnable> work = new LinkedBlockingQueue<>();
    private final Thread thread;
    private final Object kv;
    private boolean transactionOpen;
    private boolean active = true;

    private KvCallOwner(Object kv) {
        this.kv = kv;
        thread = new Thread(() -> {
            try {
                while (true) {
                    Runnable request = work.take();
                    if (request == STOP) break;
                    request.run();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                // A cancelled begin must not strand a native writer when its
                // result was never returned to the bridge caller.
                if (transactionOpen) ClojureRuntime.core("abort-kv-transaction", kv);
            }
        }, "datalevin-js-kv-transaction");
        thread.setDaemon(true);
        thread.start();
    }

    static KVTransaction begin(Object kv) {
        KvCallOwner owner = new KvCallOwner(kv);
        try {
            return owner.call(() -> {
                Object view = ClojureRuntime.core("begin-kv-transaction", kv);
                owner.transactionOpen = true;
                owner.track(view);
                return new KVTransaction(new KV(kv, false), view, owner);
            });
        } catch (RuntimeException | Error e) {
            owner.close();
            throw e;
        }
    }

    static boolean hasOwners() {
        return hasOwners;
    }

    static KvCallOwner find(Object[] arguments) {
        if (!hasOwners || arguments == null) return null;
        KvCallOwner found = null;
        synchronized (OWNERS) {
            for (Object argument : arguments) {
                KvCallOwner owner = OWNERS.get(argument);
                if (owner != null) {
                    if (found != null && found != owner) {
                        throw new IllegalArgumentException("Cannot combine different KV transaction owners");
                    }
                    found = owner;
                }
            }
        }
        return found;
    }

    Object trackPrepared(Object value) {
        if (value != null && "datalevin.prepared.PreparedRead".equals(value.getClass().getName())) {
            track(value);
        }
        return value;
    }

    private void track(Object value) {
        synchronized (this) {
            if (!active) throw new IllegalStateException("KV transaction is closed.");
            synchronized (OWNERS) {
                OWNERS.put(value, this);
                hasOwners = true;
            }
        }
    }

    void transactionFinished() {
        transactionOpen = false;
    }

    <T> T call(Supplier<T> operation) {
        FutureTask<T> request;
        synchronized (this) {
            if (!active) throw new IllegalStateException("KV transaction is closed.");
            request = new FutureTask<>(() -> {
                synchronized (this) {
                    if (!active) throw new IllegalStateException("KV transaction is closed.");
                }
                return operation.get();
            });
            if (Thread.currentThread() != thread) work.offer(request);
        }
        if (Thread.currentThread() == thread) request.run();
        return UdfCallbackScope.await(request);
    }

    synchronized void close() {
        if (!active) return;
        active = false;
        synchronized (OWNERS) {
            OWNERS.values().removeIf(owner -> owner == this);
            hasOwners = !OWNERS.isEmpty();
        }
        work.offer(STOP);
    }
}
