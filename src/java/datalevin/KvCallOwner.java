package datalevin;

import java.lang.ref.Cleaner;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

/** Keeps a bridge's explicit KV transaction on one JVM thread across async calls. */
final class KvCallOwner {
    private static final Cleaner CLEANER = Cleaner.create();
    private static final ReferenceQueue<Object> VIEWS = new ReferenceQueue<>();
    private static final Map<ViewReference, WeakReference<KvCallOwner>> OWNERS = new HashMap<>();
    private static volatile boolean hasOwners;
    private static final Runnable STOP = () -> {};

    private final Worker worker;
    private final Cleaner.Cleanable cleanable;

    private KvCallOwner(Object kv) {
        worker = new Worker(kv);
        // Neither the worker thread nor the cleanup action may retain this owner.
        cleanable = CLEANER.register(this, worker::close);
        worker.thread.start();
    }

    static KVTransaction begin(Object kv) {
        KvCallOwner owner = new KvCallOwner(kv);
        try {
            return owner.call(() -> {
                Object view = ClojureRuntime.core("begin-kv-transaction", kv);
                owner.worker.transactionOpen = true;
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

    static boolean isPrepared(Object value) {
        return value instanceof OwnedPrepared;
    }

    static Object unwrapPrepared(Object value) {
        return value instanceof OwnedPrepared prepared ? prepared.value : value;
    }

    static KvCallOwner find(Object[] arguments) {
        if (arguments == null) return null;
        KvCallOwner found = null;
        synchronized (OWNERS) {
            removeCollectedViews();
            for (Object argument : arguments) {
                KvCallOwner owner;
                if (argument instanceof OwnedPrepared prepared) {
                    owner = prepared.owner;
                } else {
                    if (!hasOwners) continue;
                    WeakReference<KvCallOwner> reference = OWNERS.get(new ViewReference(argument));
                    owner = reference == null ? null : reference.get();
                }
                if (owner != null && owner.worker.active) {
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
            // A live prepared read keeps its transaction owner alive, but the
            // process-wide routing table never retains prepared reads.
            return new OwnedPrepared(value, this);
        }
        return value;
    }

    private void track(Object value) {
        synchronized (worker) {
            if (!worker.active) throw new IllegalStateException("KV transaction is closed.");
            synchronized (OWNERS) {
                removeCollectedViews();
                OWNERS.put(new ViewReference(value, VIEWS), new WeakReference<>(this));
                hasOwners = true;
            }
        }
    }

    private static void removeCollectedViews() {
        ViewReference reference;
        while ((reference = (ViewReference) VIEWS.poll()) != null) OWNERS.remove(reference);
        hasOwners = !OWNERS.isEmpty();
    }

    private static void removeWorker(Worker worker) {
        synchronized (OWNERS) {
            removeCollectedViews();
            OWNERS.values().removeIf(reference -> {
                KvCallOwner owner = reference.get();
                return owner == null || owner.worker == worker;
            });
            hasOwners = !OWNERS.isEmpty();
        }
    }

    void transactionFinished() {
        worker.transactionOpen = false;
    }

    <T> T call(Supplier<T> operation) {
        return worker.call(operation);
    }

    void close() {
        cleanable.clean();
    }

    private static final class OwnedPrepared {
        final Object value;
        final KvCallOwner owner;

        OwnedPrepared(Object value, KvCallOwner owner) {
            this.value = value;
            this.owner = owner;
        }
    }

    /** Weak identity lookup: transaction views have identity, not value, ownership. */
    private static final class ViewReference extends WeakReference<Object> {
        private final int hash;

        ViewReference(Object value) {
            this(value, null);
        }

        ViewReference(Object value, ReferenceQueue<Object> queue) {
            super(value, queue);
            hash = System.identityHashCode(value);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            Object value = get();
            return value != null && other instanceof ViewReference reference && value == reference.get();
        }
    }

    /** Held by the daemon and Cleaner, with no reference back to the owner. */
    private static final class Worker {
        private final BlockingQueue<Runnable> work = new LinkedBlockingQueue<>();
        private final Thread thread;
        private Object kv;
        private boolean transactionOpen;
        private volatile boolean active = true;

        Worker(Object kv) {
            this.kv = kv;
            thread = new Thread(this::run, "datalevin-js-kv-transaction");
            thread.setDaemon(true);
        }

        private void run() {
            try {
                while (true) {
                    Runnable request = work.take();
                    if (request == STOP) break;
                    request.run();
                    // FutureTask's outcome can be the transaction handle from
                    // begin. Do not retain it while blocking for the next call.
                    request = null;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                try {
                    if (transactionOpen) ClojureRuntime.core("abort-kv-transaction", kv);
                } finally {
                    synchronized (this) {
                        transactionOpen = false;
                        active = false;
                        kv = null;
                        removeWorker(this);
                        // An interrupted worker must release queued suppliers
                        // and wake their callers instead of retaining owners.
                        Runnable pending;
                        while ((pending = work.poll()) != null) {
                            if (pending instanceof FutureTask<?> task) task.cancel(false);
                        }
                    }
                }
            }
        }

        <T> T call(Supplier<T> operation) {
            FutureTask<T> request;
            synchronized (this) {
                if (!active) throw new IllegalStateException("KV transaction is closed.");
                request = new FutureTask<>(() -> {
                    if (!active) throw new IllegalStateException("KV transaction is closed.");
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
            removeWorker(this);
            // Abort must run on the thread that owns the native writer.
            work.offer(STOP);
        }
    }
}
