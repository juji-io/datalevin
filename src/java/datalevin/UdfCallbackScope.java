package datalevin;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Services asynchronous bridge callback reads on the original JVM caller.
 * Native transactions and Clojure dynamic bindings stay on their owning thread;
 * borrowed database, entity, and prepared handles expire with the callback.
 */
final class UdfCallbackScope {
    private static final ExecutorService CALLBACKS = Executors.newCachedThreadPool(runnable -> {
        Thread thread = new Thread(runnable, "datalevin-js-udf");
        thread.setDaemon(true);
        return thread;
    });
    private static final ThreadLocal<BlockingQueue<Runnable>> OWNER_WORK = new ThreadLocal<>();
    private static final Runnable WAKE = () -> {};

    private final Thread owner = Thread.currentThread();
    private final BlockingQueue<Runnable> work;
    private final Set<FutureTask<?>> pending = new HashSet<>();
    private boolean active = true;

    private UdfCallbackScope(BlockingQueue<Runnable> work) {
        this.work = work;
    }

    static boolean hasOwnerWork() {
        return OWNER_WORK.get() != null;
    }

    static Object invoke(UdfFunction fn, Function<UdfCallbackScope, List<?>> arguments) {
        BlockingQueue<Runnable> work = OWNER_WORK.get();
        boolean outermost = work == null;
        if (outermost) {
            work = new LinkedBlockingQueue<>();
            OWNER_WORK.set(work);
        }
        UdfCallbackScope scope = new UdfCallbackScope(work);
        FutureTask<Object> callback = new FutureTask<>(() -> fn.invoke(arguments.apply(scope)));
        try {
            CALLBACKS.execute(() -> {
                try {
                    callback.run();
                } finally {
                    scope.work.offer(WAKE);
                }
            });
            // Nested callbacks share this queue so the owner can service reads
            // submitted through either the inner or the outer database view.
            while (!callback.isDone()) {
                work.take().run();
            }
            return await(callback);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("UDF callback interrupted", e);
        } finally {
            scope.close();
            callback.cancel(true);
            if (outermost) OWNER_WORK.remove();
        }
    }

    static <T> T call(UdfCallbackScope scope, Supplier<T> operation) {
        return scope == null ? operation.get() : scope.call(operation);
    }

    private <T> T call(Supplier<T> operation) {
        if (Thread.currentThread() == owner) {
            synchronized (this) {
                checkActive();
            }
            return operation.get();
        }
        FutureTask<T> request;
        synchronized (this) {
            checkActive();
            request = new FutureTask<>(operation::get);
            pending.add(request);
            work.offer(request);
        }
        try {
            return await(request);
        } finally {
            synchronized (this) {
                pending.remove(request);
            }
        }
    }

    private void checkActive() {
        if (!active) throw new IllegalStateException("UDF database view is no longer active");
    }

    private synchronized void close() {
        active = false;
        for (FutureTask<?> request : pending) {
            request.cancel(false);
            work.remove(request);
        }
        pending.clear();
    }

    static <T> T await(FutureTask<T> task) {
        try {
            return task.get();
        } catch (InterruptedException e) {
            task.cancel(false);
            Thread.currentThread().interrupt();
            throw new IllegalStateException("UDF callback read interrupted", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException error) throw error;
            if (cause instanceof Error error) throw error;
            throw new IllegalStateException(cause);
        }
    }
}
