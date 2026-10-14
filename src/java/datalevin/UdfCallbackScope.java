package datalevin;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
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
    private static final ThreadLocal<BlockingQueue<Runnable>> OWNER_WORK = new ThreadLocal<>();
    private static final Runnable WAKE = () -> {};

    private Thread owner = Thread.currentThread();
    private BlockingQueue<Runnable> work;
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
        BlockingQueue<Runnable> ownerWork = work;
        UdfCallbackScope scope = new UdfCallbackScope(work);
        FutureTask<Object> callback = new FutureTask<>(() -> fn.invoke(arguments.apply(scope)));
        try {
            ExecutorService callbacks = (ExecutorService) ClojureRuntime.invoke(
                    "datalevin.util", "get-udf-callback-thread-pool");
            callbacks.execute(() -> {
                try {
                    callback.run();
                } finally {
                    // Completion can race with scope cleanup after interruption.
                    ownerWork.offer(WAKE);
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
        FutureTask<T> request = null;
        synchronized (this) {
            checkActive();
            if (Thread.currentThread() != owner) {
                request = new FutureTask<>(operation::get);
                pending.add(request);
                work.offer(request);
            }
        }
        if (request == null) return operation.get();
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
        owner = null;
        work = null;
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
