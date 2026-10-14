package datalevin;

/**
 * Explicit KV write transaction handle.
 *
 * <p>Use this handle for reads and writes inside the transaction, then call
 * {@link #commit()} to persist changes or {@link #abort()} to roll them back.
 * Closing an active transaction aborts it.
 */
public final class KVTransaction extends KV {

    private final KV owner;
    private final KvCallOwner callOwner;
    private boolean finished;

    KVTransaction(KV owner, Object transactionKv) {
        this(owner, transactionKv, null);
    }

    KVTransaction(KV owner, Object transactionKv, KvCallOwner callOwner) {
        super(transactionKv, false);
        this.owner = owner;
        this.callOwner = callOwner;
        this.finished = false;
    }

    static KVTransaction begin(KV owner) {
        Object kv = owner.resource();
        Object transactionKv = ClojureRuntime.core("begin-kv-transaction", kv);
        try {
            return new KVTransaction(owner, transactionKv);
        } catch (RuntimeException | Error failure) {
            try {
                ClojureRuntime.core("abort-kv-transaction", kv);
            } catch (RuntimeException | Error cleanupFailure) {
                if (cleanupFailure != failure) failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    /**
     * Returns whether this transaction is still active.
     */
    public synchronized boolean active() {
        return !finished && !isReleased();
    }

    /**
     * Commits this transaction.
     */
    public Object commit() {
        return callOwner == null ? commitOwned() : callOwner.call(this::commitOwned);
    }

    private synchronized Object commitOwned() {
        ensureActive();
        try {
            Object result = ClojureRuntime.core("commit-kv-transaction", owner.resource());
            if (callOwner != null) callOwner.transactionFinished();
            return result;
        } finally {
            finish();
        }
    }

    /**
     * Aborts this transaction.
     */
    public Object abort() {
        return callOwner == null ? abortOwned() : callOwner.call(this::abortOwned);
    }

    private synchronized Object abortOwned() {
        ensureActive();
        try {
            Object result = ClojureRuntime.core("abort-kv-transaction", owner.resource());
            if (callOwner != null) callOwner.transactionFinished();
            return result;
        } finally {
            finish();
        }
    }

    /**
     * Aborts this transaction if it is still active.
     */
    @Override
    public void close() {
        if (active()) {
            abort();
        }
    }

    private void ensureActive() {
        if (!active()) {
            throw new IllegalStateException("KV transaction is closed.");
        }
    }

    private void finish() {
        finished = true;
        super.close();
        if (callOwner != null) callOwner.close();
    }
}
