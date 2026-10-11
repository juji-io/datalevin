/* Copyright (c) Huahai Yang. All rights reserved.
 * Distributed under the Eclipse Public License 2.0.
 */
package datalevin.utl;

import clojure.lang.IFn;
import clojure.lang.IPersistentMap;
import clojure.lang.Volatile;
import java.util.List;

/**
 * State owned by one native writer view. Only the native owner mutates it.
 * Trusted resolvers reuse the member fields and callbacks within a batch;
 * once scoped to user code, the context must never begin another member.
 * Row carriers remain lazily allocated and belong to their original member.
 */
public final class WriterContext {
    private final Thread owner;
    private Object transaction;
    private Volatile validity;
    private IFn rowCapture;
    private IFn storageStaged;
    private IFn prepareFlush;
    private IFn batchAbort;
    private IFn transactionFailed;
    private IFn beforeRead;
    private Object descriptor;
    private List<?> rows;
    private List<?> walRows;
    private IPersistentMap requestContext;

    public WriterContext(Thread owner, Object transaction) {
        this.owner = owner;
        this.transaction = transaction;
    }

    public Thread owner() { return owner; }
    public Object transaction() { return transaction; }
    public Volatile validity() { return validity; }
    public IFn rowCapture() { return rowCapture; }
    public IFn storageStaged() { return storageStaged; }
    public IFn prepareFlush() { return prepareFlush; }
    public IFn batchAbort() { return batchAbort; }
    public IFn transactionFailed() { return transactionFailed; }
    public IFn beforeRead() { return beforeRead; }
    public Object descriptor() { return descriptor; }
    public List<?> rows() { return rows; }
    public List<?> walRows() { return walRows; }
    public IPersistentMap requestContext() { return requestContext; }

    /** A marked child view inherits callbacks and the original scope lifetime. */
    public WriterContext fork(Thread owner, Object transaction) {
        WriterContext child = new WriterContext(owner, transaction);
        child.validity = validity;
        child.rowCapture = rowCapture;
        child.storageStaged = storageStaged;
        child.prepareFlush = prepareFlush;
        child.batchAbort = batchAbort;
        child.transactionFailed = transactionFailed;
        child.beforeRead = beforeRead;
        child.descriptor = descriptor;
        child.rows = rows;
        child.walRows = walRows;
        child.requestContext = requestContext;
        return child;
    }

    /** Refresh ownership after native map growth replaces the transaction. */
    public void setTransaction(Object transaction) { this.transaction = transaction; }
    public void setBeforeRead(IFn callback) { beforeRead = callback; }
    public void setTransactionFailed(IFn callback) { transactionFailed = callback; }
    public void setRequestContext(IPersistentMap context) { requestContext = context; }

    /** Install callbacks once for a reusable resolver or a private body view. */
    public void setCallbacks(IFn capture, IFn staged, IFn flush, IFn abort, IFn failed) {
        if (validity != null) {
            throw new IllegalStateException("Scoped writer callbacks cannot be replaced");
        }
        rowCapture = capture;
        storageStaged = staged;
        prepareFlush = flush;
        batchAbort = abort;
        transactionFailed = failed;
    }

    /** Bind a trusted resolver to its next request without allocating carriers. */
    public void beginMember(Object descriptor, IPersistentMap context) {
        if (validity != null) {
            throw new IllegalStateException("Scoped writer context cannot be reused");
        }
        this.descriptor = descriptor;
        rows = null;
        walRows = null;
        requestContext = context;
    }

    public void ensureRows() {
        if (rows == null) {
            rows = new RowRegions();
            walRows = new RowRegions();
        }
    }

    /** Permanently bind an escaping view and capture callback to one request. */
    public void scopeTo(Volatile validity, IFn capture) {
        if (this.validity != null) {
            throw new IllegalStateException("Writer context is already scoped");
        }
        this.validity = validity;
        rowCapture = capture;
    }

    public void invalidate() {
        if (validity != null) validity.reset(Boolean.FALSE);
    }

    /** Release member references, retaining the permanent scope retirement. */
    public void endMember() {
        invalidate();
        descriptor = null;
        rows = null;
        walRows = null;
        requestContext = null;
    }
}
