package datalevin.cpp;

import static datalevin.cpp.Txn.State.DONE;
import static datalevin.cpp.Txn.State.READY;
import static datalevin.cpp.Txn.State.RELEASED;
import static datalevin.cpp.Txn.State.RESET;

import org.bytedeco.javacpp.*;
import datalevin.dtlvnative.DTLV;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/**
 * Wrap a DTLV MDB_txn
 */
public class Txn {

    private State state;

    private DTLV.MDB_txn ptr;
    private final boolean readOnly;
    private final Thread writerOwner;
    private boolean kvInfoChanged;
    private IdentityHashMap<Dbi, ReadCursorPool> cursorPools;

    public Txn(DTLV.MDB_txn ptr, boolean readOnly) {
        this.ptr = ptr;
        this.readOnly = readOnly;
        writerOwner = readOnly ? null : Thread.currentThread();
        state = READY;
    }

    /** Native write transactions remain confined to the thread that opened them. */
    public boolean isWriterOwner() {
        return writerOwner == Thread.currentThread();
    }

    /**
     * Factory method to create a read only transaction instance
     */
    public static Txn createReadOnly(Env env) {
        return create(env, DTLV.MDB_RDONLY);
    }

    /**
     * Factory method to create a read/write transaction instance
     */
    public static Txn create(Env env) {
        return create(env, 0);
    }

    public static Txn create(Env env, int flags) {
        DTLV.MDB_txn ptr = new DTLV.MDB_txn();
        Util.checkRc(DTLV.mdb_txn_begin(env.get(), null, flags, ptr));

        if ((flags & DTLV.MDB_RDONLY) == DTLV.MDB_RDONLY) {
            return new Txn(ptr, true);
        } else {
            return new Txn(ptr, false);
        }
    }

    void checkReady() {
        if (state != READY)
            throw new Util.NotReadyException("Txn not in READY state");
    }

    /**
     * Return the MDB_txn pointer to be used in DTLV calls
     */
    public DTLV.MDB_txn get() {
        return ptr;
    }

    /** Return the snapshot ID, or the pending commit ID for a writer. */
    public long id() {
        checkReady();
        return DTLV.mdb_txn_id(ptr);
    }

    /** Record a possible change to KV metadata within this transaction. */
    void markKvInfoChanged() {
        kvInfoChanged = true;
    }

    /** Whether cached metadata from the preceding commit needs a refresh. */
    public boolean hasKvInfoChanges() {
        return kvInfoChanged;
    }

    public void abort() {
        checkReady();
        closeCursorPools();
        state = DONE;
        DTLV.mdb_txn_abort(ptr);
    }

    public void close() {
        if (state == RELEASED) return;
        closeCursorPools();
        if (state == READY || state == RESET) DTLV.mdb_txn_abort(ptr);
        state = RELEASED;
    }

    private void closeCursorPools() {
        if (cursorPools != null) {
            for (ReadCursorPool pool : cursorPools.values()) pool.close();
            cursorPools.clear();
            cursorPools = null;
        }
    }

    /**
     * Borrow a cursor using the supplied key/value buffers. Read cursors belong
     * to this transaction and survive reset/renew, but are all freed before
     * commit, abort, or close, including abandoned cursors from terminated readers.
     */
    public Cursor getCursor(Dbi dbi, BufVal key, BufVal val) {
        checkReady();
        if (!readOnly) return Cursor.create(this, dbi, key, val);
        if (cursorPools == null) cursorPools = new IdentityHashMap<>();
        ReadCursorPool pool = cursorPools.get(dbi);
        if (pool == null) {
            pool = new ReadCursorPool(this, dbi);
            cursorPools.put(dbi, pool);
        }
        return pool.borrow(key, val);
    }

    /** Access is confined to the reader, or its quiescent teardown owner. */
    static final class ReadCursorPool {
        final Txn txn;
        private final Dbi dbi;
        private final ArrayDeque<Cursor> available = new ArrayDeque<>();
        private final Set<Cursor> owned =
            Collections.newSetFromMap(new IdentityHashMap<>());

        ReadCursorPool(Txn txn, Dbi dbi) {
            this.txn = txn;
            this.dbi = dbi;
        }

        Cursor borrow(BufVal key, BufVal val) {
            Cursor cursor = available.poll();
            if (cursor != null) {
                cursor.pooled = false;
                try {
                    return cursor.renew(txn, key, val);
                } catch (RuntimeException | Error e) {
                    cursor.close();
                    throw e;
                }
            }
            cursor = Cursor.create(txn, dbi, key, val);
            cursor.pool = this;
            owned.add(cursor);
            return cursor;
        }

        void release(Cursor cursor) {
            if (!cursor.pooled) {
                available.add(cursor);
                cursor.pooled = true;
            }
        }

        void remove(Cursor cursor) {
            owned.remove(cursor);
            if (cursor.pooled) {
                available.remove(cursor);
                cursor.pooled = false;
            }
        }

        void close() {
            // Closing also removes each cursor from the ownership set.
            for (Cursor cursor : owned.toArray(new Cursor[0])) cursor.close();
        }
    }

    public void commit() {
        checkReady();
        closeCursorPools();
        state = DONE;
        Util.checkRc(DTLV.mdb_txn_commit(ptr));
    }

    public void reset() {
        if (state != READY && state != DONE)
            throw new Util.ResetException("Txn cannot be reset");
        state = RESET;
        DTLV.mdb_txn_reset(ptr);
    }

    public void renew() {
        if (state != RESET)
            throw new Util.NotResetException("Txn cannot be renew");
        state = DONE;
        Util.checkRc(DTLV.mdb_txn_renew(ptr));
        state = READY;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    enum State {
        READY, DONE, RESET, RELEASED
    }
}
