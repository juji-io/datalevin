package datalevin.cpp;

import org.bytedeco.javacpp.*;
import dtlvnative.DTLV;

/**
 * Wrap a DTLV MDB_txn
 */
public class Txn {

    private DTLV.MDB_txn ptr;
    private final boolean readOnly;

    public Txn(DTLV.MDB_txn ptr, boolean readOnly) {
        this.ptr = ptr;
        this.readOnly = readOnly;
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

    /**
     * Factory method to create a nosync read/write transaction instance
     */
    public static Txn createNoSync(Env env) {
        return create(env, DTLV.MDB_NOSYNC);
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

    /**
     * Return the MDB_txn pointer to be used in DTLV calls
     */
    public DTLV.MDB_txn get() {
        return ptr;
    }

    /**
     * Close env and free memory
     */
    public void close() {
        DTLV.mdb_txn_abort(ptr);
    }

    public void reset() {
        DTLV.mdb_txn_reset(ptr);
    }

    public void renew() {
        Util.checkRc(DTLV.mdb_txn_renew(ptr));
    }

    public void commit() {
        Util.checkRc(DTLV.mdb_txn_commit(ptr));
    }

    public boolean isReadOnly() {
        return readOnly;
    }
}
