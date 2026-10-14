package datalevin.cpp;

import org.bytedeco.javacpp.*;
import datalevin.dtlvnative.DTLV;

/**
 * Wrap MDB_dbi
 */
public class Dbi {

    private IntPointer ptr;
    private int handle;
    private String name;
    private final boolean kvInfo;

    public Dbi(Env env, String name, int flags) {
        this.name = name;
        this.kvInfo = "datalevin/kv-info".equals(name);
        this.ptr = new IntPointer(1);

        Txn txn = null;
        try {
            txn = Txn.create(env);
            Util.checkRc(DTLV.mdb_dbi_open(txn.get(), name, flags, ptr));
            handle = (int) ptr.get();
            txn.commit();
        } catch (RuntimeException | Error e) {
            ptr.close();
            throw e;
        } finally {
            if (txn != null) txn.close();
        }
    }

    /**
     * Open a DBI inside a caller-owned transaction. The caller is responsible
     * for committing or aborting the transaction; aborting also rolls back the
     * named-database catalog entry created here.
     */
    public Dbi(Txn txn, String name, int flags) {
        this.name = name;
        this.kvInfo = "datalevin/kv-info".equals(name);
        this.ptr = new IntPointer(1);
        try {
            Util.checkRc(DTLV.mdb_dbi_open(txn.get(), name, flags, ptr));
            handle = (int) ptr.get();
        } catch (RuntimeException | Error e) {
            ptr.close();
            throw e;
        }
    }

    /**
     * Factory method to create an instance
     */
    public static Dbi create(Env env, String name, int flags) {
        return new Dbi(env, name, flags);
    }

    /**
     * Factory method to open a DBI within an existing transaction
     */
    public static Dbi open(Txn txn, String name, int flags) {
        return new Dbi(txn, name, flags);
    }

    /**
     * Release JavaCPP storage. The environment owns the native DBI handle;
     * mdb_drop with del=1 also closes that handle.
     */
    public void close() {
        ptr.close();
    }

    /**
     * Return the MDB_dbi integer to be used in DTLV calls
     */
    public int get() {
        return handle;
    }

    public String getName() {
        return name;
    }

    /** Invalidate the transaction's metadata cache before a possible write. */
    void noteWrite(Txn txn) {
        if (kvInfo && !txn.isReadOnly()) txn.markKvInfoChanged();
    }

    public void put(Txn txn, BufVal k, BufVal v, int mask) {
        noteWrite(txn);
        Util.checkRc(DTLV.mdb_put(txn.get(), handle, k.ptr(), v.ptr(), mask));
    }

    public void del(Txn txn, BufVal k, BufVal v) {
        noteWrite(txn);
        DTLV.MDB_val vp;
        if (v == null) {
            vp = null;
        } else {
            vp = v.ptr();
        }
        Util.checkRc(DTLV.mdb_del(txn.get(), handle, k.ptr(), vp));
    }

}
