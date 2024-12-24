package datalevin.cpp;

import org.bytedeco.javacpp.*;
import dtlvnative.DTLV;

/**
 * Wrap MDB_dbi
 */
public class Dbi {

    private IntPointer ptr;
    private int handle;
    private String name;

    public Dbi(Env env, String name, int flags) {
        this.name = name;
        this.ptr = new IntPointer(1);

        Txn txn = Txn.create(env);
        try {
            Util.checkRc(DTLV.mdb_dbi_open(txn.get(), name, flags, ptr));
            handle = get();
            Util.checkRc(DTLV.dtlv_set_comparator(txn.get(), handle));
        } catch (Exception e) {
            txn.close();
            throw e;
        }

        txn.commit();
    }

    // public Dbi(Env env, int flags) {
    //     this.name = null;
    //     this.ptr = new IntPointer(1);

    //     Txn txn = Txn.create(env);
    //     try {
    //         Util.checkRc(DTLV.mdb_dbi_open(txn.get(),
    //                                     WordFactory.nullPointer(),
    //                                     flags,
    //                                     ptr));
    //         Util.checkRc(DTLV.dtlv_set_comparator(txn.get(), get()));
    //     } catch(Exception e) {
    //         txn.close();
    //         throw e;
    //     }

    //     txn.commit();
    // }

    /**
     * Factory method to create an instance
     */
    public static Dbi create(Env env, String name, int flags) {
        return new Dbi(env, name, flags);
    }

    // public static Dbi create(Env env, int flags) {
    //     return new Dbi(env, flags);
    // }

    /**
     * Free memory
     */
    public void close() {
        ptr.close();
    }

    /**
     * Return the MDB_dbi integer to be used in DTLV calls
     */
    public int get() {
        return (int)ptr.get();
    }

    public String getName() {
        return name;
    }

    public void put(Txn txn, BufVal k, BufVal v, int mask) {
        Util.checkRc(DTLV.mdb_put(txn.get(), handle, k.ptr(), v.ptr(), mask));
    }

    public void del(Txn txn, BufVal k, BufVal v) {
        DTLV.MDB_val vp;
        if (v == null) {
            vp = null;
        } else {
            vp = v.ptr();
        }
        Util.checkRc(DTLV.mdb_del(txn.get(), handle, k.ptr(), vp));
    }

}
