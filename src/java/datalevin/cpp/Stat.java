package datalevin.cpp;

import org.bytedeco.javacpp.*;
import dtlvnative.DTLV;

/**
 * Wrap a MDB_stat
 */
public class Stat {

    private DTLV.MDB_stat stat;

    public Stat(Txn txn, Dbi dbi) {
        stat = new DTLV.MDB_stat();
        Util.checkRc(DTLV.mdb_stat(txn.get(), dbi.get(), stat));
    }

    public Stat(Env env) {
        stat = new DTLV.MDB_stat();
        Util.checkRc(DTLV.mdb_env_stat(env.get(), stat));
    }

    /**
     * Factory methods to create an instance
     */
    public static Stat create(Txn txn, Dbi dbi) {
        return new Stat(txn, dbi);
    }

    public static Stat create(Env env) {
        return new Stat(env);
    }

    /**
     * Close env and free memory
     */
    public void close() {
        stat.close();
    }

    /**
     * Return the MDB_env pointer to be used in LMDB calls
     */
    public DTLV.MDB_stat get() {
        return stat;
    }

}
