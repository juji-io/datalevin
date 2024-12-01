package datalevin.cpp;


import org.bytedeco.javacpp.*;
import dtlvnative.DTLV;

/**
 * Wrap a MDB_envinfo
 */
public class Info {

    private DTLV.MDB_envinfo info;

    public Info(Env env) {
        info = new DTLV.MDB_envinfo();
        Util.checkRc(DTLV.mdb_env_info(env.get(), info));
    }

    /**
     * Factory method to create an instance
     */
    public static Info create(Env env) {
        return new Info(env);
    }

    /**
     * Free memory
     */
    public void close() {
        info.close();
    }

    /**
     * Return the MDB_envinfo pointer to be used in LMDB calls
     */
    public DTLV.MDB_envinfo get() {
        return info;
    }

}
