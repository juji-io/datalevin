package datalevin.cpp;

import org.bytedeco.javacpp.*;
import datalevin.dtlvnative.DTLV;
import java.util.List;

/**
 * Wrap a MDB_env
 */
public class Env {

    private boolean closed;
    private DTLV.MDB_env env;

    public Env(DTLV.MDB_env env) {
        this.closed = false;
        this.env = env;
    }

    /**
     * Factory method to create an instance
     */
    public static Env create(String dir, long size, int maxReaders, int maxDbs,
                             int flags) {

        DTLV.MDB_env env = new DTLV.MDB_env();
        Util.checkRc(DTLV.mdb_env_create(env));

        Util.checkRc(DTLV.mdb_env_set_mapsize(env, size));
        Util.checkRc(DTLV.mdb_env_set_maxreaders(env, maxReaders));
        Util.checkRc(DTLV.mdb_env_set_maxdbs(env, maxDbs));

        Util.checkRc(DTLV.mdb_env_open(env, dir, flags, 0664));

        return new Env(env);
    }

    /**
     * Return the MDB_env pointer to be used in DTLV calls
     */
    public DTLV.MDB_env get() {
        return env;
    }

    /**
     * Close and free memory
     */
    public void close() {
        if (closed) {
            return;
        }
        DTLV.mdb_env_close(env);
        closed = true;
    }

    /**
     * return closed status
     */
    public boolean isClosed() {
        return closed;
    }

    /**
     * sync to disk, non-zero force will force a synchronous flush. Otherwise,
     * respect env flags.
     */
    public void sync(final int force) {
        if (closed) {
            return;
        }
        Util.checkRc(DTLV.mdb_env_sync(env, force));
    }

    /**
     * Set flags, non-zero onOff set the flags . Otherwise, clear the flags.
     */
    public void setFlags(final int toChange, final int onOff) {
        Util.checkRc(DTLV.mdb_env_set_flags(env, toChange, onOff));
    }

    public void setMapSize(long size) {
        Util.checkRc(DTLV.mdb_env_set_mapsize(env, size));
    }

    public void copy(String dest, boolean compact) {
        int flag = compact ? DTLV.MDB_CP_COMPACT : 0;
        Util.checkRc(DTLV.mdb_env_copy2(env, dest, flag));
    }

}
