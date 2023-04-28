package datalevin.ni;

import org.graalvm.word.WordFactory;
import org.graalvm.nativeimage.UnmanagedMemory;
import org.graalvm.nativeimage.c.CContext;
import org.graalvm.nativeimage.c.type.CTypeConversion;
import org.graalvm.nativeimage.c.type.WordPointer;
import org.graalvm.nativeimage.c.struct.SizeOf;

import java.util.List;

/**
 * Wrap a MDB_env pointer pointer
 */
@CContext(Directives.class)
public class Env {

    private boolean closed;

    private WordPointer ptr;

    public Env(WordPointer ptr) {
        this.ptr = ptr;
    }

    /**
     * Factory method to create an instance
     */
    public static Env create(String dir, long size, int maxReaders, int maxDbs,
                             int flags) {

        WordPointer ptr = UnmanagedMemory.malloc(SizeOf.get(WordPointer.class));
        Lib.checkRc(Lib.mdb_env_create(ptr));

        Lib.MDB_env env = (Lib.MDB_env)ptr.read();

        Lib.checkRc(Lib.mdb_env_set_mapsize(env, size));
        Lib.checkRc(Lib.mdb_env_set_maxreaders(env, maxReaders));
        Lib.checkRc(Lib.mdb_env_set_maxdbs(env, maxDbs));
        Lib.checkRc(Lib.mdb_env_open(env,
                                     CTypeConversion.toCString(dir).get(),
                                     flags,
                                     0664));

        return new Env(ptr);
    }

    /**
     * Return the MDB_env pointer to be used in LMDB calls
     */
    public Lib.MDB_env get() {
        return (Lib.MDB_env)ptr.read();
    }

    /**
     * Close and free memory
     */
    public void close() {
        if (closed) {
            return;
        }
        Lib.mdb_env_close(get());
        UnmanagedMemory.free(ptr);
        closed = true;
    }

    /**
     * return closed status
     */
    public boolean isClosed() {
        return closed;
    }

    /**
     * Force sync to disk
     */
    public void sync() {
        if (closed) {
            return;
        }
        final int force = 1;
        Lib.checkRc(Lib.mdb_env_sync(get(), force));
    }

    public void setMapSize(long size) {
        Lib.checkRc(Lib.mdb_env_set_mapsize(get(), size));
    }

    public void copy(String dest, boolean compact) {
        int flag = compact ? Lib.MDB_CP_COMPACT() : 0;
        Lib.checkRc(Lib.mdb_env_copy2(get(),
                                      CTypeConversion.toCString(dest).get(),
                                      flag));
    }

}
