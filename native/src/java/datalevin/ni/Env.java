package datalevin.ni;

import org.graalvm.word.WordFactory;
import org.graalvm.nativeimage.UnmanagedMemory;
import org.graalvm.nativeimage.c.CContext;
import org.graalvm.nativeimage.c.type.CTypeConversion;
import org.graalvm.nativeimage.c.type.WordPointer;
import org.graalvm.nativeimage.c.struct.SizeOf;

/**
 * Wrap a MDB_env pointer pointer
 */
@CContext(Lib.Directives.class)
public class Env {

    private WordPointer ptr;

    public Env(WordPointer ptr) {
        this.ptr = ptr;
    }

    /**
     * Factory method to create an instance
     */
    public static Env create(String dir, long size, int maxReaders, int maxDbs) {

        WordPointer ptr = UnmanagedMemory.malloc(SizeOf.get(WordPointer.class));
        Lib.checkRc(Lib.mdb_env_create(ptr));

        Lib.MDB_env env = (Lib.MDB_env)ptr.read();

        Lib.checkRc(Lib.mdb_env_set_mapsize(env, size));
        Lib.checkRc(Lib.mdb_env_set_maxreaders(env, maxReaders));
        Lib.checkRc(Lib.mdb_env_set_maxdbs(env, maxDbs));
        Lib.checkRc(Lib.mdb_env_open(env,
                                     CTypeConversion.toCString(dir).get(),
                                     Mask.set(Lib.MDB_NORDAHEAD(),
                                              Lib.MDB_MAPASYNC(),
                                              Lib.MDB_WRITEMAP()),
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
        Lib.mdb_env_close(get());
        UnmanagedMemory.free(ptr);
    }

    public void setMapSize(long size) {
        Lib.checkRc(Lib.mdb_env_set_mapsize(get(), size));
    }

}
