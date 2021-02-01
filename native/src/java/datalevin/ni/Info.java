package datalevin.ni;

import org.graalvm.word.WordFactory;
import org.graalvm.nativeimage.UnmanagedMemory;
import org.graalvm.nativeimage.c.CContext;
import org.graalvm.nativeimage.c.type.CTypeConversion;
import org.graalvm.nativeimage.c.struct.SizeOf;

/**
 * Wrap a MDB_envinfo pointer
 */
@CContext(Lib.Directives.class)
public class Info {

    private Lib.MDB_envinfo info;

    public Info(Env env) {
        info = UnmanagedMemory.calloc(SizeOf.get(Lib.MDB_envinfo.class));
        Lib.checkRc(Lib.mdb_env_info(env.get(), info));
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
        UnmanagedMemory.free(info);
    }

    /**
     * Return the MDB_envinfo pointer to be used in LMDB calls
     */
    public Lib.MDB_envinfo get() {
        return info;
    }

}
