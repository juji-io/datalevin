package datalevin.ni;

import org.graalvm.word.WordFactory;
import org.graalvm.nativeimage.c.CContext;
import org.graalvm.nativeimage.c.struct.SizeOf;
import org.graalvm.nativeimage.c.type.CTypeConversion;
import org.graalvm.nativeimage.UnmanagedMemory;

/**
 * Wrap a MDB_stat pointer
 */
@CContext(Lib.Directives.class)
public class Stat {

    private Lib.MDB_stat stat;

    public Stat(Txn txn, Dbi dbi) {
        stat = UnmanagedMemory.calloc(SizeOf.get(Lib.MDB_stat.class));
        Lib.checkRc(Lib.mdb_stat(txn.get(), dbi.get(), stat));
    }

    /**
     * Factory method to create an instance
     */
    public static Stat create(Txn txn, Dbi dbi) {
        return new Stat(txn, dbi);
    }

    /**
     * Close env and free memory
     */
    public void close() {
        UnmanagedMemory.free(stat);
    }

    /**
     * Return the MDB_env pointer to be used in LMDB calls
     */
    public Lib.MDB_stat get() {
        return stat;
    }

}
