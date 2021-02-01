package datalevin.ni;

import org.graalvm.word.WordFactory;
import org.graalvm.nativeimage.UnmanagedMemory;
import org.graalvm.nativeimage.c.CContext;
import org.graalvm.nativeimage.c.type.CTypeConversion;
import org.graalvm.nativeimage.c.type.WordPointer;
import org.graalvm.nativeimage.c.struct.SizeOf;

/**
 * Wrap a LMDB MDB_txn pointer pointer
 */
@CContext(Lib.Directives.class)
public class Txn {

    private WordPointer ptr;

    public Txn(WordPointer ptr) {
        this.ptr = ptr;
    }

    /**
     * Factory method to create a read only transaction instance
     */
    public static Txn createReadOnly(Env env) {
        return create(env, Lib.MDB_RDONLY());
    }

    /**
     * Factory method to create a read/write transaction instance
     */
    public static Txn create(Env env) {
        return create(env, 0);
    }

    public static Txn create(Env env, int flags) {
        WordPointer ptr = UnmanagedMemory.malloc(SizeOf.get(WordPointer.class));
        Lib.checkRc(Lib.mdb_txn_begin(env.get(),
                                      WordFactory.nullPointer(),
                                      flags,
                                      ptr));

        return new Txn(ptr);
    }

    /**
     * Return the MDB_txn pointer to be used in LMDB calls
     */
    public Lib.MDB_txn get() {
        return (Lib.MDB_txn)ptr.read();
    }

    /**
     * Close env and free memory
     */
    public void close() {
        Lib.mdb_txn_abort(get());
        UnmanagedMemory.free(ptr);
    }

    public void reset() {
        Lib.mdb_txn_reset(get());
    }

    public void renew() {
        Lib.checkRc(Lib.mdb_txn_renew(get()));
    }

    public void commit() {
        Lib.checkRc(Lib.mdb_txn_commit(get()));
        UnmanagedMemory.free(ptr);
    }
}
