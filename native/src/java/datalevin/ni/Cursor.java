package datalevin.ni;

import org.graalvm.word.WordFactory;
import org.graalvm.nativeimage.UnmanagedMemory;
import org.graalvm.nativeimage.c.CContext;
import org.graalvm.nativeimage.c.type.CTypeConversion;
import org.graalvm.nativeimage.c.type.WordPointer;
import org.graalvm.nativeimage.c.struct.SizeOf;

/**
 * Wrap a MDB_cursor pointer pointer
 */
@CContext(Lib.Directives.class)
public class Cursor {

    private WordPointer ptr;

    public Cursor(WordPointer ptr) {
        this.ptr = ptr;
    }

    /**
     * Factory method to create an instance
     */
    public static Cursor create(Txn txn, Dbi dbi) {

        WordPointer ptr = UnmanagedMemory.malloc(SizeOf.get(WordPointer.class));
        Lib.checkRc(Lib.mdb_cursor_open(txn.get(), dbi.get(), ptr));

        return new Cursor(ptr);
    }

    /**
     * Return the MDB_cursor pointer to be used in LMDB calls
     */
    public Lib.MDB_cursor get() {
        return (Lib.MDB_cursor)ptr.read();
    }

    /**
     * Close and free memory
     */
    public void close() {
        Lib.mdb_cursor_close(get());
        UnmanagedMemory.free(ptr);
    }
}
