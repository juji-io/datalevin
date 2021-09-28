package datalevin.ni;

import org.graalvm.word.WordFactory;
import org.graalvm.nativeimage.UnmanagedMemory;
import org.graalvm.nativeimage.c.CContext;
import org.graalvm.nativeimage.c.type.CTypeConversion;
import org.graalvm.nativeimage.c.type.WordPointer;
import org.graalvm.nativeimage.c.type.CLongPointer;
import org.graalvm.nativeimage.c.struct.SizeOf;

/**
 * Wrap a MDB_cursor pointer pointer
 */
@CContext(Lib.Directives.class)
public class Cursor {

    private WordPointer ptr;
    private CLongPointer cPtr;

    public Cursor(WordPointer ptr, CLongPointer cPtr) {
        this.ptr = ptr;
        this.cPtr = cPtr;
    }

    /**
     * Factory method to create an instance
     */
    public static Cursor create(Txn txn, Dbi dbi) {

        WordPointer ptr = UnmanagedMemory.malloc(SizeOf.get(WordPointer.class));
        CLongPointer cPtr = UnmanagedMemory.malloc(SizeOf.get(CLongPointer.class));

        Lib.checkRc(Lib.mdb_cursor_open(txn.get(), dbi.get(), ptr));

        return new Cursor(ptr, cPtr);
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
        UnmanagedMemory.free(cPtr);
    }

    /**
     * Return count of duplicates for current key.
     */
    public long count() {
        Lib.checkRc(Lib.mdb_cursor_count(get(), cPtr));
        return (long)cPtr.read();
    }
}
