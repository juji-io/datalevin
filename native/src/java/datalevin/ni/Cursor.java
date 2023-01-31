package datalevin.ni;

import org.graalvm.word.WordFactory;
import org.graalvm.nativeimage.UnmanagedMemory;
import org.graalvm.nativeimage.c.CContext;
import org.graalvm.nativeimage.c.type.CTypeConversion;
import org.graalvm.nativeimage.c.type.WordPointer;
import org.graalvm.nativeimage.c.type.CLongPointer;
import org.graalvm.nativeimage.c.struct.SizeOf;

import java.nio.ByteBuffer;

/**
 * Wrap a MDB_cursor pointer pointer
 */
@CContext(Directives.class)
public class Cursor {

    private WordPointer ptr;
    private CLongPointer cPtr;

    private BufVal key;
    private BufVal val;

    public Cursor(WordPointer ptr, CLongPointer cPtr, BufVal key, BufVal val) {
        this.ptr = ptr;
        this.cPtr = cPtr;
        this.key = key;
        this.val = val;
    }

    /**
     * Factory method to create an instance
     */
    public static Cursor create(Txn txn, Dbi dbi, BufVal key, BufVal val) {

        WordPointer ptr = UnmanagedMemory.malloc(SizeOf.get(WordPointer.class));
        CLongPointer cPtr
            = UnmanagedMemory.malloc(SizeOf.get(CLongPointer.class));

        Lib.checkRc(Lib.mdb_cursor_open(txn.get(), dbi.get(), ptr));

        return new Cursor(ptr, cPtr, key, val);
    }

    /**
     * Return the MDB_cursor pointer to be used in LMDB calls
     */
    private Lib.MDB_cursor ptr() {
        return (Lib.MDB_cursor)ptr.read();
    }

    /**
     * Position cursor
     */
    public boolean seek(Lib.MDB_cursor_op op) {
        int rc = Lib.mdb_cursor_get(ptr(), key.ptr(), val.ptr(), op);
        Lib.checkRc(rc);
        if (rc == Lib.MDB_NOTFOUND()) {
            return false;
        } else {
            return true;
        }
    }

    public boolean get(BufVal k, Lib.MDB_cursor_op op) {

        key.in(k);

        int rc = Lib.mdb_cursor_get(ptr(), key.ptr(), val.ptr(), op);
        Lib.checkRc(rc);
        if (rc == Lib.MDB_NOTFOUND()) {
            return false;
        } else {
            return true;
        }
    }

    public boolean get(BufVal k, BufVal v, Lib.MDB_cursor_op op) {

        key.in(k);
        val.in(v);

        int rc = Lib.mdb_cursor_get(ptr(), key.ptr(), val.ptr(), op);
        Lib.checkRc(rc);
        if (rc == Lib.MDB_NOTFOUND()) {
            return false;
        } else {
            return true;
        }
    }

    public BufVal key() {
        return key;
    }

    public BufVal val() {
        return val;
    }

    /**
     * Close and free memory
     */
    public void close() {
        Lib.mdb_cursor_close(ptr());
        UnmanagedMemory.free(ptr);
        UnmanagedMemory.free(cPtr);
    }

    /**
     * Return count of duplicates for current key.
     */
    public long count() {
        Lib.checkRc(Lib.mdb_cursor_count(ptr(), cPtr));
        return (long)cPtr.read();
    }

    /**
     * Renew cursor.
     */
    public Cursor renew(Txn txn) {
        Lib.checkRc(Lib.mdb_cursor_renew(txn.get(), ptr()));
        return this;
    }
}
