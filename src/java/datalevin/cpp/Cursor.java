package datalevin.cpp;

import org.bytedeco.javacpp.*;
import dtlvnative.DTLV;
import java.nio.ByteBuffer;

/**
 * Wrap a MDB_cursor
 */
public class Cursor {

    private DTLV.MDB_cursor ptr;
    private SizeTPointer cPtr;

    private BufVal key;
    private BufVal val;

    public Cursor(DTLV.MDB_cursor ptr, SizeTPointer cPtr, BufVal key, BufVal val) {
        this.ptr = ptr;
        this.cPtr = cPtr;
        this.key = key;
        this.val = val;
    }

    /**
     * Factory method to create an instance
     */
    public static Cursor create(Txn txn, Dbi dbi, BufVal key, BufVal val) {

        DTLV.MDB_cursor ptr = new DTLV.MDB_cursor();
        SizeTPointer cPtr = new SizeTPointer(1);

        Util.checkRc(DTLV.mdb_cursor_open(txn.get(), dbi.get(), ptr));

        return new Cursor(ptr, cPtr, key, val);
    }

    /**
     * Return the MDB_cursor pointer to be used in DTLV calls
     */
    public DTLV.MDB_cursor ptr() {
        return ptr;
    }

    /**
     * Position cursor
     */
    public boolean seek(int op) {
        int rc = DTLV.mdb_cursor_get(ptr(), key.ptr(), val.ptr(), op);
        Util.checkRc(rc);
        if (rc == DTLV.MDB_NOTFOUND) {
            return false;
        } else {
            return true;
        }
    }

    public boolean get(BufVal k, int op) {

        key.in(k);

        int rc = DTLV.mdb_cursor_get(ptr(), key.ptr(), val.ptr(), op);
        Util.checkRc(rc);
        if (rc == DTLV.MDB_NOTFOUND) {
            return false;
        } else {
            return true;
        }
    }

    public boolean get(BufVal k, BufVal v, int op) {

        key.in(k);
        val.in(v);

        int rc = DTLV.mdb_cursor_get(ptr(), key.ptr(), val.ptr(), op);
        Util.checkRc(rc);
        if (rc == DTLV.MDB_NOTFOUND) {
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
        DTLV.mdb_cursor_close(ptr);
        ptr.close();
        cPtr.close();
    }

    /**
     * Return count of duplicates for current key.
     */
    public long count() {
        Util.checkRc(DTLV.mdb_cursor_count(ptr, cPtr));
        return (long)cPtr.get();
    }

    /**
     * Renew cursor.
     */
    public Cursor renew(Txn txn) {
        Util.checkRc(DTLV.mdb_cursor_renew(txn.get(), ptr));
        return this;
    }
}
