package datalevin.cpp;

import org.bytedeco.javacpp.*;
import dtlvnative.DTLV;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Wrap DTLV MDB_val to look like a ByteBuffer at the Java side
 */
public class BufVal {

    private long size;
    private ByteBuffer inBuf;

    private Pointer data;
    private DTLV.MDB_val ptr;

    public BufVal(long size) {

        this.size = size;

        inBuf = ByteBuffer.allocateDirect((int) size);
        inBuf.order(ByteOrder.BIG_ENDIAN);

        data = new Pointer(inBuf);

        ptr = new DTLV.MDB_val();
        ptr.mv_size(size);
        ptr.mv_data(data);
    }

    /**
     * Set the limit of internal ByteBuffer to the current position, and update
     * the MDB_val size to be the same, so no unnecessary bytes are written
     */
    public void flip() {
        inBuf.flip();
        ptr.mv_size(inBuf.limit());
    }

    /**
     * Set the limit of internal ByteBuffer to capacity, and update
     * the MDB_val size to be the same, so it is ready to accept writes
     */
    public void clear() {
        inBuf.clear();
        ptr.mv_data(data);
        ptr.mv_size(size);
    }

    public long size() {
        return (long) ptr.mv_size();
    }

    public Pointer data() {
        return ptr.mv_data();
    }

    /**
     * Return a ByteBuffer for getting data out of MDB_val
     */
    public ByteBuffer outBuf() {
        ByteBuffer buf = ptr.mv_data().position(0).limit(ptr.mv_size()).asByteBuffer();
        buf.order(ByteOrder.BIG_ENDIAN);
        return buf;
    }

    /**
     * Reset MDB_val pointer back to internal ByteBuffer, and return it
     * for putting data into MDB_val
     */
    public ByteBuffer inBuf() {
        ptr.mv_data(data);
        ptr.mv_size(size);
        return inBuf;
    }

    /**
     * Set MDB_val to that of the passed-in BufVal
     */
    public void in(BufVal ib) {
        ptr.mv_size(ib.size());
        ptr.mv_data(ib.data());
    }

    /**
     * Return the MDB_val pointer to be used in DTLV calls
     */
    public DTLV.MDB_val ptr() {
        return (DTLV.MDB_val)ptr;
    }

    /**
     * factory method to create an instance
     */
    public static BufVal create(long size) {
        return new BufVal(size);
    }
}
