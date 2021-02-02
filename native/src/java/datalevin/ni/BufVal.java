package datalevin.ni;

import org.graalvm.nativeimage.UnmanagedMemory;
import org.graalvm.nativeimage.c.CContext;
import org.graalvm.nativeimage.c.struct.SizeOf;
import org.graalvm.nativeimage.c.type.CTypeConversion;
import org.graalvm.nativeimage.c.type.VoidPointer;
import java.nio.ByteOrder;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Wrap LMDB MDB_val to look like a ByteBuffer at the Java side
 */
@CContext(Lib.Directives.class)
public class BufVal {

    private int size;
    private VoidPointer data;
    private Lib.MDB_val ptr;

    /**
     * constructor that allocates necessary memory
     */
    public BufVal(int size) {
        this.size = size;
        this.data = UnmanagedMemory.calloc(size);
        this.ptr = UnmanagedMemory.calloc(SizeOf.get(Lib.MDB_val.class));
        reset();
    }

    /**
     * reset the MDB_val to point to the internal allocated memory,
     * so that it can be used as the input for LMDB calls.
     */
    void reset() {
        ptr.set_mv_size(size);
        ptr.set_mv_data(data);
    }

    /**
     * Free memory
     */
    public void close() {
        UnmanagedMemory.free(data);
        UnmanagedMemory.free(ptr);
    }

    /**
     * Return a ByteBuffer for getting data out of MDB_val
     */
    public ByteBuffer outBuf() {
        ByteBuffer buf = CTypeConversion.asByteBuffer(ptr.get_mv_data(),
                                                      (int)ptr.get_mv_size());
        // Because we use LMDB native comparator
        buf.order(ByteOrder.LITTLE_ENDIAN);
        return buf;
    }

    /**
     * Return a ByteBuffer for putting data into MDB_val
     */
    public ByteBuffer inBuf() {
        reset();
        return outBuf();
    }

    /**
     * Return the MDB_val pointer to be used in LMDB calls
     */
    public Lib.MDB_val getVal() {
        return (Lib.MDB_val)ptr;
    }

    /**
     * factory method to create an instance
     */
    public static BufVal create(int size) {
        return new BufVal(size);
    }

}
