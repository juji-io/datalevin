package datalevin.ni;

import org.graalvm.word.WordFactory;
import org.graalvm.nativeimage.UnmanagedMemory;
import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.c.CContext;
import org.graalvm.nativeimage.c.struct.SizeOf;
import org.graalvm.nativeimage.c.type.CTypeConversion;
import org.graalvm.nativeimage.c.type.VoidPointer;
import org.graalvm.nativeimage.c.function.CEntryPointLiteral;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import com.oracle.svm.core.c.function.CEntryPointOptions;
import com.oracle.svm.core.c.function.CEntryPointSetup;


import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Wrap LMDB MDB_val to look like a ByteBuffer at the Java side
 */
@CContext(Lib.Directives.class)
public class BufVal {

    private int capacity;
    private ByteBuffer inBuf;

    private VoidPointer data;
    private Lib.MDB_val ptr;

    public BufVal(int size) {
        capacity = size;

        data = UnmanagedMemory.calloc(size);
        ptr = UnmanagedMemory.calloc(SizeOf.get(Lib.MDB_val.class));

        ptr.set_mv_size(size);
        ptr.set_mv_data(data);

        inBuf = CTypeConversion.asByteBuffer(data, size);
        inBuf.order(ByteOrder.BIG_ENDIAN);
    }

    /**
     * Set the limit of internal ByteBuffer to the current position, and update
     * the MDB_val size to be the same, so no unnecessary bytes are written
     */
    public void flip() {
        inBuf.flip();
        ptr.set_mv_size(inBuf.limit());
    }

    /**
     * Set the limit of internal ByteBuffer to capacity, and update
     * the MDB_val size to be the same, so it is ready to accept writes
     */
    public void clear() {
        inBuf.clear();
        ptr.set_mv_size(inBuf.limit());
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
        buf.order(ByteOrder.BIG_ENDIAN);
        return buf;
    }

    /**
     * Reset MDB_val pointer back to internal ByteBuffer, and return it
     * for putting data into MDB_val
     */
    public ByteBuffer inBuf() {
        ptr.set_mv_data(data);
        return inBuf;
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
