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
        // because we use LMDB native comparator
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

    /**
     * callback method to compare data
     */
    @CEntryPoint
    @CEntryPointOptions(prologue =
                        CEntryPointSetup.EnterCreateIsolatePrologue.class,
                        epilogue =
                        CEntryPointSetup.LeaveTearDownIsolateEpilogue.class)
    static int compareData(IsolateThread thread, Lib.MDB_val a, Lib.MDB_val b) {
        System.out.println("a address:" + a.rawValue());
        System.out.println("a data address:" + a.get_mv_data().rawValue());
        System.out.println("a size:" + a.get_mv_size());

        ByteBuffer bufA = CTypeConversion.asByteBuffer(a.get_mv_data(),
                                                       (int)a.get_mv_size());
        System.out.println("done asbytebuffer");
        bufA.order(ByteOrder.BIG_ENDIAN);
        System.out.println("done set order");

        System.out.println("b address:" + b.rawValue());
        System.out.println("null address:" + WordFactory.nullPointer().rawValue());
        System.out.println("b data address:" + b.get_mv_data().rawValue());
        System.out.println("b size:" + b.get_mv_size());

        ByteBuffer bufB = CTypeConversion.asByteBuffer(b.get_mv_data(),
                                                       (int)b.get_mv_size());
        System.out.println("done asbytebuffer");
        bufB.order(ByteOrder.BIG_ENDIAN);
        System.out.println("done set order");

        final int minLength = Math.min(bufA.limit(), bufB.limit());
        System.out.println("done minlength");

        final int minWords = minLength / Long.BYTES;

        for (int i = 0; i < minWords * Long.BYTES; i += Long.BYTES) {
            final long lw =  bufA.getLong(i);
            System.out.println("done getLong");
            final long rw = bufB.getLong(i);
            final int diff = Long.compareUnsigned(lw, rw);
            if (diff != 0) {
                return diff;
            }
        }

        for (int i = minWords * Long.BYTES; i < minLength; i++) {
            final int lw = Byte.toUnsignedInt(bufA.get(i));
            final int rw = Byte.toUnsignedInt(bufB.get(i));
            final int result = Integer.compareUnsigned(lw, rw);
            if (result != 0) {
                return result;
            }
        }

        return bufA.capacity() - bufB.capacity();
    }

    /**
     * hold the pointer to compareData function
     */
    public static final CEntryPointLiteral<Lib.MDB_cmp_func> cmpCallback =
        CEntryPointLiteral.create(BufVal.class,
                                  "compareData",
                                  new Class[]{
                                      IsolateThread.class,
                                      Lib.MDB_val.class,
                                      Lib.MDB_val.class
                                  });

}
