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
        // reset();

        inBuf = CTypeConversion.asByteBuffer(data, size);
        inBuf.order(ByteOrder.BIG_ENDIAN);
    }

    /**
     * Set the  limit of internal ByteBuffer to the current position, and update
     * the MDB_val size to be the same, so no unnecessary bytes are written
     */
    public void flip() {
        inBuf.flip();
        ptr.set_mv_size(inBuf.limit());
    }

    /**
     * Set the  limit of internal ByteBuffer to capacity, and update
     * the MDB_val size to be the same, do it is ready to accept writes
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

    /**
     * callback method to compare data
     */
    // @CEntryPoint
    // @CEntryPointOptions(prologue =
    //                     CEntryPointSetup.EnterCreateIsolatePrologue.class,
    //                     epilogue =
    //                     CEntryPointSetup.LeaveTearDownIsolateEpilogue.class)
    // static int compareData(IsolateThread thread, Lib.MDB_val a, Lib.MDB_val b) {

    //     System.out.println("null address:" + WordFactory.nullPointer().rawValue());

    //     System.out.println("a address:" + a.rawValue());
    //     System.out.println("b address:" + b.rawValue());

    //     if (a == b) return 0;

    //     if (a.rawValue() < 10) return -1;

    //     if (b.rawValue() < 10) return 1;

    //     System.out.println("a data address:" + a.get_mv_data().rawValue());
    //     System.out.println("a size:" + a.get_mv_size());
    //     System.out.println("b data address:" + b.get_mv_data().rawValue());
    //     System.out.println("b size:" + b.get_mv_size());

    //     ByteBuffer bufA = CTypeConversion.asByteBuffer(a.get_mv_data(),
    //                                                    (int)a.get_mv_size());
    //     System.out.println("done asbytebuffer");
    //     bufA.order(ByteOrder.BIG_ENDIAN);
    //     System.out.println("done set order");


    //     ByteBuffer bufB = CTypeConversion.asByteBuffer(b.get_mv_data(),
    //                                                    (int)b.get_mv_size());
    //     System.out.println("done asbytebuffer");
    //     bufB.order(ByteOrder.BIG_ENDIAN);
    //     System.out.println("done set order");

    //     final int minLength = Math.min(bufA.limit(), bufB.limit());
    //     System.out.println("done minlength");

    //     final int minWords = minLength / Long.BYTES;

    //     for (int i = 0; i < minWords * Long.BYTES; i += Long.BYTES) {
    //         final long lw =  bufA.getLong(i);
    //         System.out.println("done getLong");
    //         final long rw = bufB.getLong(i);
    //         final int diff = Long.compareUnsigned(lw, rw);
    //         if (diff != 0) {
    //             return diff;
    //         }
    //     }

    //     for (int i = minWords * Long.BYTES; i < minLength; i++) {
    //         final int lw = Byte.toUnsignedInt(bufA.get(i));
    //         final int rw = Byte.toUnsignedInt(bufB.get(i));
    //         final int result = Integer.compareUnsigned(lw, rw);
    //         if (result != 0) {
    //             return result;
    //         }
    //     }

    //     return bufA.capacity() - bufB.capacity();
    // }

    /**
     * hold the pointer to compareData function
     */
    // public static final CEntryPointLiteral<Lib.MDB_cmp_func> cmpCallback =
    //     CEntryPointLiteral.create(BufVal.class,
    //                               "compareData",
    //                               new Class[]{
    //                                   IsolateThread.class,
    //                                   Lib.MDB_val.class,
    //                                   Lib.MDB_val.class
    //                               });

}
