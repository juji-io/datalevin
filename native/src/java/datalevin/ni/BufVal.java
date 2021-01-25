package datalevin.ni;

import org.graalvm.nativeimage.UnmanagedMemory;
import org.graalvm.nativeimage.PinnedObject;
import org.graalvm.nativeimage.c.CContext;
import org.graalvm.nativeimage.c.struct.SizeOf;
import org.graalvm.nativeimage.c.type.CTypeConversion;

import java.nio.ByteBuffer;

/**
 * Wrap LMDB MDB_val to look like a ByteBuffer at the Java side
 */
@CContext(Lib.Directives.class)
public class BufVal {

    private int bufSize;
    private PinnedObject pin;
    private Lib.MDB_val value;

    /**
     * constructor that allocates necessary memory
     */
    public BufVal(int size) {
        bufSize = size;
        value = UnmanagedMemory.calloc(SizeOf.get(Lib.MDB_val.class));
        pin = PinnedObject.create(new byte[size]);
        reset();
    }

    /**
     * reset the MDB_val to point to the internal pinned data,
     * so that it can be used as the input for LMDB calls.
     */
    void reset() {
        value.set_mv_size(bufSize);
        value.set_mv_data(pin.addressOfArrayElement(0));
    }

    /**
     * Free memory
     */
    public void close() {
        pin.close();
        UnmanagedMemory.free(value);
    }

    /**
     * Return a ByteBuffer for getting data out of MDB_val
     */
    public ByteBuffer outBuf() {
        return CTypeConversion.asByteBuffer(value.get_mv_data(),
                                            (int)value.get_mv_size());
    }

    /**
     * Return a ByteBuffer for putting data into MDB_val
     */
    public ByteBuffer inBuf() {
        reset();
        return ByteBuffer.wrap((byte [])pin.getObject());
    }

    /**
     * Return the MDB_val to be used in LMDB calls
     */
    public Lib.MDB_val getVal() {
        return value;
    }

    /**
     * factory method to create an instance
     */
    public static BufVal create(int size) {
        return new BufVal(size);
    }

}
