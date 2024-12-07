package datalevin.cpp;

import dtlvnative.DTLV;

import static datalevin.cpp.UnsafeAccess.UNSAFE;
import java.lang.reflect.Field;

import org.bytedeco.javacpp.*;

import java.nio.*;

import static java.lang.Long.BYTES;

/**
 * Provide a Java ByteBuffer view for a LMDB MDB_val struct.
 * This is the primary method of getting data in/out of LMDB.
 */
@SuppressWarnings("removal")
public class BufVal {

    static final boolean canAccessUnsafe = UnsafeAccess.isAvailable();

    // MDB_val field offsets
    static final int STRUCT_FIELD_OFFSET_DATA = BYTES;
    static final int STRUCT_FIELD_OFFSET_SIZE = 0;

    static final String FIELD_NAME_ADDRESS = "address";
    static final String FIELD_NAME_CAPACITY = "capacity";

    static long ADDRESS_OFFSET = 0;
    static long CAPACITY_OFFSET = 0;

    static Field findField(final Class<?> c, final String name) {
        Class<?> clazz = c;
        do {
            try {
                final Field field = clazz.getDeclaredField(name);
                field.setAccessible(true);
                return field;
            } catch (final NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            }
        } while (clazz != null);
        throw new RuntimeException("Field name: " + name + " not found");
    }

    static {
        try {
            final Field address = findField(Buffer.class, FIELD_NAME_ADDRESS);
            final Field capacity = findField(Buffer.class, FIELD_NAME_CAPACITY);
            ADDRESS_OFFSET = UNSAFE.objectFieldOffset(address);
            CAPACITY_OFFSET = UNSAFE.objectFieldOffset(capacity);
        } catch (final SecurityException e) {
            // don't throw, as unsafe access is optional
        }
    }

    private Pointer data;

    private ByteBuffer inBuf;

    private long inAddr;

    private ByteBuffer outBuf;

    private DTLV.MDB_val ptr;

    public BufVal(long size) {

        inBuf = ByteBuffer.allocateDirect((int) size);
        inBuf.order(ByteOrder.BIG_ENDIAN);
        data = new Pointer(inBuf);
        inAddr = data.address();

        outBuf = ByteBuffer.allocateDirect((int) 0);
        outBuf.order(ByteOrder.BIG_ENDIAN);

        ptr = new DTLV.MDB_val();
        reset();
    }

    /**
     * Set the limit of internal ByteBuffer to capacity, and update
     * the MDB_val size to be the same, so it is ready to accept writes
     */
    public void clear() {
        inBuf.clear();
        if (canAccessUnsafe)
            UNSAFE.putInt(ptr, CAPACITY_OFFSET, (int)inBuf.limit());
        else ptr.mv_size(inBuf.limit());
    }

    /**
     * Access the size of the MDB_val
     */
    public long size() {
        if (canAccessUnsafe)
            return UNSAFE.getLong(ptr.address() + STRUCT_FIELD_OFFSET_SIZE);
        else
            return (long) ptr.mv_size();
    }

    protected Pointer data() {
        return ptr.mv_data();
    }

    protected long inAddr() {
        return inAddr;
    }

    protected ByteBuffer unsafeOut(final ByteBuffer buffer, final long ptrAddr) {
        final long addr = UNSAFE.getLong(ptrAddr + STRUCT_FIELD_OFFSET_DATA);
        final long size = UNSAFE.getLong(ptrAddr + STRUCT_FIELD_OFFSET_SIZE);
        UNSAFE.putLong(buffer, ADDRESS_OFFSET, addr);
        UNSAFE.putInt(buffer, CAPACITY_OFFSET, (int) size);
        buffer.clear();
        return buffer;
    }

    /**
     * Return a ByteBuffer for getting data out of MDB_val.
     */
    public ByteBuffer outBuf() {
        if (canAccessUnsafe) return unsafeOut(outBuf, ptr.address());
        else {
            ByteBuffer buf
                = ptr.mv_data().position(0).limit(ptr.mv_size()).asByteBuffer();
            buf.order(ByteOrder.BIG_ENDIAN);
            return buf;
        }
    }
    /**
     * Access the allocated internal data in-take ByteBuffer
     */
    public ByteBuffer inBuf() {
        return inBuf;
    }

    protected void unsafeIn(final long ptrAddr, final long address,
                            final long size) {
      UNSAFE.putLong(ptrAddr + STRUCT_FIELD_OFFSET_SIZE, size);
      UNSAFE.putLong(ptrAddr + STRUCT_FIELD_OFFSET_DATA, address);
    }

    /**
     * Reset the MDB_val to point back to the internal ByteBuffer.
     */
    public void reset() {
        if (canAccessUnsafe)
            unsafeIn(ptr.address(), inAddr, inBuf.limit());
        else {
            ptr.mv_size(inBuf.limit());
            ptr.mv_data(data);
        }
    }

    /**
     * Set MDB_val to that of the passed-in BufVal
     */
    public void in(final BufVal ib) {
        if (canAccessUnsafe)
            unsafeIn(ptr.address(), ib.inAddr(), ib.size());
        else {
            ptr.mv_size(ib.size());
            ptr.mv_data(ib.data());
        }
    }

    /**
     * Return the MDB_val pointer to be used in DTLV calls
     */
    public DTLV.MDB_val ptr() {
        return (DTLV.MDB_val)ptr;
    }

}
