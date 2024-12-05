package datalevin.cpp;

import dtlvnative.DTLV;

import org.bytedeco.javacpp.*;
import java.nio.*;

import static java.lang.Long.BYTES;

import java.lang.reflect.Field;
import static datalevin.cpp.UnsafeAccess.UNSAFE;

/**
 * Wrap a Java ByteBuffer to work as a MDB_val for data input/output to LMDB
 */
@SuppressWarnings("removal")
public class BufVal {

    static final int STRUCT_FIELD_OFFSET_DATA = BYTES;
    static final int STRUCT_FIELD_OFFSET_SIZE = 0;

    static final String FIELD_NAME_ADDRESS = "address";
    static final String FIELD_NAME_CAPACITY = "capacity";

    static final long ADDRESS_OFFSET;
    static final long CAPACITY_OFFSET;

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
        throw new RuntimeException(name + " not found");
    }

    static {
      try {
        final Field address = findField(Buffer.class, FIELD_NAME_ADDRESS);
        final Field capacity = findField(Buffer.class, FIELD_NAME_CAPACITY);
        ADDRESS_OFFSET = UNSAFE.objectFieldOffset(address);
        CAPACITY_OFFSET = UNSAFE.objectFieldOffset(capacity);
      } catch (final SecurityException e) {
        throw new RuntimeException("Field access error", e);
      }
    }

    private long size;

    private ByteBuffer inBuf;
    private ByteBuffer outBuf;

    private Pointer data;
    private DTLV.MDB_val ptr;

    public BufVal(long size) {

        this.size = size;

        inBuf = ByteBuffer.allocateDirect((int) size);
        inBuf.order(ByteOrder.BIG_ENDIAN);

        outBuf = ByteBuffer.allocateDirect((int) 0);
        outBuf.order(ByteOrder.BIG_ENDIAN);

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
        ptr.mv_size(inBuf.limit());
    }

    public long size() {
        return (long) ptr.mv_size();
    }

    public Pointer data() {
        return ptr.mv_data();
    }

    ByteBuffer out(final ByteBuffer buffer, final long ptrAddr) {
      final long addr = UNSAFE.getLong(ptrAddr + STRUCT_FIELD_OFFSET_DATA);
      final long size = UNSAFE.getLong(ptrAddr + STRUCT_FIELD_OFFSET_SIZE);
      UNSAFE.putLong(buffer, ADDRESS_OFFSET, addr);
      UNSAFE.putInt(buffer, CAPACITY_OFFSET, (int) size);
      buffer.clear();
      return buffer;
    }

    /**
     * Return a ByteBuffer for getting data out of MDB_val
     */
    public ByteBuffer outBuf() {
        if (UnsafeAccess.isAvailable()) return out(outBuf, ptr.address());
        else {
            ByteBuffer buf
                = ptr.mv_data().position(0).limit(ptr.mv_size()).asByteBuffer();
            buf.order(ByteOrder.BIG_ENDIAN);
            return buf;
        }
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
