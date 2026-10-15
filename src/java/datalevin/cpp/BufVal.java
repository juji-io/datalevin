package datalevin.cpp;

import static java.lang.Long.BYTES;
import static datalevin.cpp.UnsafeAccess.UNSAFE;

import java.lang.reflect.InaccessibleObjectException;

import datalevin.dtlvnative.DTLV;
import datalevin.io.ScratchBufferPool;
import java.lang.reflect.Field;

import org.bytedeco.javacpp.*;
import java.nio.*;

/**
 * Provide a Java ByteBuffer view for a LMDB MDB_val struct.
 * This is the primary method of getting data in/out of LMDB.
 */
@SuppressWarnings("removal")
public class BufVal implements AutoCloseable {

    static final int STRUCT_FIELD_OFFSET_DATA = BYTES;
    static final int STRUCT_FIELD_OFFSET_SIZE = 0;

    static final String FIELD_NAME_ADDRESS = "address";
    static final String FIELD_NAME_CAPACITY = "capacity";

    static long addressOffset = 0;
    static long capacityOffset = 0;

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

    static boolean UNSAFE_AVAILABLE = UnsafeAccess.isAvailable();

    static {
        if (UNSAFE_AVAILABLE) {
            try {
                final Field address = findField(Buffer.class, FIELD_NAME_ADDRESS);
                final Field capacity = findField(Buffer.class, FIELD_NAME_CAPACITY);
                addressOffset = UNSAFE.objectFieldOffset(address);
                capacityOffset = UNSAFE.objectFieldOffset(capacity);
            } catch (Exception e) {
                UNSAFE_AVAILABLE = false;
            }
        }
    }

    private Pointer data;
    private final ScratchBufferPool pool;
    private ByteBuffer inBuf;
    private ByteBuffer smallInBuf;
    private long inAddr;
    private ByteBuffer outBuf;
    private DTLV.MDB_val ptr;

    public BufVal(long size) {
        this(size, ScratchBufferPool.shared());
    }

    public BufVal(long size, ScratchBufferPool pool) {
        this.pool = pool;

        // internal fixed BF
        inBuf = pool.acquireBuffer(Math.toIntExact(size));
        inBuf.order(ByteOrder.BIG_ENDIAN);
        data = new Pointer(inBuf);
        inAddr = data.address();

        // external floating BF
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
        if (UNSAFE_AVAILABLE)
            UNSAFE.putInt(ptr, capacityOffset, (int) inBuf.limit());
        else ptr.mv_size(inBuf.limit());
    }

    /**
     * Access the size of the MDB_val
     */
    public long size() {
        if (UNSAFE_AVAILABLE)
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
        UNSAFE.putLong(buffer, addressOffset, addr);
        UNSAFE.putInt(buffer, capacityOffset, (int) size);
        buffer.clear();
        return buffer;
    }

    /**
     * Return a ByteBuffer for getting data out of MDB_val.
     */
    public ByteBuffer outBuf() {
        if (UNSAFE_AVAILABLE) return unsafeOut(outBuf, ptr.address());
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

    private void replaceInput(ByteBuffer replacement) {
        ByteBuffer previous = inBuf;
        inBuf = replacement;
        data = new Pointer(inBuf);
        inAddr = data.address();
        reset();
        if (previous.capacity() <= ScratchBufferPool.LOCAL_LIMIT && smallInBuf == null)
            smallInBuf = previous;
        else pool.releaseBuffer(previous);
    }

    /** Grow only before encoding. No views of the old input may remain in use. */
    public void ensureCapacity(long minimum) {
        if (minimum > inBuf.capacity())
            replaceInput(pool.acquireBuffer(Math.toIntExact(minimum)));
    }

    /** Reuse a previously needed size if cached, without allocating from a hint. */
    public void tryCachedCapacity(long minimum) {
        if (minimum > inBuf.capacity()) {
            ByteBuffer cached = pool.cachedBuffer(Math.toIntExact(minimum));
            if (cached != null) replaceInput(cached);
        }
    }

    /** Return large input storage after native use and restore the small buffer. */
    public void releaseLargeInput() {
        if (inBuf.capacity() > ScratchBufferPool.LOCAL_LIMIT) {
            ByteBuffer replacement = smallInBuf;
            smallInBuf = null;
            if (replacement == null) replacement = pool.acquireBuffer(16384);
            replacement.clear();
            replaceInput(replacement);
        }
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
        if (UNSAFE_AVAILABLE)
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
        if (UNSAFE_AVAILABLE)
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

    /**
     * Release the owned MDB_val struct and input direct buffer.
     * The output view buffer is not explicitly cleaned because, on the fast path,
     * it is rebound to LMDB-owned memory via Unsafe address mutation.
     */
    @Override
    public void close() {
        final DTLV.MDB_val localPtr = this.ptr;
        final ByteBuffer localInBuf = this.inBuf;
        final ByteBuffer localSmallInBuf = this.smallInBuf;

        this.ptr = null;
        this.data = null;
        this.inBuf = null;
        this.smallInBuf = null;
        this.outBuf = null;
        this.inAddr = 0L;

        try {
            if (localPtr != null) localPtr.close();
        } finally {
            pool.releaseBuffer(localInBuf);
            pool.releaseBuffer(localSmallInBuf);
        }
    }

}
