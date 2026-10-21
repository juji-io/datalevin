/* Copyright (c) Huahai Yang. All rights reserved.
 * Distributed under the Eclipse Public License 2.0. */
package datalevin.io;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdDecompressCtx;
import datalevin.cpp.UnsafeAccess;
import java.io.Closeable;
import java.nio.ByteBuffer;

/** Scratch buffers and native contexts leased exclusively by one wire codec.
 * Returned buffers remain valid until the lease is released. */
public final class WireCompression implements Closeable {
    public static final int DEFAULT_MAX_MESSAGE_SIZE = 64 * 1024 * 1024;
    private final ScratchBufferPool pool;
    private ZstdCompressCtx compressor;
    private ZstdDecompressCtx decompressor;
    private ByteBuffer raw, packed, input;
    private int level = Integer.MIN_VALUE;
    private boolean closed;

    public WireCompression() { this(ScratchBufferPool.shared()); }

    public WireCompression(ScratchBufferPool pool) { this.pool = pool; }

    private static void release(ByteBuffer buffer) {
        try { UnsafeAccess.clean(buffer); }
        catch (RuntimeException ignored) { /* Fall back to the JVM cleaner. */ }
    }

    /** Whether this completed lease should be discarded instead of cached. */
    public boolean oversized(int maximum) {
        return (raw != null && raw.capacity() > maximum)
            || (packed != null && packed.capacity() > maximum)
            || (input != null && input.capacity() > maximum);
    }

    /** Actual owned buffer capacity charged to the shared idle cache. */
    public long bufferBytes() {
        return (raw == null ? 0L : raw.capacity())
             + (packed == null ? 0L : packed.capacity())
             + (input == null ? 0L : input.capacity());
    }

    private ByteBuffer ensure(ByteBuffer buffer, int size) {
        return ensure(buffer, size, Integer.MAX_VALUE);
    }

    private ByteBuffer ensure(ByteBuffer buffer, int size, int maximum) {
        if (size < 0 || size > maximum)
            throw new IllegalArgumentException("Wire payload too large");
        if (buffer == null || buffer.capacity() < size) {
            long capacity = Math.max(8192L, size);
            if (buffer != null) capacity = Math.max(capacity, 2L * buffer.capacity());
            ByteBuffer previous = buffer;
            buffer = pool.acquireBuffer(
                (int) Math.min(maximum, capacity));
            if (buffer.capacity() > maximum) {
                pool.releaseBuffer(buffer);
                buffer = ByteBuffer.allocateDirect((int) Math.min(maximum, capacity));
            }
            pool.releaseBuffer(previous);
        }
        buffer.clear();
        return buffer;
    }

    private void checkOpen() {
        if (closed) throw new IllegalStateException("Wire compression context is closed");
    }

    /** Reusable space for an encoding that overflowed the socket buffer. */
    public ByteBuffer encodingBuffer(int minimum) {
        checkOpen();
        raw = ensure(raw, minimum);
        return raw;
    }

    private ByteBuffer directInput(ByteBuffer source, int start, int length) {
        input = ensure(input, length);
        input.put(source.duplicate().position(start).limit(start + length)).flip();
        return input;
    }

    /** Compress without changing source positions. Null means no byte saving.
     * The four-byte uncompressed length is part of the existing wire format. */
    public ByteBuffer compress(ByteBuffer source, int start, int length, int requestedLevel) {
        checkOpen();
        long bound = Zstd.compressBound(length) + 4;
        if (bound > Integer.MAX_VALUE) throw new IllegalArgumentException("Wire payload too large");
        packed = ensure(packed, (int) bound);
        if (compressor == null) compressor = new ZstdCompressCtx();
        if (level != requestedLevel) {
            compressor.setLevel(requestedLevel);
            level = requestedLevel;
        }
        if (!source.isDirect()) {
            source = directInput(source, start, length);
            start = 0;
        }
        int size = compressor.compressDirectByteBuffer(
            packed, 4, packed.capacity() - 4, source, start, length);
        if (size + 4 >= length) return null;
        packed.putInt(0, length).limit(size + 4);
        return packed;
    }

    /** Decode directly from the socket buffer into reusable direct storage. */
    public ByteBuffer decompress(ByteBuffer source) {
        return decompress(source, DEFAULT_MAX_MESSAGE_SIZE);
    }

    /** Validate the complete frame and its size before allocating output memory. */
    public ByteBuffer decompress(ByteBuffer source, int maximum) {
        checkOpen();
        if (maximum <= 0) throw new IllegalArgumentException("Invalid wire message size limit");
        if (source.remaining() > maximum)
            throw new IllegalArgumentException("Wire payload too large");
        if (source.remaining() < 4)
            throw new IllegalArgumentException("Wire message missing uncompressed length");
        int expected = source.getInt();
        if (expected < 0)
            throw new IllegalArgumentException("Negative wire uncompressed length");
        if (expected > maximum) throw new IllegalArgumentException("Wire payload too large");
        int start = source.position(), length = source.remaining();
        // Only ordinary Zstd frames are part of this protocol, not skippable frames.
        if (length < 4 || source.get(start) != (byte) 0x28
                || source.get(start + 1) != (byte) 0xb5
                || source.get(start + 2) != (byte) 0x2f
                || source.get(start + 3) != (byte) 0xfd)
            throw new IllegalArgumentException("Invalid Zstd wire frame header");
        long declared;
        if (source.isDirect()) {
            declared = Zstd.getDirectByteBufferFrameContentSize(source, start, length);
        } else if (source.hasArray()) {
            declared = Zstd.getFrameContentSize(source.array(), source.arrayOffset() + start, length);
        } else {
            // Read-only heap buffers expose no array. A Zstd header is at most 18
            // bytes; never copy an unvalidated payload into direct scratch space.
            byte[] header = new byte[Math.min(18, length)];
            source.duplicate().get(header);
            declared = Zstd.getFrameContentSize(header);
        }
        if (declared < 0)
            throw new IllegalArgumentException("Invalid or unknown Zstd wire frame content size");
        if (declared != expected)
            throw new IllegalArgumentException("Wire message decompression length mismatch");
        // Checking block boundaries requires no output buffer. Reject truncated
        // input and concatenated frames before reserving the declared output.
        if (source.hasArray()) {
            long frameSize = Zstd.findFrameCompressedSize(source.array(), source.arrayOffset() + start, length);
            if (Zstd.isError(frameSize) || frameSize != length)
                throw new IllegalArgumentException("Invalid or truncated Zstd wire frame");
        }
        ByteBuffer compressed = source;
        if (!source.isDirect()) {
            input = ensure(input, length, maximum);
            input.put(source.duplicate()).flip();
            compressed = input;
            start = 0;
        }
        if (!source.hasArray()) {
            long frameSize = Zstd.findDirectByteBufferFrameCompressedSize(compressed, start, length);
            if (Zstd.isError(frameSize) || frameSize != length)
                throw new IllegalArgumentException("Invalid or truncated Zstd wire frame");
        }
        raw = ensure(raw, expected, maximum);
        if (decompressor == null) decompressor = new ZstdDecompressCtx();
        int actual = decompressor.decompressDirectByteBuffer(
            raw, 0, expected, compressed, start, length);
        if (actual != expected)
            throw new IllegalArgumentException("Wire message decompression length mismatch");
        source.position(source.limit());
        raw.limit(actual);
        return raw;
    }

    @Override public void close() {
        closed = true;
        try {
            if (compressor != null) compressor.close();
        } finally {
            try {
                if (decompressor != null) decompressor.close();
            } finally {
                compressor = null;
                decompressor = null;
                release(raw);
                release(packed);
                release(input);
                raw = packed = input = null;
            }
        }
    }
}
