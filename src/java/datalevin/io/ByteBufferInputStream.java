package datalevin.io;

import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Wrap a given ByteBuffer so it looks like an InputStream
 */
public class ByteBufferInputStream extends InputStream {

    ByteBuffer bf;

    public ByteBufferInputStream(final ByteBuffer bf) {
        this.bf = bf;
    }

    public int read() {
        if (!bf.hasRemaining()) {
            return -1;
        }
        return bf.get() & 0xFF;
    }

    public int read(byte[] bs) {
        return read(bs, 0, bs.length);
    }

    public int read(byte[] bs, int off, int len) {
        if (!bf.hasRemaining()) {
            return -1;
        }

        len = Math.min(len, bf.remaining());
        bf.get(bs, off, len);
        return len;
    }
}
