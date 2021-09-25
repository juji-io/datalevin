package datalevin.io;

import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Wrap a given ByteBuffer so it looks like an OutputStream
 */
public class ByteBufferOutputStream extends OutputStream {

    private ByteBuffer bf;

    public ByteBufferOutputStream (final ByteBuffer bf) {
        this.bf = bf;
    }

    @Override
    public void write(final int b) {
        bf.put((byte) b);
    }

    @Override
    public void write(final byte[] bs) {
        bf.put(bs);
    }

    @Override
    public void write(final byte[] bs, final int off, final int len) {
        bf.put(bs, off, len);
    }
}
