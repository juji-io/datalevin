package datalevin.utl;

import java.nio.ByteBuffer;

public class BufOps {

    public static int compareByteBuf(final ByteBuffer a, final ByteBuffer b) {
      if (a.equals(b)) {
        return 0;
      }

      final int minLength = Math.min(a.limit(), b.limit());
      final int minWords = minLength / Long.BYTES;

      // assume Big Endian (JVM default)
      for (int i = 0; i < minWords * Long.BYTES; i += Long.BYTES) {
        final long lw = a.getLong(i);
        final long rw = b.getLong(i);
        final int diff = Long.compareUnsigned(lw, rw);
        if (diff != 0) {
          return diff;
        }
      }

      for (int i = minWords * Long.BYTES; i < minLength; i++) {
        final int lw = Byte.toUnsignedInt(a.get(i));
        final int rw = Byte.toUnsignedInt(b.get(i));
        final int result = Integer.compareUnsigned(lw, rw);
        if (result != 0) {
          return result;
        }
      }

      return a.remaining() - b.remaining();
    }
}
