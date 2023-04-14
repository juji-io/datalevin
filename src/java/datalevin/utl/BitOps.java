package datalevin.utl;

public class BitOps {
    public static int intNot (int x) {
        return ~x;
    }

    public static int intFlip (int x, long n) {
        return x ^ (1 << n);
    }

    public static int intAnd (int x, int y) {
        return x & y;
    }

    public static int compareBytes(byte[] left, byte[] right) {
        for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
            int a = (left[i] & 0xff);
            int b = (right[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
        return left.length - right.length;
    }
}
