package datalevin.utl;

public class BitOps {
    public static int intNot (int x) {
        return ~x;
    }

    public static int intFlip (int x, long n) {
        return x ^ (1 << n);
    }
}
