
package datalevin.utl;

public class BitOps {
    static public int intNot (int x) {
        return ~x;
    }

    static public int intFlip (int x, long n) {
        return x ^ (1 << n);
    }
}
