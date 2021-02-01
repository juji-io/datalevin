package datalevin.ni;

public class Mask {
    static int set(final int... flags) {
        int result = 0;
        for (final int flag : flags) {
            result |= flag;
        }
        return result;
    }
}
