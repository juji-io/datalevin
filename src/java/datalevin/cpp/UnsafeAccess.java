package datalevin.cpp;

import java.lang.reflect.Field;
import sun.misc.Unsafe;
import java.nio.Buffer;

@SuppressWarnings("removal")
final class UnsafeAccess {

    static Unsafe UNSAFE = null;

    static boolean available = false;

    static {
        try {
            final Field u = Unsafe.class.getDeclaredField("theUnsafe");
            u.setAccessible(true);
            UNSAFE = (Unsafe) u.get(null);

            final Field c = Buffer.class.getDeclaredField("capacity");
            c.setAccessible(true);
            UNSAFE.objectFieldOffset(c);

            final Field a = Buffer.class.getDeclaredField("address");
            a.setAccessible(true);
            UNSAFE.objectFieldOffset(a);

            available = true;
        } catch (final NoSuchFieldException | SecurityException
                | IllegalArgumentException | IllegalAccessException e) {
            // don't throw, as Unsafe use is optional
            available = false;
        }
    }

    public static boolean isAvailable() {
        return available;
    }

    private UnsafeAccess() {
    }
}
