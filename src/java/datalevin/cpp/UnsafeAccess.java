package datalevin.cpp;

import java.lang.reflect.Field;

import sun.misc.Unsafe;

@SuppressWarnings("sunapi")
final class UnsafeAccess {

    static Unsafe UNSAFE = null;

    static boolean available = false;

    private static final String FIELD_NAME_THE_UNSAFE = "theUnsafe";

    static {
        try {
            final Field field = Unsafe.class.getDeclaredField(FIELD_NAME_THE_UNSAFE);
            field.setAccessible(true);
            UNSAFE = (Unsafe) field.get(null);
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
