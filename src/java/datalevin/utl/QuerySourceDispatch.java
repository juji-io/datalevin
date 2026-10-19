/*
 * Copyright (c) Huahai Yang. All rights reserved.
 * The use and distribution terms for this software are covered by the
 * Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0).
 */
package datalevin.utl;

import clojure.lang.ArraySeq;
import clojure.lang.Counted;
import clojure.lang.IFn;
import clojure.lang.Indexed;
import clojure.lang.ISeq;
import clojure.lang.RT;
import clojure.lang.Util;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.MutableCallSite;
import java.util.Objects;

/** Optional argument classification, without loading the SPI or retaining
 * query values. Protocol updates invalidate the JVM's compiled type tests. */
public final class QuerySourceDispatch {
    private static final boolean IN_IMAGE =
        System.getProperty("org.graalvm.nativeimage.imagecode") != null;
    private static volatile IFn fallback;

    static {
        if (!IN_IMAGE) JvmDispatch.initialize();
    }

    private QuerySourceDispatch() {}

    /** Publish a coherent protocol snapshot. Called only on protocol changes. */
    public static void install(IFn predicate, Class<?> sourceInterface,
                               Class<?>[] extensions, boolean nilSource) {
        fallback = predicate;
        if (!IN_IMAGE) JvmDispatch.install(predicate, sourceInterface, extensions, nilSource);
    }

    public static boolean hasSource(Object inputs, Class<?> nativeDB) {
        return IN_IMAGE ? imageHasSource(inputs, nativeDB) : JvmDispatch.hasSource(inputs, nativeDB);
    }

    private static boolean imageHasSource(Object inputs, Class<?> nativeDB) {
        IFn predicate = fallback;
        if (predicate == null) return false;
        try {
            return scan(null, predicate, inputs, nativeDB);
        } catch (Throwable error) {
            throw Util.sneakyThrow(error);
        }
    }

    private static boolean isSource(MethodHandle test, IFn predicate, Object input,
                                    Class<?> nativeDB) throws Throwable {
        if (input != null && input.getClass() == nativeDB) return false;
        return test == null ? Boolean.TRUE.equals(predicate.invoke(input))
                            : (boolean) test.invokeExact(input);
    }

    private static boolean scan(MethodHandle test, IFn predicate, Object inputs,
                                Class<?> nativeDB) throws Throwable {
        if (inputs instanceof ArraySeq) {
            ArraySeq values = (ArraySeq) inputs;
            Object[] array = values.array;
            for (int i = values.index(); i < array.length; i++) {
                if (isSource(test, predicate, array[i], nativeDB)) return true;
            }
        } else if (inputs instanceof Indexed && inputs instanceof Counted) {
            Indexed values = (Indexed) inputs;
            int n = ((Counted) inputs).count();
            for (int i = 0; i < n; i++) {
                if (isSource(test, predicate, values.nth(i), nativeDB)) return true;
            }
        } else {
            for (ISeq values = RT.seq(inputs); values != null; values = values.next()) {
                if (isSource(test, predicate, values.first(), nativeDB)) return true;
            }
        }
        return false;
    }

    /** Kept out of the image path: native images do not need mutable JVM
     * callsites for the JVM-only external execution extension. */
    private static final class JvmDispatch {
        private static final MethodType ARGUMENTS = MethodType.methodType(boolean.class, Object.class, Class.class);
        private static final MethodType VALUE = MethodType.methodType(boolean.class, Object.class);
        private static final MutableCallSite SITE = new MutableCallSite(ARGUMENTS);
        private static final MethodHandle INVOKER = SITE.dynamicInvoker();
        private static final MethodHandle TRUE;
        private static final MethodHandle CLASS_TEST;
        private static final MethodHandle NIL_TEST;
        private static final MethodHandle IFN_TEST;
        private static final MethodHandle SCAN;

        static {
            try {
                MethodHandles.Lookup lookup = MethodHandles.lookup();
                TRUE = MethodHandles.dropArguments(MethodHandles.constant(boolean.class, true), 0, Object.class);
                CLASS_TEST = lookup.findVirtual(Class.class, "isInstance", VALUE);
                NIL_TEST = lookup.findStatic(Objects.class, "isNull", VALUE);
                IFN_TEST = lookup.findVirtual(IFn.class, "invoke", MethodType.methodType(Object.class, Object.class));
                SCAN = lookup.findStatic(QuerySourceDispatch.class, "scan",
                    MethodType.methodType(boolean.class, MethodHandle.class, IFn.class, Object.class, Class.class));
                SITE.setTarget(MethodHandles.dropArguments(MethodHandles.constant(boolean.class, false),
                                                          0, Object.class, Class.class));
            } catch (ReflectiveOperationException error) {
                throw new ExceptionInInitializerError(error);
            }
        }

        private static void initialize() {
            hasSource(null, null); // Link the initial constant target during core loading.
        }

        private static void install(IFn predicate, Class<?> sourceInterface,
                                    Class<?>[] extensions, boolean nilSource) {
            MethodHandle test;
            if (extensions.length <= 4) {
                test = CLASS_TEST.bindTo(sourceInterface);
                for (Class<?> extension : extensions) {
                    test = MethodHandles.guardWithTest(CLASS_TEST.bindTo(extension), TRUE, test);
                }
                if (nilSource) test = MethodHandles.guardWithTest(NIL_TEST, TRUE, test);
            } else {
                test = IFN_TEST.bindTo(predicate).asType(VALUE);
            }
            SITE.setTarget(MethodHandles.insertArguments(SCAN, 0, test, null));
            MutableCallSite.syncAll(new MutableCallSite[] {SITE});
        }

        private static boolean hasSource(Object inputs, Class<?> nativeDB) {
            try {
                return (boolean) INVOKER.invokeExact(inputs, nativeDB);
            } catch (Throwable error) {
                throw Util.sneakyThrow(error);
            }
        }
    }
}
