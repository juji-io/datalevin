package datalevin;

import java.util.List;

/**
 * Functional interface for Java-backed Datalevin UDFs.
 *
 * <p>The provided argument list contains Datalevin call arguments. Raw
 * database values are represented by the bridge-safe {@link DatabaseValue};
 * for transaction functions it is the first argument.
 * JavaScript callbacks may await reads through these handles: the JVM caller
 * services those reads on the transaction's owning thread. Such borrowed handles
 * are valid only until that callback returns.
 */
@FunctionalInterface
public interface UdfFunction {

    Object invoke(List<?> args);
}
