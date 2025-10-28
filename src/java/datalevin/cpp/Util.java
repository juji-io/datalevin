package datalevin.cpp;

import org.bytedeco.javacpp.*;
import datalevin.dtlvnative.DTLV;
import java.lang.reflect.Field;

public class Util {

    public static class DTLVException extends RuntimeException {
        public DTLVException(String msg) {
            super(msg);
        }
    }

    public static class BadReaderLockException extends DTLVException {
        public BadReaderLockException(String msg) {
            super(msg);
        }
    }

    public static class MapFullException extends DTLVException {
        public MapFullException(String msg) {
            super(msg);
        }
    }

    public static class NotReadyException extends DTLVException {
        public NotReadyException(String msg) {
            super(msg);
        }
    }

    public static class NotResetException extends DTLVException {
        public NotResetException(String msg) {
            super(msg);
        }
    }

    public static class ResetException extends DTLVException {
        public ResetException(String msg) {
            super(msg);
        }
    }

    /**
     * Handle some return codes that we care about
     */
    public static void checkRc(int code) {
        String msg;
        if (code == DTLV.MDB_SUCCESS || code == DTLV.MDB_NOTFOUND) {
            return;
        } else if (code == DTLV.MDB_BAD_RSLOT) {
            throw new BadReaderLockException("");
        } else if (code == DTLV.MDB_MAP_FULL) {
            throw new MapFullException("");
        } else {
            BytePointer p = DTLV.mdb_strerror(code);
            msg = p.getString();
            throw new DTLVException(msg);
        }
    }

    /**
     * Return version of LMDB as a string
     */
    public static String version() {
        String version;
        BytePointer v = DTLV.mdb_version((IntPointer)null,
                                         (IntPointer)null,
                                         (IntPointer)null);
        version = v.getString();
        return version;
    }
}
