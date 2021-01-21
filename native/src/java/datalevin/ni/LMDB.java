package datalevin.ni;

import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.c.CContext;
import org.graalvm.nativeimage.c.constant.CConstant;
import org.graalvm.nativeimage.c.function.CFunction;
import org.graalvm.nativeimage.c.function.CFunctionPointer;
import org.graalvm.nativeimage.c.constant.CEnum;
import org.graalvm.nativeimage.c.constant.CEnumLookup;
import org.graalvm.nativeimage.c.constant.CEnumValue;
import org.graalvm.nativeimage.c.function.InvokeCFunctionPointer;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CCharPointerPointer;
import org.graalvm.nativeimage.c.type.CIntPointer;
import org.graalvm.nativeimage.c.type.CLongPointer;
import org.graalvm.nativeimage.c.type.WordPointer;
import org.graalvm.nativeimage.c.type.VoidPointer;
import org.graalvm.nativeimage.c.struct.CField;
import org.graalvm.nativeimage.c.struct.CFieldAddress;
import org.graalvm.nativeimage.c.struct.CStruct;
import org.graalvm.word.Pointer;
import org.graalvm.word.PointerBase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * GraalVM native image bindings to LMDB.
 */
@CContext(LMDB.Directives.class)
final class LMDB {

    /**
     * Sets up the context required for interacting with native libraries.
     */
    public static final class Directives implements CContext.Directives {
        @Override
        public List<String> getHeaderFiles() {
            return Collections.singletonList("<lmdb.h>");
        }

        @Override
        public List<String> getLibraries() {
            return Arrays.asList("lmdb");
        }
    }

    /**
     * Generic structure used for passing keys and data in and out
     * of the database.
     */
    @CStruct("MDB_val")
    interface MDB_val extends PointerBase {

        @CField("mv_size")
        long get_mv_size();

        @CField("mv_size")
        void set_mv_size(long value);

        @CField("mv_data")
        VoidPointer get_mv_data();

        @CField("mv_data")
        void set_mv_data(VoidPointer value);
    }

    /**
     * A callback function used to compare two keys in a database,
     * used by <code>mdb_set_compare</code>.
     */
    public interface MDB_cmp_func extends CFunctionPointer {

        @InvokeCFunctionPointer
        int invoke(IsolateThread thread, MDB_val a, MDB_val b);
    }

    /**
     * mdb_env Environment flags
     */
    @CConstant("MDB_FIXEDMAP") static native int MDB_FIXEDMAP();
    @CConstant("MDB_NOSUBDIR") static native int MDB_NOSUBDIR();
    @CConstant("MDB_NOSYNC") static native int MDB_NOSYNC();
    @CConstant("MDB_RDONLY") static native int MDB_RDONLY();
    @CConstant("MDB_NOMETASYNC") static native int MDB_NOMETASYNC();
    @CConstant("MDB_WRITEMAP") static native int MDB_WRITEMAP();
    @CConstant("MDB_MAPASYNC") static native int MDB_MAPASYNC();
    @CConstant("MDB_NOTLS") static native int MDB_NOTLS();
    @CConstant("MDB_NOLOCK") static native int MDB_NOLOCK();
    @CConstant("MDB_NORDAHEAD") static native int MDB_NORDAHEAD();
    @CConstant("MDB_NOMEMINIT") static native int MDB_NOMEMINIT();
    // @CConstant("MDB_PREVSNAPSHOT") static native int MDB_PREVSNAPSHOT();

    /**
     * mdb_dbi_open Database flags
     */
    @CConstant("MDB_REVERSEKEY") static native int MDB_REVERSEKEY();
    @CConstant("MDB_DUPSORT") static native int MDB_DUPSORT();
    @CConstant("MDB_INTEGERKEY") static native int MDB_INTEGERKEY();
    @CConstant("MDB_DUPFIXED") static native int MDB_DUPFIXED();
    @CConstant("MDB_INTEGERDUP") static native int MDB_INTEGERDUP();
    @CConstant("MDB_REVERSEDUP") static native int MDB_REVERSEDUP();
    @CConstant("MDB_CREATE") static native int MDB_CREATE();

    /**
     * mdb_put Write flags
     */
    @CConstant("MDB_NOOVERWRITE") static native int MDB_NOOVERWRITE();
    @CConstant("MDB_NODUPDATA") static native int MDB_NODUPDATA();
    @CConstant("MDB_CURRENT") static native int MDB_CURRENT();
    @CConstant("MDB_RESERVE") static native int MDB_RESERVE();
    @CConstant("MDB_APPEND") static native int MDB_APPEND();
    @CConstant("MDB_APPENDDUP") static native int MDB_APPENDDUP();
    @CConstant("MDB_MULTIPLE") static native int MDB_MULTIPLE();

    /**
     * mdb_copy Copy flags
     */
    @CConstant("MDB_CP_COMPACT") static native int MDB_CP_COMPACT();

    /**
     * Cursor Get operations.
     */
    @CEnum("Pointer_op")
    enum Pointer_op {
        MDB_FIRST,				/**<  Position at first key/data item */
        MDB_FIRST_DUP,		/**< Position at first data item of current key.
                             Only for #MDB_DUPSORT */
        MDB_GET_BOTH,			/**< Position at key/data pair. Only for #MDB_DUPSORT */
        MDB_GET_BOTH_RANGE,		/**< position at key, nearest data.
                                 Only for #MDB_DUPSORT */
        MDB_GET_CURRENT,		/**< Return key/data at current cursor position */
        MDB_GET_MULTIPLE,		/**< Return up to a page of duplicate data items
                               from current cursor position. Move cursor to prepare
                               for #MDB_NEXT_MULTIPLE. Only for #MDB_DUPFIXED */
        MDB_LAST,				/**< Position at last key/data item */
        MDB_LAST_DUP,			/**< Position at last data item of current key.
                             Only for #MDB_DUPSORT */
        MDB_NEXT,				/**< Position at next data item */
        MDB_NEXT_DUP,			/**< Position at next data item of current key.
                             Only for #MDB_DUPSORT */
        MDB_NEXT_MULTIPLE,		/**< Return up to a page of duplicate data items
                                 from next cursor position. Move cursor to prepare
                                 for #MDB_NEXT_MULTIPLE. Only for #MDB_DUPFIXED */
        MDB_NEXT_NODUP,			/**< Position at first data item of next key */
        MDB_PREV,				/**< Position at previous data item */
        MDB_PREV_DUP,			/**< Position at previous data item of current key.
                             Only for #MDB_DUPSORT */
        MDB_PREV_NODUP,			/**< Position at last data item of previous key */
        MDB_SET,				/**< Position at specified key */
        MDB_SET_KEY,			/**< Position at specified key, return key + data */
        MDB_SET_RANGE,			/**< Position at first key greater than or equal
                               to specified key. */
        MDB_PREV_MULTIPLE;		/**< Position at previous page and return up to
                               a page of duplicate data items.
                               Only for #MDB_DUPFIXED */

        @CEnumValue
        public native int getCValue();

        @CEnumLookup
        public static native Pointer_op fromCValue(int value);
    }

    /**
     * Return codes.
     */
    @CConstant("MDB_SUCCESS") static native int MDB_SUCCESS();
    @CConstant("MDB_KEYEXIST") static native int MDB_KEYEXIST();
    @CConstant("MDB_NOTFOUND") static native int MDB_NOTFOUND();
    @CConstant("MDB_PAGE_NOTFOUND") static native int MDB_PAGE_NOTFOUND();
    @CConstant("MDB_CORRUPTED") static native int MDB_CORRUPTED();
    @CConstant("MDB_PANIC") static native int MDB_PANIC();
    @CConstant("MDB_VERSION_MISMATCH") static native int MDB_VERSION_MISMATCH();
    @CConstant("MDB_INVALID") static native int MDB_INVALID();
    @CConstant("MDB_MAP_FULL") static native int MDB_MAP_FULL();
    @CConstant("MDB_DBS_FULL") static native int MDB_DBS_FULL();
    @CConstant("MDB_READERS_FULL") static native int MDB_READERS_FULL();
    @CConstant("MDB_TLS_FULL") static native int MDB_TLS_FULL();
    @CConstant("MDB_TXN_FULL") static native int MDB_TXN_FULL();
    @CConstant("MDB_CURSOR_FULL") static native int MDB_CURSOR_FULL();
    @CConstant("MDB_PAGE_FULL") static native int MDB_PAGE_FULL();
    @CConstant("MDB_MAP_RESIZED") static native int MDB_MAP_RESIZED();
    @CConstant("MDB_INCOMPATIBLE") static native int MDB_INCOMPATIBLE();
    @CConstant("MDB_BAD_RSLOT") static native int MDB_BAD_RSLOT();
    @CConstant("MDB_BAD_TXN") static native int MDB_BAD_TXN();
    @CConstant("MDB_BAD_VALSIZE") static native int MDB_BAD_VALSIZE();
    @CConstant("MDB_BAD_DBI") static native int MDB_BAD_DBI();
    // @CConstant("MDB_PROBLEM") static native int MDB_PROBLEM();
    // @CConstant("MDB_LAST_ERRCODE") static native int MDB_LAST_ERRCODE();

    /**
     * Statistics for a database in the environment
     */
    @CStruct("MDB_stat")
    interface MDB_stat extends PointerBase {

        @CField("ms_psize")
        int ms_psize();

        @CField("ms_depth")
        int ms_depth();

        @CField("ms_branch_pages")
        long ms_branch_pages();

        @CField("ms_leaf_pages")
        long ms_leaf_pages();

        @CField("ms_overflow_pages")
        long ms_overflow_pages();

        @CField("ms_entries")
        long ms_entries();
    }

    /**
     * Information about the environment
     */
    @CStruct("MDB_envinfo")
    interface MDB_envinfo extends PointerBase {

        @CFieldAddress("me_mapaddr")
        VoidPointer me_mapaddr();

        @CField("me_mapsize")
        long me_mapsize();

        @CField("me_last_pgno")
        long me_last_pgno();

        @CField("me_last_txnid")
        long me_last_txnid();

        @CField("me_maxreaders")
        int me_maxreaders();

        @CField("me_numreaders")
        int me_numreaders();
    }

    /**
     * General functions
     */
    @CFunction("mdb_version")
    static native CCharPointer mdb_version(CIntPointer major, CIntPointer minor,
                                           CIntPointer patch);

    @CFunction("mdb_strerror")
    static native CCharPointer mdb_strerror(int err);

    /**
     * LMDB environment functions
     */
    @CFunction("mdb_env_create")
    static native int mdb_env_create(WordPointer envPtr);

    @CFunction("mdb_env_open")
    static native int mdb_env_open(Pointer env, CCharPointer path, int flags,
                                   int mode);

    @CFunction("mdb_env_copy")
    static native int mdb_env_copy(Pointer env, CCharPointer path);

    @CFunction("mdb_env_copyfd")
    static native int mdb_env_copyfd(Pointer env, int fd);

    @CFunction("mdb_env_copy2")
    static native int mdb_env_copy2(Pointer env, CCharPointer path, int flags);

    @CFunction("mdb_env_copyfd2")
    static native int mdb_env_copyfd2(Pointer env, int fd, int flags);

    @CFunction("mdb_env_stat")
    static native int mdb_env_stat(Pointer env, MDB_stat stat);

    @CFunction("mdb_env_info")
    static native int mdb_env_info(Pointer env, MDB_envinfo info);

    @CFunction("mdb_env_sync")
    static native int mdb_env_sync(Pointer env, int force);

    @CFunction("mdb_env_close")
    static native void mdb_env_close(Pointer env);

    @CFunction("mdb_env_set_flags")
    static native int mdb_env_set_flags(Pointer env, int flags, int onoff);

    @CFunction("mdb_env_get_flags")
    static native int mdb_env_get_flags(Pointer env, CIntPointer flags);

    @CFunction("mdb_env_get_path")
    static native int mdb_env_get_path(Pointer env, CCharPointerPointer path);

    @CFunction("mdb_env_get_fd")
    static native int mdb_env_get_fd(Pointer env, CIntPointer fd);

    @CFunction("mdb_env_set_mapsize")
    static native int mdb_env_set_mapsize(Pointer env, long size);

    @CFunction("mdb_env_set_maxreaders")
    static native int mdb_env_set_maxreaders(Pointer env, int readers);

    @CFunction("mdb_env_get_maxreaders")
    static native int mdb_env_get_maxreaders(Pointer env, CIntPointer readers);

    @CFunction("mdb_env_set_maxdbs")
    static native int mdb_env_set_maxdbs(Pointer env, int dbs);

    @CFunction("mdb_env_get_maxkeysize")
    static native int mdb_env_get_maxkeysize(Pointer env);

    @CFunction("mdb_env_set_userctx")
    static native int mdb_env_set_userctx(Pointer env, VoidPointer ctx);

    @CFunction("mdb_env_get_userctx")
    static native VoidPointer mdb_env_get_userctx(Pointer env);

    /**
     * A callback function for most LMDB assert() failures,
     * called before printing the message and aborting.
     */
    public interface MDB_assert_func extends CFunctionPointer {

        @InvokeCFunctionPointer
        int invoke(IsolateThread thread, Pointer env, CCharPointer msg);
    }

    @CFunction("mdb_env_set_assert")
    static native VoidPointer mdb_env_set_assert(Pointer env,
                                                 MDB_assert_func func);

    /**
     * LMDB transaction functions
     */
    @CFunction("mdb_txn_begin")
    static native int mdb_txn_begin(Pointer env, Pointer parentTx, int flags,
                                    WordPointer txPtr);

    @CFunction("mdb_txn_env")
    static native Pointer mdb_txn_env(Pointer txn);

    @CFunction("mdb_txn_id")
    static native long mdb_txn_id(Pointer txn);

    @CFunction("mdb_txn_commit")
    static native int mdb_txn_commit(Pointer txn);

    @CFunction("mdb_txn_abort")
    static native void mdb_txn_abort(Pointer txn);

    @CFunction("mdb_txn_reset")
    static native void mdb_txn_reset(Pointer txn);

    @CFunction("mdb_txn_renew")
    static native int mdb_txn_renew(Pointer txn);

    /**
     * LMDB dbi functions
     */
    @CFunction("mdb_dbi_open")
    static native int mdb_dbi_open(Pointer txn, CCharPointer name, int flags,
                                   CIntPointer dbi);

    @CFunction("mdb_stat")
    static native int mdb_stat(Pointer txn, int dbi, MDB_stat stat);

    @CFunction("mdb_dbi_flags")
    static native int mdb_dbi_flags(Pointer txn, int dbi, CIntPointer flags);

    @CFunction("mdb_dbi_close")
    static native void mdb_dbi_close(Pointer env, int dbi);

    @CFunction("mdb_drop")
    static native int mdb_drop(Pointer txn, int dbi, int del);

    /**
     * LMDB data access functions
     */
    @CFunction("mdb_set_compare")
    static native int mdb_set_compare(Pointer txn, int dbi, MDB_cmp_func cmp);

    @CFunction("mdb_set_dupsort")
    static native int mdb_set_dupsort(Pointer txn, int dbi, MDB_cmp_func cmp);

    @CFunction("mdb_get")
    static native int mdb_get(Pointer txn, int dbi, MDB_val key, MDB_val data);

    @CFunction("mdb_put")
    static native int mdb_get(Pointer txn, int dbi, MDB_val key,
                              MDB_val data, int flags);

    @CFunction("mdb_del")
    static native int mdb_del(Pointer txn, int dbi, MDB_val key, MDB_val data);

    /**
     * Cursor functions
     */
    @CFunction("mdb_cursor_open")
    static native int mdb_cursor_open(Pointer txn, int dbi,
                                      WordPointer cursorPtr);

    @CFunction("mdb_cursor_close")
    static native void mdb_cursor_close(Pointer cursor);

    @CFunction("mdb_cursor_renew")
    static native int mdb_cursor_renew(Pointer txn, Pointer cursor);

    @CFunction("mdb_cursor_txn")
    static native Pointer mdb_cursor_txn(Pointer cursor);

    @CFunction("mdb_cursor_dbi")
    static native int mdb_cursor_dbi(Pointer cursor);

    @CFunction("mdb_cursor_get")
    static native int mdb_cursor_get(Pointer cursor, MDB_val k, MDB_val v,
                                     Pointer_op cursorOp);

    @CFunction("mdb_cursor_put")
    static native int mdb_cursor_put(Pointer cursor, MDB_val key, MDB_val data,
                                     int flags);

    @CFunction("mdb_cursor_del")
    static native int mdb_cursor_del(Pointer cursor, int flags);

    @CFunction("mdb_cursor_count")
    static native int mdb_cursor_count(Pointer cursor, CLongPointer countp);

    /**
     * Utitily functions
     */
    @CFunction("mdb_cmp")
    static native int mdb_cmp(Pointer txn, int dbi, MDB_val a, MDB_val b);

    @CFunction("mdb_dcmp")
    static native int mdb_dcmp(Pointer txn, int dbi, MDB_val a, MDB_val b);

    /**
     * A callback function used to print a message from the library.
     */
    public interface MDB_msg_func extends CFunctionPointer {

        @InvokeCFunctionPointer
        int invoke(IsolateThread thread, CCharPointer msg, VoidPointer ctx);
    }

    @CFunction("mdb_reader_list")
    static native int mdb_reader_check(Pointer env, MDB_msg_func func,
                                       VoidPointer ctx);

    @CFunction("mdb_reader_check")
    static native int mdb_reader_check(Pointer env, CIntPointer dead);

}
