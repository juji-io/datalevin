;; ;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.binding.cpp
  "Native binding using JavaCPP"
  (:refer-clojure :exclude [sync])
  (:require
   [clojure.stacktrace :as stt]
   [clojure.java.io :as io]
   [clojure.string :as s]
   [datalevin.bits :as b]
   [datalevin.util :as u :refer [raise]]
   [datalevin.constants :as c]
   [datalevin.compress :as cp]
   [datalevin.buffer :as bf]
   [datalevin.async :as a]
   [datalevin.migrate :as m]
   [datalevin.validate :as vld]
   [datalevin.wal :as wal]
   [datalevin.scan :as scan]
   [datalevin.interface :as i
    :refer [IList ILMDB IAdmin open-dbi close-kv env-dir close-vecs
            transact-kv get-range range-filter stat key-compressor
            val-compressor set-max-val-size max-val-size
            set-key-compressor set-val-compressor
            bf-compress bf-uncompress]]
   [datalevin.lmdb :as l
    :refer [open-kv IBuffer IRange IRtx IDB IKV IWriting ICompress
            IListRandKeyValIterable IListRandKeyValIterator]])
  (:import
   [datalevin.dtlvnative DTLV DTLV$MDB_envinfo DTLV$MDB_stat DTLV$dtlv_key_iter
    DTLV$dtlv_list_iter DTLV$dtlv_list_sample_iter DTLV$dtlv_list_val_iter
    DTLV$dtlv_list_rank_sample_iter DTLV$dtlv_list_val_full_iter DTLV$MDB_val
    DTLV$dtlv_list_key_range_full_val_iter DTLV$dtlv_key_sample_iter
    DTLV$dtlv_key_rank_sample_iter]
   [datalevin.cpp BufVal Env Txn Dbi Cursor Stat Info Util Util$MapFullException]
   [datalevin.lmdb RangeContext KVTxData]
   [datalevin.async IAsyncWork]
   [datalevin.utl BitOps]
   [java.util.concurrent ConcurrentSkipListMap TimeUnit ScheduledExecutorService ScheduledFuture]
   [java.lang AutoCloseable]
   [java.io File]
   [java.util Iterator HashMap ArrayDeque Arrays]
   [java.util.function Supplier]
   [java.nio BufferOverflowException ByteBuffer]
   [java.nio.charset StandardCharsets]
   [org.bytedeco.javacpp SizeTPointer LongPointer]
   [clojure.lang IObj Seqable IReduceInit]))

(defn- version-file
  [^File dir]
  (io/file dir c/version-file-name))

(defn- write-version-file
  [^File dir version]
  (when (and version (not (s/blank? ^String version)))
    (spit (version-file dir) version)
    version))

(defn- read-version-file
  [^File dir]
  (try
    (let [^File f (version-file dir)]
      (when (.exists f)
        (some-> (slurp f) s/trim not-empty)))
    (catch Exception e
      (raise "Unable to read VERSION file"
             {:msg (.getMessage e)}))))

(defprotocol IPool
  (pool-add [_ x])
  (pool-take [_]))

(deftype Pool [^ThreadLocal que]
  IPool
  (pool-add [_ x] (.add ^ArrayDeque (.get que) x))
  (pool-take [_] (.poll ^ArrayDeque (.get que))))

(defn- new-pools
  []
  (Pool. (ThreadLocal/withInitial
           (reify Supplier
             (get [_] (ArrayDeque.))))))

(defn- new-bufval [size] (BufVal. size))

(defn- flag-value
  "flag key to int value, cover all flags"
  [k]
  (case k
    :fixedmap   DTLV/MDB_FIXEDMAP
    :nosubdir   DTLV/MDB_NOSUBDIR
    :rdonly-env DTLV/MDB_RDONLY
    :writemap   DTLV/MDB_WRITEMAP
    :nometasync DTLV/MDB_NOMETASYNC
    :nosync     DTLV/MDB_NOSYNC
    :mapasync   DTLV/MDB_MAPASYNC
    :notls      DTLV/MDB_NOTLS
    :nolock     DTLV/MDB_NOLOCK
    :nordahead  DTLV/MDB_NORDAHEAD
    :nomeminit  DTLV/MDB_NOMEMINIT

    :cp-compact DTLV/MDB_CP_COMPACT

    :reversekey         DTLV/MDB_REVERSEKEY
    :dupsort            DTLV/MDB_DUPSORT
    :integerkey         DTLV/MDB_INTEGERKEY
    :dupfixed           DTLV/MDB_DUPFIXED
    :integerdup         DTLV/MDB_INTEGERDUP
    :reversedup         DTLV/MDB_REVERSEDUP
    :create             DTLV/MDB_CREATE
    :prefix-compression DTLV/MDB_PREFIX_COMPRESSION
    :counted            DTLV/MDB_COUNTED

    :nooverwrite DTLV/MDB_NOOVERWRITE
    :nodupdata   DTLV/MDB_NODUPDATA
    :current     DTLV/MDB_CURRENT
    :reserve     DTLV/MDB_RESERVE
    :append      DTLV/MDB_APPEND
    :appenddup   DTLV/MDB_APPENDDUP
    :multiple    DTLV/MDB_MULTIPLE

    :rdonly-txn DTLV/MDB_RDONLY))

(defn- kv-flags
  [flags]
  (if (seq flags)
    (reduce (fn [r f] (bit-or ^int r ^int f))
            0 (mapv flag-value flags))
    (int 0)))

(defonce env-flag-map
  {0x01      :fixedmap
   0x4000    :nosubdir
   0x10000   :nosync
   0x20000   :rdonly-env
   0x40000   :nometasync
   0x80000   :writemap
   0x100000  :mapasync
   0x200000  :notls
   0x400000  :nolock
   0x800000  :nordahead
   0x1000000 :nomeminit})

(defn- env-flag-keys
  [v]
  (reduce-kv
    (fn [s i k]
      (if (not= 0 (bit-and ^int i ^int v))
        (conj s k)
        s))
    #{} env-flag-map))

(defn- put-bufval
  [^BufVal vp k kt compressor ^ByteBuffer cbf]
  (when-some [x k]
    (let [^ByteBuffer bf (.inBuf vp)]
      (.clear bf)
      (if compressor
        (do (b/put-buffer (.clear cbf) x kt)
            (bf-compress compressor (.flip cbf) bf))
        (b/put-buffer bf x kt))
      (.flip bf)
      (.reset vp))))

(deftype Rtx [lmdb
              ^Txn txn
              depth
              ^BufVal kp
              ^BufVal vp
              ^BufVal start-kp
              ^BufVal stop-kp
              ^BufVal start-vp
              ^BufVal stop-vp
              ^ByteBuffer k-comp-bf
              ^:volatile-mutable ^ByteBuffer v-comp-bf
              aborted?
              wal-ops
              kv-overlay-private]

  ICompress
  (key-bf [_] (.clear k-comp-bf))
  (val-bf [_] (.clear v-comp-bf))

  IBuffer
  (put-key [_ x t]
    (try
      (put-bufval kp x t (key-compressor lmdb) k-comp-bf)
      (catch BufferOverflowException _
        (raise "Key cannot be larger than 511 bytes." {:input x}))
      (catch Exception e
        (raise "Error putting read-only transaction key buffer: "
               e {:value x :type t}))))
  (put-val [_ _ _]
    (raise "put-val not allowed for read only txn buffer" {}))

  IRange
  (range-info [_ range-type k1 k2 kt]
    (put-bufval start-kp k1 kt (key-compressor lmdb) k-comp-bf)
    (put-bufval stop-kp k2 kt (key-compressor lmdb) k-comp-bf)
    (l/range-table range-type start-kp stop-kp))

  (list-range-info [_ k-range-type k1 k2 kt v-range-type v1 v2 vt]
    (put-bufval start-kp k1 kt (key-compressor lmdb) k-comp-bf)
    (put-bufval stop-kp k2 kt (key-compressor lmdb) k-comp-bf)
    (put-bufval start-vp v1 vt (val-compressor lmdb) v-comp-bf)
    (put-bufval stop-vp v2 vt (val-compressor lmdb) v-comp-bf)
    [(l/range-table k-range-type start-kp stop-kp)
     (l/range-table v-range-type start-vp stop-vp)])

  IRtx
  (read-only? [_] (.isReadOnly txn))
  (reset [this]
    (vswap! depth u/long-dec)
    (when (zero? ^long @depth)
      (.reset txn))
    this)

  (renew [this]
    (when (zero? ^long @depth)
      (.renew txn))
    (vswap! depth u/long-inc)
    this))

(defn- v-bf
  [^BufVal vp lmdb rtx]
  (let [bf (.outBuf vp)]
    (if-let [compressor (val-compressor lmdb)]
      (let [^ByteBuffer cbf (l/val-bf rtx)]
        (bf-uncompress compressor bf cbf)
        (.flip cbf))
      bf)))

(deftype KV [^BufVal kp ^BufVal vp lmdb rtx]
  IKV
  (k [_]
    (let [bf (.outBuf kp)]
      (if-let [compressor (key-compressor lmdb)]
        (let [^ByteBuffer cbf (l/key-bf rtx)]
          (bf-uncompress compressor bf cbf)
          (.flip cbf))
        bf)))

  (v [_] (v-bf vp lmdb rtx)))

(defn- stat-map [^Stat stat]
  (let [^DTLV$MDB_stat s (.get stat)]
    {:psize          (.ms_psize s)
     :depth          (.ms_depth s)
     :branch-pages   (.ms_branch_pages s)
     :leaf-pages     (.ms_leaf_pages s)
     :overflow-pages (.ms_overflow_pages s)
     :entries        (.ms_entries s)}))

(declare ->KeyIterable ->KeySampleIterable ->ListIterable
         ->ListRandKeyValIterable ->ListFullValIterable ->ListSampleIterable
         ->ListKeyRangeFullValIterable)

(defn- val-size
  [x]
  (let [^long val-size (b/measure-size x)]
    (if (< Integer/MAX_VALUE val-size)
      (raise "Value size is too large" {:size val-size})
      (let [try-size (* ^long c/+buffer-grow-factor+ val-size)]
        (if (< Integer/MAX_VALUE try-size)
          val-size
          try-size)))))

(deftype DBI [lmdb
              ^Dbi db
              ^Pool curs
              ^BufVal kp
              ^:volatile-mutable ^BufVal vp
              ^ByteBuffer k-comp-bf
              ^:volatile-mutable ^ByteBuffer v-comp-bf
              ^boolean dupsort?
              ^boolean counted?
              ^boolean validate-data?]
  IBuffer
  (put-key [this x t]
    (try
      (put-bufval kp x t (key-compressor lmdb) k-comp-bf)
      (catch BufferOverflowException _
        (raise "Key cannot be larger than 511 bytes." {:input x}))
      (catch Exception e
        (raise "Error putting r/w key buffer of "
               (.dbi-name this)": " e {:value x :type t}))))
  (put-val [this x t]
    (try
      (put-bufval vp x t (val-compressor lmdb) v-comp-bf)
      (catch BufferOverflowException _
        (let [size (val-size x)]
          (set! vp (new-bufval size))
          (set! v-comp-bf (bf/allocate-buffer size))
          (set-max-val-size lmdb size)
          (put-bufval vp x t (val-compressor lmdb) v-comp-bf)))
      (catch Exception e
        (raise "Error putting r/w value buffer of "
               (.dbi-name this)": " e {:value x :type t}))))

  IDB
  (dbi [_] db)
  (dbi-name [_] (.getName db))
  (put [_ txn flags] (.put db txn kp vp (kv-flags flags)))
  (put [this txn] (.put this txn nil))
  (del [_ txn all?] (if all? (.del db txn kp nil) (.del db txn kp vp)))
  (del [this txn] (.del this txn true))
  (get-kv [_ rtx]
    (let [^BufVal kp (.-kp ^Rtx rtx)
          ^BufVal vp (.-vp ^Rtx rtx)
          rc         (DTLV/mdb_get (.get ^Txn (.-txn ^Rtx rtx))
                                   (.get db) (.ptr kp) (.ptr vp))]
      (Util/checkRc ^int rc)
      (when-not (= rc DTLV/MDB_NOTFOUND)
        (.outBuf vp))))
  (get-key-rank [_ rtx]
    (let [^BufVal kp   (.-kp ^Rtx rtx)
          ^LongPointer rp (LongPointer. 1)
          rc           (DTLV/mdb_get_key_rank (.get ^Txn (.-txn ^Rtx rtx))
                                              (.get db) (.ptr kp) nil rp)]
      (Util/checkRc ^int rc)
      (when-not (= rc DTLV/MDB_NOTFOUND)
        (.get rp))))
  (get-key-by-rank [_ rtx rank]
    (let [^BufVal kp (.-kp ^Rtx rtx)
          ^BufVal vp (.-vp ^Rtx rtx)
          rc         (DTLV/mdb_get_rank (.get ^Txn (.-txn ^Rtx rtx))
                                        (.get db) (long rank) (.ptr kp) (.ptr vp))]
      (Util/checkRc ^int rc)
      (when-not (= rc DTLV/MDB_NOTFOUND)
        [(.outBuf kp) (.outBuf vp)])))
  (iterate-key [this rtx cur [range-type k1 k2] k-type]
    (let [ctx (l/range-info rtx range-type k1 k2 k-type)]
      (->KeyIterable lmdb this cur rtx ctx)))
  (iterate-key-sample [this rtx cur indices budget step [range-type k1 k2] k-type]
    (let [ctx (l/range-info rtx range-type k1 k2 k-type)]
      (->KeySampleIterable lmdb this indices budget step cur rtx ctx)))
  (iterate-list [this rtx cur [k-range-type k1 k2] k-type
                 [v-range-type v1 v2] v-type]
    (let [ctx (l/list-range-info rtx k-range-type k1 k2 k-type
                                 v-range-type v1 v2 v-type)]
      (->ListIterable lmdb this cur rtx ctx)))
  (iterate-list-sample [this rtx cur indices budget step [k-range-type k1 k2] k-type]
    (let [ctx (l/range-info rtx k-range-type k1 k2 k-type)]
      (->ListSampleIterable lmdb this indices budget step cur rtx ctx)))
  (iterate-list-val [this rtx cur [v-range-type v1 v2] v-type]
    (let [ctx (l/range-info rtx v-range-type v1 v2 v-type)]
      (->ListRandKeyValIterable lmdb this cur rtx ctx)))
  (iterate-list-val-full [this rtx cur]
    (->ListFullValIterable lmdb this cur rtx))
  (iterate-list-key-range-val-full [this rtx cur [range-type k1 k2] k-type]
    (let [ctx (l/range-info rtx range-type k1 k2 k-type)]
      (->ListKeyRangeFullValIterable lmdb this cur rtx ctx)))
  (iterate-kv [this rtx cur k-range k-type v-type]
    (if dupsort?
      (.iterate-list this rtx cur k-range k-type [:all] v-type)
      (.iterate-key this rtx cur k-range k-type)))
  (get-cursor [_ rtx]
    (let [^Rtx rtx rtx
          ^Txn txn (.-txn rtx)]
      (or (when (.isReadOnly txn)
            (when-let [^Cursor cur (pool-take curs)]
              (.renew cur txn)
              cur))
          (Cursor/create txn db (.-kp rtx) (.-vp rtx)))))
  (cursor-count [_ cur] (.count ^Cursor cur))
  (close-cursor [_ cur] (.close ^Cursor cur))
  (return-cursor [_ cur] (pool-add curs cur)))

(defn- dtlv-bool [x] (if x DTLV/DTLV_TRUE DTLV/DTLV_FALSE))

(defn- dtlv-val ^DTLV$MDB_val [x] (when x (.ptr ^BufVal x)))

(defn- dtlv-rc [x]
  (condp = x
    DTLV/DTLV_TRUE  true
    DTLV/DTLV_FALSE false
    (u/raise "Native iterator returns error code" x {})))

(defn- dtlv-c [^long x]
  (if (< x 0)
    (u/raise "Native counter returns error code" x {})
    x))

(deftype KeyIterable [lmdb
                      ^DBI db
                      ^Cursor cur
                      ^Rtx rtx
                      ^RangeContext ctx]
  Iterable
  (iterator [_]
    (let [forward?       (dtlv-bool (.-forward? ctx))
          include-start? (dtlv-bool (.-include-start? ctx))
          include-stop?  (dtlv-bool (.-include-stop? ctx))
          sk             (dtlv-val (.-start-bf ctx))
          ek             (dtlv-val (.-stop-bf ctx))
          k              (.key cur)
          v              (.val cur)
          iter           (DTLV$dtlv_key_iter.)]
      (Util/checkRc
        (DTLV/dtlv_key_iter_create
          iter (.ptr cur) (.ptr k) (.ptr v)
          ^int forward? ^int include-start? ^int include-stop? sk ek))
      (reify
        Iterator
        (hasNext [_] (dtlv-rc (DTLV/dtlv_key_iter_has_next iter)))
        (next [_] (KV. k v lmdb rtx))

        AutoCloseable
        (close [_] (DTLV/dtlv_key_iter_destroy iter))))))

(deftype KeySampleIterable [lmdb
                            ^DBI db
                            ^longs indices
                            ^long budget
                            ^long step
                            ^Cursor cur
                            ^Rtx rtx
                            ^RangeContext ctx]
  Iterable
  (iterator [_]
    (let [sk             (dtlv-val (.-start-bf ctx))
          ek             (dtlv-val (.-stop-bf ctx))
          forward?       (dtlv-bool (.-forward? ctx))
          include-start? (dtlv-bool (.-include-start? ctx))
          include-stop?  (dtlv-bool (.-include-stop? ctx))
          k              (.key cur)
          v              (.val cur)
          dlmdb?         (l/dlmdb?)
          iter           (if dlmdb?
                           (DTLV$dtlv_key_rank_sample_iter.)
                           (DTLV$dtlv_key_sample_iter.))
          samples        (alength indices)
          sizets         (SizeTPointer. samples)]
      (dotimes [i samples] (.put sizets i (aget indices i)))
      (Util/checkRc
        (if dlmdb?
          (DTLV/dtlv_key_rank_sample_iter_create
            ^DTLV$dtlv_key_rank_sample_iter iter
            sizets samples (.ptr cur) (.ptr k) (.ptr v) sk ek)
          (DTLV/dtlv_key_sample_iter_create
            ^DTLV$dtlv_key_sample_iter iter
            sizets samples budget step (.ptr cur) (.ptr k) (.ptr v)
            ^int forward? ^int include-start? ^int include-stop? sk ek)))
      (reify
        Iterator
        (hasNext [_]
          (dtlv-rc
            (if dlmdb?
              (DTLV/dtlv_key_rank_sample_iter_has_next iter)
              (DTLV/dtlv_key_sample_iter_has_next iter))))
        (next [_] (KV. k v lmdb rtx))

        AutoCloseable
        (close [_]
          (if dlmdb?
            (DTLV/dtlv_key_rank_sample_iter_destroy iter)
            (DTLV/dtlv_key_sample_iter_destroy iter)))))))

(deftype ListIterable [lmdb
                       ^DBI db
                       ^Cursor cur
                       ^Rtx rtx
                       ctx]
  Iterable
  (iterator [_]
    (let [[^RangeContext kctx ^RangeContext vctx] ctx

          forward-key?       (dtlv-bool (.-forward? kctx))
          include-start-key? (dtlv-bool (.-include-start? kctx))
          include-stop-key?  (dtlv-bool (.-include-stop? kctx))
          sk                 (dtlv-val (.-start-bf kctx))
          ek                 (dtlv-val (.-stop-bf kctx))
          forward-val?       (dtlv-bool (.-forward? vctx))
          include-start-val? (dtlv-bool (.-include-start? vctx))
          include-stop-val?  (dtlv-bool (.-include-stop? vctx))
          sv                 (dtlv-val (.-start-bf vctx))
          ev                 (dtlv-val (.-stop-bf vctx))
          k                  (.key cur)
          v                  (.val cur)
          iter               (DTLV$dtlv_list_iter.)]
      (Util/checkRc
        (DTLV/dtlv_list_iter_create
          iter (.ptr cur) (.ptr k) (.ptr v)
          ^int forward-key? ^int include-start-key? ^int include-stop-key? sk ek
          ^int forward-val? ^int include-start-val? ^int include-stop-val?
          sv ev))
      (reify
        Iterator
        (hasNext [_] (dtlv-rc (DTLV/dtlv_list_iter_has_next iter)))
        (next [_] (KV. k v lmdb rtx))

        AutoCloseable
        (close [_] (DTLV/dtlv_list_iter_destroy iter))))))

(deftype ListSampleIterable [lmdb
                             ^DBI db
                             ^longs indices
                             ^long budget
                             ^long step
                             ^Cursor cur
                             ^Rtx rtx
                             ^RangeContext ctx]
  Iterable
  (iterator [_]
    (let [sk      (dtlv-val (.-start-bf ctx))
          ek      (dtlv-val (.-stop-bf ctx))
          k       (.key cur)
          v       (.val cur)
          dlmdb?  (l/dlmdb?)
          iter    (if dlmdb?
                    (DTLV$dtlv_list_rank_sample_iter.)
                    (DTLV$dtlv_list_sample_iter.))
          samples (alength indices)
          sizets  (SizeTPointer. samples)]
      (dotimes [i samples] (.put sizets i (aget indices i)))
      (Util/checkRc
        (if dlmdb?
          (DTLV/dtlv_list_rank_sample_iter_create
            ^DTLV$dtlv_list_rank_sample_iter iter
            sizets samples (.ptr cur) (.ptr k) (.ptr v) sk ek)
          (DTLV/dtlv_list_sample_iter_create
            ^DTLV$dtlv_list_sample_iter iter
            sizets samples budget step (.ptr cur) (.ptr k) (.ptr v) sk ek)))
      (reify
        Iterator
        (hasNext [_]
          (dtlv-rc
            (if dlmdb?
              (DTLV/dtlv_list_rank_sample_iter_has_next iter)
              (DTLV/dtlv_list_sample_iter_has_next iter))))
        (next [_] (KV. k v lmdb rtx))

        AutoCloseable
        (close [_]
          (if l/dlmdb?
            (DTLV/dtlv_list_rank_sample_iter_destroy iter)
            (DTLV/dtlv_list_sample_iter_destroy iter)))))))

(deftype ListRandKeyValIterable [lmdb
                                 ^DBI db
                                 ^Cursor cur
                                 ^Rtx rtx
                                 ^RangeContext ctx]
  IListRandKeyValIterable
  (val-iterator [_]
    (let [sv        (dtlv-val (.-start-bf ctx))
          ev        (dtlv-val (.-stop-bf ctx))
          ^BufVal k (.key cur)
          ^BufVal v (.val cur)
          iter      (DTLV$dtlv_list_val_iter.)]
      (Util/checkRc
        (DTLV/dtlv_list_val_iter_create iter (.ptr cur) (.ptr k) (.ptr v) sv ev))
      (reify
        IListRandKeyValIterator
        (seek-key [_ x t]
          (l/put-key rtx x t)
          (dtlv-rc (DTLV/dtlv_list_val_iter_seek iter (.ptr ^BufVal (.-kp rtx)))))
        (has-next-val [_] (dtlv-rc (DTLV/dtlv_list_val_iter_has_next iter)))
        (next-val [_] (v-bf v lmdb rtx))

        AutoCloseable
        (close [_] (DTLV/dtlv_list_val_iter_destroy iter))))))

(deftype ListKeyRangeFullValIterable [lmdb
                                      ^DBI db
                                      ^Cursor cur
                                      ^Rtx rtx
                                      ^RangeContext ctx]
  Iterable
  (iterator [_]
    (let [include-start? (dtlv-bool (.-include-start? ctx))
          include-stop?  (dtlv-bool (.-include-stop? ctx))
          sk             (dtlv-val (.-start-bf ctx))
          ek             (dtlv-val (.-stop-bf ctx))
          k              (.key cur)
          v              (.val cur)
          iter           (DTLV$dtlv_list_key_range_full_val_iter.)]
      (Util/checkRc
        (DTLV/dtlv_list_key_range_full_val_iter_create
          iter (.ptr cur) (.ptr k) (.ptr v)
          ^int include-start? ^int include-stop? sk ek))
      (reify
        Iterator
        (hasNext [_]
          (dtlv-rc (DTLV/dtlv_list_key_range_full_val_iter_has_next iter)))
        (next [_] (KV. k v lmdb rtx))

        AutoCloseable
        (close [_] (DTLV/dtlv_list_key_range_full_val_iter_destroy iter))))))

(deftype ListFullValIterable [lmdb
                              ^DBI db
                              ^Cursor cur
                              ^Rtx rtx]
  IListRandKeyValIterable
  (val-iterator [_]
    (let [^BufVal k (.key cur)
          ^BufVal v (.val cur)
          iter      (DTLV$dtlv_list_val_full_iter.)]
      (Util/checkRc
        (DTLV/dtlv_list_val_full_iter_create iter (.ptr cur) (.ptr k) (.ptr v)))
      (reify
        IListRandKeyValIterator
        (seek-key [_ x t]
          (l/put-key rtx x t)
          (dtlv-rc
            (DTLV/dtlv_list_val_full_iter_seek iter (.ptr ^BufVal (.-kp rtx)))))
        (has-next-val [_]
          (dtlv-rc (DTLV/dtlv_list_val_full_iter_has_next iter)))
        (next-val [_] (v-bf v lmdb rtx))

        AutoCloseable
        (close [_] (DTLV/dtlv_list_val_full_iter_destroy iter))))))

(defn- put-tx
  [^DBI dbi txn ^KVTxData tx]
  (case (.-op tx)
    :put      (do (.put-key dbi (.-k tx) (.-kt tx))
                  (.put-val dbi (.-v tx) (.-vt tx))
                  (if-let [f (.-flags tx)]
                    (.put dbi txn f)
                    (.put dbi txn)))
    :del      (do (.put-key dbi (.-k tx) (.-kt tx))
                  (.del dbi txn))
    :put-list (let [vs (.-v tx)]
                (.put-key dbi (.-k tx) (.-kt tx))
                (doseq [v vs]
                  (.put-val dbi v (.-vt tx))
                  (.put dbi txn)))
    :del-list (let [vs (.-v tx)]
                (.put-key dbi (.-k tx) (.-kt tx))
                (doseq [v vs]
                  (.put-val dbi v (.-vt tx))
                  (.del dbi txn false)))))

(defn- transact1*
  [txs ^DBI dbi txn kt vt]
  (let [dbi-name (.dbi-name dbi)
        validate? (.-validate-data? dbi)]
    (doseq [t txs]
      (let [tx (l/->kv-tx-data t kt vt)]
        (vld/validate-kv-tx-data tx validate?)
        (put-tx dbi txn tx)))))

(defn- transact*
  [txs ^HashMap dbis txn]
  (doseq [t txs]
    (let [^KVTxData tx (l/->kv-tx-data t)
          dbi-name     (.-dbi-name tx)
          ^DBI dbi     (or (.get dbis dbi-name)
                           (raise dbi-name " is not open" {}))
          validate?    (.-validate-data? dbi)]
      (vld/validate-kv-tx-data tx validate?)
      (put-tx dbi txn tx))))

(defn- wal-system-dbi?
  [dbi-name]
  (= dbi-name c/kv-wal))

(defn- dbi-dupsort?
  [^HashMap dbis dbi-name]
  (when dbi-name
    (let [^DBI dbi (.get dbis dbi-name)]
      (boolean (and dbi (.-dupsort? dbi))))))

(defn- normalize-kv-type
  [t]
  (or t :data))

(def ^:private ^ThreadLocal tl-overlay-encode-buf
  (ThreadLocal/withInitial
    (reify java.util.function.Supplier
      (get [_] (ByteBuffer/allocate 256)))))

(defn- encode-kv-bytes
  [x t]
  (let [t (normalize-kv-type t)]
    (loop [^ByteBuffer buf (.get tl-overlay-encode-buf)]
      (let [result (try
                     (b/put-bf buf x t)
                     (b/get-bytes (.duplicate buf))
                     (catch BufferOverflowException _
                       ::overflow))]
        (if (not= result ::overflow)
          result
          (let [new-buf (ByteBuffer/allocate (* 2 (.capacity buf)))]
            (.set tl-overlay-encode-buf new-buf)
            (recur new-buf)))))))

(defn- canonical-kv-op
  ([^KVTxData tx fallback-dbi ^HashMap dbis]
   (canonical-kv-op tx fallback-dbi dbis nil))
  ([^KVTxData tx fallback-dbi ^HashMap dbis cached-dbi-bytes]
   (let [dbi-name (or (.-dbi-name tx) fallback-dbi)
         kt       (normalize-kv-type (.-kt tx))]
     {:op        (.-op tx)
      :dbi       dbi-name
      :k         (.-k tx)
      :v         (.-v tx)
      :kt        kt
      :vt        (.-vt tx)
      :flags     (.-flags tx)
      :dupsort?  (dbi-dupsort? dbis dbi-name)
      :k-bytes   (encode-kv-bytes (.-k tx) kt)
      :dbi-bytes (or cached-dbi-bytes
                     (.getBytes ^String dbi-name StandardCharsets/UTF_8))})))

(defn- validate-kv-txs!
  [txs dbi-name kt vt ^HashMap dbis]
  (if dbi-name
    (let [^DBI dbi (or (.get dbis dbi-name)
                       (raise dbi-name " is not open" {}))
          validate? (.-validate-data? dbi)
          tx-data   (mapv #(l/->kv-tx-data % kt vt) txs)]
      (doseq [tx tx-data]
        (vld/validate-kv-tx-data tx validate?))
      tx-data)
    (let [tx-data (mapv l/->kv-tx-data txs)]
      (doseq [^KVTxData tx tx-data]
        (let [dbi-name (.-dbi-name tx)
              ^DBI dbi (or (.get dbis dbi-name)
                           (raise dbi-name " is not open" {}))
              validate? (.-validate-data? dbi)]
          (vld/validate-kv-tx-data tx validate?)))
      tx-data)))

(defn- compare-bytes
  ^long [^bytes a ^bytes b]
  (Arrays/compareUnsigned a b))

(defn- empty-overlay-keys-map
  ^ConcurrentSkipListMap []
  (ConcurrentSkipListMap.
    (reify java.util.Comparator
      (compare [_ a b] (compare-bytes a b)))))

(defn- empty-overlay-committed-map
  ^ConcurrentSkipListMap []
  (ConcurrentSkipListMap.))

(def ^:private overlay-deleted ::overlay-deleted)
(def ^:private overlay-present (Object.))

(defn- list-overlay-entry
  [k]
  {:k    k
   :ops  []})

(defn- list-overlay-entry?
  [entry]
  (map? entry))

(defn- list-overlay-op-entry
  [k op vs vt]
  (let [values (cond
                 (nil? vs) nil
                 (sequential? vs) (vec vs)
                 :else [vs])]
    {:k    k
     :ops  [[op values (when vt (normalize-kv-type vt))]]}))

(defn- merge-list-overlay-entry
  [existing incoming]
  (let [base (if (list-overlay-entry? existing)
               existing
               (list-overlay-entry (:k incoming)))]
    (assoc base :ops (into (or (:ops base) []) (:ops incoming)))))

(defn- merge-overlay-entry
  [existing incoming]
  (if (list-overlay-entry? incoming)
    (merge-list-overlay-entry existing incoming)
    incoming))

(defn- range-backward?
  [range-type]
  (contains? #{:all-back :at-most-back :at-least-back :closed-back
               :closed-open-back :less-than-back :greater-than-back
               :open-back :open-closed-back}
             range-type))

(defn- key-in-k-range?
  [^bytes kbs range-type ^bytes low ^bytes high]
  (let [c-low  (when low (compare-bytes kbs low))
        c-high (when high (compare-bytes kbs high))]
    (case range-type
      (:all :all-back) true
      (:at-least :at-least-back) (>= c-low 0)
      (:at-most :at-most-back) (<= c-low 0)
      (:closed :closed-back) (and (>= c-low 0) (<= c-high 0))
      (:closed-open :closed-open-back) (and (>= c-low 0) (< c-high 0))
      (:greater-than :greater-than-back) (> c-low 0)
      (:less-than :less-than-back) (< c-low 0)
      (:open :open-back) (and (> c-low 0) (< c-high 0))
      (:open-closed :open-closed-back) (and (> c-low 0) (<= c-high 0))
      false)))

(defn- wal-op->overlay-entry
  [{:keys [op dbi k v kt vt dupsort? k-bytes]}]
  (when (and dbi (not (wal-system-dbi? dbi)))
    (let [kbs (or k-bytes (encode-kv-bytes k (normalize-kv-type kt)))]
      (case op
        :put (if dupsort?
               [dbi kbs (list-overlay-op-entry k :put [v] vt)]
               [dbi kbs [k v]])
        :del (if dupsort?
               [dbi kbs (list-overlay-op-entry k :wipe nil nil)]
               [dbi kbs overlay-deleted])
        :put-list [dbi kbs (list-overlay-op-entry k :put v vt)]
        :del-list [dbi kbs (list-overlay-op-entry k :del v vt)]
        nil))))

(defn- overlay-delta-by-dbi
  [ops]
  (reduce (fn [m op]
            (if-let [[dbi kbs entry] (wal-op->overlay-entry op)]
              (let [^ConcurrentSkipListMap dbi-delta (or (get m dbi)
                                           (empty-overlay-keys-map))]
                (.put dbi-delta kbs
                      (merge-overlay-entry (.get dbi-delta kbs) entry))
                (if (contains? m dbi)
                  m
                  (assoc m dbi dbi-delta)))
              m))
          {}
          ops))

(defn- merge-overlay-delta
  "Create a new overlay by copying existing maps and merging delta."
  [overlay delta]
  (reduce-kv (fn [m dbi dbi-delta]
               (let [^ConcurrentSkipListMap merged
                     (if-let [^ConcurrentSkipListMap dbi-overlay (get m dbi)]
                       (ConcurrentSkipListMap. dbi-overlay)
                       (empty-overlay-keys-map))]
                 (doseq [[k entry] dbi-delta]
                   (.put merged k (merge-overlay-entry (.get merged k) entry)))
                 (assoc m dbi merged)))
             (or overlay {})
             delta))

(defn- merge-overlay-delta!
  "Merge delta into overlay in-place (mutates existing ConcurrentSkipListMaps).
   Only creates new maps for previously-unseen DBIs."
  [overlay delta]
  (reduce-kv (fn [m dbi dbi-delta]
               (let [existing? (contains? m dbi)
                     ^ConcurrentSkipListMap merged
                     (or (get m dbi) (empty-overlay-keys-map))]
                 (doseq [[k entry] dbi-delta]
                   (.put merged k (merge-overlay-entry (.get merged k) entry)))
                 (if existing? m (assoc m dbi merged))))
             (or overlay {})
             delta))

(defn- rebuild-overlay-by-dbi
  [committed-by-tx]
  (reduce (fn [m [_ delta]]
            (merge-overlay-delta m delta))
          {}
          committed-by-tx))

(defn- ensure-wal-channel!
  "Return a cached FileChannel for the given segment, opening a new one if
  needed (or if the segment rotated). Closes the old channel on rotation."
  [lmdb dir seg-id]
  (let [info @(.-info lmdb)
        ^java.nio.channels.FileChannel ch (:wal-channel info)
        ch-seg (long (or (:wal-channel-segment-id info) -1))]
    (if (and ch (.isOpen ch) (= ch-seg (long seg-id)))
      ch
      (do
        (wal/close-segment-channel! ch)
        (let [new-ch (wal/open-segment-channel dir seg-id)]
          (vswap! (.-info lmdb) assoc
                  :wal-channel new-ch
                  :wal-channel-segment-id seg-id)
          new-ch)))))

(defn- append-kv-wal-record!
  [lmdb ops tx-time]
  (when (seq ops)
    (let [info          @(.-info lmdb)
          wal-id        (inc (long (or (:wal-next-tx-id info) 0)))
          user-tx-id    wal-id
          record        (wal/record-bytes wal-id user-tx-id tx-time ops)
          record-bytes  (alength ^bytes record)
          now-ms        (long (or tx-time (System/currentTimeMillis)))
          max-bytes     (long (or (:wal-segment-max-bytes info)
                                  c/*wal-segment-max-bytes*))
          max-ms        (long (or (:wal-segment-max-ms info)
                                  c/*wal-segment-max-ms*))
          seg-id        (long (or (:wal-segment-id info) 1))
          seg-bytes     (long (or (:wal-segment-bytes info) 0))
          seg-start-ms  (long (or (:wal-segment-created-ms info) now-ms))
          rotate?       (or (>= (+ seg-bytes record-bytes) max-bytes)
                             (>= (- now-ms seg-start-ms) max-ms))
          seg-id        (if rotate?
                          (inc seg-id)
                          seg-id)
          seg-start-ms  (if rotate? now-ms seg-start-ms)
          seg-bytes     (if rotate? 0 seg-bytes)
          sync-mode     (or (:wal-sync-mode info) c/*wal-sync-mode*)
          group-size    (long (or (:wal-group-commit info) c/*wal-group-commit*))
          unsynced      (inc (long (or (:wal-unsynced-count info) 0)))
          sync?         (or rotate? (>= unsynced group-size))
          dir           (env-dir lmdb)
          ch            (ensure-wal-channel! lmdb dir seg-id)]
      (.write ch (ByteBuffer/wrap record))
      (when sync? (wal/sync-channel! ch sync-mode))
      (vswap! (.-info lmdb) assoc
              :wal-next-tx-id wal-id
              :last-committed-wal-tx-id wal-id
              :last-committed-user-tx-id wal-id
              :committed-last-modified-ms now-ms
              :wal-segment-id seg-id
              :wal-segment-created-ms seg-start-ms
              :wal-segment-bytes (+ seg-bytes record-bytes)
              :wal-unsynced-count (if sync? 0 unsynced))
      {:wal-id wal-id
       :tx-time now-ms
       :ops ops})))

(defn- get-kv-info-id
  [lmdb k]
  (i/get-value lmdb c/kv-info k :attr :id))

(defn- refresh-kv-wal-info!
  [lmdb]
  (try
    (let [dir          (env-dir lmdb)
          now-ms       (System/currentTimeMillis)
          meta         (wal/read-wal-meta dir)
          scanned      (when-not meta (wal/scan-last-wal dir))
          committed-id (long (or (get meta c/last-committed-wal-tx-id)
                                 (:last-wal-id scanned)
                                 0))
          indexed-id   (long (or (get-kv-info-id lmdb c/last-indexed-wal-tx-id)
                                 (get meta c/last-indexed-wal-tx-id)
                                 0))
          committed-ms (long (or (get meta c/committed-last-modified-ms) 0))
          user-id      (long (or (get meta c/last-committed-user-tx-id)
                                 committed-id))
          next-id      committed-id
          applied-id   (long (or (get-kv-info-id lmdb c/applied-wal-tx-id)
                                 (get-kv-info-id lmdb c/legacy-applied-tx-id)
                                 0))
          seg-id       (long (or (get meta :wal/last-segment-id)
                                 (:last-segment-id scanned)
                                 1))
          seg-file     (io/file (wal/segment-path dir seg-id))
          seg-bytes    (if (.exists seg-file) (.length seg-file) 0)
          seg-created  (long (or (:last-segment-ms scanned)
                                 (when (.exists seg-file) (.lastModified seg-file))
                                 now-ms))]
      (vswap! (.-info lmdb) assoc
              :wal-next-tx-id next-id
              :applied-wal-tx-id applied-id
              :last-committed-wal-tx-id committed-id
              :last-indexed-wal-tx-id indexed-id
              :last-committed-user-tx-id user-id
              :committed-last-modified-ms committed-ms
              :overlay-published-wal-tx-id committed-id
              :wal-segment-id seg-id
              :wal-segment-created-ms seg-created
              :wal-segment-bytes seg-bytes))
    (catch Exception _ nil)))

(defn- read-kv-wal-meta
  [lmdb]
  (wal/read-wal-meta (env-dir lmdb)))

(defn- refresh-kv-wal-meta-info!
  [lmdb]
  (let [now-ms (System/currentTimeMillis)]
    (if-let [meta (read-kv-wal-meta lmdb)]
      (let [last-ms (long (or (get meta c/committed-last-modified-ms) 0))]
        (vswap! (.-info lmdb) assoc
                :wal-meta-revision
                (or (get meta c/wal-meta-revision) 0)
                :wal-meta-last-modified-ms last-ms
                :wal-meta-last-flush-ms (if (pos? last-ms) last-ms now-ms)
                :wal-meta-pending-txs 0))
      (vswap! (.-info lmdb) assoc
              :wal-meta-revision 0
              :wal-meta-last-modified-ms 0
              :wal-meta-last-flush-ms now-ms
              :wal-meta-pending-txs 0))))

(defn- publish-kv-wal-watermarks!
  [lmdb wal-id tx-time]
  (vswap! (.-info lmdb) assoc
          :wal-next-tx-id wal-id
          :last-committed-wal-tx-id wal-id
          :last-committed-user-tx-id wal-id
          :committed-last-modified-ms tx-time))

(defn- publish-kv-committed-overlay!
  [lmdb wal-id ops]
  (let [delta (overlay-delta-by-dbi ops)]
    (when (seq delta)
      (let [info       @(.-info lmdb)
            ^ConcurrentSkipListMap committed-by-tx
            (or (:kv-overlay-committed-by-tx info)
                (empty-overlay-committed-map))
            by-dbi     (merge-overlay-delta!
                         (:kv-overlay-by-dbi info) delta)]
        (.put committed-by-tx wal-id delta)
        (vswap! (.-info lmdb) assoc
                :kv-overlay-committed-by-tx committed-by-tx
                :kv-overlay-by-dbi by-dbi)))))

(defn- publish-kv-private-overlay!
  [^Rtx wtxn ops]
  (let [delta (overlay-delta-by-dbi ops)]
    (when (seq delta)
      (let [current (or (some-> (.-kv-overlay-private wtxn) deref) {})
            merged  (merge-overlay-delta current delta)]
        (vreset! (.-kv-overlay-private wtxn) merged)))))

(defn- publish-kv-overlay-watermark!
  [lmdb wal-id]
  (vswap! (.-info lmdb) assoc :overlay-published-wal-tx-id wal-id))

(defn- prune-kv-committed-overlay!
  [lmdb upto-wal-id]
  (let [upto (long (or upto-wal-id 0))]
    (when (pos? upto)
      (vswap! (.-info lmdb)
              (fn [m]
                (let [^ConcurrentSkipListMap committed-by-tx
                      (or (:kv-overlay-committed-by-tx m)
                          (empty-overlay-committed-map))
                      ^ConcurrentSkipListMap remaining
                      (empty-overlay-committed-map)]
                  (doseq [[wal-id delta] committed-by-tx]
                    (when (> (long wal-id) upto)
                      (.put remaining wal-id delta)))
                  (assoc m
                         :kv-overlay-committed-by-tx remaining
                         :kv-overlay-by-dbi
                         (rebuild-overlay-by-dbi remaining))))))))

(def ^:private overlay-miss ::overlay-miss)
(def ^:private overlay-tombstone ::overlay-tombstone)

(defn- private-overlay-by-dbi
  [lmdb]
  (when (and (:kv-wal? @(.-info lmdb)) (l/writing? lmdb))
    (when-let [^Rtx wtxn @(.-write-txn lmdb)]
      (when-let [v (.-kv-overlay-private wtxn)]
        @v))))

(defn- overlay-by-dbi
  [lmdb dbi-name]
  (let [committed (get-in @(.-info lmdb) [:kv-overlay-by-dbi dbi-name])
        priv      (get-in (private-overlay-by-dbi lmdb) [dbi-name])]
    (cond
      (and (seq committed) (seq priv))
      (get (merge-overlay-delta {dbi-name committed} {dbi-name priv}) dbi-name)
      (seq priv) priv
      (seq committed) committed
      :else nil)))

(defn- kv-overlay-eligible?
  [lmdb dbi-name]
  (and (:kv-wal? @(.-info lmdb))
       (not (wal-system-dbi? dbi-name))))

(defn- ordered-map-seq
  [^ConcurrentSkipListMap m desc?]
  (seq (if desc? (.descendingMap m) m)))

(defn- group-base-kvs-by-key
  [base-kvs k-type]
  (let [^ConcurrentSkipListMap grouped (empty-overlay-keys-map)]
    (doseq [[k _ :as kv] base-kvs]
      (let [kbs (encode-kv-bytes k k-type)
            acc (or (.get grouped kbs) [])]
        (.put grouped kbs (conj acc kv))))
    grouped))

(defn- decode-kv-bytes
  [^bytes bs t]
  (b/read-buffer (ByteBuffer/wrap bs) t))

(defn- materialize-list-overlay-values
  [entry base-kvs base-v-type]
  (let [^ConcurrentSkipListMap vals (empty-overlay-keys-map)]
    (doseq [[_ v] base-kvs]
      (when (some? v)
        (.put vals (encode-kv-bytes v base-v-type) overlay-present)))
    (doseq [[op vs vt] (:ops entry)]
      (let [t (normalize-kv-type (or vt base-v-type))]
        (case op
          :wipe
          (.clear vals)

          :put
          (doseq [v (or vs [])]
            (.put vals (encode-kv-bytes v t) overlay-present))

          :del
          (doseq [v (or vs [])]
            (.remove vals (encode-kv-bytes v t)))

          nil)))
    vals))

(defn- get-overlay-entry
  [lmdb dbi-name k k-type]
  (let [kbs (encode-kv-bytes k (normalize-kv-type k-type))
        priv (get-in (private-overlay-by-dbi lmdb) [dbi-name])
        entry (when priv (.get ^ConcurrentSkipListMap priv kbs))]
    (if (some? entry)
      entry
      (let [overlay (get-in @(.-info lmdb) [:kv-overlay-by-dbi dbi-name])]
        (when overlay (.get ^ConcurrentSkipListMap overlay kbs))))))

(defn- overlay-get-value
  [lmdb dbi-name k k-type v-type ignore-key?]
  (if-let [entry (get-overlay-entry lmdb dbi-name k k-type)]
    (cond
      (= entry overlay-deleted)
      overlay-tombstone

      (list-overlay-entry? entry)
      (let [kt          (normalize-kv-type k-type)
            base-v-type (if (= v-type :ignore) :data v-type)
            base-kvs    (mapv (fn [v0] [k v0])
                              (or (scan/get-list lmdb dbi-name k kt base-v-type)
                                  []))
            ^ConcurrentSkipListMap vals (materialize-list-overlay-values
                            entry base-kvs base-v-type)
            first-ent  (.firstEntry vals)]
        (if first-ent
          (let [v0 (if (= v-type :ignore)
                     nil
                     (decode-kv-bytes (.getKey ^java.util.Map$Entry first-ent)
                                      base-v-type))]
            (if ignore-key?
              v0
              [(b/expected-return k k-type) v0]))
          overlay-tombstone))

      :else
      (let [[_ v] entry]
        (if ignore-key?
          v
          [(b/expected-return k k-type) v])))
    overlay-miss))

(defn- overlay-entry-key
  [entry k-type key-bs]
  (cond
    key-bs
    (decode-kv-bytes key-bs k-type)

    (list-overlay-entry? entry)
    (b/expected-return (:k entry) k-type)

    :else
    (b/expected-return (first entry) k-type)))

(defn- overlay-entry-item
  [entry key-bs k-type v-type]
  [(overlay-entry-key entry k-type key-bs)
   (if (= v-type :ignore)
     nil
     (second entry))])

(defn- overlay-entry-items
  [entry base-kvs key-bs k-type v-type base-v-type]
  (cond
    (= entry overlay-deleted)
    []

    (list-overlay-entry? entry)
    (let [k      (overlay-entry-key entry k-type key-bs)
          ^ConcurrentSkipListMap vals (materialize-list-overlay-values
                          entry base-kvs base-v-type)]
      (if (= v-type :ignore)
        (let [out (transient [])]
          (doseq [_ (.entrySet vals)]
            (conj! out [k nil]))
          (persistent! out))
        (let [out (transient [])]
          (doseq [^java.util.Map$Entry e (.entrySet vals)]
            (conj! out [k (decode-kv-bytes (.getKey e) base-v-type)]))
          (persistent! out))))

    :else
    [(overlay-entry-item entry key-bs k-type v-type)]))

(defn- overlay-get-range
  [lmdb dbi-name [range-type k1 k2 :as k-range] k-type v-type ignore-key?]
  (let [^ConcurrentSkipListMap overlay (overlay-by-dbi lmdb dbi-name)]
    (if-not (seq overlay)
      overlay-miss
      (let [k-type       (normalize-kv-type k-type)
            lower-bs     (when (some? k1) (encode-kv-bytes k1 k-type))
            upper-bs     (when (some? k2) (encode-kv-bytes k2 k-type))
            base-v-type  (if (and (i/list-dbi? lmdb dbi-name)
                                  (= v-type :ignore))
                           :data
                           v-type)
            base-kvs     (scan/get-range lmdb dbi-name k-range k-type
                                         base-v-type false)
            base-by-key  (group-base-kvs-by-key base-kvs k-type)
            desc?        (range-backward? range-type)
            overlay-seq  (->> (ordered-map-seq overlay desc?)
                              (filter (fn [[kbs _]]
                                        (key-in-k-range? kbs range-type
                                                         lower-bs upper-bs))))
            base-seq     (ordered-map-seq base-by-key desc?)
            merged       (loop [os  (seq overlay-seq)
                                bs  (seq base-seq)
                                out (transient [])]
                           (cond
                             (and (nil? os) (nil? bs))
                             (persistent! out)

                             (nil? os)
                             (let [[_ bgroup] (first bs)]
                               (recur nil (next bs) (reduce conj! out bgroup)))

                             (nil? bs)
                            (let [[okbs entry] (first os)
                                  items      (overlay-entry-items entry [] okbs k-type
                                                                 v-type
                                                                 base-v-type)]
                              (recur (next os) nil (reduce conj! out items)))

                             :else
                             (let [[okbs entry] (first os)
                                   [bkbs bgroup] (first bs)
                                   cmp            (if desc?
                                                    (compare-bytes bkbs okbs)
                                                    (compare-bytes okbs bkbs))]
                               (cond
                                 (zero? cmp)
                                 (let [items (overlay-entry-items entry bgroup
                                                                  okbs
                                                                  k-type
                                                                  v-type
                                                                  base-v-type)]
                                   (recur (next os) (next bs)
                                          (reduce conj! out items)))

                                 (neg? cmp)
                                 (let [items (overlay-entry-items entry []
                                                                  okbs
                                                                  k-type
                                                                  v-type
                                                                  base-v-type)]
                                   (recur (next os) bs (reduce conj! out items)))

                                 :else
                                 (recur os (next bs)
                                        (reduce conj! out bgroup))))))]
        (if ignore-key?
          (mapv second merged)
          merged)))))

(defn- kv-overlay-active?
  [lmdb dbi-name]
  (and (kv-overlay-eligible? lmdb dbi-name)
       (or (seq (get-in @(.-info lmdb) [:kv-overlay-by-dbi dbi-name]))
           (seq (get-in (private-overlay-by-dbi lmdb) [dbi-name])))))

(defn- overlay-key-seq
  [kvs k-type list-dbi?]
  (if-not list-dbi?
    (mapv first kvs)
    (loop [xs   (seq kvs)
           prev nil
           out  (transient [])]
      (if-let [[k _] (first xs)]
        (let [kbs (encode-kv-bytes k k-type)]
          (if (and prev (zero? (compare-bytes prev kbs)))
            (recur (next xs) prev out)
            (recur (next xs) kbs (conj! out k))))
        (persistent! out)))))

(defn- overlay-key-count
  [kvs k-type list-dbi? cap]
  (if-not list-dbi?
    (let [cnt (count kvs)]
      (if cap
        (min (long cap) (long cnt))
        (long cnt)))
    (loop [xs   (seq kvs)
           prev nil
           cnt  0]
      (if (or (nil? xs) (and cap (>= cnt (long cap))))
        cnt
        (let [[k _] (first xs)
              kbs   (encode-kv-bytes k k-type)]
          (if (and prev (zero? (compare-bytes prev kbs)))
            (recur (next xs) prev cnt)
            (recur (next xs) kbs (unchecked-inc cnt))))))))

(defn- overlay-pred-v-type
  [v-type raw-pred?]
  (if (and raw-pred? (= v-type :ignore))
    :data
    v-type))

(defn- overlay-out-value
  [v v-type]
  (if (= v-type :ignore) nil v))

(defn- overlay-raw-key
  ^ByteBuffer [k k-type]
  (ByteBuffer/wrap (encode-kv-bytes k k-type)))

(defn- overlay-raw-kv
  [k v k-type v-type]
  (let [^ByteBuffer kb (ByteBuffer/wrap (encode-kv-bytes k k-type))
        ^ByteBuffer vb (when (some? v)
                         (ByteBuffer/wrap (encode-kv-bytes v v-type)))]
    (reify IKV
      (k [_] (.duplicate kb))
      (v [_] (when vb (.duplicate vb))))))

(defn- overlay-list-append-group
  [out group desc?]
  (if (seq group)
    (if desc?
      (reduce conj! out (rseq group))
      (reduce conj! out group))
    out))

(defn- overlay-list-range
  [lmdb dbi-name k-range k-type [v-range-type v1 v2] v-type]
  (let [k-type    (normalize-kv-type k-type)
        v-type    (normalize-kv-type v-type)
        lower-bs  (when (some? v1) (encode-kv-bytes v1 v-type))
        upper-bs  (when (some? v2) (encode-kv-bytes v2 v-type))
        desc?     (range-backward? v-range-type)
        kvs       (.get-range lmdb dbi-name k-range k-type v-type false)]
    (loop [xs       (seq kvs)
           prev-kbs nil
           group    []
           out      (transient [])]
      (if-let [[k v :as kv] (first xs)]
        (let [kbs      (encode-kv-bytes k k-type)
              same-key (and prev-kbs (zero? (compare-bytes prev-kbs kbs)))
              keep?    (key-in-k-range? (encode-kv-bytes v v-type)
                                        v-range-type lower-bs upper-bs)]
          (if same-key
            (recur (next xs) prev-kbs (if keep? (conj group kv) group) out)
            (recur (next xs) kbs (if keep? [kv] [])
                   (overlay-list-append-group out group desc?))))
        (persistent! (overlay-list-append-group out group desc?))))))

(defn- overlay-list-values
  [lmdb dbi-name k kt vt]
  (when k
    (mapv second (overlay-list-range
                   lmdb dbi-name [:closed k k] kt [:all] vt))))

(defn- overlay-list-near-val-buf
  [lmdb dbi-name k v kt vt]
  (when (and k v)
    (when-let [[_ v0] (first (overlay-list-range
                               lmdb dbi-name [:closed k k] kt [:at-least v]
                               vt))]
      (ByteBuffer/wrap (encode-kv-bytes v0 vt)))))

(defn- overlay-range-seq
  [lmdb dbi-name k-range k-type v-type ignore-key? opts]
  (assert (not (and (= v-type :ignore) ignore-key?))
          "Cannot ignore both key and value")
  (let [batch-size (long (max 1 (or (:batch-size opts) 100)))
        read-vt    (if (= v-type :ignore) :raw v-type)
        kvs        (.get-range lmdb dbi-name k-range k-type read-vt false)
        item       (fn [[k v]]
                     (let [v' (overlay-out-value v v-type)]
                       (if ignore-key?
                         v'
                         [k v'])))
        batches    (->> kvs
                        (map item)
                        (partition-all batch-size)
                        (mapv vec))]
    (reify
      Seqable
      (seq [_] (seq batches))

      IReduceInit
      (reduce [_ rf init] (reduce rf init batches))

      AutoCloseable
      (close [_] nil)

      Object
      (toString [_] (str (apply list batches))))))

(defn- overlay-rank
  [lmdb dbi-name k k-type]
  (let [k-type (normalize-kv-type k-type)
        target (encode-kv-bytes k k-type)
        ks     (.key-range lmdb dbi-name [:all] k-type)]
    (loop [xs (seq ks)
           i  0]
      (when-let [k0 (first xs)]
        (let [cmp (compare-bytes (encode-kv-bytes k0 k-type) target)]
          (cond
            (zero? cmp) i
            (pos? cmp)  nil
            :else       (recur (next xs) (unchecked-inc i))))))))

(defn- overlay-get-by-rank
  [lmdb dbi-name rank k-type v-type ignore-key?]
  (when-not (neg? (long rank))
    (let [k-type (normalize-kv-type k-type)
          ks     (.key-range lmdb dbi-name [:all] k-type)]
      (loop [xs (seq ks)
             i  0]
        (when-let [k (first xs)]
          (if (= i (long rank))
            (.get-value lmdb dbi-name k k-type v-type ignore-key?)
            (recur (next xs) (unchecked-inc i))))))))

(defn- overlay-sample-kv
  [lmdb dbi-name n k-type v-type ignore-key?]
  (let [k-type      (normalize-kv-type k-type)
        sample-vt   (if (= v-type :ignore) :raw v-type)
        kvs         (vec (.get-range lmdb dbi-name [:all] k-type sample-vt false))
        total       (long (count kvs))
        indices     (u/reservoir-sampling total (long n))
        out-value   (fn [v] (overlay-out-value v v-type))
        format-item (fn [[k v]]
                      (let [v' (out-value v)]
                        (if ignore-key? v' [k v'])))]
    (when indices
      (loop [xs  (seq indices)
             out (transient [])]
        (if-let [idx0 (first xs)]
          (let [idx (long idx0)]
            (if (or (neg? idx) (>= idx total))
              (recur (next xs) out)
              (recur (next xs) (conj! out (format-item (nth kvs idx))))))
          (persistent! out))))))

(defn- overlay-visit-key-sample
  [lmdb dbi-name indices visitor k-range k-type raw-pred?]
  (when indices
    (let [k-type (normalize-kv-type k-type)
          ks     (vec (.key-range lmdb dbi-name k-range k-type))
          total  (long (count ks))]
      (loop [xs (seq indices)]
        (when-let [idx0 (first xs)]
          (let [idx (long idx0)]
            (if (or (neg? idx) (>= idx total))
              (recur (next xs))
              (let [k   (nth ks idx)
                    res (if raw-pred?
                          (visitor (overlay-raw-key k k-type))
                          (visitor k))]
                (when-not (identical? res :datalevin/terminate-visit)
                  (recur (next xs)))))))))))

(defn- overlay-visit-list-sample
  [lmdb dbi-name indices visitor k-range k-type v-type raw-pred?]
  (when indices
    (let [k-type (normalize-kv-type k-type)
          v-type (normalize-kv-type v-type)
          kvs    (vec (overlay-list-range lmdb dbi-name k-range
                                          k-type [:all] v-type))
          total  (long (count kvs))]
      (loop [xs (seq indices)]
        (when-let [idx0 (first xs)]
          (let [idx (long idx0)]
            (if (or (neg? idx) (>= idx total))
              (recur (next xs))
              (let [[k v] (nth kvs idx)
                    res   (if raw-pred?
                            (visitor (overlay-raw-kv k v k-type v-type))
                            (visitor k v))]
                (when-not (identical? res :datalevin/terminate-visit)
                  (recur (next xs)))))))))))

(defn- overlay-list-val-iterable
  [lmdb dbi-name v-range vt]
  (let [vt          (normalize-kv-type vt)
        kvs         (overlay-list-range lmdb dbi-name [:all] :raw v-range vt)
        ^ConcurrentSkipListMap m  (empty-overlay-keys-map)]
    (doseq [[kbs v] kvs]
      (let [acc (or (.get m kbs) [])]
        (.put m kbs (conj acc v))))
    (reify IListRandKeyValIterable
      (val-iterator [_]
        (let [vals (volatile! nil)
              idx  (volatile! 0)]
          (reify
            IListRandKeyValIterator
            (seek-key [_ k-value k-type]
              (let [target (encode-kv-bytes k-value (normalize-kv-type k-type))
                    found  (.get m target)]
                (vreset! vals found)
                (vreset! idx 0)
                (boolean (seq found))))
            (has-next-val [_]
              (let [vs @vals
                    i  (long @idx)]
                (boolean (and vs (< i (long (count vs)))))))
            (next-val [_]
              (let [vs @vals
                    i  (long @idx)]
                (when-let [v (when (and vs (< i (long (count vs))))
                               (nth vs i))]
                  (vreset! idx (unchecked-inc i))
                  (ByteBuffer/wrap (encode-kv-bytes v vt)))))
            AutoCloseable
            (close [_] nil)))))))

(defn- publish-kv-wal-meta!
  [lmdb wal-id now-ms]
  (let [info     @(.-info lmdb)
        seg-id   (long (or (:wal-segment-id info) 0))
        indexed  (long (or (:last-indexed-wal-tx-id info) 0))
        user-id  (long (or (:last-committed-user-tx-id info) wal-id))
        commit-ms (long (or (:committed-last-modified-ms info) now-ms))
        snapshot {c/last-committed-wal-tx-id   wal-id
                  c/last-indexed-wal-tx-id     indexed
                  c/last-committed-user-tx-id  user-id
                  c/committed-last-modified-ms commit-ms
                  :wal/last-segment-id         seg-id
                  :wal/enabled?                true}
        meta'    (wal/publish-wal-meta! (env-dir lmdb) snapshot)
        rev      (long (or (:wal-meta-revision meta') 0))]
    (vswap! (.-info lmdb) assoc
            :wal-meta-revision rev
            :wal-meta-last-modified-ms now-ms
            :wal-meta-last-flush-ms now-ms
            :wal-meta-pending-txs 0)
    meta'))

(defn- maybe-publish-kv-wal-meta!
  [lmdb wal-id]
  (let [now-ms      (System/currentTimeMillis)
        info'       (vswap! (.-info lmdb) update :wal-meta-pending-txs
                            (fnil u/long-inc 0))
        pending     (long (or (:wal-meta-pending-txs info') 0))
        flush-txs   (max 1 (long (or (:wal-meta-flush-max-txs info')
                                     c/*wal-meta-flush-max-txs*)))
        flush-ms    (max 0 (long (or (:wal-meta-flush-max-ms info')
                                     c/*wal-meta-flush-max-ms*)))
        last-flush  (long (or (:wal-meta-last-flush-ms info') 0))
        due?        (or (>= pending flush-txs)
                        (>= (- now-ms last-flush) flush-ms))]
    (when due?
      (publish-kv-wal-meta! lmdb wal-id now-ms))))

(defn- run-writer-step!
  [step f]
  (vld/check-failpoint step :before)
  (vld/check-failpoint step :during)
  (let [ret (f)]
    (vld/check-failpoint step :after)
    ret))

(defn- list-count*
  [^Rtx rtx ^Cursor cur k kt]
  (.put-key rtx k kt)
  (dtlv-c (DTLV/dtlv_list_val_count
            (.ptr cur) (.ptr ^BufVal (.-kp rtx)) (.ptr ^BufVal (.-vp rtx)))))

(defn- in-list?*
  [^Rtx rtx ^Cursor cur k kt v vt]
  (l/list-range-info rtx :at-least k nil kt :at-least v nil vt)
  (.get cur ^BufVal (.-start-kp rtx) ^BufVal (.-start-vp rtx)
        DTLV/MDB_GET_BOTH))

(defn- near-list*
  [^Rtx rtx ^Cursor cur k kt v vt]
  (l/list-range-info rtx :at-least k nil kt :at-least v nil vt)
  (when (.get cur ^BufVal (.-start-kp rtx) ^BufVal (.-start-vp rtx)
              DTLV/MDB_GET_BOTH_RANGE)
    (.outBuf (.val cur))))

(declare ->CppLMDB)

(defn- up-db-size [^Env env]
  (let [^Info info (Info/create env)]
    (.setMapSize env (* ^long c/+buffer-grow-factor+
                        (.me_mapsize ^DTLV$MDB_envinfo (.get info))))
    (.close info)))

(defn- sync-key* [dir] (->> dir hash (str "lmdb-sync-") keyword))

(def sync-key (memoize sync-key*))

(deftype AsyncSync [dir ^Env env]
  IAsyncWork
  (work-key [_] (sync-key dir))
  (do-work [_] (.sync env 1))
  (combine [_] first)
  (callback [_] nil))

(defn- start-scheduled-sync
  [scheduled-sync dir ^Env env]
  (let [scheduler ^ScheduledExecutorService (u/get-scheduler)
        fut       (.scheduleWithFixedDelay
                    scheduler
                    ^Runnable #(let [exe (a/get-executor)]
                                 (when (a/running? exe)
                                   (a/exec exe (AsyncSync. dir env))))
                    ^long (rand-int c/lmdb-sync-interval)
                    ^long c/lmdb-sync-interval
                    TimeUnit/SECONDS)]
    (vreset! scheduled-sync fut)))

(defn- stop-scheduled-sync
  [scheduled-sync]
  (when-let [fut @scheduled-sync]
    (.cancel ^ScheduledFuture fut true)
    (vreset! scheduled-sync nil)))

(defn- copy-version-file
  [lmdb dest]
  (let [src (str (env-dir lmdb) u/+separator+ c/version-file-name)
        dst (str dest u/+separator+ c/version-file-name)]
    (u/copy-file src dst)))

(defn- copy-keycode-file
  [lmdb dest]
  (let [src (str (env-dir lmdb) u/+separator+ c/keycode-file-name)
        dst (str dest u/+separator+ c/keycode-file-name)]
    (when (.exists (io/file src))
      (u/copy-file src dst))))

(declare key-range-list-count-fast key-range-list-count-slow)

(deftype CppLMDB [^Env env
                  info
                  ^ThreadLocal tl-reader
                  ^HashMap dbis
                  scheduled-sync
                  ^BufVal kp-w
                  ^BufVal vp-w
                  ^BufVal start-kp-w
                  ^BufVal stop-kp-w
                  ^BufVal start-vp-w
                  ^BufVal stop-vp-w
                  ^ByteBuffer k-comp-bf-w
                  ^:volatile-mutable ^ByteBuffer v-comp-bf-w
                  write-txn
                  writing?
                  ^:volatile-mutable k-comp
                  ^:volatile-mutable v-comp
                  ^:unsynchronized-mutable meta]

  IWriting
  (writing? [_] writing?)

  (write-txn [_] write-txn)

  (mark-write [_]
    (->CppLMDB
      env info tl-reader dbis scheduled-sync kp-w vp-w start-kp-w
      stop-kp-w start-vp-w stop-vp-w k-comp-bf-w v-comp-bf-w
      write-txn true k-comp v-comp meta))

  (reset-write
    [this]
    (.clear kp-w)
    (.clear vp-w)
    (.clear start-kp-w)
    (.clear stop-kp-w)
    (.clear start-vp-w)
    (.clear stop-vp-w)
    (.clear k-comp-bf-w)
    (.clear v-comp-bf-w)
    (vreset! write-txn (Rtx. this
                             (Txn/create env)
                             (volatile! 1)
                             kp-w
                             vp-w
                             start-kp-w
                             stop-kp-w
                             start-vp-w
                             stop-vp-w
                             k-comp-bf-w
                             v-comp-bf-w
                             (volatile! false)
                             (volatile! [])
                             (volatile! {}))))

  IObj
  (withMeta [this m] (set! meta m) this)
  (meta [_] meta)

  ILMDB
  (max-val-size [_] (or (:max-val-size @info) c/*init-val-size*))

  (set-max-val-size [_ size]
    (set! v-comp-bf-w (bf/allocate-buffer size))
    (vswap! info assoc :max-val-size size :max-val-size-changed? true))

  (close-kv [this]
    (when-not (.isClosed env)
      (stop-scheduled-sync scheduled-sync)
      ;; Sync any unsynced WAL records before closing
      (when-let [^java.nio.channels.FileChannel ch (:wal-channel @info)]
        (when (.isOpen ch)
          (let [sync-mode (or (:wal-sync-mode @info) c/*wal-sync-mode*)]
            (try (wal/sync-channel! ch sync-mode) (catch Exception _ nil)))))
      (wal/close-segment-channel! (:wal-channel @info))
      (swap! l/lmdb-dirs disj (env-dir this))
      (when (zero? (count @l/lmdb-dirs))
        (a/shutdown-executor)
        (u/shutdown-worker-thread-pool)
        (u/shutdown-scheduler))
      (.sync env 1)
      (.close env)
      (doseq [idx (keep @l/vector-indices (u/list-files (.env-dir this)))]
        (close-vecs idx))
      (when (@info :temp?) (u/delete-files (@info :dir)))
      nil))

  (closed-kv? [_] (.isClosed env))

  (check-ready [this] (assert (not (.closed-kv? this)) "LMDB env is closed."))

  (env-dir [_] (@info :dir))

  (env-opts [_] (dissoc @info :dbis))

  (dbi-opts [_ dbi-name] (get-in @info [:dbis dbi-name]))

  (key-compressor [_] k-comp)

  (set-key-compressor [_ c] (set! k-comp c))

  (val-compressor [_] v-comp)

  (set-val-compressor [_ c] (set! v-comp c))

  (open-dbi [this dbi-name]
    (.open-dbi this dbi-name nil))
  (open-dbi [this dbi-name
             {:keys [key-size val-size flags validate-data?]
              :or   {key-size       (or (get-in @info [:dbis dbi-name :key-size])
                                        c/+max-key-size+)
                     val-size       (or (get-in @info [:dbis dbi-name :val-size])
                                        c/*init-val-size*)
                     flags          (or (get-in @info [:dbis dbi-name :flags])
                                        c/default-dbi-flags)
                     validate-data? (or (get-in @info
                                                [:dbis dbi-name :validate-data?])
                                        false)}}]
    (.check-ready this)
    (assert (< ^long key-size 512) "Key size cannot be greater than 511 bytes")
    (let [{info-dbis :dbis max-dbis :max-dbs} @info]
      (if (< (count info-dbis) ^long max-dbis)
        (let [opts     {:key-size       key-size
                        :val-size       val-size
                        :flags          flags
                        :validate-data? validate-data?}
              flags    (set flags)
              dupsort? (if (:dupsort flags) true false)
              counted? (if (:counted flags) true false)
              kp       (new-bufval key-size)
              vp       (new-bufval val-size)
              kc       (bf/allocate-buffer key-size)
              vc       (bf/allocate-buffer val-size)
              dbi      (Dbi/create env dbi-name (kv-flags flags))
              db       (DBI. this dbi (new-pools) kp vp kc vc
                             dupsort? counted? validate-data?)]
          (when (not= dbi-name c/kv-info)
            (vswap! info assoc-in [:dbis dbi-name] opts)
            (transact-kv this [(l/kv-tx :put c/kv-info [:dbis dbi-name] opts
                                        [:keyword :string])]))
          (.put dbis dbi-name db)
          db)
        (u/raise (str "Reached maximal number of DBI: " max-dbis) {}))))

  (get-dbi [this dbi-name]
    (.get-dbi this dbi-name true))
  (get-dbi [this dbi-name create?]
    (or (.get dbis dbi-name)
        (if create?
          (.open-dbi this dbi-name)
          (u/raise (str "DBI " dbi-name " is not open") {}))))

  (clear-dbi [this dbi-name]
    (.check-ready this)
    (try
      (let [^Dbi dbi (.-db ^DBI (.get-dbi this dbi-name))
            ^Txn txn (Txn/create env)]
        (Util/checkRc (DTLV/mdb_drop (.get txn) (.get dbi) 0))
        (.commit txn))
      (catch Util$MapFullException _
        (let [^Info info (Info/create env)]
          (.setMapSize env (* ^long c/+buffer-grow-factor+
                              (.me_mapsize ^DTLV$MDB_envinfo (.get info))))
          (.close info))
        (.clear-dbi this dbi-name))
      (catch Exception e
        (raise "Fail to clear DBI: " dbi-name " " e {}))))

  (drop-dbi [this dbi-name]
    (.check-ready this)
    (try
      (let [^Dbi dbi (.-db ^DBI (.get-dbi this dbi-name))
            ^Txn txn (Txn/create env)]
        (Util/checkRc (DTLV/mdb_drop (.get txn) (.get dbi) 1))
        (.commit txn)
        (vswap! info update :dbis dissoc dbi-name)
        (transact-kv this c/kv-info
                     [[:del [:dbis dbi-name]]] [:keyword :string])
        (.remove dbis dbi-name)
        nil)
      (catch Exception e (raise "Fail to drop DBI: " dbi-name e {}))))

  (list-dbis [_] (keys (@info :dbis)))

  (copy [this dest]
    (.copy this dest false))
  (copy [this dest compact?]
    (if (-> dest u/file u/empty-dir?)
      (do (.copy env dest (if compact? true false))
          (copy-version-file this dest)
          (copy-keycode-file this dest))
      (raise "Destination directory is not empty." {})))

  (get-rtx [this]
    (when-not (.closed-kv? this)
      (try
        (or (when-not (.isVirtual (Thread/currentThread))
              (when-let [^Rtx rtx (.get tl-reader)]
                (when (<= ^long (.max-val-size this)
                          ^int (.capacity ^ByteBuffer (l/val-bf rtx)))
                  (.renew rtx))))
            (let [rtx (Rtx. this
                            (Txn/createReadOnly env)
                            (volatile! 1)
                            (new-bufval c/+max-key-size+)
                            (new-bufval 0)
                            (new-bufval c/+max-key-size+)
                            (new-bufval c/+max-key-size+)
                            (new-bufval c/+max-key-size+)
                            (new-bufval c/+max-key-size+)
                            (bf/allocate-buffer c/+max-key-size+)
                            (bf/allocate-buffer (.max-val-size this))
                            (volatile! false)
                            (volatile! nil)
                            (volatile! nil))]
              (.set tl-reader rtx)
              rtx))
        (catch Exception e
          (raise "Please do not open multiple LMDB connections to the same DB
           in the same process. Instead, a LMDB connection should be held onto
           and managed like a stateful resource. Refer to the documentation of
           `datalevin.core/open-kv` for more details."
                 {:cause (.getMessage e)})))))

  (return-rtx [this rtx]
    (when-not  (.closed-kv? this)
      (if (.isVirtual (Thread/currentThread))
        (do (.abort ^Txn (.-txn ^Rtx rtx))
            (.remove tl-reader))
        (.reset ^Rtx rtx))))

  (stat [_]
    (try
      (let [stat ^Stat (Stat/create env)
            m    (stat-map stat)]
        (.close stat)
        m)
      (catch Exception e
        (raise "Fail to get statistics: " e {}))))
  (stat [this dbi-name]
    (if dbi-name
      (let [^Rtx rtx (.get-rtx this)]
        (try
          (let [^DBI dbi   (.get-dbi this dbi-name false)
                ^Dbi db    (.-db dbi)
                ^Txn txn   (.-txn rtx)
                ^Stat stat (Stat/create txn db)
                m          (stat-map stat)]
            (.close stat)
            m)
          (catch Exception e
            (raise "Fail to get statistics: " e {:dbi dbi-name}))
          (finally (.return-rtx this rtx))))
      (stat this)))

  (entries [this dbi-name]
    (let [^DBI dbi (.get-dbi this dbi-name)
          ^Rtx rtx (.get-rtx this)
          ^Dbi db  (.-db dbi)
          ^Txn txn (.-txn rtx)]
      (try
        (if (.-counted? dbi)
          (with-open [^LongPointer ptr (LongPointer. 1)]
            (DTLV/mdb_count_all (.get txn) (.get db) (int 0) ptr)
            (.get ptr))
          (let [
                ^Stat stat (Stat/create txn db)
                entries    (.ms_entries ^DTLV$MDB_stat (.get stat))]
            (.close stat)
            entries))
        (catch Exception e
          (raise "Fail to get entries: " (ex-message e) {:dbi dbi-name}))
        (finally (.return-rtx this rtx)))))

  (open-transact-kv [this]
    (.check-ready this)
    (try
      (.reset-write this)
      (.mark-write this)
      (catch Exception e
        (raise "Fail to open read/write transaction in LMDB: " e {}))))

  (close-transact-kv [this]
    (if-let [^Rtx wtxn @write-txn]
      (when-let [^Txn txn (.-txn wtxn)]
        (let [aborted?     @(.-aborted? wtxn)
              wal-enabled? (and (:kv-wal? @info) (not c/*trusted-apply*))
              ops          (when (and wal-enabled? (not aborted?))
                             (seq @(.-wal-ops wtxn)))]
          (try
            (let [wal-entry (when (seq ops)
                              (vld/check-failpoint :step-3 :before)
                              (let [entry (append-kv-wal-record!
                                            this (vec ops)
                                            (System/currentTimeMillis))]
                                (vld/check-failpoint :step-3 :after)
                                entry))
                  wal-id    (:wal-id wal-entry)
                  tx-time   (:tx-time wal-entry)]
              (when wal-id
                (run-writer-step! :step-4
                                  #(publish-kv-wal-watermarks!
                                     this wal-id tx-time))
                (run-writer-step! :step-5
                                  #(publish-kv-committed-overlay!
                                     this wal-id (:ops wal-entry)))
                (run-writer-step! :step-6 (fn [] nil))
                (run-writer-step! :step-7
                                  #(publish-kv-overlay-watermark!
                                     this wal-id)))
              (if (or aborted? wal-enabled?)
                (.abort txn)
                (try
                  (.commit txn)
                  (catch Util$MapFullException _
                    (.close txn)
                    (up-db-size env)
                    (vreset! write-txn nil)
                    (raise "DB resized" {:resized true}))
                  (catch Exception e
                    (.close txn)
                    (vreset! write-txn nil)
                    (raise "Fail to commit read/write transaction in LMDB: "
                           e {}))))
              (vreset! write-txn nil)
              (.close txn)
              ;; Step 8 (`wal/meta` checkpoint publish) is outside commit
              ;; critical path; failures are non-fatal to the caller.
              (when wal-id
                (try
                  (run-writer-step!
                    :step-8
                    #(maybe-publish-kv-wal-meta! this wal-id))
                  (catch Exception _ nil)))
              (when (.-wal-ops wtxn)
                (vreset! (.-wal-ops wtxn) []))
              (when (.-kv-overlay-private wtxn)
                (vreset! (.-kv-overlay-private wtxn) {}))
              (if aborted? :aborted :committed))
            (catch Exception e
              (when (and wal-enabled? (not aborted?))
                (refresh-kv-wal-info! this)
                (refresh-kv-wal-meta-info! this))
              (when-let [^Txn t (.-txn wtxn)]
                (try
                  (.close t)
                  (catch Exception _ nil)))
              (vreset! write-txn nil)
              (if-let [edata (ex-data e)]
                (raise "Fail to close read/write transaction in LMDB: "
                       e edata)
                (raise "Fail to close read/write transaction in LMDB: "
                       e {}))))))
      (raise "Calling `close-transact-kv` without opening" {})))

  (abort-transact-kv [_]
    (when-let [^Rtx wtxn @write-txn]
      (vreset! (.-aborted? wtxn) true)
      (when (.-wal-ops wtxn)
        (vreset! (.-wal-ops wtxn) []))
      (when (.-kv-overlay-private wtxn)
        (vreset! (.-kv-overlay-private wtxn) {}))
      (vreset! write-txn wtxn)
      nil))

  (transact-kv [this txs] (.transact-kv this nil txs))
  (transact-kv [this dbi-name txs]
    (.transact-kv this dbi-name txs :data :data))
  (transact-kv [this dbi-name txs k-type]
    (.transact-kv this dbi-name txs k-type :data))
  (transact-kv [this dbi-name txs k-type v-type]
    (locking write-txn
      (.check-ready this)
      (let [^Rtx rtx     @write-txn
            one-shot?    (nil? rtx)
            trusted?     c/*trusted-apply*
            wal-enabled? (and (:kv-wal? @info) (not trusted?))
            writeable?   (or trusted? (not wal-enabled?))
            ^DBI dbi     (when dbi-name
                           (or (.get dbis dbi-name)
                               (raise dbi-name " is not open" {})))]
        (if (and wal-enabled? one-shot?)
          ;; Fast WAL one-shot path — no LMDB transaction needed
          (try
            (let [tx-data    (validate-kv-txs! txs dbi-name k-type v-type dbis)
                  dbi-bs     (when dbi-name
                               (.getBytes ^String dbi-name StandardCharsets/UTF_8))
                  ops        (mapv #(canonical-kv-op % dbi-name dbis dbi-bs)
                                   tx-data)
                  max-val-op (when (:max-val-size-changed? @info)
                               (let [tx (l/->kv-tx-data
                                          [:put :max-val-size
                                           (:max-val-size @info)]
                                          :attr :id)]
                                 (vswap! info assoc :max-val-size-changed? false)
                                 (canonical-kv-op tx c/kv-info dbis)))
                  ops        (if max-val-op (conj ops max-val-op) ops)
                  wal-entry  (when (seq ops)
                               (vld/check-failpoint :step-3 :before)
                               (let [entry (append-kv-wal-record!
                                             this ops
                                             (System/currentTimeMillis))]
                                 (vld/check-failpoint :step-3 :after)
                                 entry))
                  wal-id     (:wal-id wal-entry)]
              (when wal-id
                (vld/check-failpoint :step-4 :before)
                (vld/check-failpoint :step-4 :after)
                (vld/check-failpoint :step-5 :before)
                (publish-kv-committed-overlay! this wal-id
                                               (:ops wal-entry))
                (vld/check-failpoint :step-5 :after)
                (vld/check-failpoint :step-6 :before)
                (vld/check-failpoint :step-6 :after)
                (vld/check-failpoint :step-7 :before)
                (publish-kv-overlay-watermark! this wal-id)
                (vld/check-failpoint :step-7 :after)
                (try
                  (vld/check-failpoint :step-8 :before)
                  (maybe-publish-kv-wal-meta! this wal-id)
                  (vld/check-failpoint :step-8 :after)
                  (catch Exception _ nil)))
              :transacted)
            (catch Exception e
              (refresh-kv-wal-info! this)
              (refresh-kv-wal-meta-info! this)
              (if-let [edata (ex-data e)]
                (raise "Fail to transact to LMDB: " e edata)
                (raise "Fail to transact to LMDB: " e {}))))
          ;; Standard path with LMDB transaction
          (let [^Txn txn (if one-shot?
                           (Txn/create env)
                           (.-txn rtx))]
            (try
              (let [tx-data    (validate-kv-txs! txs dbi-name k-type v-type dbis)
                    dbi-bs     (when (and wal-enabled? dbi-name)
                                 (.getBytes ^String dbi-name StandardCharsets/UTF_8))
                    ops        (when wal-enabled?
                                 (->> tx-data
                                      (mapv #(canonical-kv-op % dbi-name dbis dbi-bs))
                                      vec))
                    max-val-op (when (and wal-enabled?
                                          (:max-val-size-changed? @info))
                                 (let [tx (l/->kv-tx-data
                                            [:put :max-val-size
                                             (:max-val-size @info)]
                                            :attr :id)]
                                   (vswap! info assoc :max-val-size-changed? false)
                                   (canonical-kv-op tx c/kv-info dbis)))
                    ops        (cond
                                 (and ops max-val-op) (conj ops max-val-op)
                                 max-val-op           [max-val-op]
                                 :else                ops)]
                (if writeable?
                  (do
                    (if dbi
                      (transact1* txs dbi txn k-type v-type)
                      (transact* txs dbis txn))
                    (when (:max-val-size-changed? @info)
                      (transact* [[:put c/kv-info :max-val-size
                                   (:max-val-size @info)]]
                                 dbis txn)
                      (vswap! info assoc :max-val-size-changed? false))
                    (when one-shot? (.commit txn))
                    :transacted)
                  ;; WAL inside open-transact-kv/close-transact-kv
                  (do
                    (when (seq ops)
                      (vswap! (.-wal-ops rtx) into ops)
                      (publish-kv-private-overlay! rtx ops))
                    :transacted)))
              (catch Util$MapFullException _
                (.close txn)
                (up-db-size env)
                (if (and one-shot? (not wal-enabled?))
                  (.transact-kv this dbi-name txs k-type v-type)
                  (do (.reset-write this)
                      (raise "DB resized" {:resized true}))))
              (catch Exception e
                (when one-shot? (.close txn))
                (when (and one-shot? wal-enabled?)
                  (refresh-kv-wal-info! this)
                  (refresh-kv-wal-meta-info! this))
                (if-let [edata (ex-data e)]
                  (raise "Fail to transact to LMDB: " e edata)
                  (raise "Fail to transact to LMDB: " e {})))))))))

  (set-env-flags [_ ks on-off] (.setFlags env (kv-flags ks) (if on-off 1 0)))

  (get-env-flags [_] (env-flag-keys (.getFlags env)))

  (sync [_] (.sync env 1))
  (sync [_ force] (.sync env force))

  (kv-wal-watermarks [this]
    (l/kv-wal-watermarks this))

  (flush-kv-indexer! [this]
    (.flush-kv-indexer! this nil))
  (flush-kv-indexer! [this upto-wal-id]
    ;; Force-flush the cached WAL segment channel so that reads via a
    ;; separate FileInputStream see all written data.
    (when-let [^java.nio.channels.FileChannel ch (:wal-channel @info)]
      (when (.isOpen ch)
        (try (.force ch true) (catch Exception _ nil))))
    (let [res (l/flush-kv-indexer! this upto-wal-id)]
      (vswap! (.-info this) assoc
              :last-indexed-wal-tx-id (:indexed-wal-tx-id res)
              :applied-wal-tx-id (long (or (get-kv-info-id this
                                                           c/applied-wal-tx-id)
                                           0)))
      (prune-kv-committed-overlay! this (:indexed-wal-tx-id res))
      res))

  (get-value [this dbi-name k]
    (.get-value this dbi-name k :data :data true))
  (get-value [this dbi-name k k-type]
    (.get-value this dbi-name k k-type :data true))
  (get-value [this dbi-name k k-type v-type]
    (.get-value this dbi-name k k-type v-type true))
  (get-value [this dbi-name k k-type v-type ignore-key?]
    (if (kv-overlay-eligible? this dbi-name)
      (let [ret (overlay-get-value this dbi-name k k-type v-type ignore-key?)]
        (case ret
          ::overlay-miss
          (scan/get-value this dbi-name k k-type v-type ignore-key?)
          ::overlay-tombstone nil
          ret))
      (scan/get-value this dbi-name k k-type v-type ignore-key?)))

  (get-rank [this dbi-name k]
    (.get-rank this dbi-name k :data))
  (get-rank [this dbi-name k k-type]
    (if (kv-overlay-active? this dbi-name)
      (overlay-rank this dbi-name k k-type)
      (scan/get-rank this dbi-name k k-type)))

  (get-by-rank [this dbi-name rank]
    (.get-by-rank this dbi-name rank :data :data true))
  (get-by-rank [this dbi-name rank k-type]
    (.get-by-rank this dbi-name rank k-type :data true))
  (get-by-rank [this dbi-name rank k-type v-type]
    (.get-by-rank this dbi-name rank k-type v-type true))
  (get-by-rank [this dbi-name rank k-type v-type ignore-key?]
    (if (kv-overlay-active? this dbi-name)
      (overlay-get-by-rank this dbi-name rank k-type v-type ignore-key?)
      (scan/get-by-rank this dbi-name rank k-type v-type ignore-key?)))

  (sample-kv [this dbi-name n]
    (.sample-kv this dbi-name n :data :data true))
  (sample-kv [this dbi-name n k-type]
    (.sample-kv this dbi-name n k-type :data true))
  (sample-kv [this dbi-name n k-type v-type]
    (.sample-kv this dbi-name n k-type v-type true))
  (sample-kv [this dbi-name n k-type v-type ignore-key?]
    (if (kv-overlay-active? this dbi-name)
      (overlay-sample-kv this dbi-name n k-type v-type ignore-key?)
      (scan/sample-kv this dbi-name n k-type v-type ignore-key?)))

  (get-first [this dbi-name k-range]
    (.get-first this dbi-name k-range :data :data false))
  (get-first [this dbi-name k-range k-type]
    (.get-first this dbi-name k-range k-type :data false))
  (get-first [this dbi-name k-range k-type v-type]
    (.get-first this dbi-name k-range k-type v-type false))
  (get-first [this dbi-name k-range k-type v-type ignore-key?]
    (if (kv-overlay-active? this dbi-name)
      (when-let [[k v] (first (.get-range this dbi-name k-range
                                          k-type v-type false))]
        (if ignore-key?
          (if (= v-type :ignore) true v)
          [k v]))
      (scan/get-first this dbi-name k-range k-type v-type ignore-key?)))

  (get-first-n [this dbi-name n k-range]
    (.get-first-n this dbi-name n k-range :data :data false))
  (get-first-n [this dbi-name n k-range k-type]
    (.get-first-n this dbi-name n k-range k-type :data false))
  (get-first-n [this dbi-name n k-range k-type v-type]
    (.get-first-n this dbi-name n k-range k-type v-type false))
  (get-first-n [this dbi-name n k-range k-type v-type ignore-key?]
    (if (kv-overlay-active? this dbi-name)
      (let [kvs (take n (.get-range this dbi-name k-range
                                    k-type v-type false))]
        (if ignore-key?
          (mapv (fn [[_ v]]
                  (if (= v-type :ignore) nil v))
                kvs)
          (vec kvs)))
      (scan/get-first-n this dbi-name n k-range k-type v-type ignore-key?)))

  (get-range [this dbi-name k-range]
    (.get-range this dbi-name k-range :data :data false))
  (get-range [this dbi-name k-range k-type]
    (.get-range this dbi-name k-range k-type :data false))
  (get-range [this dbi-name k-range k-type v-type]
    (.get-range this dbi-name k-range k-type v-type false))
  (get-range [this dbi-name k-range k-type v-type ignore-key?]
    (if (kv-overlay-eligible? this dbi-name)
      (let [ret (overlay-get-range this dbi-name k-range
                                   k-type v-type ignore-key?)]
        (if (= ret ::overlay-miss)
          (scan/get-range this dbi-name k-range k-type v-type ignore-key?)
          ret))
      (scan/get-range this dbi-name k-range k-type v-type ignore-key?)))

  (key-range [this dbi-name k-range]
    (.key-range this dbi-name k-range :data))
  (key-range [this dbi-name k-range k-type]
    (if (kv-overlay-active? this dbi-name)
      (let [k-type (normalize-kv-type k-type)
            kvs    (.get-range this dbi-name k-range k-type :ignore false)]
        (overlay-key-seq kvs k-type (i/list-dbi? this dbi-name)))
      (scan/key-range this dbi-name k-range k-type)))

  (visit-key-range [this dbi-name visitor k-range]
    (.visit-key-range this dbi-name visitor k-range :data true))
  (visit-key-range [this dbi-name visitor k-range k-type]
    (.visit-key-range this dbi-name visitor k-range k-type true))
  (visit-key-range [this dbi-name visitor k-range k-type raw-pred?]
    (if (kv-overlay-active? this dbi-name)
      (let [k-type (normalize-kv-type k-type)
            ks     (.key-range this dbi-name k-range k-type)]
        (loop [xs (seq ks)]
          (when-let [k (first xs)]
            (let [res (if raw-pred?
                        (visitor (overlay-raw-key k k-type))
                        (visitor k))]
              (when-not (identical? res :datalevin/terminate-visit)
                (recur (next xs)))))))
      (scan/visit-key-range this dbi-name visitor k-range k-type raw-pred?)))

  (key-range-count [lmdb dbi-name k-range]
    (.key-range-count lmdb dbi-name k-range :data))
  (key-range-count [lmdb dbi-name k-range k-type]
    (.key-range-count lmdb dbi-name k-range k-type nil))
  (key-range-count [lmdb dbi-name [range-type k1 k2 :as k-range] k-type cap]
    (if (kv-overlay-active? lmdb dbi-name)
      (let [k-type (normalize-kv-type k-type)
            kvs    (.get-range lmdb dbi-name k-range k-type :ignore false)]
        (overlay-key-count kvs k-type (i/list-dbi? lmdb dbi-name) cap))
      (scan/scan
        (let [^RangeContext ctx (l/range-info rtx range-type k1 k2 k-type)
              forward           (dtlv-bool (.-forward? ctx))
              start             (dtlv-bool (.-include-start? ctx))
              end               (dtlv-bool (.-include-stop? ctx))
              sk                (dtlv-val (.-start-bf ctx))
              ek                (dtlv-val (.-stop-bf ctx))]
          (dtlv-c
            (if cap
              (DTLV/dtlv_key_range_count_cap
                (.ptr ^Cursor cur) cap
                (.ptr ^BufVal (.-kp ^Rtx rtx)) (.ptr ^BufVal (.-vp ^Rtx rtx))
                forward start end sk ek)
              (if (l/dlmdb?)
                (let [flag (BitOps/intOr
                             (if (.-include-start? ctx)
                               (int DTLV/MDB_COUNT_LOWER_INCL) 0)
                             (if (.-include-stop? ctx)
                               (int DTLV/MDB_COUNT_UPPER_INCL) 0))]
                  (with-open [total (LongPointer. 1)]
                    (DTLV/mdb_range_count_keys
                      (.get ^Txn (.-txn ^Rtx rtx)) (.get ^Dbi (.-db ^DBI dbi))
                      sk ek flag total)
                    (.get ^LongPointer total)))
                (DTLV/dtlv_key_range_count
                  (.ptr ^Cursor cur)
                  (.ptr ^BufVal (.-kp ^Rtx rtx)) (.ptr ^BufVal (.-vp ^Rtx rtx))
                  forward start end sk ek)))))
        (raise "Fail to count key range: " e {:dbi dbi-name}))))

  (range-seq [this dbi-name k-range]
    (.range-seq this dbi-name k-range :data :data false nil))
  (range-seq [this dbi-name k-range k-type]
    (.range-seq this dbi-name k-range k-type :data false nil))
  (range-seq [this dbi-name k-range k-type v-type]
    (.range-seq this dbi-name k-range k-type v-type false nil))
  (range-seq [this dbi-name k-range k-type v-type ignore-key?]
    (.range-seq this dbi-name k-range k-type v-type ignore-key? nil))
  (range-seq [this dbi-name k-range k-type v-type ignore-key? opts]
    (if (kv-overlay-active? this dbi-name)
      (overlay-range-seq this dbi-name k-range k-type v-type ignore-key? opts)
      (scan/range-seq this dbi-name k-range k-type v-type ignore-key? opts)))

  (range-count [this dbi-name k-range]
    (.range-count this dbi-name k-range :data))
  (range-count [lmdb dbi-name k-range k-type]
    (if (kv-overlay-active? lmdb dbi-name)
      (long (count (.get-range lmdb dbi-name k-range k-type :ignore false)))
      (let [dupsort? (.-dupsort? ^DBI (.get dbis dbi-name))]
        (if dupsort?
          (.list-range-count lmdb dbi-name k-range k-type [:all] nil)
          (.key-range-count lmdb dbi-name k-range k-type)))))

  (get-some [this dbi-name pred k-range]
    (.get-some this dbi-name pred k-range :data :data false true))
  (get-some [this dbi-name pred k-range k-type]
    (.get-some this dbi-name pred k-range k-type :data false true))
  (get-some [this dbi-name pred k-range k-type v-type]
    (.get-some this dbi-name pred k-range k-type v-type false true))
  (get-some [this dbi-name pred k-range k-type v-type ignore-key?]
    (.get-some this dbi-name pred k-range k-type v-type  ignore-key? true))
  (get-some [this dbi-name pred k-range k-type v-type ignore-key? raw-pred?]
    (if (kv-overlay-active? this dbi-name)
      (do
        (assert (not (and (= v-type :ignore) ignore-key?))
                "Cannot ignore both key and value")
        (let [k-type      (normalize-kv-type k-type)
              pred-v-type (normalize-kv-type
                            (overlay-pred-v-type v-type raw-pred?))
              kvs         (.get-range this dbi-name k-range
                                      k-type pred-v-type false)]
          (loop [xs (seq kvs)]
            (when-let [[k v] (first xs)]
              (let [v-out   (overlay-out-value v v-type)
                    matched (if raw-pred?
                              (pred (overlay-raw-kv k v k-type pred-v-type))
                              (pred k v-out))]
                (if matched
                  (if ignore-key? v-out [k v-out])
                  (recur (next xs))))))))
      (scan/get-some this dbi-name pred k-range k-type v-type ignore-key?
                     raw-pred?)))

  (range-filter [this dbi-name pred k-range]
    (.range-filter this dbi-name pred k-range :data :data false true))
  (range-filter [this dbi-name pred k-range k-type]
    (.range-filter this dbi-name pred k-range k-type :data false true))
  (range-filter [this dbi-name pred k-range k-type v-type]
    (.range-filter this dbi-name pred k-range k-type v-type false true))
  (range-filter [this dbi-name pred k-range k-type v-type ignore-key?]
    (.range-filter this dbi-name pred k-range k-type v-type  ignore-key? true))
  (range-filter [this dbi-name pred k-range k-type v-type ignore-key? raw-pred?]
    (if (kv-overlay-active? this dbi-name)
      (do
        (assert (not (and (= v-type :ignore) ignore-key?))
                "Cannot ignore both key and value")
        (let [k-type      (normalize-kv-type k-type)
              pred-v-type (normalize-kv-type
                            (overlay-pred-v-type v-type raw-pred?))
              kvs         (.get-range this dbi-name k-range
                                      k-type pred-v-type false)]
          (loop [xs  (seq kvs)
                 out (transient [])]
            (if-let [[k v] (first xs)]
              (let [v-out   (overlay-out-value v v-type)
                    matched (if raw-pred?
                              (pred (overlay-raw-kv k v k-type pred-v-type))
                              (pred k v-out))]
                (if matched
                  (recur (next xs) (conj! out (if ignore-key? v-out [k v-out])))
                  (recur (next xs) out)))
              (persistent! out)))))
      (scan/range-filter this dbi-name pred k-range k-type v-type ignore-key?
                         raw-pred?)))

  (range-keep [this dbi-name pred k-range]
    (.range-keep this dbi-name pred k-range :data :data true))
  (range-keep [this dbi-name pred k-range k-type]
    (.range-keep this dbi-name pred k-range k-type :data true))
  (range-keep [this dbi-name pred k-range k-type v-type]
    (.range-keep this dbi-name pred k-range k-type v-type true))
  (range-keep [this dbi-name pred k-range k-type v-type raw-pred?]
    (if (kv-overlay-active? this dbi-name)
      (let [k-type      (normalize-kv-type k-type)
            pred-v-type (normalize-kv-type
                          (overlay-pred-v-type v-type raw-pred?))
            kvs         (.get-range this dbi-name k-range
                                    k-type pred-v-type false)]
        (loop [xs  (seq kvs)
               out (transient [])]
          (if-let [[k v] (first xs)]
            (let [res (if raw-pred?
                        (pred (overlay-raw-kv k v k-type pred-v-type))
                        (pred k (overlay-out-value v v-type)))]
              (if (nil? res)
                (recur (next xs) out)
                (recur (next xs) (conj! out res))))
            (persistent! out))))
      (scan/range-keep this dbi-name pred k-range k-type v-type raw-pred?)))

  (range-some [this dbi-name pred k-range]
    (.range-some this dbi-name pred k-range :data :data true))
  (range-some [this dbi-name pred k-range k-type]
    (.range-some this dbi-name pred k-range k-type :data true))
  (range-some [this dbi-name pred k-range k-type v-type]
    (.range-some this dbi-name pred k-range k-type v-type true))
  (range-some [this dbi-name pred k-range k-type v-type raw-pred?]
    (if (kv-overlay-active? this dbi-name)
      (let [k-type      (normalize-kv-type k-type)
            pred-v-type (normalize-kv-type
                          (overlay-pred-v-type v-type raw-pred?))
            kvs         (.get-range this dbi-name k-range
                                    k-type pred-v-type false)]
        (loop [xs (seq kvs)]
          (when-let [[k v] (first xs)]
            (let [res (if raw-pred?
                        (pred (overlay-raw-kv k v k-type pred-v-type))
                        (pred k (overlay-out-value v v-type)))]
              (or res (recur (next xs)))))))
      (scan/range-some this dbi-name pred k-range k-type v-type raw-pred?)))

  (range-filter-count [this dbi-name pred k-range]
    (.range-filter-count this dbi-name pred k-range :data :data true))
  (range-filter-count [this dbi-name pred k-range k-type]
    (.range-filter-count this dbi-name pred k-range k-type :data true))
  (range-filter-count [this dbi-name pred k-range k-type v-type]
    (.range-filter-count this dbi-name pred k-range k-type v-type true))
  (range-filter-count [this dbi-name pred k-range k-type v-type raw-pred?]
    (if (kv-overlay-active? this dbi-name)
      (let [k-type      (normalize-kv-type k-type)
            pred-v-type (normalize-kv-type
                          (overlay-pred-v-type v-type raw-pred?))
            kvs         (.get-range this dbi-name k-range
                                    k-type pred-v-type false)]
        (loop [xs  (seq kvs)
               cnt 0]
          (if-let [[k v] (first xs)]
            (let [matched (if raw-pred?
                            (pred (overlay-raw-kv k v k-type pred-v-type))
                            (pred k (overlay-out-value v v-type)))]
              (if matched
                (recur (next xs) (unchecked-inc cnt))
                (recur (next xs) cnt)))
            cnt)))
      (scan/range-filter-count this dbi-name pred k-range k-type
                               v-type raw-pred?)))

  (visit [this dbi-name visitor k-range]
    (.visit this dbi-name visitor k-range :data :data true))
  (visit [this dbi-name visitor k-range k-type]
    (.visit this dbi-name visitor k-range k-type :data true))
  (visit [this dbi-name visitor k-range k-type v-type]
    (.visit this dbi-name visitor k-range k-type v-type true))
  (visit [this dbi-name visitor k-range k-type v-type raw-pred?]
    (if (kv-overlay-active? this dbi-name)
      (let [k-type      (normalize-kv-type k-type)
            pred-v-type (normalize-kv-type
                          (overlay-pred-v-type v-type raw-pred?))
            kvs         (.get-range this dbi-name k-range
                                    k-type pred-v-type false)]
        (loop [xs (seq kvs)]
          (when-let [[k v] (first xs)]
            (let [res (if raw-pred?
                        (visitor (overlay-raw-kv k v k-type pred-v-type))
                        (visitor k (overlay-out-value v v-type)))]
              (when-not (identical? res :datalevin/terminate-visit)
                (recur (next xs)))))))
      (scan/visit this dbi-name visitor k-range k-type v-type raw-pred?)))

  (visit-key-sample
    [db dbi-name indices budget step visitor k-range k-type]
    (.visit-key-sample
      db dbi-name indices budget step visitor k-range k-type true))
  (visit-key-sample
    [db dbi-name indices budget step visitor k-range k-type raw-pred?]
    (if (kv-overlay-active? db dbi-name)
      (overlay-visit-key-sample db dbi-name indices visitor k-range
                                k-type raw-pred?)
      (scan/visit-key-sample
        db dbi-name indices budget step visitor k-range k-type raw-pred?)))

  (open-list-dbi [this dbi-name {:keys [key-size val-size flags]
                                 :or   {key-size c/+max-key-size+
                                        val-size c/+max-key-size+
                                        flags    c/default-dbi-flags}}]
    (.check-ready this)
    (assert (and (>= c/+max-key-size+ ^long key-size)
                 (>= c/+max-key-size+ ^long val-size))
            "Data size cannot be larger than 511 bytes")
    (.open-dbi this dbi-name
               {:key-size key-size :val-size val-size
                :flags    (conj flags :dupsort)}))
  (open-list-dbi [lmdb dbi-name]
    (.open-list-dbi lmdb dbi-name nil))

  IList
  (list-dbi? [this dbi-name]
    (get-in (.dbi-opts this dbi-name) [:flags :dupsort]))
  (put-list-items [this dbi-name k vs kt vt]
    (.transact-kv this [(l/kv-tx :put-list dbi-name k vs kt vt)]))

  (del-list-items [this dbi-name k kt]
    (.transact-kv this [(l/kv-tx :del dbi-name k kt)]))
  (del-list-items [this dbi-name k vs kt vt]
    (.transact-kv this [(l/kv-tx :del-list dbi-name k vs kt vt)]))

  (get-list [this dbi-name k kt vt]
    (if (kv-overlay-active? this dbi-name)
      (overlay-list-values this dbi-name k kt vt)
      (scan/get-list this dbi-name k kt vt)))

  (visit-list [this dbi-name visitor k kt]
    (.visit-list this dbi-name visitor k kt :data true))
  (visit-list [this dbi-name visitor k kt vt]
    (.visit-list this dbi-name visitor k kt vt true))
  (visit-list [this dbi-name visitor k kt vt raw-pred?]
    (if (kv-overlay-active? this dbi-name)
      (let [vt (normalize-kv-type vt)]
        (doseq [v (or (overlay-list-values this dbi-name k kt vt) [])]
          (if raw-pred?
            (visitor (ByteBuffer/wrap (encode-kv-bytes v vt)))
            (visitor v))))
      (scan/visit-list this dbi-name visitor k kt vt raw-pred?)))

  (list-count [lmdb dbi-name k kt]
    (if (kv-overlay-active? lmdb dbi-name)
      (if k
        (long (count (overlay-list-range lmdb dbi-name
                                         [:closed k k] kt [:all] :raw)))
        0)
      (do
        (.check-ready lmdb)
        (if k
          (scan/scan
            (list-count* rtx cur k kt)
            (raise "Fail to count list: " e {:dbi dbi-name :k k}))
          0))))

  (near-list [lmdb dbi-name k v kt vt]
    (if (kv-overlay-active? lmdb dbi-name)
      (overlay-list-near-val-buf lmdb dbi-name k v kt vt)
      (do
        (.check-ready lmdb)
        (scan/scan
          (near-list* rtx cur k kt v vt)
          (raise "Fail to get an item that is near in a list: "
                 e {:dbi dbi-name :k k :v v})))))

  (in-list? [lmdb dbi-name k v kt vt]
    (if (kv-overlay-active? lmdb dbi-name)
      (if (and k v)
        (boolean (seq (overlay-list-range lmdb dbi-name [:closed k k] kt
                                          [:closed v v] vt)))
        false)
      (do
        (.check-ready lmdb)
        (if (and k v)
          (scan/scan
            (in-list?* rtx cur k kt v vt)
            (raise "Fail to test if an item is in list: "
                   e {:dbi dbi-name :k k :v v}))
          false))))

  (key-range-list-count [lmdb dbi-name k-range k-type]
    (.key-range-list-count lmdb dbi-name k-range k-type nil nil))
  (key-range-list-count [lmdb dbi-name k-range k-type cap]
    (.key-range-list-count lmdb dbi-name k-range k-type cap nil))
  (key-range-list-count [lmdb dbi-name k-range k-type cap budget]
    (if (kv-overlay-active? lmdb dbi-name)
      (let [k-type (normalize-kv-type k-type)
            kvs    (overlay-list-range lmdb dbi-name k-range k-type
                                       [:all] :raw)]
        (overlay-key-count kvs k-type true cap))
      (if (l/dlmdb?)
        (key-range-list-count-fast lmdb dbi-name k-range k-type cap)
        (key-range-list-count-slow lmdb dbi-name k-range k-type cap budget))))

  (list-range [this dbi-name k-range kt v-range vt]
    (if (kv-overlay-active? this dbi-name)
      (overlay-list-range this dbi-name k-range kt v-range vt)
      (scan/list-range this dbi-name k-range kt v-range vt)))

  (list-range-count [lmdb dbi-name k-range kt v-range vt]
    (.list-range-count lmdb dbi-name k-range kt v-range vt nil))
  (list-range-count [lmdb dbi-name [k-range-type k1 k2] k-type
                     [v-range-type v1 v2] v-type cap]
    (if (kv-overlay-active? lmdb dbi-name)
      (let [cnt (long (count (overlay-list-range
                               lmdb dbi-name
                               [k-range-type k1 k2] k-type
                               [v-range-type v1 v2] v-type)))]
        (if cap (min (long cap) cnt) cnt))
      (scan/scan
        (let [[^RangeContext kctx ^RangeContext vctx]
              (l/list-range-info rtx k-range-type k1 k2 k-type
                                 v-range-type v1 v2 v-type)
              kforward (dtlv-bool (.-forward? kctx))
              kstart   (dtlv-bool (.-include-start? kctx))
              kend     (dtlv-bool (.-include-stop? kctx))
              sk       (dtlv-val (.-start-bf kctx))
              ek       (dtlv-val (.-stop-bf kctx))
              vforward (dtlv-bool (.-forward? vctx))
              vstart   (dtlv-bool (.-include-start? vctx))
              vend     (dtlv-bool (.-include-stop? vctx))
              sv       (dtlv-val (.-start-bf vctx))
              ev       (dtlv-val (.-stop-bf vctx))]
          (dtlv-c
            (if cap
              (DTLV/dtlv_list_range_count_cap
                (.ptr ^Cursor cur) cap
                (.ptr ^BufVal (.-kp ^Rtx rtx)) (.ptr ^BufVal (.-vp ^Rtx rtx))
                kforward kstart kend sk ek vforward vstart vend sv ev)
              (DTLV/dtlv_list_range_count
                (.ptr ^Cursor cur)
                (.ptr ^BufVal (.-kp ^Rtx rtx)) (.ptr ^BufVal (.-vp ^Rtx rtx))
                kforward kstart kend sk ek vforward vstart vend sv ev))))
        (raise "Fail to count list range: " e {:dbi dbi-name}))))

  (list-range-first [this dbi-name k-range kt v-range vt]
    (if (kv-overlay-active? this dbi-name)
      (first (overlay-list-range this dbi-name k-range kt v-range vt))
      (scan/list-range-first this dbi-name k-range kt v-range vt)))

  (list-range-first-n [this dbi-name n k-range kt v-range vt]
    (if (kv-overlay-active? this dbi-name)
      (vec (take n (overlay-list-range this dbi-name k-range kt v-range vt)))
      (scan/list-range-first-n this dbi-name n k-range kt v-range vt)))

  (list-range-filter [this dbi-name pred k-range kt v-range vt]
    (.list-range-filter this dbi-name pred k-range kt v-range vt true))
  (list-range-filter [this dbi-name pred k-range kt v-range vt raw-pred?]
    (if (kv-overlay-active? this dbi-name)
      (let [kt  (normalize-kv-type kt)
            vt  (normalize-kv-type vt)
            kvs (overlay-list-range this dbi-name k-range kt v-range vt)]
        (loop [xs  (seq kvs)
               out (transient [])]
          (if-let [[k v] (first xs)]
            (let [matched (if raw-pred?
                            (pred (overlay-raw-kv k v kt vt))
                            (pred k v))]
              (if matched
                (recur (next xs) (conj! out [k v]))
                (recur (next xs) out)))
            (persistent! out))))
      (scan/list-range-filter this dbi-name pred k-range kt v-range vt raw-pred?)))

  (list-range-keep [this dbi-name pred k-range kt v-range vt]
    (.list-range-keep this dbi-name pred k-range kt v-range vt true))
  (list-range-keep [this dbi-name pred k-range kt v-range vt raw-pred?]
    (if (kv-overlay-active? this dbi-name)
      (let [kt  (normalize-kv-type kt)
            vt  (normalize-kv-type vt)
            kvs (overlay-list-range this dbi-name k-range kt v-range vt)]
        (loop [xs  (seq kvs)
               out (transient [])]
          (if-let [[k v] (first xs)]
            (let [res (if raw-pred?
                        (pred (overlay-raw-kv k v kt vt))
                        (pred k v))]
              (if (nil? res)
                (recur (next xs) out)
                (recur (next xs) (conj! out res))))
            (persistent! out))))
      (scan/list-range-keep this dbi-name pred k-range kt v-range vt raw-pred?)))

  (list-range-some [this list-name pred k-range k-type v-range v-type]
    (.list-range-some this list-name pred k-range k-type v-range v-type
                      true))
  (list-range-some [this dbi-name pred k-range kt v-range vt raw-pred?]
    (if (kv-overlay-active? this dbi-name)
      (let [kt  (normalize-kv-type kt)
            vt  (normalize-kv-type vt)
            kvs (overlay-list-range this dbi-name k-range kt v-range vt)]
        (loop [xs (seq kvs)]
          (when-let [[k v] (first xs)]
            (let [res (if raw-pred?
                        (pred (overlay-raw-kv k v kt vt))
                        (pred k v))]
              (or res (recur (next xs)))))))
      (scan/list-range-some this dbi-name pred k-range kt v-range vt raw-pred?)))

  (list-range-filter-count
    [this list-name pred k-range k-type v-range v-type]
    (.list-range-filter-count this list-name pred k-range k-type v-range
                              v-type true nil))
  (list-range-filter-count
    [this dbi-name pred k-range kt v-range vt raw-pred?]
    (.list-range-filter-count this dbi-name pred k-range kt v-range
                              vt raw-pred? nil))
  (list-range-filter-count
    [this dbi-name pred k-range kt v-range vt raw-pred? cap]
    (if (kv-overlay-active? this dbi-name)
      (let [kt  (normalize-kv-type kt)
            vt  (normalize-kv-type vt)
            kvs (overlay-list-range this dbi-name k-range kt v-range vt)]
        (loop [xs  (seq kvs)
               cnt 0]
          (if (or (nil? xs) (and cap (>= cnt (long cap))))
            cnt
            (let [[k v]   (first xs)
                  matched (if raw-pred?
                            (pred (overlay-raw-kv k v kt vt))
                            (pred k v))]
              (if matched
                (recur (next xs) (unchecked-inc cnt))
                (recur (next xs) cnt))))))
      (scan/list-range-filter-count this dbi-name pred k-range kt v-range
                                    vt raw-pred? cap)))

  (visit-list-range
    [this list-name visitor k-range k-type v-range v-type]
    (.visit-list-range this list-name visitor k-range k-type v-range
                       v-type true))
  (visit-list-range
    [this dbi-name visitor k-range kt v-range vt raw-pred?]
    (if (kv-overlay-active? this dbi-name)
      (let [kt  (normalize-kv-type kt)
            vt  (normalize-kv-type vt)
            kvs (overlay-list-range this dbi-name k-range kt v-range vt)]
        (loop [xs (seq kvs)]
          (when-let [[k v] (first xs)]
            (let [res (if raw-pred?
                        (visitor (overlay-raw-kv k v kt vt))
                        (visitor k v))]
              (when-not (identical? res :datalevin/terminate-visit)
                (recur (next xs)))))))
      (scan/visit-list-range this dbi-name visitor k-range kt v-range
                             vt raw-pred?)))

  (visit-list-key-range
    [this dbi-name visitor k-range k-type v-type]
    (.visit-list-key-range this dbi-name visitor k-range k-type
                           v-type true))
  (visit-list-key-range
    [this dbi-name visitor k-range k-type v-type raw-pred?]
    (if (kv-overlay-active? this dbi-name)
      (let [k-type (normalize-kv-type k-type)
            v-type (normalize-kv-type v-type)
            kvs    (overlay-list-range this dbi-name k-range k-type
                                       [:all] v-type)]
        (loop [xs (seq kvs)]
          (when-let [[k v] (first xs)]
            (let [res (if raw-pred?
                        (visitor (overlay-raw-kv k v k-type v-type))
                        (visitor k v))]
              (when-not (identical? res :datalevin/terminate-visit)
                (recur (next xs)))))))
      (scan/visit-list-key-range this dbi-name visitor k-range k-type
                                 v-type raw-pred?)))

  (visit-list-sample
    [this list-name indices budget step visitor k-range k-type v-type]
    (.visit-list-sample this list-name indices budget step visitor k-range
                        k-type v-type true))
  (visit-list-sample
    [this dbi-name indices budget step visitor k-range kt vt raw-pred?]
    (if (kv-overlay-active? this dbi-name)
      (overlay-visit-list-sample this dbi-name indices visitor k-range
                                 kt vt raw-pred?)
      (scan/visit-list-sample this dbi-name indices budget step visitor k-range
                              kt vt raw-pred?)))

  (operate-list-val-range
    [this dbi-name operator v-range vt]
    (if (kv-overlay-active? this dbi-name)
      (operator (overlay-list-val-iterable this dbi-name v-range vt))
      (scan/operate-list-val-range this dbi-name operator v-range vt)))

  IAdmin
  (re-index [this opts] (l/re-index* this opts)))

  (defn- key-range-list-count-fast
    [lmdb dbi-name [range-type k1 k2] k-type cap]
    (scan/scan
      (let [^RangeContext ctx (l/range-info rtx range-type k1 k2 k-type)
            flag              (BitOps/intOr
                                (if (.-include-start? ctx)
                                  (int DTLV/MDB_COUNT_LOWER_INCL) 0)
                                (if (.-include-stop? ctx)
                                  (int DTLV/MDB_COUNT_UPPER_INCL) 0))]
        (with-open [ptr (LongPointer. 1)]
          (DTLV/mdb_range_count_values
            (.get ^Txn (.-txn ^Rtx rtx)) (.get ^Dbi (.-db ^DBI dbi))
            (dtlv-val (.-start-bf ctx)) (dtlv-val (.-stop-bf ctx))
            flag ptr)
          (let [res (.get ^LongPointer ptr)]
            (if (and cap (> ^long res ^long cap)) cap res))))
      (raise "Fail to count (fast) list in key range: " e {:dbi dbi-name})))

(defn- key-range-list-count-slow
  [lmdb dbi-name [range-type k1 k2] k-type cap budget]
  (scan/scan
    (let [^RangeContext ctx (l/range-info rtx range-type k1 k2 k-type)
          forward           (dtlv-bool (.-forward? ctx))
          start             (dtlv-bool (.-include-start? ctx))
          end               (dtlv-bool (.-include-stop? ctx))
          sk                (dtlv-val (.-start-bf ctx))
          ek                (dtlv-val (.-stop-bf ctx))]
      (dtlv-c
        (cond
          budget
          (DTLV/dtlv_key_range_list_count_cap_budget
            (.ptr ^Cursor cur) cap budget c/range-count-iteration-step
            (.ptr ^BufVal (.-kp ^Rtx rtx)) (.ptr ^BufVal (.-vp ^Rtx rtx))
            forward start end sk ek)
          cap
          (let [res (DTLV/dtlv_key_range_list_count_cap
                      (.ptr ^Cursor cur) cap
                      (.ptr ^BufVal (.-kp ^Rtx rtx)) (.ptr ^BufVal (.-vp ^Rtx rtx))
                      forward start end sk ek)]
            (if (> ^long res ^long cap) cap res))
          :else
          (DTLV/dtlv_key_range_list_count
            (.ptr ^Cursor cur)
            (.ptr ^BufVal (.-kp ^Rtx rtx)) (.ptr ^BufVal (.-vp ^Rtx rtx))
            forward start end sk ek))))
    (raise "Fail to count (slow) list in key range: " e {:dbi dbi-name})))

(defn- init-info
  [^CppLMDB lmdb new-info]
  (transact-kv lmdb c/kv-info (map (fn [[k v]] [:put k v]) new-info))
  (let [dbis (into {}
                   (map (fn [[[_ dbi-name] opts]] [dbi-name opts]))
                   (get-range lmdb c/kv-info
                              [:closed
                               [:dbis :db.value/sysMin]
                               [:dbis :db.value/sysMax]]
                              [:keyword :string]))
        info (into {}
                   (range-filter lmdb c/kv-info
                                 (fn [kv]
                                   (let [b (b/read-buffer (l/k kv) :byte)]
                                     (not= b c/type-hete-tuple)))
                                 [:all]))]
    (vreset! (.-info lmdb) (assoc info :dbis dbis))))

(defn- open-kv*
  [dir dir-file db-file {:keys [mapsize max-readers flags max-dbs temp?
                                kv-wal?
                                wal-meta-flush-max-txs
                                wal-meta-flush-max-ms
                                wal-group-commit
                                key-compress val-compress]
                         :or   {max-readers c/*max-readers*
                                max-dbs     c/*max-dbs*
                                mapsize     c/*init-db-size*
                                flags       c/default-env-flags
                                temp?       false
                                wal-meta-flush-max-txs
                                c/*wal-meta-flush-max-txs*
                                wal-meta-flush-max-ms
                                c/*wal-meta-flush-max-ms*
                                wal-group-commit
                                c/*wal-group-commit*
                                kv-wal?     c/*enable-kv-wal*}
                         :as   opts}]
  (try
    (let [mapsize       (* (long (if (.exists ^File db-file)
                                   (c/pick-mapsize db-file)
                                   mapsize))
                           1024 1024)
          flags         (if temp?
                          (conj flags :nosync)
                          flags)
          ^Env env      (Env/create dir mapsize max-readers max-dbs
                                    (kv-flags flags))
          now-ms        (System/currentTimeMillis)
          info          (cond-> (merge opts {:dir          dir
                                             :flags        flags
                                             :max-readers  max-readers
                                             :max-dbs      max-dbs
                                             :temp?        temp?
                                             :kv-wal?      (boolean kv-wal?)
                                             :wal-meta-flush-max-txs
                                             (long wal-meta-flush-max-txs)
                                             :wal-meta-flush-max-ms
                                             (long wal-meta-flush-max-ms)
                                             :wal-meta-pending-txs 0
                                             :wal-meta-last-flush-ms now-ms
                                             :kv-overlay-committed-by-tx
                                             (empty-overlay-committed-map)
                                             :kv-overlay-by-dbi {}
                                             :overlay-published-wal-tx-id 0})
                          key-compress (assoc :key-compress key-compress)
                          val-compress (assoc :val-compress val-compress))
          ^CppLMDB lmdb (->CppLMDB env
                                   (volatile! info)
                                   (ThreadLocal.)
                                   (HashMap.)
                                   (volatile! nil)
                                   (new-bufval c/+max-key-size+)
                                   (new-bufval 0)
                                   (new-bufval c/+max-key-size+)
                                   (new-bufval c/+max-key-size+)
                                   (new-bufval c/+max-key-size+)
                                   (new-bufval c/+max-key-size+)
                                   (bf/allocate-buffer c/+max-key-size+)
                                   nil
                                   (volatile! nil)
                                   false
                                   nil
                                   nil
                                   nil)]
      (swap! l/lmdb-dirs conj dir)
      (open-dbi lmdb c/kv-info) ;; never compressed
      (when kv-wal?
        (u/file (str dir u/+separator+ c/wal-dir)))
      (if temp?
        (u/delete-on-exit dir-file)
        (let [k-comp (when (and key-compress
                                (.exists (io/file dir c/keycode-file-name)))
                       (cp/load-key-compressor
                         (str dir u/+separator+ c/keycode-file-name)))
              v-comp (when (and val-compress
                                (.exists (io/file dir c/valcode-file-name)))
                       (cp/load-val-compressor
                         (str dir u/+separator+ c/valcode-file-name)))]
          (init-info lmdb (dissoc info
                                  :kv-wal?
                                  :kv-overlay-committed-by-tx
                                  :kv-overlay-by-dbi
                                  :overlay-published-wal-tx-id))
          (vswap! (.-info lmdb) assoc
                  :kv-wal? (boolean kv-wal?)
                  :kv-overlay-committed-by-tx (empty-overlay-committed-map)
                  :kv-overlay-by-dbi {}
                  :overlay-published-wal-tx-id 0)
          (when kv-wal?
            (refresh-kv-wal-info! lmdb)
            (refresh-kv-wal-meta-info! lmdb))
          (set-max-val-size lmdb (max-val-size lmdb))
          (set-key-compressor lmdb k-comp)
          (set-val-compressor lmdb v-comp)
          (.addShutdownHook (Runtime/getRuntime)
                            (Thread. #(close-kv lmdb)))
          (start-scheduled-sync (.-scheduled-sync lmdb) dir env)))
      lmdb)
    (catch Exception e
      (stt/print-stack-trace e)
      (raise "Fail to open database: " e {:dir dir}))))

(defmethod open-kv :cpp
  ([dir] (open-kv dir {}))
  ([dir opts]
   (assert (string? dir) "directory should be a string.")
   (let [dir-file  (u/file dir)
         db-file   (io/file dir c/data-file-name)
         exist-db? (.exists db-file)
         version   (read-version-file dir-file)]
     (cond
       (not exist-db?)
       (let [lmdb (open-kv* dir dir-file db-file opts)]
         (write-version-file dir-file c/version)
         lmdb)
       version
       (do
         (when (not= version c/version)
           (if-let [{:keys [major minor patch]} (c/parse-version version)]
             (let [{cmajor :major cminor :minor} (c/parse-version c/version)]
               (when (and c/require-migration?
                          (or (< ^long major ^long cmajor)
                              (< ^long minor ^long cminor)))
                 (m/perform-migration dir major minor patch))
               (write-version-file dir-file c/version))
             (raise "Corrupt VERSION file" {:input version})))
         (open-kv* dir dir-file db-file opts))
       :else
       (raise "Database requires migration. Please follow instruction at https://github.com/datalevin/datalevin/blob/master/doc/upgrade.md"
              {:dir dir})))))
