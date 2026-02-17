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
   [datalevin.spill :as sp]
   [datalevin.scan :as scan]
   [datalevin.overlay :as ol]
   [datalevin.interface :as i
    :refer [IList ILMDB IAdmin open-dbi close-kv env-dir close-vecs
            transact-kv get-range range-filter stat key-compressor
            val-compressor set-max-val-size max-val-size get-value
            set-key-compressor set-val-compressor key-range list-range
            bf-compress bf-uncompress]]
   [datalevin.lmdb :as l
    :refer [open-kv IBuffer IRange IRtx IDB IKV IWriting ICompress
            IListRandKeyValIterable IListRandKeyValIterator]])
  (:import
   [datalevin.dtlvnative DTLV DTLV$MDB_envinfo DTLV$MDB_stat DTLV$dtlv_key_iter
    DTLV$dtlv_list_iter DTLV$dtlv_list_sample_iter
    DTLV$dtlv_list_rank_sample_iter DTLV$dtlv_list_val_full_iter DTLV$MDB_val
    DTLV$dtlv_list_key_range_full_val_iter DTLV$dtlv_key_sample_iter
    DTLV$dtlv_key_rank_sample_iter]
   [datalevin.cpp BufVal Env Txn Dbi Cursor Stat Info Util Util$MapFullException]
   [datalevin.lmdb RangeContext KVTxData]
   [datalevin.async IAsyncWork]
   [datalevin.utl BitOps]
   [java.util.concurrent TimeUnit ScheduledExecutorService
    ScheduledFuture]
   [java.lang AutoCloseable]
   [java.io File]
   [java.util Iterator HashMap ArrayDeque Map$Entry]
   [java.util.function Supplier]
   [java.nio BufferOverflowException ByteBuffer]
   [java.nio.channels FileChannel]
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
         ->ListFullValIterable ->ListSampleIterable
         ->ListKeyRangeFullValIterable
         maybe-flush-kv-indexer-on-pressure!)

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
    (let [^BufVal kp      (.-kp ^Rtx rtx)
          ^LongPointer rp (LongPointer. 1)
          rc              (DTLV/mdb_get_key_rank
                            (.get ^Txn (.-txn ^Rtx rtx))
                            (.get db) (.ptr kp) nil rp)]
      (Util/checkRc ^int rc)
      (when-not (= rc DTLV/MDB_NOTFOUND)
        (.get rp))))
  (get-key-by-rank [_ rtx rank]
    (let [^BufVal kp (.-kp ^Rtx rtx)
          ^BufVal vp (.-vp ^Rtx rtx)
          rc         (DTLV/mdb_get_rank
                       (.get ^Txn (.-txn ^Rtx rtx))
                       (.get db) rank (.ptr kp) (.ptr vp))]
      (Util/checkRc ^int rc)
      (when-not (= rc DTLV/MDB_NOTFOUND)
        [(.outBuf kp) (.outBuf vp)])))
  (iterate-key [this rtx cur [range-type k1 k2] k-type]
    (let [ctx  (l/range-info rtx range-type k1 k2 k-type)
          base (->KeyIterable lmdb this cur rtx ctx)]
      (if-let [ov (ol/overlay-for-dbi @(l/kv-info lmdb) (l/write-txn lmdb)
                                       (.dbi-name this))]
        (ol/overlay-iterate-key base ctx ov range-type)
        base)))
  (iterate-key-sample [this rtx cur indices budget step [range-type k1 k2]
                       k-type]
    (let [ctx (l/range-info rtx range-type k1 k2 k-type)]
      (->KeySampleIterable lmdb this indices budget step cur rtx ctx)))
  (iterate-list [this rtx cur [k-range-type k1 k2] k-type
                 [v-range-type v1 v2] v-type]
    (let [ctx  (l/list-range-info rtx k-range-type k1 k2 k-type
                                  v-range-type v1 v2 v-type)
          base (->ListIterable lmdb this cur rtx ctx)]
      (if-let [ov (ol/overlay-for-dbi @(l/kv-info lmdb) (l/write-txn lmdb)
                                       (.dbi-name this))]
        (ol/overlay-iterate-list base ctx ov k-range-type v-range-type)
        base)))
  (iterate-list-sample [this rtx cur indices budget step [k-range-type k1 k2]
                        k-type]
    (let [ctx (l/range-info rtx k-range-type k1 k2 k-type)]
      (->ListSampleIterable lmdb this indices budget step cur rtx ctx)))
  (iterate-list-val-full [this rtx cur]
    (let [base (->ListFullValIterable lmdb this cur rtx)]
      (if-let [ov (ol/overlay-for-dbi @(l/kv-info lmdb) (l/write-txn lmdb)
                                       (.dbi-name this))]
        (ol/wrap-list-full-val-iterable base ov)
        base)))
  (iterate-list-key-range-val-full [this rtx cur [range-type k1 k2]
                                    k-type]
    (let [ctx  (l/range-info rtx range-type k1 k2 k-type)
          base (->ListKeyRangeFullValIterable lmdb this cur rtx ctx)]
      (if-let [ov (ol/overlay-for-dbi @(l/kv-info lmdb) (l/write-txn lmdb)
                                       (.dbi-name this))]
        (ol/overlay-iterate-list-key-range-val-full base ctx ov range-type)
        base)))
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
  (let [validate? (.-validate-data? dbi)]
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

(defn- dbi-dupsort?
  [^HashMap dbis dbi-name]
  (when dbi-name
    (let [^DBI dbi (.get dbis dbi-name)]
      (and dbi (.-dupsort? dbi)))))

(defn- canonical-kv-op
  ([^KVTxData tx fallback-dbi ^HashMap dbis]
   (canonical-kv-op tx fallback-dbi dbis nil))
  ([^KVTxData tx fallback-dbi ^HashMap dbis cached-dbi-bytes]
   (let [dbi-name (or (.-dbi-name tx) fallback-dbi)
         kt       (ol/normalize-kv-type (.-kt tx))]
     {:op        (.-op tx)
      :dbi       dbi-name
      :k         (.-k tx)
      :v         (.-v tx)
      :kt        kt
      :vt        (.-vt tx)
      :flags     (.-flags tx)
      :dupsort?  (dbi-dupsort? dbis dbi-name)
      :k-bytes   (ol/encode-kv-bytes (.-k tx) kt)
      :dbi-bytes (or cached-dbi-bytes
                     (.getBytes ^String dbi-name StandardCharsets/UTF_8))})))

(defn- validate-kv-txs!
  [txs dbi-name kt vt ^HashMap dbis]
  (if dbi-name
    (let [^DBI dbi  (or (.get dbis dbi-name)
                        (raise dbi-name " is not open" {}))
          validate? (.-validate-data? dbi)
          tx-data   (mapv #(l/->kv-tx-data % kt vt) txs)]
      (doseq [tx tx-data]
        (vld/validate-kv-tx-data tx validate?))
      tx-data)
    (let [tx-data (mapv l/->kv-tx-data txs)]
      (doseq [^KVTxData tx tx-data]
        (let [dbi-name  (.-dbi-name tx)
              ^DBI dbi  (or (.get dbis dbi-name)
                            (raise dbi-name " is not open" {}))
              validate? (.-validate-data? dbi)]
          (vld/validate-kv-tx-data tx validate?)))
      tx-data)))

(declare ensure-wal-channel! append-kv-wal-record! refresh-kv-wal-info!
         refresh-kv-wal-meta-info! publish-kv-wal-meta!
         maybe-publish-kv-wal-meta!)

(defn- get-kv-info-id [lmdb k] (i/get-value lmdb c/kv-info k :data :data))

(defn- read-kv-wal-meta [lmdb] (wal/read-wal-meta (env-dir lmdb)))

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

(defn- wal-checkpoint!
  "Flush WAL to base LMDB, persist vector indices, and GC old segments."
  [lmdb info]
  (try
    (i/flush-kv-indexer! lmdb)
    (doseq [idx (keep @l/vector-indices (u/list-files (i/env-dir lmdb)))]
      (i/persist-vecs idx))
    (i/gc-wal-segments! lmdb)
    (vswap! info assoc :wal-checkpoint-fail-count 0)
    (catch Exception e
      (let [n (long (vswap! info update :wal-checkpoint-fail-count
                            (fnil inc 0)))]
        (when (or (= n 1) (zero? (mod n 10)))
          (binding [*out* *err*]
            (println (str "WARNING: WAL checkpoint failed ("
                         n " consecutive failure(s))"))
            (stt/print-stack-trace e)))))))

(defn- start-scheduled-wal-checkpoint
  [info lmdb]
  (let [scheduler ^ScheduledExecutorService (u/get-scheduler)
        fut       (.scheduleWithFixedDelay
                    scheduler
                    ^Runnable #(wal-checkpoint! lmdb info)
                    ^long c/wal-checkpoint-interval
                    ^long c/wal-checkpoint-interval
                    TimeUnit/SECONDS)]
    (vswap! info assoc :scheduled-wal-checkpoint fut)))

(defn- stop-scheduled-wal-checkpoint
  [info]
  (when-let [fut ^ScheduledFuture (:scheduled-wal-checkpoint @info)]
    (.cancel fut true)
    (vswap! info dissoc :scheduled-wal-checkpoint)))

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
                  ^boolean kv-wal
                  ^:unsynchronized-mutable meta]

  IWriting
  (writing? [_] writing?)

  (write-txn [_] write-txn)

  (kv-info [_] info)

  (mark-write [_]
    (->CppLMDB
      env info tl-reader dbis scheduled-sync kp-w vp-w start-kp-w
      stop-kp-w start-vp-w stop-vp-w k-comp-bf-w v-comp-bf-w
      write-txn true k-comp v-comp kv-wal meta))

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
      (stop-scheduled-wal-checkpoint info)
      ;; Sync any unsynced WAL records before closing — force at least
      ;; fdatasync regardless of configured sync mode so that the
      ;; watermark promotion below is truthful after unclean shutdown.
      (when-let [^FileChannel ch (:wal-channel @info)]
        (when (.isOpen ch)
          (let [sync-mode (or (:wal-sync-mode @info) c/*wal-sync-mode*)
                close-mode (if (= sync-mode :none) :fdatasync sync-mode)]
            (try (wal/sync-channel! ch close-mode) (catch Exception _ nil)))))
      ;; Promote synced watermark after final sync
      (when (:kv-wal? @info)
        (when-let [committed (:last-committed-wal-tx-id @info)]
          (vswap! info assoc :last-synced-wal-tx-id committed)))
      ;; Flush WAL meta so reopen recovers the correct committed watermark
      (when (:kv-wal? @info)
        (when-let [wal-id (:last-committed-wal-tx-id @info)]
          (when (pos? ^long wal-id)
            (try (publish-kv-wal-meta! this wal-id
                                       (System/currentTimeMillis))
                 (catch Exception e
                   (binding [*out* *err*]
                     (println "WARNING: Failed to flush WAL meta on close")
                     (stt/print-stack-trace e)))))))
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
              wal-enabled? (and kv-wal (not c/*bypass-wal*))
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
                (locking write-txn
                  (run-writer-step! :step-4 (fn [] nil))
                  (run-writer-step! :step-5
                                    #(ol/publish-kv-committed-overlay!
                                       info write-txn wal-id
                                       (:ops wal-entry)))
                  (run-writer-step! :step-6 (fn [] nil))
                  (run-writer-step! :step-7
                                    #(ol/publish-kv-overlay-watermark!
                                       info wal-id))))
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
              (when wal-id
                (maybe-flush-kv-indexer-on-pressure! this))
              (when (.-wal-ops wtxn)
                (vreset! (.-wal-ops wtxn) []))
              (ol/reset-private-overlay! wtxn info)
              (if aborted? :aborted :committed))
            (catch Exception e
              (when (and wal-enabled? (not aborted?))
                (refresh-kv-wal-info! this)
                (refresh-kv-wal-meta-info! this)
                (ol/recover-kv-overlay! info write-txn (env-dir this)))
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
      (ol/reset-private-overlay! wtxn info)
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
            wal-enabled? (and kv-wal (not c/*bypass-wal*))
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
                                          nil nil)]
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
                (ol/publish-kv-committed-overlay! info write-txn wal-id
                                                  (:ops wal-entry))
                (vld/check-failpoint :step-5 :after)
                (vld/check-failpoint :step-6 :before)
                (vld/check-failpoint :step-6 :after)
                (vld/check-failpoint :step-7 :before)
                (ol/publish-kv-overlay-watermark! info wal-id)
                (vld/check-failpoint :step-7 :after)
                (try
                  (vld/check-failpoint :step-8 :before)
                  (maybe-publish-kv-wal-meta! this wal-id)
                  (vld/check-failpoint :step-8 :after)
                  (catch Exception _ nil)))
              (when wal-id
                (maybe-flush-kv-indexer-on-pressure! this))
              :transacted)
            (catch Exception e
              (refresh-kv-wal-info! this)
              (refresh-kv-wal-meta-info! this)
              (ol/recover-kv-overlay! info write-txn (env-dir this))
              (if-let [edata (ex-data e)]
                (raise "Fail to transact to LMDB: " e edata)
                (raise "Fail to transact to LMDB: " e {}))))
          ;; Standard path with LMDB transaction
          (let [^Txn txn (if one-shot?
                           (Txn/create env)
                           (.-txn rtx))]
            (try
              (if (not wal-enabled?)
                ;; Direct LMDB write — transact1*/transact* validate internally
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
                ;; WAL inside open-transact-kv/close-transact-kv — needs validation + ops
                (let [tx-data    (validate-kv-txs! txs dbi-name k-type v-type dbis)
                      dbi-bs     (when dbi-name
                                   (.getBytes ^String dbi-name StandardCharsets/UTF_8))
                      ops        (->> tx-data
                                      (mapv #(canonical-kv-op % dbi-name dbis dbi-bs))
                                      vec)
                      max-val-op (when (:max-val-size-changed? @info)
                                   (let [tx (l/->kv-tx-data
                                              [:put :max-val-size
                                               (:max-val-size @info)]
                                              nil nil)]
                                     (vswap! info assoc :max-val-size-changed? false)
                                     (canonical-kv-op tx c/kv-info dbis)))
                      ops        (if max-val-op (conj ops max-val-op) ops)]
                  (when (seq ops)
                    (vswap! (.-wal-ops rtx) into ops)
                    (ol/publish-kv-private-overlay! rtx info ops))
                  :transacted))
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
    (when-let [^FileChannel ch (:wal-channel @info)]
      (when (.isOpen ch)
        (try (.force ch true) (catch Exception _ nil))))
    (let [res (l/flush-kv-indexer! this upto-wal-id)]
      ;; Hold write-txn lock while updating watermarks and pruning overlay
      ;; so we don't race with a concurrent publish-kv-committed-overlay!.
      (locking write-txn
        (vswap! (.-info this) assoc
                :last-indexed-wal-tx-id (:indexed-wal-tx-id res)
                :applied-wal-tx-id (or (get-kv-info-id this c/applied-wal-tx-id)
                                       0))
        (ol/prune-kv-committed-overlay! info (:indexed-wal-tx-id res)))
      res))

  (open-tx-log [this from-wal-id]
    (l/open-tx-log this from-wal-id))
  (open-tx-log [this from-wal-id upto-wal-id]
    (l/open-tx-log this from-wal-id upto-wal-id))

  (gc-wal-segments! [this]
    (l/gc-wal-segments! this))
  (gc-wal-segments! [this retain-wal-id]
    (l/gc-wal-segments! this retain-wal-id))

  (get-value [this dbi-name k]
    (.get-value this dbi-name k :data :data true))
  (get-value [this dbi-name k k-type]
    (.get-value this dbi-name k k-type :data true))
  (get-value [this dbi-name k k-type v-type]
    (.get-value this dbi-name k k-type v-type true))
  (get-value [this dbi-name k k-type v-type ignore-key?]
    (if kv-wal
      (let [ret (ol/overlay-get-value this dbi-name k k-type v-type ignore-key?)]
        (case ret
          ::ol/overlay-miss
          (scan/get-value this dbi-name k k-type v-type ignore-key?)
          ::ol/overlay-tombstone nil
          ret))
      (scan/get-value this dbi-name k k-type v-type ignore-key?)))

  (get-rank [this dbi-name k]
    (.get-rank this dbi-name k :data))
  (get-rank [this dbi-name k k-type]
    (if (ol/kv-overlay-active? @info write-txn dbi-name)
      (ol/overlay-rank this dbi-name k k-type)
      (scan/get-rank this dbi-name k k-type)))

  (get-by-rank [this dbi-name rank]
    (.get-by-rank this dbi-name rank :data :data true))
  (get-by-rank [this dbi-name rank k-type]
    (.get-by-rank this dbi-name rank k-type :data true))
  (get-by-rank [this dbi-name rank k-type v-type]
    (.get-by-rank this dbi-name rank k-type v-type true))
  (get-by-rank [this dbi-name rank k-type v-type ignore-key?]
    (if (ol/kv-overlay-active? @info write-txn dbi-name)
      (ol/overlay-get-by-rank this dbi-name rank k-type v-type ignore-key?)
      (scan/get-by-rank this dbi-name rank k-type v-type ignore-key?)))

  (sample-kv [this dbi-name n]
    (.sample-kv this dbi-name n :data :data true))
  (sample-kv [this dbi-name n k-type]
    (.sample-kv this dbi-name n k-type :data true))
  (sample-kv [this dbi-name n k-type v-type]
    (.sample-kv this dbi-name n k-type v-type true))
  (sample-kv [this dbi-name n k-type v-type ignore-key?]
    (if (ol/kv-overlay-active? @info write-txn dbi-name)
      (ol/overlay-sample-kv this dbi-name n k-type v-type ignore-key?)
      (scan/sample-kv this dbi-name n k-type v-type ignore-key?)))

  (get-first [this dbi-name k-range]
    (.get-first this dbi-name k-range :data :data false))
  (get-first [this dbi-name k-range k-type]
    (.get-first this dbi-name k-range k-type :data false))
  (get-first [this dbi-name k-range k-type v-type]
    (.get-first this dbi-name k-range k-type v-type false))
  (get-first [this dbi-name k-range k-type v-type ignore-key?]
    (scan/get-first this dbi-name k-range k-type v-type ignore-key?))

  (get-first-n [this dbi-name n k-range]
    (.get-first-n this dbi-name n k-range :data :data false))
  (get-first-n [this dbi-name n k-range k-type]
    (.get-first-n this dbi-name n k-range k-type :data false))
  (get-first-n [this dbi-name n k-range k-type v-type]
    (.get-first-n this dbi-name n k-range k-type v-type false))
  (get-first-n [this dbi-name n k-range k-type v-type ignore-key?]
    (scan/get-first-n this dbi-name n k-range k-type v-type ignore-key?))

  (get-range [this dbi-name k-range]
    (.get-range this dbi-name k-range :data :data false))
  (get-range [this dbi-name k-range k-type]
    (.get-range this dbi-name k-range k-type :data false))
  (get-range [this dbi-name k-range k-type v-type]
    (.get-range this dbi-name k-range k-type v-type false))
  (get-range [this dbi-name k-range k-type v-type ignore-key?]
    (scan/get-range this dbi-name k-range k-type v-type ignore-key?))

  (key-range [this dbi-name k-range]
    (.key-range this dbi-name k-range :data))
  (key-range [this dbi-name k-range k-type]
    (scan/key-range this dbi-name k-range k-type))

  (visit-key-range [this dbi-name visitor k-range]
    (.visit-key-range this dbi-name visitor k-range :data true))
  (visit-key-range [this dbi-name visitor k-range k-type]
    (.visit-key-range this dbi-name visitor k-range k-type true))
  (visit-key-range [this dbi-name visitor k-range k-type raw-pred?]
    (scan/visit-key-range this dbi-name visitor k-range k-type raw-pred?))

  (key-range-count [lmdb dbi-name k-range]
    (.key-range-count lmdb dbi-name k-range :data))
  (key-range-count [lmdb dbi-name k-range k-type]
    (.key-range-count lmdb dbi-name k-range k-type nil))
  (key-range-count [lmdb dbi-name [range-type k1 k2 :as k-range] k-type cap]
    (if kv-wal
      (ol/wal-key-range-count lmdb dbi-name k-range k-type cap)
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
    (scan/range-seq this dbi-name k-range k-type v-type ignore-key? opts))

  (range-count [this dbi-name k-range]
    (.range-count this dbi-name k-range :data))
  (range-count [lmdb dbi-name k-range k-type]
    (let [dupsort? (.-dupsort? ^DBI (.get dbis dbi-name))]
      (if dupsort?
        (.list-range-count lmdb dbi-name k-range k-type [:all] nil)
        (.key-range-count lmdb dbi-name k-range k-type))))

  (get-some [this dbi-name pred k-range]
    (.get-some this dbi-name pred k-range :data :data false true))
  (get-some [this dbi-name pred k-range k-type]
    (.get-some this dbi-name pred k-range k-type :data false true))
  (get-some [this dbi-name pred k-range k-type v-type]
    (.get-some this dbi-name pred k-range k-type v-type false true))
  (get-some [this dbi-name pred k-range k-type v-type ignore-key?]
    (.get-some this dbi-name pred k-range k-type v-type  ignore-key? true))
  (get-some [this dbi-name pred k-range k-type v-type ignore-key? raw-pred?]
    (scan/get-some this dbi-name pred k-range k-type v-type ignore-key?
                   raw-pred?))

  (range-filter [this dbi-name pred k-range]
    (.range-filter this dbi-name pred k-range :data :data false true))
  (range-filter [this dbi-name pred k-range k-type]
    (.range-filter this dbi-name pred k-range k-type :data false true))
  (range-filter [this dbi-name pred k-range k-type v-type]
    (.range-filter this dbi-name pred k-range k-type v-type false true))
  (range-filter [this dbi-name pred k-range k-type v-type ignore-key?]
    (.range-filter this dbi-name pred k-range k-type v-type  ignore-key? true))
  (range-filter [this dbi-name pred k-range k-type v-type ignore-key? raw-pred?]
    (scan/range-filter this dbi-name pred k-range k-type v-type ignore-key?
                       raw-pred?))

  (range-keep [this dbi-name pred k-range]
    (.range-keep this dbi-name pred k-range :data :data true))
  (range-keep [this dbi-name pred k-range k-type]
    (.range-keep this dbi-name pred k-range k-type :data true))
  (range-keep [this dbi-name pred k-range k-type v-type]
    (.range-keep this dbi-name pred k-range k-type v-type true))
  (range-keep [this dbi-name pred k-range k-type v-type raw-pred?]
    (scan/range-keep this dbi-name pred k-range k-type v-type raw-pred?))

  (range-some [this dbi-name pred k-range]
    (.range-some this dbi-name pred k-range :data :data true))
  (range-some [this dbi-name pred k-range k-type]
    (.range-some this dbi-name pred k-range k-type :data true))
  (range-some [this dbi-name pred k-range k-type v-type]
    (.range-some this dbi-name pred k-range k-type v-type true))
  (range-some [this dbi-name pred k-range k-type v-type raw-pred?]
    (scan/range-some this dbi-name pred k-range k-type v-type raw-pred?))

  (range-filter-count [this dbi-name pred k-range]
    (.range-filter-count this dbi-name pred k-range :data :data true))
  (range-filter-count [this dbi-name pred k-range k-type]
    (.range-filter-count this dbi-name pred k-range k-type :data true))
  (range-filter-count [this dbi-name pred k-range k-type v-type]
    (.range-filter-count this dbi-name pred k-range k-type v-type true))
  (range-filter-count [this dbi-name pred k-range k-type v-type raw-pred?]
    (scan/range-filter-count this dbi-name pred k-range k-type
                             v-type raw-pred?))

  (visit [this dbi-name visitor k-range]
    (.visit this dbi-name visitor k-range :data :data true))
  (visit [this dbi-name visitor k-range k-type]
    (.visit this dbi-name visitor k-range k-type :data true))
  (visit [this dbi-name visitor k-range k-type v-type]
    (.visit this dbi-name visitor k-range k-type v-type true))
  (visit [this dbi-name visitor k-range k-type v-type raw-pred?]
    (scan/visit this dbi-name visitor k-range k-type v-type raw-pred?))

  (visit-key-sample
    [db dbi-name indices budget step visitor k-range k-type]
    (.visit-key-sample
      db dbi-name indices budget step visitor k-range k-type true))
  (visit-key-sample
    [db dbi-name indices budget step visitor k-range k-type raw-pred?]
    (if (ol/kv-overlay-active? @info write-txn dbi-name)
      (ol/overlay-visit-key-sample db dbi-name indices visitor k-range
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
    (scan/get-list this dbi-name k kt vt))

  (visit-list [this dbi-name visitor k kt]
    (.visit-list this dbi-name visitor k kt :data true))
  (visit-list [this dbi-name visitor k kt vt]
    (.visit-list this dbi-name visitor k kt vt true))
  (visit-list [this dbi-name visitor k kt vt raw-pred?]
    (scan/visit-list this dbi-name visitor k kt vt raw-pred?))

  (list-count [lmdb dbi-name k kt]
    (if kv-wal
      (if k
        (count (.get-list lmdb dbi-name k kt :raw))
        0)
      (do
        (.check-ready lmdb)
        (if k
          (scan/scan
            (list-count* rtx cur k kt)
            (raise "Fail to count list: " e {:dbi dbi-name :k k}))
          0))))

  (near-list [lmdb dbi-name k v kt vt]
    (if kv-wal
      (when (and k v)
        (scan/list-range-first-raw-v
          lmdb dbi-name [:closed k k] kt [:at-least v] vt))
      (do
        (.check-ready lmdb)
        (scan/scan
          (near-list* rtx cur k kt v vt)
          (raise "Fail to get an item that is near in a list: "
                 e {:dbi dbi-name :k k :v v})))))

  (in-list? [lmdb dbi-name k v kt vt]
    (if kv-wal
      (if (and k v)
        (boolean (seq (.list-range lmdb dbi-name [:closed k k] kt
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
    (if kv-wal
      (ol/wal-key-range-list-count lmdb dbi-name k-range k-type cap)
      (if (l/dlmdb?)
        (key-range-list-count-fast lmdb dbi-name k-range k-type cap)
        (key-range-list-count-slow lmdb dbi-name k-range k-type cap budget))))

  (list-range [this dbi-name k-range kt v-range vt]
    (scan/list-range this dbi-name k-range kt v-range vt))

  (list-range-count [lmdb dbi-name k-range kt v-range vt]
    (.list-range-count lmdb dbi-name k-range kt v-range vt nil))
  (list-range-count [lmdb dbi-name [k-range-type k1 k2] k-type
                     [v-range-type v1 v2] v-type cap]
    (if kv-wal
      (ol/wal-list-range-count lmdb dbi-name
                               [k-range-type k1 k2] k-type
                               [v-range-type v1 v2] v-type cap)
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
    (scan/list-range-first this dbi-name k-range kt v-range vt))

  (list-range-first-n [this dbi-name n k-range kt v-range vt]
    (scan/list-range-first-n this dbi-name n k-range kt v-range vt))

  (list-range-filter [this dbi-name pred k-range kt v-range vt]
    (.list-range-filter this dbi-name pred k-range kt v-range vt true))
  (list-range-filter [this dbi-name pred k-range kt v-range vt raw-pred?]
    (scan/list-range-filter this dbi-name pred k-range kt v-range vt raw-pred?))

  (list-range-keep [this dbi-name pred k-range kt v-range vt]
    (.list-range-keep this dbi-name pred k-range kt v-range vt true))
  (list-range-keep [this dbi-name pred k-range kt v-range vt raw-pred?]
    (scan/list-range-keep this dbi-name pred k-range kt v-range vt raw-pred?))

  (list-range-some [this list-name pred k-range k-type v-range v-type]
    (.list-range-some this list-name pred k-range k-type v-range v-type
                      true))
  (list-range-some [this dbi-name pred k-range kt v-range vt raw-pred?]
    (scan/list-range-some this dbi-name pred k-range kt v-range vt raw-pred?))

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
    (scan/list-range-filter-count this dbi-name pred k-range kt v-range
                                  vt raw-pred? cap))

  (visit-list-range
    [this list-name visitor k-range k-type v-range v-type]
    (.visit-list-range this list-name visitor k-range k-type v-range
                       v-type true))
  (visit-list-range
    [this dbi-name visitor k-range kt v-range vt raw-pred?]
    (scan/visit-list-range this dbi-name visitor k-range kt v-range
                           vt raw-pred?))

  (visit-list-key-range
    [this dbi-name visitor k-range k-type v-type]
    (.visit-list-key-range this dbi-name visitor k-range k-type
                           v-type true))
  (visit-list-key-range
    [this dbi-name visitor k-range k-type v-type raw-pred?]
    (scan/visit-list-key-range this dbi-name visitor k-range k-type
                               v-type raw-pred?))

  (visit-list-sample
    [this list-name indices budget step visitor k-range k-type v-type]
    (.visit-list-sample this list-name indices budget step visitor k-range
                        k-type v-type true))
  (visit-list-sample
    [this dbi-name indices budget step visitor k-range kt vt raw-pred?]
    (if (ol/kv-overlay-active? @info write-txn dbi-name)
      (ol/overlay-visit-list-sample this dbi-name indices visitor k-range
                                    kt vt raw-pred?)
      (scan/visit-list-sample this dbi-name indices budget step visitor k-range
                              kt vt raw-pred?)))

  IAdmin
  (re-index [this opts] (l/re-index* this opts)))

(defn- ensure-wal-channel!
  "Return a cached FileChannel for the given segment, opening a new one if
  needed (or if the segment rotated). Closes the old channel on rotation."
  [^CppLMDB lmdb dir ^long seg-id]
  (let [info            @(.-info lmdb)
        ^FileChannel ch (:wal-channel info)
        ^long ch-seg    (or (:wal-channel-segment-id info) -1)]
    (if (and ch (.isOpen ch) (= ch-seg seg-id))
      ch
      (do (wal/close-segment-channel! ch)
          (let [new-ch (wal/open-segment-channel dir seg-id)]
            (vswap! (.-info lmdb) assoc
                    :wal-channel new-ch
                    :wal-channel-segment-id seg-id)
            new-ch)))))

(defn- append-kv-wal-record!
  [^CppLMDB lmdb ops tx-time]
  (when (seq ops)
    (let [now-ms       (or tx-time (System/currentTimeMillis))
          dir          (env-dir lmdb)]
      (locking (.-info lmdb)
        (let [info         @(.-info lmdb)
              max-bytes    (or (:wal-segment-max-bytes info)
                               c/*wal-segment-max-bytes*)
              max-ms       (or (:wal-segment-max-ms info)
                               c/*wal-segment-max-ms*)
              sync-mode    (or (:wal-sync-mode info)
                               c/*wal-sync-mode*)
              group-size   (or (:wal-group-commit info)
                               c/*wal-group-commit*)
              group-ms     (or (:wal-group-commit-ms info)
                               c/*wal-group-commit-ms*)
              wal-id       (u/long-inc (or (:wal-next-tx-id info) 0))
              user-tx-id   wal-id
              record       (wal/record-bytes wal-id user-tx-id tx-time ops)
              record-bytes (alength ^bytes record)
              seg-id       (or (:wal-segment-id info) 1)
              seg-bytes    (or (:wal-segment-bytes info) 0)
              seg-start-ms (or (:wal-segment-created-ms info) now-ms)
              rotate?      (or (>= (+ ^long seg-bytes record-bytes)
                                   ^long max-bytes)
                               (>= (- ^long now-ms ^long seg-start-ms)
                                   ^long max-ms))
              new-seg-id   (if rotate? (u/long-inc seg-id) seg-id)
              new-seg-start-ms (if rotate? now-ms seg-start-ms)
              new-seg-bytes    (if rotate? 0 seg-bytes)
              unsynced     (u/long-inc (or (:wal-unsynced-count info) 0))
              last-sync-ms (or (:wal-last-sync-ms info) 0)
              sync?        (or rotate?
                               (>= ^long unsynced ^long group-size)
                               (and (pos? ^long group-ms)
                                    (pos? ^long last-sync-ms)
                                    (>= (- ^long now-ms ^long last-sync-ms)
                                        ^long group-ms)))
              ch           (ensure-wal-channel! lmdb dir new-seg-id)]
          (.write ^FileChannel ch (ByteBuffer/wrap record))
          (when sync? (wal/sync-channel! ch sync-mode))
          ;; Use vswap! to merge updates without losing channel changes
          (vswap! (.-info lmdb)
                  (fn [current-info]
                    (cond-> (assoc current-info
                                   :wal-next-tx-id wal-id
                                   :last-committed-wal-tx-id wal-id
                                   :last-committed-user-tx-id wal-id
                                   :committed-last-modified-ms now-ms
                                   :wal-segment-id new-seg-id
                                   :wal-segment-created-ms new-seg-start-ms
                                   :wal-segment-bytes (+ ^long new-seg-bytes record-bytes)
                                   :wal-unsynced-count (if sync? 0 unsynced)
                                   :wal-last-sync-ms (if sync? now-ms last-sync-ms))
                      sync? (assoc :last-synced-wal-tx-id wal-id))))
          {:wal-id  wal-id
           :tx-time now-ms
           :ops     ops})))))

(defn- refresh-kv-wal-info!
  [^CppLMDB lmdb]
  (try
    (let [dir          (env-dir lmdb)
          now-ms       (System/currentTimeMillis)
          meta         (wal/read-wal-meta dir)
          scanned      (when-not meta (wal/scan-last-wal dir))
          committed-id ^long (or (get meta c/last-committed-wal-tx-id)
                                 (:last-wal-id scanned)
                                 0)
          indexed-id   ^long (or (get-kv-info-id lmdb c/last-indexed-wal-tx-id)
                                 (get meta c/last-indexed-wal-tx-id)
                                 0)
          committed-ms ^long (or (get meta c/committed-last-modified-ms) 0)
          user-id      ^long (or (get meta c/last-committed-user-tx-id)
                                 committed-id)
          next-id      committed-id
          applied-id   ^long (or (get-kv-info-id lmdb c/applied-wal-tx-id)
                                 (get-kv-info-id lmdb c/legacy-applied-tx-id)
                                 0)
          seg-id       ^long (or (get meta :wal/last-segment-id)
                                 (:last-segment-id scanned)
                                 1)
          seg-file     (io/file (wal/segment-path dir seg-id))
          seg-bytes    (if (.exists seg-file) (.length seg-file) 0)
          seg-created  ^long (or (:last-segment-ms scanned)
                                 (when (.exists seg-file)
                                   (.lastModified seg-file))
                                 now-ms)]
      (vswap! (.-info lmdb) assoc
              :wal-next-tx-id next-id
              :applied-wal-tx-id applied-id
              :last-committed-wal-tx-id committed-id
              :last-synced-wal-tx-id committed-id
              :last-indexed-wal-tx-id indexed-id
              :last-committed-user-tx-id user-id
              :committed-last-modified-ms committed-ms
              :overlay-published-wal-tx-id committed-id
              :wal-segment-id seg-id
              :wal-segment-created-ms seg-created
              :wal-segment-bytes seg-bytes))
    (catch Exception e
      (stt/print-stack-trace e)
      (raise "Fail to read WAL info: " e {:dir (env-dir lmdb)}))))

(defn- refresh-kv-wal-meta-info!
  [^CppLMDB lmdb]
  (let [now-ms (System/currentTimeMillis)]
    (if-let [meta (read-kv-wal-meta lmdb)]
      (let [^long last-ms (or (get meta c/committed-last-modified-ms) 0)]
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

(defn- publish-kv-wal-meta!
  [^CppLMDB lmdb wal-id now-ms]
  (let [info      @(.-info lmdb)
        ;; Only persist watermarks that have been durably synced to avoid
        ;; claiming records are committed when they may be lost on crash.
        synced-id ^long (or (:last-synced-wal-tx-id info) 0)
        durable-id (min ^long wal-id synced-id)
        seg-id    (or (:wal-segment-id info) 0)
        indexed   (or (:last-indexed-wal-tx-id info) 0)
        user-id   (or (:last-committed-user-tx-id info) durable-id)
        commit-ms (or (:committed-last-modified-ms info) now-ms)
        snapshot  {c/last-committed-wal-tx-id   durable-id
                   c/last-indexed-wal-tx-id     indexed
                   c/last-committed-user-tx-id  (min ^long user-id
                                                     ^long durable-id)
                   c/committed-last-modified-ms commit-ms
                   :wal/last-segment-id         seg-id
                   :wal/enabled?                true}
        meta'     (wal/publish-wal-meta! (env-dir lmdb) snapshot)
        rev       (or (:wal-meta-revision meta') 0)]
    (vswap! (.-info lmdb) assoc
            :wal-meta-revision rev
            :wal-meta-last-modified-ms now-ms
            :wal-meta-last-flush-ms now-ms
            :wal-meta-pending-txs 0)
    meta'))

(defn- maybe-publish-kv-wal-meta!
  [^CppLMDB lmdb wal-id]
  (let [now-ms     (System/currentTimeMillis)
        info'      (vswap! (.-info lmdb) update :wal-meta-pending-txs
                           (fnil u/long-inc 0))
        pending    (or (:wal-meta-pending-txs info') 0)
        flush-txs  (max 1 (long (or (:wal-meta-flush-max-txs info')
                                    c/*wal-meta-flush-max-txs*)))
        flush-ms   (max 0 (long (or (:wal-meta-flush-max-ms info')
                                    c/*wal-meta-flush-max-ms*)))
        last-flush (or (:wal-meta-last-flush-ms info') 0)
        due?       (or (>= ^long pending ^long flush-txs)
                       (>= (- ^long now-ms ^long last-flush) ^long flush-ms))]
    (when due?
      (publish-kv-wal-meta! lmdb wal-id now-ms))))

(defn- maybe-flush-kv-indexer-on-pressure!
  [^CppLMDB lmdb]
  (try
    (let [info @(.-info lmdb)]
      (when (:kv-wal? info)
        (sp/memory-updater)
        (let [pressure  @sp/memory-pressure
              threshold (or (get-in info [:spill-opts :spill-threshold])
                            c/default-spill-threshold)
              committed (or (:last-committed-wal-tx-id info) 0)
              indexed   (or (:last-indexed-wal-tx-id info) 0)]
          (when (and (>= ^long pressure ^long threshold)
                     (> ^long committed ^long indexed))
            (i/flush-kv-indexer! lmdb)))))
    (catch Exception e
      (binding [*out* *err*]
        (println "WARNING: WAL memory-pressure flush failed")
        (stt/print-stack-trace e)))))

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
                                wal-group-commit-ms
                                wal-retention-bytes
                                wal-retention-ms
                                key-compress val-compress]
                         :or   {max-readers      c/*max-readers*
                                max-dbs          c/*max-dbs*
                                mapsize          c/*init-db-size*
                                flags            c/default-env-flags
                                temp?            false
                                wal-meta-flush-max-txs
                                c/*wal-meta-flush-max-txs*
                                wal-meta-flush-max-ms
                                c/*wal-meta-flush-max-ms*
                                wal-group-commit c/*wal-group-commit*
                                wal-group-commit-ms
                                c/*wal-group-commit-ms*
                                wal-retention-bytes
                                c/*wal-retention-bytes*
                                wal-retention-ms
                                c/*wal-retention-ms*
                                kv-wal?          c/*enable-kv-wal*}
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
          info          (cond-> (merge opts
                                       {:dir          dir
                                        :flags        flags
                                        :max-readers  max-readers
                                        :max-dbs      max-dbs
                                        :temp?        temp?
                                        :kv-wal?      (boolean kv-wal?)
                                        :wal-meta-flush-max-txs
                                        wal-meta-flush-max-txs
                                        :wal-meta-flush-max-ms
                                        wal-meta-flush-max-ms
                                        :wal-group-commit-ms
                                        wal-group-commit-ms
                                        :wal-retention-bytes
                                        wal-retention-bytes
                                        :wal-retention-ms
                                        wal-retention-ms
                                        :wal-meta-pending-txs 0
                                        :wal-meta-last-flush-ms now-ms
                                        :wal-last-sync-ms now-ms
                                        :wal-indexer-last-flush-ms now-ms
                                        :wal-indexer-last-flush-duration-ms 0
                                        :kv-overlay-committed-by-tx
                                        (ol/empty-overlay-committed-map)
                                        :kv-overlay-by-dbi {}
                                        :kv-overlay-private-entries 0
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
                                   (boolean kv-wal?)
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
          (vswap! (l/kv-info lmdb) assoc
                  :kv-wal? (boolean kv-wal?)
                  :kv-overlay-committed-by-tx (ol/empty-overlay-committed-map)
                  :kv-overlay-by-dbi {}
                  :overlay-published-wal-tx-id 0)
          (when kv-wal?
            (refresh-kv-wal-info! lmdb)
            (refresh-kv-wal-meta-info! lmdb)
            (ol/recover-kv-overlay! (l/kv-info lmdb) (l/write-txn lmdb)
                                    (env-dir lmdb)))
          (set-max-val-size lmdb (max-val-size lmdb))
          (set-key-compressor lmdb k-comp)
          (set-val-compressor lmdb v-comp)
          (.addShutdownHook (Runtime/getRuntime)
                            (Thread. #(close-kv lmdb)))
          (start-scheduled-sync (.-scheduled-sync lmdb) dir env)
          (when kv-wal?
            (start-scheduled-wal-checkpoint (l/kv-info lmdb) lmdb))))
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
