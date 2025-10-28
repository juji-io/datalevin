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
   [datalevin.bits :as b]
   [datalevin.util :as u :refer [raise]]
   [datalevin.constants :as c]
   [datalevin.async :as a]
   [datalevin.scan :as scan]
   [datalevin.vector :as v]
   [datalevin.lmdb :as l
    :refer [open-kv IBuffer IRange IRtx IDB IKV IList ILMDB IWriting IAdmin
            IListRandKeyValIterable IListRandKeyValIterator]])
  (:import
   [datalevin.dtlvnative DTLV DTLV$MDB_envinfo DTLV$MDB_stat DTLV$dtlv_key_iter
    DTLV$dtlv_list_iter DTLV$dtlv_list_sample_iter DTLV$dtlv_list_val_iter
    DTLV$dtlv_list_val_full_iter DTLV$MDB_val]
   [datalevin.cpp BufVal Env Txn Dbi Cursor Stat Info Util
    Util$BadReaderLockException Util$MapFullException]
   [datalevin.lmdb RangeContext KVTxData]
   [datalevin.async IAsyncWork]
   [datalevin.utl BitOps]
   [java.util.concurrent TimeUnit ScheduledExecutorService ScheduledFuture]
   [java.lang AutoCloseable]
   [java.util Iterator HashMap ArrayDeque]
   [java.util.function Supplier]
   [java.nio BufferOverflowException ByteBuffer]
   [org.bytedeco.javacpp SizeTPointer LongPointer]
   [clojure.lang IObj]))

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

(deftype KV [^BufVal kp ^BufVal vp]
  IKV
  (k [_] (.outBuf kp))
  (v [_] (.outBuf vp)))

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
  [^BufVal vp k kt]
  (when-some [x k]
    (let [^ByteBuffer bf (.inBuf vp)]
      (.clear bf)
      (b/put-buffer bf x kt)
      (.flip bf)
      (.reset vp))))

(deftype Rtx [lmdb
              ^Txn txn
              ^BufVal kp
              ^BufVal vp
              ^BufVal start-kp
              ^BufVal stop-kp
              ^BufVal start-vp
              ^BufVal stop-vp
              aborted?]

  IBuffer
  (put-key [_ x t]
    (try
      (put-bufval kp x t)
      (catch BufferOverflowException _
        (raise "Key cannot be larger than 511 bytes." {:input x}))
      (catch Exception e
        (raise "Error putting read-only transaction key buffer: "
               e {:value x :type t}))))
  (put-val [_ _ _]
    (raise "put-val not allowed for read only txn buffer" {}))

  IRange
  (range-info [_ range-type k1 k2 kt]
    (put-bufval start-kp k1 kt)
    (put-bufval stop-kp k2 kt)
    (l/range-table range-type start-kp stop-kp))

  (list-range-info [_ k-range-type k1 k2 kt v-range-type v1 v2 vt]
    (put-bufval start-kp k1 kt)
    (put-bufval stop-kp k2 kt)
    (put-bufval start-vp v1 vt)
    (put-bufval stop-vp v2 vt)
    [(l/range-table k-range-type start-kp stop-kp)
     (l/range-table v-range-type start-vp stop-vp)])

  IRtx
  (read-only? [_] (.isReadOnly txn))
  (close-rtx [_] (.close txn))
  (reset [this] (.reset txn) this)
  (renew [this] (.renew txn) this))

(defn- stat-map [^Stat stat]
  {:psize          (.ms_psize ^DTLV$MDB_stat (.get stat))
   :depth          (.ms_depth ^DTLV$MDB_stat (.get stat))
   :branch-pages   (.ms_branch_pages ^DTLV$MDB_stat (.get stat))
   :leaf-pages     (.ms_leaf_pages ^DTLV$MDB_stat (.get stat))
   :overflow-pages (.ms_overflow_pages ^DTLV$MDB_stat (.get stat))
   :entries        (.ms_entries ^DTLV$MDB_stat (.get stat))})

(declare ->KeyIterable ->ListIterable ->ListRandKeyValIterable
         ->ListFullValIterable ->ListSampleIterable)

(deftype DBI [^Dbi db
              ^Pool curs
              ^BufVal kp
              ^:volatile-mutable ^BufVal vp
              ^boolean dupsort?
              ^boolean counted?
              ^boolean validate-data?]
  IBuffer
  (put-key [this x t]
    (or (not validate-data?)
        (b/valid-data? x t)
        (raise "Invalid data, expecting " t " got " x {:input x}))
    (try
      (if (nil? x)
        (raise "Key cannot be nil" {})
        (put-bufval kp x t))
      (catch BufferOverflowException _
        (raise "Key cannot be larger than 511 bytes." {:input x}))
      (catch Exception e
        (raise "Error putting r/w key buffer of "
               (.dbi-name this)": " e {:value x :type t}))))
  (put-val [this x t]
    (or (not validate-data?)
        (b/valid-data? x t)
        (raise "Invalid data, expecting " t " got " x {:input x}))
    (try
      (if (nil? x)
        (raise "Value cannot be nil" {})
        (put-bufval vp x t))
      (catch BufferOverflowException _
        (let [size (* ^long c/+buffer-grow-factor+ ^long (b/measure-size x))]
          (set! vp (new-bufval size))
          (put-bufval vp x t)))
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
      (Util/checkRc rc)
      (when-not (= rc DTLV/MDB_NOTFOUND)
        (.outBuf vp))))
  (iterate-key [this rtx cur [range-type k1 k2] k-type]
    (let [ctx (l/range-info rtx range-type k1 k2 k-type)]
      (->KeyIterable this cur rtx ctx)))
  (iterate-list [this rtx cur [k-range-type k1 k2] k-type
                 [v-range-type v1 v2] v-type]
    (let [ctx (l/list-range-info rtx k-range-type k1 k2 k-type
                                 v-range-type v1 v2 v-type)]
      (->ListIterable this cur rtx ctx)))
  (iterate-list-sample [this rtx cur indices budget step [k-range-type k1 k2]
                        k-type [v-range-type v1 v2] v-type]
    (let [ctx (l/list-range-info rtx k-range-type k1 k2 k-type
                                 v-range-type v1 v2 v-type)]
      (->ListSampleIterable this indices budget step cur rtx ctx)))
  (iterate-list-val [this rtx cur [v-range-type v1 v2] v-type]
    (let [ctx (l/range-info rtx v-range-type v1 v2 v-type)]
      (->ListRandKeyValIterable this cur rtx ctx)))
  (iterate-list-val-full [this rtx cur]
    (->ListFullValIterable this cur rtx))
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

(deftype KeyIterable [^DBI db
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
        (next [_] (KV. k v))

        AutoCloseable
        (close [_] (DTLV/dtlv_key_iter_destroy iter))))))

(deftype ListIterable [^DBI db
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
        (next [_] (KV. k v))

        AutoCloseable
        (close [_] (DTLV/dtlv_list_iter_destroy iter))))))

(deftype ListSampleIterable [^DBI db
                             ^longs indices
                             ^long budget
                             ^long step
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
          iter               (DTLV$dtlv_list_sample_iter.)
          samples            (alength indices)
          sizets             (SizeTPointer. samples)]
      (dotimes [i samples] (.put sizets i (aget indices i)))
      (Util/checkRc
        (DTLV/dtlv_list_sample_iter_create
          iter sizets samples budget step (.ptr cur) (.ptr k) (.ptr v)
          ^int forward-key? ^int include-start-key? ^int include-stop-key? sk ek
          ^int forward-val? ^int include-start-val? ^int include-stop-val?
          sv ev))
      (reify
        Iterator
        (hasNext [_] (dtlv-rc (DTLV/dtlv_list_sample_iter_has_next iter)))
        (next [_] (KV. k v))

        AutoCloseable
        (close [_] (DTLV/dtlv_list_sample_iter_destroy iter))))))

(deftype ListRandKeyValIterable [^DBI db
                                 ^Cursor cur
                                 ^Rtx rtx
                                 ^RangeContext ctx]
  IListRandKeyValIterable
  (val-iterator [_]
    (let [include-start-val? (dtlv-bool (.-include-start? ctx))
          include-stop-val?  (dtlv-bool (.-include-stop? ctx))
          sv                 (dtlv-val (.-start-bf ctx))
          ev                 (dtlv-val (.-stop-bf ctx))
          ^BufVal k          (.key cur)
          ^BufVal v          (.val cur)
          iter               (DTLV$dtlv_list_val_iter.)]
      (Util/checkRc
        (DTLV/dtlv_list_val_iter_create
          iter (.ptr cur) (.ptr k) (.ptr v)
          ^int include-start-val? ^int include-stop-val? sv ev))
      (reify
        IListRandKeyValIterator
        (seek-key [_ x t]
          (l/put-key rtx x t)
          (dtlv-rc (DTLV/dtlv_list_val_iter_seek iter (.ptr ^BufVal (.-kp rtx)))))
        (has-next-val [_] (dtlv-rc (DTLV/dtlv_list_val_iter_has_next iter)))
        (next-val [_] (.outBuf v))

        AutoCloseable
        (close [_] (DTLV/dtlv_list_val_iter_destroy iter))))))

(deftype ListFullValIterable [^DBI db
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
        (next-val [_] (.outBuf v))

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
                  (.del dbi txn false)))
    (raise "Unknown kv transact operator: " (.-op tx) {})))

(defn- transact1*
  [txs ^DBI dbi txn kt vt]
  (doseq [t txs]
    (put-tx dbi txn (l/->kv-tx-data t kt vt))))

(defn- transact*
  [txs ^HashMap dbis txn]
  (doseq [t txs]
    (let [^KVTxData tx (l/->kv-tx-data t)
          dbi-name     (.-dbi-name tx)
          ^DBI dbi     (or (.get dbis dbi-name)
                           (raise dbi-name " is not open" {}))]
      (put-tx dbi txn tx))))

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

(declare create-rw-txn reset-write-txn ->CppLMDB)

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

(defn- stop-scheduled-sync [scheduled-sync]
  (when-let [fut @scheduled-sync]
    (.cancel ^ScheduledFuture fut true)
    (vreset! scheduled-sync nil)))

(declare key-range-list-count-fast key-range-list-count-slow)

(deftype CppLMDB [^Env env
                  info
                  ^Pool pools
                  ^HashMap dbis
                  scheduled-sync
                  ^BufVal kp-w
                  ^BufVal vp-w
                  ^BufVal start-kp-w
                  ^BufVal stop-kp-w
                  ^BufVal start-vp-w
                  ^BufVal stop-vp-w
                  write-txn
                  writing?
                  ^:unsynchronized-mutable meta]

  IWriting
  (writing? [_] writing?)

  (write-txn [_] write-txn)

  (mark-write [_]
    (->CppLMDB
      env info pools dbis scheduled-sync kp-w vp-w start-kp-w
      stop-kp-w start-vp-w stop-vp-w write-txn true meta))

  IObj
  (withMeta [this m] (set! meta m) this)
  (meta [_] meta)

  ILMDB
  (close-kv [this]
    (when-not (.isClosed env)
      (stop-scheduled-sync scheduled-sync)
      (swap! l/lmdb-dirs disj (l/dir this))
      (when (zero? (count @l/lmdb-dirs))
        (a/shutdown-executor)
        (u/shutdown-query-thread-pool)
        (u/shutdown-scheduler))
      (.sync env 1)
      (.close env)
      (doseq [idx (keep @l/vector-indices (u/list-files (.dir this)))]
        (v/close-vecs idx))
      (when (@info :temp?) (u/delete-files (@info :dir)))
      nil))

  (closed-kv? [_] (.isClosed env))

  (check-ready [this] (assert (not (.closed-kv? this)) "LMDB env is closed."))

  (dir [_] (@info :dir))

  (opts [_] (dissoc @info :dbis))

  (dbi-opts [_ dbi-name] (get-in @info [:dbis dbi-name]))

  (open-dbi [this dbi-name]
    (.open-dbi this dbi-name nil))
  (open-dbi [this dbi-name {:keys [key-size val-size flags validate-data?]
                            :or   {key-size       c/+max-key-size+
                                   val-size       c/*init-val-size*
                                   flags          c/default-dbi-flags
                                   validate-data? false}}]
    (.check-ready this)
    (assert (< ^long key-size 512) "Key size cannot be greater than 511 bytes")
    (let [{info-dbis :dbis max-dbis :max-dbs} @info]
      (if (< (count info-dbis) ^long max-dbis)
        (let [opts     (merge (get info-dbis dbi-name)
                              {:key-size       key-size
                               :val-size       val-size
                               :flags          flags
                               :validate-data? validate-data?})
              flags    (set flags)
              dupsort? (if (:dupsort flags) true false)
              counted? (if (:counted flags) true false)
              kp       (new-bufval key-size)
              vp       (new-bufval val-size)
              dbi      (Dbi/create env dbi-name (kv-flags flags))
              db       (DBI. dbi (new-pools) kp vp dupsort? counted? validate-data?)]
          (when (not= dbi-name c/kv-info)
            (vswap! info assoc-in [:dbis dbi-name] opts)
            (l/transact-kv this [(l/kv-tx :put c/kv-info [:dbis dbi-name] opts
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
        (l/transact-kv this c/kv-info
                       [[:del [:dbis dbi-name]]] [:keyword :string])
        (.remove dbis dbi-name)
        nil)
      (catch Exception e (raise "Fail to drop DBI: " dbi-name e {}))))

  (list-dbis [_] (keys (@info :dbis)))

  (copy [this dest]
    (.copy this dest false))
  (copy [_ dest compact?]
    (if (-> dest u/file u/empty-dir?)
      (.copy env dest (if compact? true false))
      (raise "Destination directory is not empty." {})))

  (get-rtx [this]
    (or (when-let [^Rtx rtx (pool-take pools)]
          (try
            (.renew rtx)
            (catch Util$BadReaderLockException _
              (raise
                "Please do not open multiple LMDB connections to the same DB
           in the same process. Instead, a LMDB connection should be held onto
           and managed like a stateful resource. Refer to the documentation of
           `datalevin.core/open-kv` for more details." {}))))
        (Rtx. this
              (Txn/createReadOnly env)
              (new-bufval c/+max-key-size+)
              (new-bufval 0)
              (new-bufval c/+max-key-size+)
              (new-bufval c/+max-key-size+)
              (new-bufval c/+max-key-size+)
              (new-bufval c/+max-key-size+)
              (volatile! false))))

  (return-rtx [this rtx]
    (when-not  (.closed-kv? this)
      (.reset rtx)
      (pool-add pools rtx)))

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
      (l/stat this)))

  (entries [this dbi-name]
    (let [^DBI dbi (.get-dbi this dbi-name)
          ^Rtx rtx (.get-rtx this)]
      (try
        (let [^Dbi db    (.-db dbi)
              ^Txn txn   (.-txn rtx)
              ^Stat stat (Stat/create txn db)
              entries    (.ms_entries ^DTLV$MDB_stat (.get stat))]
          (.close stat)
          entries)
        (catch Exception e
          (raise "Fail to get entries: " (ex-message e) {:dbi dbi-name}))
        (finally (.return-rtx this rtx)))))

  (open-transact-kv [this]
    (.check-ready this)
    (try
      (reset-write-txn this)
      (.mark-write this)
      (catch Exception e
        (raise "Fail to open read/write transaction in LMDB: " e {}))))

  (close-transact-kv [_]
    (if-let [^Rtx wtxn @write-txn]
      (when-let [^Txn txn (.-txn wtxn)]
        (let [aborted? @(.-aborted? wtxn)]
          (if aborted?
            (.close txn)
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
          (if aborted? :aborted :committed)))
      (raise "Calling `close-transact-kv` without opening" {})))

  (abort-transact-kv [_]
    (when-let [^Rtx wtxn @write-txn]
      (vreset! (.-aborted? wtxn) true)
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
      (let [^Rtx rtx  @write-txn
            one-shot? (nil? rtx)
            ^DBI dbi  (when dbi-name
                        (or (.get dbis dbi-name)
                            (raise dbi-name " is not open" {})))
            ^Txn txn  (if one-shot?
                        (create-rw-txn this)
                        (.-txn rtx))]
        (try
          (if dbi
            (transact1* txs dbi txn k-type v-type)
            (transact* txs dbis txn))
          (when one-shot? (.commit txn))
          :transacted
          (catch Util$MapFullException _
            (.close txn)
            (up-db-size env)
            (if one-shot?
              (.transact-kv this dbi-name txs k-type v-type)
              (do (reset-write-txn this)
                  (raise "DB resized" {:resized true}))))
          (catch Exception e
            (when one-shot? (.close txn))
            (raise "Fail to transact to LMDB: " e {}))))))

  (set-env-flags [_ ks on-off] (.setFlags env (kv-flags ks) (if on-off 1 0)))

  (get-env-flags [_] (env-flag-keys (.getFlags env)))

  (sync [_] (.sync env 1))
  (sync [_ force] (.sync env force))

  (get-value [this dbi-name k]
    (.get-value this dbi-name k :data :data true))
  (get-value [this dbi-name k k-type]
    (.get-value this dbi-name k k-type :data true))
  (get-value [this dbi-name k k-type v-type]
    (.get-value this dbi-name k k-type v-type true))
  (get-value [this dbi-name k k-type v-type ignore-key?]
    (scan/get-value this dbi-name k k-type v-type ignore-key?))

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
  (key-range-count [lmdb dbi-name [range-type k1 k2] k-type cap]
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
            (DTLV/dtlv_key_range_count
              (.ptr ^Cursor cur)
              (.ptr ^BufVal (.-kp ^Rtx rtx)) (.ptr ^BufVal (.-vp ^Rtx rtx))
              forward start end sk ek))))
      (raise "Fail to count key range: " e {:dbi dbi-name})))

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
    (scan/range-filter-count this dbi-name pred k-range k-type v-type raw-pred?))

  (visit [this dbi-name visitor k-range]
    (.visit this dbi-name visitor k-range :data :data true))
  (visit [this dbi-name visitor k-range k-type]
    (.visit this dbi-name visitor k-range k-type :data true))
  (visit [this dbi-name visitor k-range k-type v-type]
    (.visit this dbi-name visitor k-range k-type v-type true))
  (visit [this dbi-name visitor k-range k-type v-type raw-pred?]
    (scan/visit this dbi-name visitor k-range k-type v-type raw-pred?))

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
    (.check-ready lmdb)
    (if k
      (scan/scan
        (list-count* rtx cur k kt)
        (raise "Fail to count list: " e {:dbi dbi-name :k k}))
      0))

  (near-list [lmdb dbi-name k v kt vt]
    (.check-ready lmdb)
    (scan/scan
      (near-list* rtx cur k kt v vt)
      (raise "Fail to get an item that is near in a list: "
             e {:dbi dbi-name :k k :v v})))

  (in-list? [lmdb dbi-name k v kt vt]
    (.check-ready lmdb)
    (if (and k v)
      (scan/scan
        (in-list?* rtx cur k kt v vt)
        (raise "Fail to test if an item is in list: "
               e {:dbi dbi-name :k k :v v}))
      false))

  (key-range-list-count [lmdb dbi-name k-range k-type]
    (.key-range-list-count lmdb dbi-name k-range k-type nil nil))
  (key-range-list-count [lmdb dbi-name k-range k-type cap]
    (.key-range-list-count lmdb dbi-name k-range k-type cap nil))
  (key-range-list-count [lmdb dbi-name k-range k-type cap budget]
    (key-range-list-count-slow lmdb dbi-name k-range k-type cap budget)
    #_(if (and (l/dlmdb?) )
        (key-range-list-count-fast lmdb dbi-name k-range k-type cap)
        (key-range-list-count-slow lmdb dbi-name k-range k-type cap budget)))

  (list-range [this dbi-name k-range kt v-range vt]
    (scan/list-range this dbi-name k-range kt v-range vt))

  (list-range-count [lmdb dbi-name k-range kt v-range vt]
    (.list-range-count lmdb dbi-name k-range kt v-range vt nil))
  (list-range-count [lmdb dbi-name [k-range-type k1 k2] k-type
                     [v-range-type v1 v2] v-type cap]
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
      (raise "Fail to count list range: " e {:dbi dbi-name})))

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

  (visit-list-sample
    [this list-name indices budget step visitor k-range k-type v-range v-type]
    (.visit-list-sample this list-name indices budget step visitor k-range
                        k-type v-range v-type true))
  (visit-list-sample
    [this dbi-name indices budget step visitor k-range kt v-range vt raw-pred?]
    (scan/visit-list-sample this dbi-name indices budget step visitor k-range
                            kt v-range vt raw-pred?))

  (operate-list-val-range
    [this dbi-name operator v-range vt]
    (scan/operate-list-val-range this dbi-name operator v-range vt))

  IAdmin
  (re-index [this opts] (l/re-index* this opts)))

(defn- key-range-list-count-fast
  [lmdb dbi-name [range-type k1 k2] k-type cap]
  (scan/scan
    (let [^RangeContext ctx (l/range-info rtx range-type k1 k2 k-type)]
      (with-open [ptr (LongPointer. 1)]
        (DTLV/mdb_range_count_values
          (.get ^Txn (.-txn ^Rtx rtx))
          (.get ^Dbi (.-db ^DBI dbi))
          (dtlv-val (.-start-bf ctx))
          (dtlv-val (.-stop-bf ctx))
          (BitOps/intOr
            (if (.-include-start? ctx) (int DTLV/MDB_COUNT_LOWER_INCL) 0)
            (if (.-include-stop? ctx) (int DTLV/MDB_COUNT_UPPER_INCL) 0))
          ptr)
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
            (if (> res cap) cap res))
          :else
          (DTLV/dtlv_key_range_list_count
            (.ptr ^Cursor cur)
            (.ptr ^BufVal (.-kp ^Rtx rtx)) (.ptr ^BufVal (.-vp ^Rtx rtx))
            forward start end sk ek))))
    (raise "Fail to count (slow) list in key range: " e {:dbi dbi-name})))

(defn- create-rw-txn [^CppLMDB lmdb] (Txn/create (.-env lmdb)))

(defn- reset-write-txn
  [^CppLMDB lmdb]
  (let [kp-w       ^BufVal (.-kp-w lmdb)
        vp-w       ^BufVal (.-vp-w lmdb)
        start-kp-w ^BufVal (.-start-kp-w lmdb)
        stop-kp-w  ^BufVal (.-stop-kp-w lmdb)
        start-vp-w ^BufVal (.-start-vp-w lmdb)
        stop-vp-w  ^BufVal (.-stop-vp-w lmdb)]
    (.clear kp-w)
    (.clear vp-w)
    (.clear start-kp-w)
    (.clear stop-kp-w)
    (.clear start-vp-w)
    (.clear stop-vp-w)
    (vreset! (.-write-txn lmdb)
             (Rtx. lmdb
                   (create-rw-txn lmdb)
                   kp-w
                   vp-w
                   start-kp-w
                   stop-kp-w
                   start-vp-w
                   stop-vp-w
                   (volatile! false)))))

(defn- init-info
  [^CppLMDB lmdb new-info]
  (l/transact-kv lmdb c/kv-info (map (fn [[k v]] [:put k v]) new-info))
  (let [dbis (into {}
                   (map (fn [[[_ dbi-name] opts]] [dbi-name opts]))
                   (l/get-range lmdb c/kv-info
                                [:closed
                                 [:dbis :db.value/sysMin]
                                 [:dbis :db.value/sysMax]]
                                [:keyword :string]))
        info (into {}
                   (l/range-filter lmdb c/kv-info
                                   (fn [kv]
                                     (let [b (b/read-buffer (l/k kv) :byte)]
                                       (not= b c/type-hete-tuple)))
                                   [:all]))]
    (vreset! (.-info lmdb) (assoc info :dbis dbis))))

(defmethod open-kv :cpp
  ([dir] (open-kv dir {}))
  ([dir {:keys [mapsize max-readers flags max-dbs temp?]
         :or   {max-readers c/*max-readers*
                max-dbs     c/*max-dbs*
                mapsize     c/*init-db-size*
                flags       c/default-env-flags
                temp?       false}
         :as   opts}]
   (assert (string? dir) "directory should be a string.")
   (try
     (let [file          (u/file dir)
           mapsize       (* (long (if (u/empty-dir? file)
                                    mapsize
                                    (c/pick-mapsize dir)))
                            1024 1024)
           flags         (if temp?
                           (conj flags :nosync)
                           flags)
           ^Env env      (Env/create dir mapsize max-readers max-dbs
                                     (kv-flags flags))
           info          (merge opts {:dir         dir
                                      :version     c/version
                                      :flags       flags
                                      :max-readers max-readers
                                      :max-dbs     max-dbs
                                      :temp?       temp?})
           ^CppLMDB lmdb (->CppLMDB env
                                    (volatile! info)
                                    (new-pools)
                                    (HashMap.)
                                    (volatile! nil)
                                    (new-bufval c/+max-key-size+)
                                    (new-bufval 0)
                                    (new-bufval c/+max-key-size+)
                                    (new-bufval c/+max-key-size+)
                                    (new-bufval c/+max-key-size+)
                                    (new-bufval c/+max-key-size+)
                                    (volatile! nil)
                                    false
                                    nil)]
       (swap! l/lmdb-dirs conj dir)
       (l/open-dbi lmdb c/kv-info)
       (if temp?
         (u/delete-on-exit file)
         (do (init-info lmdb info)
             (.addShutdownHook (Runtime/getRuntime)
                               (Thread. #(l/close-kv lmdb)))
             (start-scheduled-sync (.-scheduled-sync lmdb) dir env)))
       lmdb)
     (catch Exception e
       (stt/print-stack-trace e)
       (raise "Fail to open database: " e {:dir dir})))))
