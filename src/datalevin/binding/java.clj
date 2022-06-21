(ns ^:no-doc datalevin.binding.java
  "LMDB binding for Java"
  (:require [datalevin.bits :as b]
            [datalevin.util :refer [raise] :as u]
            [datalevin.constants :as c]
            [datalevin.scan :as scan]
            [clojure.stacktrace :as st]
            [datalevin.lmdb :as l
             :refer [open-kv open-list-dbi IBuffer IRange IRtx
                     IDB IKV IList ILMDB]])
  (:import [org.lmdbjava Env EnvFlags Env$MapFullException Stat Dbi DbiFlags
            PutFlags Txn TxnFlags KeyRange Txn$BadReaderLockException CopyFlags
            Cursor CursorIterable$KeyVal GetOp SeekOp]
           [datalevin.lmdb Range]
           [java.util.concurrent ConcurrentLinkedQueue]
           [java.util Iterator]
           [org.eclipse.collections.impl.map.mutable UnifiedMap]
           [java.nio ByteBuffer BufferOverflowException]))

(extend-protocol IKV
  CursorIterable$KeyVal
  (k [this] (.key ^CursorIterable$KeyVal this))
  (v [this] (.val ^CursorIterable$KeyVal this)))

(defn- flag
  [flag-key]
  (case flag-key
    :fixedmap   EnvFlags/MDB_FIXEDMAP
    :nosubdir   EnvFlags/MDB_NOSUBDIR
    :rdonly-env EnvFlags/MDB_RDONLY_ENV
    :writemap   EnvFlags/MDB_WRITEMAP
    :nometasync EnvFlags/MDB_NOMETASYNC
    :nosync     EnvFlags/MDB_NOSYNC
    :mapasync   EnvFlags/MDB_MAPASYNC
    :notls      EnvFlags/MDB_NOTLS
    :nolock     EnvFlags/MDB_NOLOCK
    :nordahead  EnvFlags/MDB_NORDAHEAD
    :nomeminit  EnvFlags/MDB_NOMEMINIT

    :cp-compact CopyFlags/MDB_CP_COMPACT

    :reversekey DbiFlags/MDB_REVERSEKEY
    :dupsort    DbiFlags/MDB_DUPSORT
    :integerkey DbiFlags/MDB_INTEGERKEY
    :dupfixed   DbiFlags/MDB_DUPFIXED
    :integerdup DbiFlags/MDB_INTEGERDUP
    :reversedup DbiFlags/MDB_REVERSEDUP
    :create     DbiFlags/MDB_CREATE

    :nooverwrite PutFlags/MDB_NOOVERWRITE
    :nodupdata   PutFlags/MDB_NODUPDATA
    :current     PutFlags/MDB_CURRENT
    :reserve     PutFlags/MDB_RESERVE
    :append      PutFlags/MDB_APPEND
    :appenddup   PutFlags/MDB_APPENDDUP
    :multiple    PutFlags/MDB_MULTIPLE

    :rdonly-txn TxnFlags/MDB_RDONLY_TXN))

(defn- flag-type
  [type-key]
  (case type-key
    :env  EnvFlags
    :copy CopyFlags
    :dbi  DbiFlags
    :put  PutFlags
    :txn  TxnFlags))

(defn- kv-flags
  [type flags]
  (let [t (flag-type type)]
    (if (empty? flags)
      (make-array t 0)
      (into-array t (mapv flag flags)))))

(defn- key-range
  [range-type kb1 kb2]
  (case range-type
    :all               (KeyRange/all)
    :all-back          (KeyRange/allBackward)
    :at-least          (KeyRange/atLeast kb1)
    :at-most-back      (KeyRange/atLeastBackward kb1)
    :at-most           (KeyRange/atMost kb1)
    :at-least-back     (KeyRange/atMostBackward kb1)
    :closed            (KeyRange/closed kb1 kb2)
    :closed-back       (KeyRange/closedBackward kb1 kb2)
    :closed-open       (KeyRange/closedOpen kb1 kb2)
    :closed-open-back  (KeyRange/closedOpenBackward kb1 kb2)
    :greater-than      (KeyRange/greaterThan kb1)
    :less-than-back    (KeyRange/greaterThanBackward kb1)
    :less-than         (KeyRange/lessThan kb1)
    :greater-than-back (KeyRange/lessThanBackward kb1)
    :open              (KeyRange/open kb1 kb2)
    :open-back         (KeyRange/openBackward kb1 kb2)
    :open-closed       (KeyRange/openClosed kb1 kb2)
    :open-closed-back  (KeyRange/openClosedBackward kb1 kb2)))

(defn- value-range
  [range-type kb1 kb2]
  (case range-type
    :all               (l/->Range true false false false false nil nil)
    :all-back          (l/->Range false false false false false nil nil)
    :at-least          (l/->Range true true true false false kb1 nil)
    :at-most-back      (l/->Range false true true false false kb1 nil)
    :at-most           (l/->Range true false false true true nil kb1)
    :at-least-back     (l/->Range false false false true true nil kb1)
    :closed            (l/->Range true true true true true kb1 kb2)
    :closed-back       (l/->Range false true true true true kb1 kb2)
    :closed-open       (l/->Range true true true true false kb1 kb2)
    :closed-open-back  (l/->Range false true true true false kb1 kb2)
    :greater-than      (l/->Range true true false false false kb1 nil)
    :less-than-back    (l/->Range false true false false false kb1 nil)
    :less-than         (l/->Range true false false true false nil kb1)
    :greater-than-back (l/->Range false false false true false nil kb1)
    :open              (l/->Range true true false true false kb1 kb2)
    :open-back         (l/->Range false true false true false kb1 kb2)
    :open-closed       (l/->Range true true false true true kb1 kb2)
    :open-closed-back  (l/->Range false true false true true kb1 kb2)))

(deftype Rtx [lmdb
              ^Txn txn
              ^ByteBuffer kb
              ^ByteBuffer start-kb
              ^ByteBuffer stop-kb]
  IBuffer
  (put-key [_ x t]
    (try
      (.clear kb)
      (b/put-buffer kb x t)
      (.flip kb)
      (catch Exception e
        (st/print-stack-trace e)
        (raise "Error putting read-only transaction key buffer: "
               (ex-message e)
               {:value x :type t}))))
  (put-val [_ x t]
    (raise "put-val not allowed for read only txn buffer" {}))

  IRange
  (range-info [this range-type k1 k2 kt]
    (.range-info this range-type k1 k2 kt false))
  (range-info [this range-type k1 k2 kt value?]
    (let [kb1 (.-start-kb this)
          kb2 (.-stop-kb this)]
      (when k1 (.put-start-key this k1 kt))
      (when k2 (.put-stop-key this k2 kt))
      (if value?
        (value-range range-type kb1 kb2)
        (key-range range-type kb1 kb2))))
  (put-start-key [_ x t]
    (when x
      (try
        (.clear start-kb)
        (b/put-buffer start-kb x t)
        (.flip start-kb)
        (catch Exception e
          (st/print-stack-trace e)
          (raise "Error putting read-only transaction start key buffer: "
                 (ex-message e)
                 {:value x :type t})))))
  (put-stop-key [_ x t]
    (when x
      (try
        (.clear stop-kb)
        (b/put-buffer stop-kb x t)
        (.flip stop-kb)
        (catch Exception e
          (st/print-stack-trace e)
          (raise "Error putting read-only transaction stop key buffer: "
                 (ex-message e)
                 {:value x :type t})))))

  IRtx
  (read-only? [_]
    (.isReadOnly txn))
  (get-txn [_]
    txn)
  (close-rtx [_]
    (.close txn))
  (reset [this]
    (.reset txn)
    this)
  (renew [this]
    (.renew txn)
    this))

(defn- stat-map [^Stat stat]
  {:psize          (.-pageSize stat)
   :depth          (.-depth stat)
   :branch-pages   (.-branchPages stat)
   :leaf-pages     (.-leafPages stat)
   :overflow-pages (.-overflowPages stat)
   :entries        (.-entries stat)})

(deftype DBI [^Dbi db
              ^ConcurrentLinkedQueue curs
              ^ByteBuffer kb
              ^:volatile-mutable ^ByteBuffer vb
              dupsort?
              ^boolean validate-data?]
  IBuffer
  (put-key [this x t]
    (or (not validate-data?)
        (b/valid-data? x t)
        (raise "Invalid data, expecting " t {:input x}))
    (try
      (.clear kb)
      (b/put-buffer kb x t)
      (.flip kb)
      (catch Exception e
        (st/print-stack-trace e)
        (raise "Error putting r/w key buffer of "
               (.dbi-name this) "with value" x ": " (ex-message e)
               {:type t}))))
  (put-val [this x t]
    (or (not validate-data?)
        (b/valid-data? x t)
        (raise "Invalid data, expecting " t {:input x}))
    (try
      (.clear vb)
      (b/put-buffer vb x t)
      (.flip vb)
      (catch BufferOverflowException _
        (let [size (* ^long c/+buffer-grow-factor+ ^long (b/measure-size x))]
          (set! vb (b/allocate-buffer size))
          (b/put-buffer vb x t)
          (.flip vb)))
      (catch Exception e
        (st/print-stack-trace e)
        (raise "Error putting r/w value buffer of "
               (.dbi-name this) ": " (ex-message e)
               {:value x :type t :dbi (.dbi-name this)}))))

  IDB
  (dbi [_] db)
  (dbi-name [_]
    (b/text-ba->str (.getName db)))
  (put [_ txn flags]
    (if flags
      (.put db txn kb vb (kv-flags :put flags))
      (.put db txn kb vb (kv-flags :put c/default-put-flags))))
  (put [this txn]
    (.put this txn nil))
  (del [_ txn all?]
    (if all?
      (.delete db txn kb)
      (.delete db txn kb vb)))
  (del [this txn]
    (.del this txn true))
  (get-kv [_ rtx]
    (let [^ByteBuffer kb (.-kb ^Rtx rtx)]
      (.get db (.-txn ^Rtx rtx) kb)))
  (iterate-kv [_ rtx range-info]
    (.iterate db (.-txn ^Rtx rtx) range-info))
  (iterate-list [_ rtx k k-type v-range]
    (assert dupsort? "Can only iterate list in a list Dbi")
    )
  (get-cursor [_ txn]
    (or (when (.isReadOnly ^Txn txn)
          (when-let [^Cursor cur (.poll curs)]
            (.renew cur txn)
            cur))
        (.openCursor db txn)))
  (close-cursor [_ cur]
    (.close ^Cursor cur))
  (return-cursor [_ cur]
    (.add curs cur)))

(defn- up-db-size [^Env env]
  (.setMapSize env
               (* ^long c/+buffer-grow-factor+ ^long (-> env .info .mapSize))))

(defn- transact*
  [txs ^UnifiedMap dbis txn]
  (doseq [[op dbi-name k & r] txs
          :when               op
          :let                [^DBI dbi (or (.get dbis dbi-name)
                                            (raise dbi-name " is not open"
                                                   {}))]]
    (case op
      :put      (let [[v kt vt flags] r]
                  (.put-key dbi k kt)
                  (.put-val dbi v vt)
                  (if flags
                    (.put dbi txn flags)
                    (.put dbi txn)))
      :del      (let [[kt] r]
                  (.put-key dbi k kt)
                  (.del dbi txn))
      :put-list (let [[vs kt vt] r]
                  (.put-key dbi k kt)
                  (doseq [v vs]
                    (.put-val dbi v vt)
                    (.put dbi txn)))
      :del-list (let [[vs kt vt] r]
                  (.put-key dbi k kt)
                  (doseq [v vs]
                    (.put-val dbi v vt)
                    (.del dbi txn false)))
      (raise "Unknown kv operator: " op {}))))

(defn- list-range*
  [^Rtx rtx ^Cursor cur k kt vt]
  (.put-key rtx k kt)
  (when (.get cur (.-kb rtx) GetOp/MDB_SET)
    (let [holder (transient [])]
      (.seek cur SeekOp/MDB_FIRST_DUP)
      (conj! holder (b/read-buffer (.val cur) vt))
      (dotimes [_ (dec (.count cur))]
        (.seek cur SeekOp/MDB_NEXT_DUP)
        (conj! holder (b/read-buffer (.val cur) vt)))
      (persistent! holder))))

(defn- visit-list*
  [^Rtx rtx ^Cursor cur k kt visitor]
  (let [kv (reify IKV
             (k [_] (.key cur))
             (v [_] (.val cur)))]
    (.put-key rtx k kt)
    (when (.get cur (.-kb rtx) GetOp/MDB_SET)
      (.seek cur SeekOp/MDB_FIRST_DUP)
      (visitor kv)
      (dotimes [_ (dec (.count cur))]
        (.seek cur SeekOp/MDB_NEXT_DUP)
        (visitor kv)))))

(defn- list-count*
  [^Rtx rtx ^Cursor cur k kt]
  (.put-key rtx k kt)
  (if (.get cur (.-kb rtx) GetOp/MDB_SET)
    (.count cur)
    0))

(defn- in-list?*
  [^Rtx rtx ^Cursor cur k kt v vt]
  (.put-start-key rtx k kt)
  (.put-stop-key rtx v vt)
  (.get cur (.-start-kb rtx) (.-stop-kb rtx) SeekOp/MDB_GET_BOTH))

(deftype LMDB [^Env env
               ^String dir
               ^ConcurrentLinkedQueue pool
               ^UnifiedMap dbis
               write-txn]
  ILMDB
  (close-kv [_]
    (when-not (.isClosed env)
      (loop [^Iterator iter (.iterator pool)]
        (when (.hasNext iter)
          (.close-rtx ^Rtx (.next iter))
          (.remove iter)
          (recur iter)))
      (.sync env true)
      (.close env))
    nil)

  (closed-kv? [_]
    (.isClosed env))

  (dir [_]
    dir)

  (open-dbi [this dbi-name]
    (.open-dbi this dbi-name nil))
  (open-dbi [this dbi-name {:keys [key-size val-size flags validate-data?]
                            :or   {key-size       c/+max-key-size+
                                   val-size       c/+default-val-size+
                                   flags          c/default-dbi-flags
                                   validate-data? false}}]
    (assert (not (.closed-kv? this)) "LMDB env is closed.")
    (assert (< ^long key-size 512) "Key size cannot be greater than 511 bytes")
    (let [kb  (b/allocate-buffer key-size)
          vb  (b/allocate-buffer val-size)
          db  (.openDbi env ^String dbi-name
                        ^"[Lorg.lmdbjava.DbiFlags;" (kv-flags :dbi flags))
          dbi (->DBI db (ConcurrentLinkedQueue.) kb vb
                     (some #{:dupsort} flags) validate-data?)]
      (.put dbis dbi-name dbi)
      dbi))

  (get-dbi [this dbi-name]
    (.get-dbi this dbi-name true))
  (get-dbi [this dbi-name create?]
    (or (.get dbis dbi-name)
        (if create?
          (.open-dbi this dbi-name)
          (.open-dbi this dbi-name {:flags c/read-dbi-flags}))))

  (clear-dbi [this dbi-name]
    (assert (not (.closed-kv? this)) "LMDB env is closed.")
    (try
      (let [^DBI dbi (.get-dbi this dbi-name )]
        (with-open [txn (.txnWrite env)]
          (.drop ^Dbi (.-db dbi) txn)
          (.commit txn)))
      (catch Exception e
        (st/print-stack-trace e)
        (raise "Fail to clear DBI: " dbi-name " " (ex-message e) {}))))

  (drop-dbi [this dbi-name]
    (assert (not (.closed-kv? this)) "LMDB env is closed.")
    (try
      (let [^DBI dbi (.get-dbi this dbi-name)]
        (with-open [txn (.txnWrite env)]
          (.drop ^Dbi (.-db dbi) txn true)
          (.commit txn))
        (.remove dbis dbi-name)
        nil)
      (catch Exception e
        (st/print-stack-trace e)
        (raise "Fail to drop DBI: " dbi-name (ex-message e) {}))))

  (list-dbis [this]
    (assert (not (.closed-kv? this)) "LMDB env is closed.")
    (try
      (mapv b/text-ba->str (.getDbiNames env))
      (catch Exception e
        (st/print-stack-trace e)
        (raise "Fail to list DBIs: " (ex-message e) {}))))

  (copy [this dest]
    (.copy this dest false))
  (copy [this dest compact?]
    (assert (not (.closed-kv? this)) "LMDB env is closed.")
    (let [d (u/file dest)]
      (if (u/empty-dir? d)
        (.copy env d (kv-flags :copy (if compact? [:cp-compact] [])))
        (raise "Destination directory is not empty." {}))))

  (get-rtx [this]
    (try
      (or (when-let [^Rtx rtx (.poll pool)]
            (.renew rtx))
          (->Rtx this
                 (.txnRead env)
                 (b/allocate-buffer c/+max-key-size+)
                 (b/allocate-buffer c/+max-key-size+)
                 (b/allocate-buffer c/+max-key-size+)))
      (catch Txn$BadReaderLockException _
        (raise
          "Please do not open multiple LMDB connections to the same DB
           in the same process. Instead, a LMDB connection should be held onto
           and managed like a stateful resource. Refer to the documentation of
           `datalevin.core/open-kv` for more details." {}))))

  (return-rtx [this rtx]
    (.reset ^Rtx rtx)
    (.add pool rtx))

  (stat [this]
    (assert (not (.closed-kv? this)) "LMDB env is closed.")
    (try
      (stat-map (.stat env))
      (catch Exception e
        (st/print-stack-trace e)
        (raise "Fail to get statistics: " (ex-message e) {}))))
  (stat [this dbi-name]
    (assert (not (.closed-kv? this)) "LMDB env is closed.")
    (if dbi-name
      (let [^Rtx rtx (.get-rtx this)]
        (try
          (let [^DBI dbi (.get-dbi this dbi-name false)
                ^Dbi db  (.-db dbi)
                ^Txn txn (.-txn rtx)]
            (stat-map (.stat db txn)))
          (catch Exception e
            (st/print-stack-trace e)
            (raise "Fail to get stat: " (ex-message e) {:dbi dbi-name}))
          (finally (.return-rtx this rtx))))
      (l/stat this)))

  (entries [this dbi-name]
    (assert (not (.closed-kv? this)) "LMDB env is closed.")
    (let [^DBI dbi (.get-dbi this dbi-name false)
          ^Rtx rtx (.get-rtx this)]
      (try
        (.-entries ^Stat (.stat ^Dbi (.-db dbi) (.-txn rtx)))
        (catch Exception e
          (st/print-stack-trace e)
          (raise "Fail to get entries: " (ex-message e)
                 {:dbi dbi-name}))
        (finally (.return-rtx this rtx)))))

  (open-transact-kv [this]
    (assert (not (.closed-kv? this)) "LMDB env is closed.")
    (try
      (vreset! write-txn (->Rtx this
                                (.txnWrite env)
                                (b/allocate-buffer c/+max-key-size+)
                                (b/allocate-buffer c/+max-key-size+)
                                (b/allocate-buffer c/+max-key-size+)))
      (catch Exception e
        (st/print-stack-trace e)
        (raise "Fail to open read/write transaction in LMDB: "
               (ex-message e) {}))))

  (close-transact-kv [this]
    (try
      (when-let [^Rtx wtxn @write-txn]
        (when-let [^Txn txn (.-txn wtxn)]
          (.commit txn)
          (.close txn)
          (vreset! write-txn nil)
          :committed))
      (catch Exception e
        (st/print-stack-trace e)
        (raise "Fail to commit read/write transaction in LMDB: "
               (ex-message e) {}))))

  (write-txn [this]
    write-txn)

  (transact-kv [this txs]
    (assert (not (.closed-kv? this)) "LMDB env is closed.")
    (try
      (if @write-txn
        (transact* txs dbis (.-txn ^Rtx @write-txn))
        (with-open [txn (.txnWrite env)]
          (transact* txs dbis txn)
          (.commit txn)))
      :transacted
      (catch Env$MapFullException _
        (when @write-txn (.close ^Txn (.-txn ^Rtx @write-txn)))
        (up-db-size env)
        (if @write-txn
          (raise "Map is resized" {:resized true})
          (.transact-kv this txs)))
      (catch Exception e
        (st/print-stack-trace e)
        (raise "Fail to transact to LMDB: " (ex-message e) {}))))

  (get-value [this dbi-name k]
    (.get-value this dbi-name k :data :data true false))
  (get-value [this dbi-name k k-type]
    (.get-value this dbi-name k k-type :data true false))
  (get-value [this dbi-name k k-type v-type]
    (.get-value this dbi-name k k-type v-type true false))
  (get-value [this dbi-name k k-type v-type ignore-key?]
    (.get-value this dbi-name k k-type v-type ignore-key? false))
  (get-value [this dbi-name k k-type v-type ignore-key? writing?]
    (scan/get-value this dbi-name k k-type v-type ignore-key? writing?))

  (get-first [this dbi-name k-range]
    (.get-first this dbi-name k-range :data :data false false))
  (get-first [this dbi-name k-range k-type]
    (.get-first this dbi-name k-range k-type :data false false))
  (get-first [this dbi-name k-range k-type v-type]
    (.get-first this dbi-name k-range k-type v-type false false))
  (get-first [this dbi-name k-range k-type v-type ignore-key?]
    (.get-first this dbi-name k-range k-type v-type ignore-key? false))
  (get-first [this dbi-name k-range k-type v-type ignore-key? writing?]
    (scan/get-first this dbi-name k-range k-type v-type ignore-key? writing?))

  (get-range [this dbi-name k-range]
    (.get-range this dbi-name k-range :data :data false false))
  (get-range [this dbi-name k-range k-type]
    (.get-range this dbi-name k-range k-type :data false false))
  (get-range [this dbi-name k-range k-type v-type]
    (.get-range this dbi-name k-range k-type v-type false false))
  (get-range [this dbi-name k-range k-type v-type ignore-key?]
    (.get-range this dbi-name k-range k-type v-type ignore-key? false))
  (get-range [this dbi-name k-range k-type v-type ignore-key? writing?]
    (scan/get-range this dbi-name k-range k-type v-type ignore-key? writing?))

  (range-count [this dbi-name k-range]
    (.range-count this dbi-name k-range :data false))
  (range-count [this dbi-name k-range k-type]
    (.range-count this dbi-name k-range k-type false))
  (range-count [this dbi-name k-range k-type writing?]
    (scan/range-count this dbi-name k-range k-type writing?))

  (get-some [this dbi-name pred k-range]
    (.get-some this dbi-name pred k-range :data :data false false))
  (get-some [this dbi-name pred k-range k-type]
    (.get-some this dbi-name pred k-range k-type :data false false))
  (get-some [this dbi-name pred k-range k-type v-type]
    (.get-some this dbi-name pred k-range k-type v-type false false))
  (get-some [this dbi-name pred k-range k-type v-type ignore-key?]
    (.get-some this dbi-name pred k-range k-type v-type ignore-key? false))
  (get-some [this dbi-name pred k-range k-type v-type ignore-key? writing?]
    (scan/get-some this dbi-name pred k-range k-type v-type ignore-key? writing?))

  (range-filter [this dbi-name pred k-range]
    (.range-filter this dbi-name pred k-range :data :data false false))
  (range-filter [this dbi-name pred k-range k-type]
    (.range-filter this dbi-name pred k-range k-type :data false false))
  (range-filter [this dbi-name pred k-range k-type v-type]
    (.range-filter this dbi-name pred k-range k-type v-type false false))
  (range-filter [this dbi-name pred k-range k-type v-type ignore-key?]
    (.range-filter this dbi-name pred k-range k-type v-type ignore-key? false))
  (range-filter [this dbi-name pred k-range k-type v-type ignore-key? writing?]
    (scan/range-filter this dbi-name pred k-range k-type v-type ignore-key? writing?))

  (range-filter-count [this dbi-name pred k-range]
    (.range-filter-count this dbi-name pred k-range :data false))
  (range-filter-count [this dbi-name pred k-range k-type]
    (.range-filter-count this dbi-name pred k-range k-type false))
  (range-filter-count [this dbi-name pred k-range k-type writing?]
    (scan/range-filter-count this dbi-name pred k-range k-type writing?))

  (visit [this dbi-name visitor k-range]
    (.visit this dbi-name visitor k-range :data false))
  (visit [this dbi-name visitor k-range k-type]
    (.visit this dbi-name visitor k-range k-type false))
  (visit [this dbi-name visitor k-range k-type writing?]
    (scan/visit this dbi-name visitor k-range k-type writing?))

  (open-list-dbi [this dbi-name {:keys [key-size val-size]
                                 :or   {key-size c/+max-key-size+
                                        val-size c/+max-key-size+}}]
    (assert (and (>= c/+max-key-size+ ^long key-size)
                 (>= c/+max-key-size+ ^long val-size))
            "Data size cannot be larger than 511 bytes")
    (.open-dbi this dbi-name {:key-size key-size :val-size val-size
                              :flags    (conj c/default-dbi-flags :dupsort)}))
  (open-list-dbi [lmdb dbi-name]
    (.open-list-dbi lmdb dbi-name nil))

  IList
  (put-list-items [this dbi-name k vs kt vt]
    (.transact-kv this [[:put-list dbi-name k vs kt vt]]))

  (del-list-items [this dbi-name k kt]
    (.transact-kv this [[:del dbi-name k kt]]))
  (del-list-items [this dbi-name k vs kt vt]
    (.transact-kv this [[:del-list dbi-name k vs kt vt]]))

  (visit-list [this dbi-name visitor k kt]
    (.visit-list this dbi-name visitor k kt false))
  (visit-list [this dbi-name visitor k kt writing?]
    (when k
      (scan/scan-list
        (visit-list* rtx cur k kt visitor)
        (raise "Fail to visit list: " (ex-message e) {:dbi dbi-name :k k}))))

  (list-count [this dbi-name k kt]
    (.list-count this dbi-name k kt false))
  (list-count [this dbi-name k kt writing?]
    (if k
      (scan/scan-list
        (list-count* rtx cur k kt)
        (raise "Fail to count list: " (ex-message e) {:dbi dbi-name :k k}))
      0))

  (in-list? [this dbi-name k v kt vt]
    (.in-list? this dbi-name k v kt vt false))
  (in-list? [this dbi-name k v kt vt writing?]
    (if (and k v)
      (scan/scan-list
        (in-list?* rtx cur k kt v vt)
        (raise "Fail to test if an item is in list: "
               (ex-message e) {:dbi dbi-name :k k :v v}))
      false))

  (list-range [this dbi-name k kt v-range vt]
    (.list-range this dbi-name k kt v-range vt false))
  (list-range [this dbi-name k kt v-range vt writing?]
    (when k
      (scan/scan-list
        (list-range* rtx cur k kt vt)
        (raise "Fail to get list range: " (ex-message e)
               {:dbi dbi-name :k k}))))

  (list-range-filter [this dbi-name pred k kt v-range vt]
    (.list-range-filter this dbi-name pred k kt v-range vt false))
  (list-range-filter [this dbi-name pred k kt v-range vt writing?]
    (when k
      ))
  )

(defmethod open-kv :java
  ([dir]
   (open-kv dir {}))
  ([dir {:keys [mapsize flags]
         :or   {mapsize c/+init-db-size+
                flags   c/default-env-flags}}]
   (try
     (let [file     (u/file dir)
           builder  (doto (Env/create)
                      (.setMapSize (* ^long mapsize 1024 1024))
                      (.setMaxReaders c/+max-readers+)
                      (.setMaxDbs c/+max-dbs+))
           ^Env env (.open builder file (kv-flags :env flags))
           lmdb     (->LMDB env
                            dir
                            (ConcurrentLinkedQueue.)
                            (UnifiedMap.)
                            (volatile! nil))]
       lmdb)
     (catch Exception e
       (st/print-stack-trace e)
       (raise
         "Fail to open database: " (ex-message e)
         {:dir dir})))))
