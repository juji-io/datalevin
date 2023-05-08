(ns ^:no-doc datalevin.binding.java
  "LMDB binding for Java"
  (:require
   [datalevin.bits :as b]
   [datalevin.util :refer [raise] :as u]
   [datalevin.constants :as c]
   [datalevin.scan :as scan]
   [datalevin.lmdb :as l :refer [open-kv IBuffer IRange IAdmin
                                 IRtx IDB IKV IList ILMDB IWriting]]
   [clojure.stacktrace :as st]
   [clojure.java.io :as io]
   [clojure.string :as s])
  (:import
   [org.lmdbjava Env EnvFlags Env$MapFullException Stat Dbi DbiFlags
    PutFlags Txn TxnFlags KeyRange Txn$BadReaderLockException CopyFlags
    Cursor CursorIterable$KeyVal GetOp SeekOp]
   [java.util.concurrent ConcurrentLinkedQueue]
   [java.util Iterator UUID]
   [java.io File InputStream OutputStream]
   [java.nio.file Files OpenOption StandardOpenOption]
   [clojure.lang IPersistentVector]
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

(deftype Rtx [lmdb
              ^Txn txn
              ^ByteBuffer kb
              ^ByteBuffer start-kb
              ^ByteBuffer stop-kb
              aborted?]
  IBuffer
  (put-key [_ x t]
    (try
      (.clear kb)
      (b/put-buffer kb x t)
      (.flip kb)
      (catch BufferOverflowException _
        (raise "Key cannot be larger than 511 bytes." {:input x}))
      (catch Exception e
        (raise "Error putting read-only transaction key buffer: "
               (ex-message e)
               {:value x :type t}))))
  (put-val [_ x t]
    (raise "put-val not allowed for read only txn buffer" {}))

  IRange
  (range-info [this range-type _ _]
    (let [kb1 (.-start-kb this)
          kb2 (.-stop-kb this)]
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
        :open-closed-back  (KeyRange/openClosedBackward kb1 kb2))))
  (put-start-key [_ x t]
    (when x
      (try
        (.clear start-kb)
        (b/put-buffer start-kb x t)
        (.flip start-kb)
        (catch Exception e
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
              ^boolean validate-data?]
  IBuffer
  (put-key [this x t]
    (or (not validate-data?)
        (b/valid-data? x t)
        (raise "Invalid data, expecting" t {:input x}))
    (try
      (.clear kb)
      (b/put-buffer kb x t)
      (.flip kb)
      (catch BufferOverflowException _
        (raise "Key cannot be larger than 511 bytes." {:input x}))
      (catch Exception e
        (raise "Error putting r/w key buffer of "
               (.dbi-name this) "with value" x ": " e
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
  (doseq [^IPersistentVector tx txs]
    (let [cnt      (.length tx)
          op       (.nth tx 0)
          dbi-name (.nth tx 1)
          k        (.nth tx 2)
          ^DBI dbi (or (.get dbis dbi-name)
                       (raise dbi-name " is not open" {}))]
      (case op
        :put      (let [v     (.nth tx 3)
                        kt    (when (< 4 cnt) (.nth tx 4))
                        vt    (when (< 5 cnt) (.nth tx 5))
                        flags (when (< 6 cnt) (.nth tx 6))]
                    (.put-key dbi k kt)
                    (.put-val dbi v vt)
                    (if flags
                      (.put dbi txn flags)
                      (.put dbi txn)))
        :del      (let [kt (when (< 3 cnt) (.nth tx 3)) ]
                    (.put-key dbi k kt)
                    (.del dbi txn))
        :put-list (let [vs (.nth tx 3)
                        kt (when (< 4 cnt) (.nth tx 4))
                        vt (when (< 5 cnt) (.nth tx 5))]
                    (.put-key dbi k kt)
                    (doseq [v vs]
                      (.put-val dbi v vt)
                      (.put dbi txn)))
        :del-list (let [vs (.nth tx 3)
                        kt (when (< 4 cnt) (.nth tx 4))
                        vt (when (< 5 cnt) (.nth tx 5))]
                    (.put-key dbi k kt)
                    (doseq [v vs]
                      (.put-val dbi v vt)
                      (.del dbi txn false)))
        (raise "Unknown kv operator: " op {})))))

(declare ->LMDB reset-write-txn)

(deftype LMDB [^Env env
               ^String dir
               temp?
               opts
               ^ConcurrentLinkedQueue pool
               ^UnifiedMap dbis
               ^ByteBuffer kb-w
               ^ByteBuffer start-kb-w
               ^ByteBuffer stop-kb-w
               write-txn
               writing?]
  IWriting
  (writing? [_] writing?)

  (write-txn [_] write-txn)

  (mark-write [_]
    (->LMDB env dir temp? opts pool dbis kb-w start-kb-w stop-kb-w
            write-txn true))

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
    (when temp? (u/delete-files dir))
    nil)

  (closed-kv? [_] (.isClosed env))

  (check-ready [this]
    (assert (not (.closed-kv? this)) "LMDB env is closed."))

  (dir [_] dir)

  (opts [_] opts)

  (open-dbi [this dbi-name]
    (.open-dbi this dbi-name nil))
  (open-dbi [this dbi-name {:keys [key-size val-size flags validate-data?]
                            :or   {key-size       c/+max-key-size+
                                   val-size       c/+default-val-size+
                                   flags          c/default-dbi-flags
                                   validate-data? false}}]
    (.check-ready this)
    (assert (< ^long key-size 512) "Key size cannot be greater than 511 bytes")
    (let [kb  (b/allocate-buffer key-size)
          vb  (b/allocate-buffer val-size)
          db  (.openDbi env ^String dbi-name
                        ^"[Lorg.lmdbjava.DbiFlags;" (kv-flags :dbi flags))
          dbi (->DBI db (ConcurrentLinkedQueue.) kb vb validate-data?)]
      (.put dbis dbi-name dbi)
      dbi))

  (get-dbi [this dbi-name]
    (.get-dbi this dbi-name true))
  (get-dbi [this dbi-name create?]
    (or (.get dbis dbi-name)
        (if create?
          (.open-dbi this dbi-name)
          (.open-dbi this dbi-name {:key-size c/+max-key-size+
                                    :val-size c/+default-val-size+
                                    :flags    c/read-dbi-flags}))))

  (clear-dbi [this dbi-name]
    (.check-ready this)
    (try
      (let [^DBI dbi (.get-dbi this dbi-name )]
        (with-open [txn (.txnWrite env)]
          (.drop ^Dbi (.-db dbi) txn)
          (.commit txn)))
      (catch Exception e
        (raise "Fail to clear DBI: " dbi-name " " e {}))))

  (drop-dbi [this dbi-name]
    (.check-ready this)
    (try
      (let [^DBI dbi (.get-dbi this dbi-name)]
        (with-open [txn (.txnWrite env)]
          (.drop ^Dbi (.-db dbi) txn true)
          (.commit txn))
        (.remove dbis dbi-name)
        nil)
      (catch Exception e
        (raise "Fail to drop DBI: " dbi-name " " e {}))))

  (list-dbis [this]
    (.check-ready this)
    (try
      (mapv b/text-ba->str (.getDbiNames env))
      (catch Exception e
        (raise "Fail to list DBIs: " (ex-message e) {}))))

  (copy [this dest]
    (.copy this dest false))
  (copy [this dest compact?]
    (.check-ready this)
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
                 (b/allocate-buffer c/+max-key-size+)
                 (volatile! false)))
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
    (.check-ready this)
    (try
      (stat-map (.stat env))
      (catch Exception e
        (raise "Fail to get statistics: " (ex-message e) {}))))
  (stat [this dbi-name]
    (.check-ready this)
    (if dbi-name
      (let [^Rtx rtx (.get-rtx this)]
        (try
          (let [^DBI dbi (.get-dbi this dbi-name false)
                ^Dbi db  (.-db dbi)
                ^Txn txn (.-txn rtx)]
            (stat-map (.stat db txn)))
          (catch Exception e
            (raise "Fail to get stat: " (ex-message e) {:dbi dbi-name}))
          (finally (.return-rtx this rtx))))
      (l/stat this)))

  (entries [this dbi-name]
    (.check-ready this)
    (let [^DBI dbi (.get-dbi this dbi-name false)
          ^Rtx rtx (.get-rtx this)]
      (try
        (.-entries ^Stat (.stat ^Dbi (.-db dbi) (.-txn rtx)))
        (catch Exception e
          (raise "Fail to get entries: " (ex-message e)
                 {:dbi dbi-name}))
        (finally (.return-rtx this rtx)))))

  (open-transact-kv [this]
    (.check-ready this)
    (try
      (reset-write-txn this)
      (.mark-write this)
      (catch Exception e
        ;; (st/print-stack-trace e)
        (raise "Fail to open read/write transaction in LMDB: "
               (ex-message e) {}))))

  (close-transact-kv [_]
    (if-let [^Rtx wtxn @write-txn]
      (when-let [^Txn txn (.-txn wtxn)]
        (let [aborted? @(.-aborted? wtxn)]
          (when-not aborted?
            (try
              (.commit txn)
              (catch Env$MapFullException _
                (vreset! write-txn nil)
                (.close txn)
                (up-db-size env)
                (raise "DB resized" {:resized true}))
              (catch Exception e
                (raise "Fail to commit read/write transaction in LMDB: "
                       e {}))))
          (vreset! write-txn nil)
          (.close txn)
          (if aborted? :aborted :committed)))
      (raise "Calling `close-transact-kv` without opening" {})))

  (abort-transact-kv [this]
    (when-let [^Rtx wtxn @write-txn]
      (vreset! (.-aborted? wtxn) true)
      (vreset! write-txn wtxn)
      nil))

  (transact-kv [this txs]
    (.check-ready this)
    (locking  write-txn
      (let [^Rtx rtx  @write-txn
            one-shot? (nil? rtx)]
        (try
          (if one-shot?
            (with-open [txn (.txnWrite env)]
              (transact* txs dbis txn)
              (.commit txn))
            (transact* txs dbis (.-txn rtx)))
          :transacted
          (catch Env$MapFullException _
            (when-not one-shot? (.close ^Txn (.-txn rtx)))
            (up-db-size env)
            (if one-shot?
              (.transact-kv this txs)
              (do (reset-write-txn this)
                  (raise "DB resized" {:resized true}))))
          (catch Exception e
            ;; (st/print-stack-trace e)
            (raise "Fail to transact to LMDB: " e {}))))))

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

  (get-range [this dbi-name k-range]
    (.get-range this dbi-name k-range :data :data false))
  (get-range [this dbi-name k-range k-type]
    (.get-range this dbi-name k-range k-type :data false))
  (get-range [this dbi-name k-range k-type v-type]
    (.get-range this dbi-name k-range k-type v-type false))
  (get-range [this dbi-name k-range k-type v-type ignore-key?]
    (scan/get-range this dbi-name k-range k-type v-type ignore-key?))

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
  (range-count [this dbi-name k-range k-type]
    (scan/range-count this dbi-name k-range k-type))

  (get-some [this dbi-name pred k-range]
    (.get-some this dbi-name pred k-range :data :data false))
  (get-some [this dbi-name pred k-range k-type]
    (.get-some this dbi-name pred k-range k-type :data false))
  (get-some [this dbi-name pred k-range k-type v-type]
    (.get-some this dbi-name pred k-range k-type v-type false))
  (get-some [this dbi-name pred k-range k-type v-type ignore-key?]
    (scan/get-some this dbi-name pred k-range k-type v-type ignore-key?))

  (range-filter [this dbi-name pred k-range]
    (.range-filter this dbi-name pred k-range :data :data false))
  (range-filter [this dbi-name pred k-range k-type]
    (.range-filter this dbi-name pred k-range k-type :data false))
  (range-filter [this dbi-name pred k-range k-type v-type]
    (.range-filter this dbi-name pred k-range k-type v-type false))
  (range-filter [this dbi-name pred k-range k-type v-type ignore-key?]
    (scan/range-filter this dbi-name pred k-range k-type v-type ignore-key?))

  (range-filter-count [this dbi-name pred k-range]
    (.range-filter-count this dbi-name pred k-range :data))
  (range-filter-count [this dbi-name pred k-range k-type]
    (scan/range-filter-count this dbi-name pred k-range k-type))

  (visit [this dbi-name visitor k-range]
    (.visit this dbi-name visitor k-range :data))
  (visit [this dbi-name visitor k-range k-type]
    (scan/visit this dbi-name visitor k-range k-type))

  (open-list-dbi [this dbi-name {:keys [key-size val-size]
                                 :or   {key-size c/+max-key-size+
                                        val-size c/+max-key-size+}}]
    (.check-ready this)
    (assert (and (>= c/+max-key-size+ ^long key-size)
                 (>= c/+max-key-size+ ^long val-size))
            "Data size cannot be larger than 511 bytes")
    (.open-dbi this dbi-name {:key-size key-size :val-size val-size
                              :flags    (conj c/default-dbi-flags :dupsort)}))
  (open-list-dbi [lmdb dbi-name]
    (.open-list-dbi lmdb dbi-name nil))

  IList
  (put-list-items [this dbi-name k vs kt vt]
    (.check-ready this)
    (try
      (let [^DBI dbi (.get-dbi this dbi-name false)]
        (with-open [txn (.txnWrite env)]
          (.put-key dbi k kt)
          (doseq [v vs]
            (.put-val dbi v vt)
            (.put dbi txn))
          (.commit txn)
          :transacted))
      (catch Exception e
        (raise "Fail to put an inverted list: " (ex-message e) {}))))

  (del-list-items [this dbi-name k kt]
    (.check-ready this)
    (try
      (let [^DBI dbi (.get-dbi this dbi-name false)]
        (with-open [txn (.txnWrite env)]
          (.put-key dbi k kt)
          (.del dbi txn)
          (.commit txn)
          :transacted))
      (catch Exception e
        (raise "Fail to delete an inverted list: " (ex-message e) {}))))
  (del-list-items [this dbi-name k vs kt vt]
    (.check-ready this)
    (try
      (let [^DBI dbi (.get-dbi this dbi-name false)]
        (with-open [txn (.txnWrite env)]
          (.put-key dbi k kt)
          (doseq [v vs]
            (.put-val dbi v vt)
            (.del dbi txn false))
          (.commit txn)
          :transacted))
      (catch Exception e
        (raise "Fail to delete items from an inverted list: "
               (ex-message e) {}))))

  (get-list [this dbi-name k kt vt]
    (.check-ready this)
    (when k
      (let [^DBI dbi    (.get-dbi this dbi-name false)
            ^Rtx rtx    (.get-rtx this)
            txn         (.-txn rtx)
            ^Cursor cur (.get-cursor dbi txn)]
        (try
          (.put-start-key rtx k kt)
          (when (.get cur (.-start-kb rtx) GetOp/MDB_SET)
            (let [holder (transient [])]
              (.seek cur SeekOp/MDB_FIRST_DUP)
              (conj! holder (b/read-buffer (.val cur) vt))
              (dotimes [_ (dec (.count cur))]
                (.seek cur SeekOp/MDB_NEXT_DUP)
                (conj! holder (b/read-buffer (.val cur) vt)))
              (persistent! holder)))
          (catch Exception e
            (raise "Fail to get inverted list: " (ex-message e)
                   {:dbi dbi-name}))
          (finally (.return-rtx this rtx)
                   (.return-cursor dbi cur))))))

  (visit-list [this dbi-name visitor k kt]
    (.check-ready this)
    (when k
      (let [^DBI dbi    (.get-dbi this dbi-name false)
            ^Rtx rtx    (.get-rtx this)
            txn         (.-txn rtx)
            ^Cursor cur (.get-cursor dbi txn)
            kv          (reify IKV
                          (k [_] (.key cur))
                          (v [_] (.val cur)))]
        (try
          (.put-start-key rtx k kt)
          (when (.get cur (.-start-kb rtx) GetOp/MDB_SET)
            (.seek cur SeekOp/MDB_FIRST_DUP)
            (visitor kv)
            (dotimes [_ (dec (.count cur))]
              (.seek cur SeekOp/MDB_NEXT_DUP)
              (visitor kv)))
          (catch Exception e
            (raise "Fail to get count of inverted list: " (ex-message e)
                   {:dbi dbi-name}))
          (finally (.return-rtx this rtx)
                   (.return-cursor dbi cur))))))

  (list-count [this dbi-name k kt]
    (.check-ready this)
    (if k
      (let [^DBI dbi    (.get-dbi this dbi-name false)
            ^Rtx rtx    (.get-rtx this)
            txn         (.-txn rtx)
            ^Cursor cur (.get-cursor dbi txn)]
        (try
          (.put-start-key rtx k kt)
          (if (.get cur (.-start-kb rtx) GetOp/MDB_SET)
            (.count cur)
            0)
          (catch Exception e
            (raise "Fail to get count of inverted list: " (ex-message e)
                   {:dbi dbi-name}))
          (finally (.return-rtx this rtx)
                   (.return-cursor dbi cur))))
      0))

  (filter-list [this dbi-name k pred k-type v-type]
    (.check-ready this)
    (.range-filter this dbi-name pred [:closed k k] k-type v-type true))

  (filter-list-count [this dbi-name k pred k-type]
    (.check-ready this)
    (.range-filter-count this dbi-name pred [:closed k k] k-type))

  (in-list? [this dbi-name k v kt vt]
    (.check-ready this)
    (if (and k v)
      (let [^DBI dbi    (.get-dbi this dbi-name false)
            ^Rtx rtx    (.get-rtx this)
            txn         (.-txn rtx)
            ^Cursor cur (.get-cursor dbi txn)]
        (try
          (.put-start-key rtx k kt)
          (.put-stop-key rtx v vt)
          (.get cur (.-start-kb rtx) (.-stop-kb rtx) SeekOp/MDB_GET_BOTH)
          (catch Exception e
            (raise "Fail to test if an item is in an inverted list: "
                   (ex-message e) {:dbi dbi-name}))
          (finally (.return-rtx this rtx)
                   (.return-cursor dbi cur))))
      false))

  IAdmin
  (re-index [this opts] (l/re-index* this opts)))

(defn- reset-write-txn
  [^LMDB lmdb]
  (let [kb-w       ^ByteBuffer (.-kb-w lmdb)
        start-kb-w ^ByteBuffer (.-start-kb-w lmdb)
        stop-kb-w  ^ByteBuffer (.-stop-kb-w lmdb)]
    (.clear kb-w)
    (.clear start-kb-w)
    (.clear stop-kb-w)
    (vreset! (.-write-txn lmdb) (->Rtx lmdb
                                       (.txnWrite ^Env (.-env lmdb))
                                       kb-w
                                       start-kb-w
                                       stop-kb-w
                                       (volatile! false)))))

(defmethod open-kv :java
  ([dir]
   (open-kv dir {}))
  ([dir {:keys [mapsize max-readers max-dbs flags temp?]
         :or   {max-readers c/+max-readers+
                max-dbs     c/+max-dbs+
                mapsize     c/+init-db-size+
                flags       c/default-env-flags
                temp?       false}
         :as   opts}]
   (assert (string? dir) "directory should be a string.")
   (try
     (let [file     (u/file dir)
           mapsize  (* (long (if (u/empty-dir? file)
                               mapsize
                               (c/pick-mapsize dir)))
                       1024 1024)
           builder  (doto (Env/create)
                      (.setMapSize mapsize)
                      (.setMaxReaders max-readers)
                      (.setMaxDbs max-dbs))
           ^Env env (.open builder file (kv-flags :env flags))
           lmdb     (->LMDB env
                            dir
                            temp?
                            opts
                            (ConcurrentLinkedQueue.)
                            (UnifiedMap.)
                            (b/allocate-buffer c/+max-key-size+)
                            (b/allocate-buffer c/+max-key-size+)
                            (b/allocate-buffer c/+max-key-size+)
                            (volatile! nil)
                            false)]
       (when temp? (u/delete-on-exit file))
       lmdb)
     (catch Exception e
       (raise "Fail to open database: " e {:dir dir})))))

;; TODO remove after LMDBJava supports apple silicon
(defn apple-silicon-lmdb []
  (when (and (u/apple-silicon?)
             (not (System/getenv "DTLV_COMPILE_NATIVE"))
             (not (u/graal?)))
    (try
      (let [dir             (u/tmp-dir (str "lmdbjava-native-lib-"
                                            (UUID/randomUUID)) )
            ^File file      (File. ^String dir "liblmdb.dylib")
            path            (.toPath file)
            fpath           (.getAbsolutePath file)
            ^ClassLoader cl (.getContextClassLoader (Thread/currentThread))]
        (u/create-dirs dir)
        (.deleteOnExit file)
        (System/setProperty "lmdbjava.native.lib" fpath)

        (with-open [^InputStream in
                    (.getResourceAsStream
                      cl "dtlvnative/macos-latest-aarch64-shared/liblmdb.dylib")
                    ^OutputStream out
                    (Files/newOutputStream path (into-array OpenOption []))]
          (io/copy in out))
        (println "Library extraction is successful:" fpath
                 "with size" (Files/size path)))
      (catch Exception e
        ;; (st/print-stack-trace e)
        (u/raise "Failed to extract LMDB library" {})))))

(apple-silicon-lmdb)
