(ns ^:no-doc datalevin.binding.java
  "LMDB binding for Java"
  (:require [datalevin.bits :as b]
            [datalevin.util :refer [raise] :as u]
            [datalevin.constants :as c]
            [datalevin.scan :as scan]
            [datalevin.lmdb :as l :refer [open-kv kv-flags IBuffer IRange IKV
                                          IRtx IRtxPool IDB ILMDB]])
  (:import [org.lmdbjava Env EnvFlags Env$MapFullException Stat Dbi DbiFlags
            PutFlags Txn TxnFlags KeyRange Txn$BadReaderLockException CopyFlags
            CursorIterable$KeyVal]
           [java.util.concurrent ConcurrentHashMap]
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

(defmethod kv-flags :java
  [type flags]
  (let [t (flag-type type)]
    (if (seq flags)
      (into-array t (mapv flag flags))
      (make-array t 0))))

(def default-env-flags (kv-flags :env [:nordahead :mapasync :writemap]))

(def default-dbi-flags (kv-flags :dbi [:create]))

(def read-dbi-flags (kv-flags :dbi []))

(def default-put-flags (kv-flags :put []))

(deftype Rtx [^Txn txn
              ^:volatile-mutable ^Boolean use?
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
  (close-rtx [_]
    (.close txn)
    (set! use? false))
  (reset [this]
    (.reset txn)
    (set! use? false)
    this)
  (renew [this]
    (when-not use?
      (set! use? true)
      (.renew txn)
      this)))

(deftype RtxPool [^Env env
                  ^ConcurrentHashMap rtxs
                  ^:volatile-mutable ^long cnt]
  IRtxPool
  (close-pool [this]
    (doseq [^Rtx rtx (.values rtxs)] (.close-rtx rtx))
    (.clear rtxs)
    (set! cnt 0))
  (new-rtx [this]
    (when (< cnt c/+use-readers+)
      (let [^Rtx rtx (->Rtx (.txnRead env)
                            false
                            (b/allocate-buffer c/+max-key-size+)
                            (b/allocate-buffer c/+max-key-size+)
                            (b/allocate-buffer c/+max-key-size+))]
        (.put rtxs cnt rtx)
        (set! cnt (inc cnt))
        (.reset rtx)
        (.renew rtx))))
  (get-rtx [this]
    (try
      (locking this
        (if (zero? cnt)
          (.new-rtx this)
          (loop [i (.getId ^Thread (Thread/currentThread))]
            (let [^long i' (mod i cnt)
                  ^Rtx rtx (.get rtxs i')]
              (or (.renew rtx)
                  (.new-rtx this)
                  (recur (long (inc i'))))))))
      (catch Txn$BadReaderLockException _
        (raise
          "Please do not open multiple LMDB connections to the same DB
           in the same process. Instead, a LMDB connection should be held onto
           and managed like a stateful resource. Refer to the documentation of
           `datalevin.lmdb/open-kv` for more details." {})))))

(defn- stat-map [^Stat stat]
  {:psize          (.-pageSize stat)
   :depth          (.-depth stat)
   :branch-pages   (.-branchPages stat)
   :leaf-pages     (.-leafPages stat)
   :overflow-pages (.-overflowPages stat)
   :entries        (.-entries stat)})

(deftype DBI [^Dbi db ^ByteBuffer kb ^:volatile-mutable ^ByteBuffer vb]
  IBuffer
  (put-key [this x t]
    (try
      (.clear kb)
      (b/put-buffer kb x t)
      (.flip kb)
      (catch Exception e
        (raise "Error putting r/w key buffer of "
               (.dbi-name this) ": " (ex-message e)
               {:value x :type t :dbi (.dbi-name this)}))))
  (put-val [this x t]
    (try
      (.clear vb)
      (b/put-buffer vb x t)
      (.flip vb)
      (catch BufferOverflowException _
        (let [size (* c/+buffer-grow-factor+ ^long (b/measure-size x))]
          (set! vb (b/allocate-buffer size))
          (b/put-buffer vb x t)
          (.flip vb)))
      (catch Exception e
        (raise "Error putting r/w value buffer of "
               (.dbi-name this) ": " (ex-message e)
               {:value x :type t :dbi (.dbi-name this)}))))

  IDB
  (dbi-name [_]
    (b/text-ba->str (.getName db)))
  (put [_ txn append?]
    (if append?
      (.put db txn kb vb (kv-flags :put [:append]))
      (.put db txn kb vb default-put-flags)))
  (put [this txn]
    (.put this txn nil))
  (del [_ txn]
    (.delete db txn kb))
  (get-kv [_ rtx]
    (let [^ByteBuffer kb (.-kb ^Rtx rtx)]
      (.get db (.-txn ^Rtx rtx) kb)))
  (iterate-kv [_ rtx range-info]
    (.iterate db (.-txn ^Rtx rtx) range-info)))

(defn- up-db-size [^Env env]
  (.setMapSize env (* c/+buffer-grow-factor+ (-> env .info .mapSize))))

(deftype LMDB [^Env env ^String dir ^RtxPool pool ^ConcurrentHashMap dbis]
  ILMDB
  (close-kv [_]
    (.close-pool pool)
    (when-not (.isClosed env) (.sync env true))
    (.close env)
    nil)

  (closed-kv? [_]
    (.isClosed env))

  (dir [_]
    dir)

  (open-dbi [this dbi-name]
    (.open-dbi this dbi-name c/+max-key-size+ c/+default-val-size+
               default-dbi-flags))
  (open-dbi [this dbi-name key-size]
    (.open-dbi this dbi-name key-size c/+default-val-size+ default-dbi-flags))
  (open-dbi [this dbi-name key-size val-size]
    (.open-dbi this dbi-name key-size val-size default-dbi-flags))
  (open-dbi [this dbi-name key-size val-size flags]
    (assert (not (.closed-kv? this)) "LMDB env is closed.")
    (let [kb  (b/allocate-buffer key-size)
          vb  (b/allocate-buffer val-size)
          db  (.openDbi env ^String dbi-name ^"[Lorg.lmdbjava.DbiFlags;" flags)
          dbi (->DBI db kb vb)]
      (.put dbis dbi-name dbi)
      dbi))

  (get-dbi [this dbi-name]
    (.get-dbi this dbi-name true))
  (get-dbi [this dbi-name create?]
    (or (.get dbis dbi-name)
        (if create?
          (.open-dbi this dbi-name)
          (.open-dbi this dbi-name c/+max-key-size+ c/+default-val-size+
                     read-dbi-flags))))

  (clear-dbi [this dbi-name]
    (assert (not (.closed-kv? this)) "LMDB env is closed.")
    (try
      (let [^DBI dbi (.get-dbi this dbi-name )]
        (with-open [txn (.txnWrite env)]
          (.drop ^Dbi (.-db dbi) txn)
          (.commit txn)))
      (catch Exception e
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
        (raise "Fail to drop DBI: " dbi-name (ex-message e) {}))))

  (list-dbis [this]
    (assert (not (.closed-kv? this)) "LMDB env is closed.")
    (try
      (mapv b/text-ba->str (.getDbiNames env))
      (catch Exception e
        (raise "Fail to list DBIs: " (ex-message e) {}))))

  (copy [this dest]
    (.copy this dest false))
  (copy [this dest compact?]
    (assert (not (.closed-kv? this)) "LMDB env is closed.")
    (let [d (u/file dest)]
      (if (u/empty-dir? d)
        (.copy env d (kv-flags :copy (if compact? [:cp-compact] [])))
        (raise "Destination directory is not empty."))))

  (stat [this]
    (assert (not (.closed-kv? this)) "LMDB env is closed.")
    (try
      (stat-map (.stat env))
      (catch Exception e
        (raise "Fail to get statistics: " (ex-message e) {}))))
  (stat [this dbi-name]
    (assert (not (.closed-kv? this)) "LMDB env is closed.")
    (if dbi-name
      (let [^Rtx rtx (.get-rtx pool)]
        (try
          (let [^DBI dbi (.get-dbi this dbi-name false)
                ^Dbi db  (.-db dbi)
                ^Txn txn (.-txn rtx)]
            (stat-map (.stat db txn)))
          (catch Exception e
            (raise "Fail to get entries: " (ex-message e) {:dbi dbi-name}))
          (finally (.reset rtx))))
      (l/stat this)))

  (entries [this dbi-name]
    (assert (not (.closed-kv? this)) "LMDB env is closed.")
    (let [^DBI dbi (.get-dbi this dbi-name false)
          ^Rtx rtx (.get-rtx pool)]
      (try
        (.-entries ^Stat (.stat ^Dbi (.-db dbi) (.-txn rtx)))
        (catch Exception e
          (raise "Fail to get entries: " (ex-message e)
                 {:dbi dbi-name}))
        (finally (.reset rtx)))))

  (get-txn [this]
    (.get-rtx pool))

  (transact-kv [this txs]
    (assert (not (.closed-kv? this)) "LMDB env is closed.")
    (try
      (with-open [txn (.txnWrite env)]
        (doseq [[op dbi-name k & r] txs
                :let                [^DBI dbi (or (.get dbis dbi-name)
                                                  (raise dbi-name
                                                         " is not open" {}))]]
          (case op
            :put (let [[v kt vt flags] r]
                   (.put-key dbi k kt)
                   (.put-val dbi v vt)
                   (if flags
                     (.put dbi txn flags)
                     (.put dbi txn)))
            :del (let [[kt] r]
                   (.put-key dbi k kt)
                   (.del dbi txn))))
        (.commit txn)
        :transacted)
      (catch Env$MapFullException _
        (up-db-size env)
        (.transact-kv this txs))
      (catch Exception e
        (raise "Fail to transact to LMDB: " (ex-message e) {:txs txs}))))

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
    (scan/range-filter-count this dbi-name pred k-range k-type)))

(defmethod open-kv :java
  [dir]
  (try
    (let [file          (u/file dir)
          builder       (doto (Env/create)
                          (.setMapSize (* ^long c/+init-db-size+ 1024 1024))
                          (.setMaxReaders c/+max-readers+)
                          (.setMaxDbs c/+max-dbs+))
          ^Env env      (.open builder file default-env-flags)
          ^RtxPool pool (->RtxPool env (ConcurrentHashMap.) 0)
          lmdb          (->LMDB env dir pool (ConcurrentHashMap.))]
      (.addShutdownHook (Runtime/getRuntime)
                        (Thread.
                          #(when-not (l/closed-kv? lmdb)
                             (l/close-kv lmdb))))
      lmdb)
    (catch Exception e
      (raise
        "Fail to open database: " (ex-message e)
        {:dir dir}))))
