(ns ^:no-doc datalevin.binding.java
  "LMDB binding for Java"
  (:require [datalevin.bits :as b]
            [datalevin.util :refer [raise]]
            [datalevin.constants :as c]
            [datalevin.scan :as scan]
            [datalevin.lmdb :as lmdb
             :refer [open-lmdb IBuffer IRange IKV IRtx IDB ILMDB]]
            [clojure.string :as s])
  (:import [org.lmdbjava Env EnvFlags Env$MapFullException Stat Dbi DbiFlags
            PutFlags Txn KeyRange Txn$BadReaderLockException
            CursorIterable$KeyVal]
           [java.util.concurrent ConcurrentHashMap]
           [java.nio.charset StandardCharsets]
           [java.nio ByteBuffer]))

(extend-protocol IKV
  CursorIterable$KeyVal
  (k [this] (.key ^CursorIterable$KeyVal this))
  (v [this] (.val ^CursorIterable$KeyVal this)))

(def default-env-flags [EnvFlags/MDB_NORDAHEAD
                        EnvFlags/MDB_MAPASYNC
                        EnvFlags/MDB_WRITEMAP])

(def default-dbi-flags [DbiFlags/MDB_CREATE])

(def default-put-flags (make-array PutFlags 0))

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

(defprotocol IRtxPool
  (close-pool [this] "Close all transactions in the pool")
  (new-rtx [this] "Create a new read-only transaction")
  (get-rtx [this] "Obtain a ready-to-use read-only transaction"))

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
          (new-rtx this)
          (loop [i (.getId ^Thread (Thread/currentThread))]
            (let [^long i' (mod i cnt)
                  ^Rtx rtx (.get rtxs i')]
              (or (.renew rtx)
                  (new-rtx this)
                  (recur (long (inc i'))))))))
      (catch Txn$BadReaderLockException _
        (raise
          "Please do not open multiple LMDB connections to the same DB
           in the same process. Instead, a LMDB connection should be held onto
           and managed like a stateful resource. Refer to the documentation of
           `datalevin.lmdb/open-lmdb` for more details." {})))))

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
      (catch Exception e
        (if (s/includes? (ex-message e) c/buffer-overflow)
          (let [size (* 2 ^long (b/measure-size x))]
            (set! vb (b/allocate-buffer size))
            (b/put-buffer vb x t)
            (.flip vb))
          (raise "Error putting r/w value buffer of "
                 (.dbi-name this) ": " (ex-message e)
                 {:value x :type t :dbi (.dbi-name this)})))))

  IDB
  (dbi-name [_]
    (String. (.getName db) StandardCharsets/UTF_8))
  (put [_ txn flags]
    (if flags
      (.put db txn kb vb (into-array PutFlags flags))
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
  (.setMapSize env (* 10 (-> env .info .mapSize))))

(deftype LMDB [^Env env ^String dir ^RtxPool pool ^ConcurrentHashMap dbis]
  ILMDB
  (close-env [_]
    (close-pool pool)
    (.close env))

  (closed? [_]
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
    (assert (not (.closed? this)) "LMDB env is closed.")
    (let [kb  (b/allocate-buffer key-size)
          vb  (b/allocate-buffer val-size)
          db  (.openDbi env
                        ^String dbi-name
                        ^"[Lorg.lmdbjava.DbiFlags;" (into-array DbiFlags flags))
          dbi (->DBI db kb vb)]
      (.put dbis dbi-name dbi)
      dbi))

  (get-dbi [_ dbi-name]
    (or (.get dbis dbi-name)
        (raise "`open-dbi` was not called for " dbi-name {})))

  (clear-dbi [this dbi-name]
    (assert (not (.closed? this)) "LMDB env is closed.")
    (try
      (with-open [txn (.txnWrite env)]
        (let [^DBI dbi (.get-dbi this dbi-name)]
          (.drop ^Dbi (.-db dbi) txn))
        (.commit txn))
      (catch Exception e
        (raise "Fail to clear DBI: " dbi-name " " (ex-message e) {}))))

  (drop-dbi [this dbi-name]
    (assert (not (.closed? this)) "LMDB env is closed.")
    (try
      (with-open [txn (.txnWrite env)]
        (let [^DBI dbi (.get-dbi this dbi-name)]
          (.drop ^Dbi (.-db dbi) txn true)
          (.remove dbis dbi-name))
        (.commit txn))
      (catch Exception e
        (raise "Fail to drop DBI: " dbi-name (ex-message e) {}))))

  (entries [this dbi-name]
    (assert (not (.closed? this)) "LMDB env is closed.")
    (let [^DBI dbi (.get-dbi this dbi-name)
          ^Rtx rtx (get-rtx pool)]
      (try
        (.-entries ^Stat (.stat ^Dbi (.-db dbi) (.-txn rtx)))
        (catch Exception e
          (raise "Fail to get entries: " (ex-message e)
                 {:dbi dbi-name}))
        (finally (.reset rtx)))))

  (transact [this txs]
    (assert (not (.closed? this)) "LMDB env is closed.")
    (try
      (with-open [txn (.txnWrite env)]
        (doseq [[op dbi-name k & r] txs
                :let                [^DBI dbi (.get-dbi this dbi-name)]]
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
        (.commit txn))
      (catch Env$MapFullException _
        (up-db-size env)
        (.transact this txs))
      (catch Exception e
        (raise "Fail to transact to LMDB: " (ex-message e) {:txs txs}))))

  (get-value [this dbi-name k]
    (.get-value this dbi-name k :data :data true))
  (get-value [this dbi-name k k-type]
    (.get-value this dbi-name k k-type :data true))
  (get-value [this dbi-name k k-type v-type]
    (.get-value this dbi-name k k-type v-type true))
  (get-value [this dbi-name k k-type v-type ignore-key?]
    (assert (not (.closed? this)) "LMDB env is closed.")
    (let [dbi      (.get-dbi this dbi-name)
          ^Rtx rtx (get-rtx pool)]
      (try
        (scan/fetch-value dbi rtx k k-type v-type ignore-key?)
        (catch Exception e
          (raise "Fail to get-value: " (ex-message e)
                 {:dbi dbi-name :k k :k-type k-type :v-type v-type}))
        (finally (.reset rtx)))))

  (get-first [this dbi-name k-range]
    (.get-first this dbi-name k-range :data :data false))
  (get-first [this dbi-name k-range k-type]
    (.get-first this dbi-name k-range k-type :data false))
  (get-first [this dbi-name k-range k-type v-type]
    (.get-first this dbi-name k-range k-type v-type false))
  (get-first [this dbi-name k-range k-type v-type ignore-key?]
    (assert (not (.closed? this)) "LMDB env is closed.")
    (let [dbi      (.get-dbi this dbi-name)
          ^Rtx rtx (get-rtx pool)]
      (try
        (scan/fetch-first dbi rtx k-range k-type v-type ignore-key?)
        (catch Exception e
          (raise "Fail to get-first: " (ex-message e)
                 {:dbi    dbi-name :k-range k-range
                  :k-type k-type   :v-type  v-type}))
        (finally (.reset rtx)))))

  (get-range [this dbi-name k-range]
    (.get-range this dbi-name k-range :data :data false))
  (get-range [this dbi-name k-range k-type]
    (.get-range this dbi-name k-range k-type :data false))
  (get-range [this dbi-name k-range k-type v-type]
    (.get-range this dbi-name k-range k-type v-type false))
  (get-range [this dbi-name k-range k-type v-type ignore-key?]
    (assert (not (.closed? this)) "LMDB env is closed.")
    (let [dbi      (.get-dbi this dbi-name)
          ^Rtx rtx (get-rtx pool)]
      (try
        (scan/fetch-range dbi rtx k-range k-type v-type ignore-key?)
        (catch Exception e
          (raise "Fail to get-range: " (ex-message e)
                 {:dbi    dbi-name :k-range k-range
                  :k-type k-type   :v-type  v-type}))
        (finally (.reset rtx)))))

  (range-count [this dbi-name k-range]
    (.range-count this dbi-name k-range :data))
  (range-count [this dbi-name k-range k-type]
    (assert (not (.closed? this)) "LMDB env is closed.")
    (let [dbi      (.get-dbi this dbi-name)
          ^Rtx rtx (get-rtx pool)]
      (try
        (scan/fetch-range-count dbi rtx k-range k-type)
        (catch Exception e
          (raise "Fail to range-count: " (ex-message e)
                 {:dbi dbi-name :k-range k-range :k-type k-type}))
        (finally (.reset rtx)))))

  (get-some [this dbi-name pred k-range]
    (.get-some this dbi-name pred k-range :data :data false))
  (get-some [this dbi-name pred k-range k-type]
    (.get-some this dbi-name pred k-range k-type :data false))
  (get-some [this dbi-name pred k-range k-type v-type]
    (.get-some this dbi-name pred k-range k-type v-type false))
  (get-some [this dbi-name pred k-range k-type v-type ignore-key?]
    (assert (not (.closed? this)) "LMDB env is closed.")
    (let [dbi      (.get-dbi this dbi-name)
          ^Rtx rtx (get-rtx pool)]
      (try
        (scan/fetch-some dbi rtx pred k-range k-type v-type ignore-key?)
        (catch Exception e
          (raise "Fail to get-some: " (ex-message e)
                 {:dbi    dbi-name :k-range k-range
                  :k-type k-type   :v-type  v-type}))
        (finally (.reset rtx)))))

  (range-filter [this dbi-name pred k-range]
    (.range-filter this dbi-name pred k-range :data :data false))
  (range-filter [this dbi-name pred k-range k-type]
    (.range-filter this dbi-name pred k-range k-type :data false))
  (range-filter [this dbi-name pred k-range k-type v-type]
    (.range-filter this dbi-name pred k-range k-type v-type false))
  (range-filter [this dbi-name pred k-range k-type v-type ignore-key?]
    (assert (not (.closed? this)) "LMDB env is closed.")
    (let [dbi      (.get-dbi this dbi-name)
          ^Rtx rtx (get-rtx pool)]
      (try
        (scan/fetch-range-filtered dbi rtx pred k-range k-type v-type ignore-key?)
        (catch Exception e
          (raise "Fail to range-filter: " (ex-message e)
                 {:dbi    dbi-name :k-range k-range
                  :k-type k-type   :v-type  v-type}))
        (finally (.reset rtx)))))

  (range-filter-count [this dbi-name pred k-range]
    (.range-filter-count this dbi-name pred k-range :data))
  (range-filter-count [this dbi-name pred k-range k-type]
    (assert (not (.closed? this)) "LMDB env is closed.")
    (let [dbi      (.get-dbi this dbi-name)
          ^Rtx rtx (get-rtx pool)]
      (try
        (scan/fetch-range-filtered-count dbi rtx pred k-range k-type)
        (catch Exception e
          (raise "Fail to range-filter-count: " (ex-message e)
                 {:dbi dbi-name :k-range k-range :k-type k-type}))
        (finally (.reset rtx))))))

(defmethod open-lmdb :java
  [dir]
  (try
    (let [file          (b/file dir)
          builder       (doto (Env/create)
                          (.setMapSize (* ^long c/+init-db-size+ 1024 1024))
                          (.setMaxReaders c/+max-readers+)
                          (.setMaxDbs c/+max-dbs+))
          ^Env env      (.open builder
                               file
                               (into-array EnvFlags default-env-flags))
          ^RtxPool pool (->RtxPool env (ConcurrentHashMap.) 0)]
      (->LMDB env dir pool (ConcurrentHashMap.)))
    (catch Exception e
      (raise
        "Fail to open LMDB database: " (ex-message e)
        {:dir dir}))))
