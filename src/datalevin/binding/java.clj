(ns ^:no-doc datalevin.binding.java
  "LMDB binding for Java"
  (:require [datalevin.bits :as b]
            [datalevin.util :refer [raise]]
            [datalevin.constants :as c]
            [datalevin.lmdb :refer [open-lmdb IBuffer IRange IRtx IKV ILMDB]]
            [clojure.string :as s])
  (:import [org.lmdbjava Env EnvFlags Env$MapFullException Stat Dbi DbiFlags
            PutFlags Txn CursorIterable CursorIterable$KeyVal KeyRange
            Txn$BadReaderLockException]
           [clojure.lang IMapEntry]
           [java.util Iterator]
           [java.util.concurrent ConcurrentHashMap]
           [java.nio.charset StandardCharsets]
           [java.nio ByteBuffer]))

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
    (set! use? false)
    (.close txn))
  (reset [this]
    (.reset txn)
    (set! use? false)
    this)
  (renew [this]
    (when-not use?
      (.renew txn)
      (set! use? true)
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
      (let [rtx (->Rtx (.txnRead env)
                       false
                       (ByteBuffer/allocateDirect c/+max-key-size+)
                       (ByteBuffer/allocateDirect c/+max-key-size+)
                       (ByteBuffer/allocateDirect c/+max-key-size+))]
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

(defn- key-range
  [range-type kb1 kb2]
  (case range-type
    :all               (KeyRange/all)
    :all-back          (KeyRange/allBackward)
    :at-least          (KeyRange/atLeast kb1)
    :at-least-back     (KeyRange/atLeastBackward kb1)
    :at-most           (KeyRange/atMost kb1)
    :at-most-back      (KeyRange/atMostBackward kb1)
    :closed            (KeyRange/closed kb1 kb2)
    :closed-back       (KeyRange/closedBackward kb1 kb2)
    :closed-open       (KeyRange/closedOpen kb1 kb2)
    :closed-open-back  (KeyRange/closedOpenBackward kb1 kb2)
    :greater-than      (KeyRange/greaterThan kb1)
    :greater-than-back (KeyRange/greaterThanBackward kb1)
    :less-than         (KeyRange/lessThan kb1)
    :less-than-back    (KeyRange/lessThanBackward kb1)
    :open              (KeyRange/open kb1 kb2)
    :open-back         (KeyRange/openBackward kb1 kb2)
    :open-closed       (KeyRange/openClosed kb1 kb2)
    :open-closed-back  (KeyRange/openClosedBackward kb1 kb2)))

(deftype ^:no-doc DBI [^Dbi db ^ByteBuffer kb ^:volatile-mutable ^ByteBuffer vb]
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
            (set! vb (ByteBuffer/allocateDirect size))
            (b/put-buffer vb x t)
            (.flip vb))
          (raise "Error putting r/w value buffer of "
                 (.dbi-name this) ": " (ex-message e)
                 {:value x :type t :dbi (.dbi-name this)})))))

  IKV
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
  (iterate-kv [this rtx range-type]
    (let [^ByteBuffer start-kb (.-start-kb ^Rtx rtx)
          ^ByteBuffer stop-kb  (.-stop-kb ^Rtx rtx)]
      (.iterate db (.-txn ^Rtx rtx) (key-range range-type start-kb stop-kb)))))

(defn- ^IMapEntry map-entry
  [^CursorIterable$KeyVal kv]
  (reify IMapEntry
    (key [_] (.key kv))
    (val [_] (.val kv))
    (equals [_ o] (and (= (.key kv) (key o)) (= (.val kv) (val o))))
    (getKey [_] (.key kv))
    (getValue [_] (.val kv))
    (setValue [_ _] (raise "IMapEntry is immutable" {}))
    (hashCode [_] (hash-combine (hash (.key kv)) (hash (.val kv))))))

(defn- up-db-size [^Env env]
  (.setMapSize env (* 10 (-> env .info .mapSize))))

(defn- fetch-value
  [^DBI dbi ^Rtx rtx k k-type v-type ignore-key?]
  (.put-key rtx k k-type)
  (when-let [^ByteBuffer bb (.get-kv dbi rtx)]
    (if ignore-key?
      (b/read-buffer bb v-type)
      [(b/expected-return k k-type) (b/read-buffer bb v-type)])))

(defn- read-key
  ([kv k-type v]
   (read-key kv k-type v false))
  ([kv k-type v rewind?]
   (if (and v (not= v c/normal) (c/index-types k-type))
     b/overflown-key
     (b/read-buffer (if rewind?
                      (.rewind ^ByteBuffer (key kv))
                      (key kv))
                    k-type))))

(defn- fetch-first
  [^DBI dbi ^Rtx rtx [range-type k1 k2] k-type v-type ignore-key?]
  (.put-start-key rtx k1 k-type)
  (.put-stop-key rtx k2 k-type)
  (with-open [^CursorIterable iterable (.iterate-kv dbi rtx range-type)]
    (let [^Iterator iter (.iterator iterable)]
      (when (.hasNext iter)
        (let [kv (map-entry (.next iter))
              v  (when (not= v-type :ignore) (b/read-buffer (val kv) v-type))]
          (if ignore-key?
            (if v v true)
            [(read-key kv k-type v) v]))))))

(defn- fetch-range
  [^DBI dbi ^Rtx rtx [range-type k1 k2] k-type v-type ignore-key?]
  (assert (not (and (= v-type :ignore) ignore-key?))
          "Cannot ignore both key and value")
  (.put-start-key rtx k1 k-type)
  (.put-stop-key rtx k2 k-type)
  (with-open [^CursorIterable iterable (.iterate-kv dbi rtx range-type)]
    (loop [^Iterator iter (.iterator iterable)
           holder         (transient [])]
      (if (.hasNext iter)
        (let [kv      (map-entry (.next iter))
              v       (when (not= v-type :ignore)
                        (b/read-buffer (val kv) v-type))
              holder' (conj! holder
                             (if ignore-key?
                               v
                               [(read-key kv k-type v) v]))]
          (recur iter holder'))
        (persistent! holder)))))

(defn- fetch-range-count
  [^DBI dbi ^Rtx rtx [range-type k1 k2] k-type]
  (.put-start-key rtx k1 k-type)
  (.put-stop-key rtx k2 k-type)
  (with-open [^CursorIterable iterable (.iterate-kv dbi rtx range-type)]
    (loop [^Iterator iter (.iterator iterable)
           c              0]
      (if (.hasNext iter)
        (do (.next iter) (recur iter (inc c)))
        c))))

(defn- fetch-some
  [^DBI dbi ^Rtx rtx pred [range-type k1 k2] k-type v-type ignore-key?]
  (assert (not (and (= v-type :ignore) ignore-key?))
          "Cannot ignore both key and value")
  (.put-start-key rtx k1 k-type)
  (.put-stop-key rtx k2 k-type)
  (with-open [^CursorIterable iterable (.iterate-kv dbi rtx range-type)]
    (loop [^Iterator iter (.iterator iterable)]
      (when (.hasNext iter)
        (let [kv (map-entry (.next iter))]
          (if (pred kv)
            (let [v (when (not= v-type :ignore)
                      (b/read-buffer (.rewind ^ByteBuffer (val kv)) v-type))]
              (if ignore-key?
                v
                [(read-key kv k-type v true) v]))
            (recur iter)))))))

(defn- fetch-range-filtered
  [^DBI dbi ^Rtx rtx pred [range-type k1 k2] k-type v-type ignore-key?]
  (assert (not (and (= v-type :ignore) ignore-key?))
          "Cannot ignore both key and value")
  (.put-start-key rtx k1 k-type)
  (.put-stop-key rtx k2 k-type)
  (with-open [^CursorIterable iterable (.iterate-kv dbi rtx range-type)]
    (loop [^Iterator iter (.iterator iterable)
           holder         (transient [])]
      (if (.hasNext iter)
        (let [kv (map-entry (.next iter))]
          (if (pred kv)
            (let [v       (when (not= v-type :ignore)
                            (b/read-buffer
                              (.rewind ^ByteBuffer (val kv)) v-type))
                  holder' (conj! holder
                                 (if ignore-key?
                                   v
                                   [(read-key kv k-type v true) v]))]
              (recur iter holder'))
            (recur iter holder)))
        (persistent! holder)))))

(defn- fetch-range-filtered-count
  [^DBI dbi ^Rtx rtx pred [range-type k1 k2] k-type]
  (.put-start-key rtx k1 k-type)
  (.put-stop-key rtx k2 k-type)
  (with-open [^CursorIterable iterable (.iterate-kv dbi rtx range-type)]
    (loop [^Iterator iter (.iterator iterable)
           c              0]
      (if (.hasNext iter)
        (let [kv (map-entry (.next iter))]
          (if (pred kv)
            (recur iter (inc c))
            (recur iter c)))
        c))))

(deftype LMDB [^Env env ^String dir ^RtxPool pool ^ConcurrentHashMap dbis]
  ILMDB

  (close [_]
    (close-pool pool)
    (.close env))

  (closed? [_]
    (.isClosed env))

  (open-dbi [this dbi-name]
    (.open-dbi this dbi-name c/+max-key-size+ c/+default-val-size+
               default-dbi-flags))
  (open-dbi [this dbi-name key-size]
    (.open-dbi this dbi-name key-size c/+default-val-size+ default-dbi-flags))
  (open-dbi [this dbi-name key-size val-size]
    (.open-dbi this dbi-name key-size val-size default-dbi-flags))
  (open-dbi [this dbi-name key-size val-size flags]
    (assert (not (.closed? this)) "LMDB env is closed.")
    (let [kb  (ByteBuffer/allocateDirect key-size)
          vb  (ByteBuffer/allocateDirect val-size)
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
                :let                [dbi (.get-dbi this dbi-name)]]
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
    (let [dbi (.get-dbi this dbi-name)
          rtx (get-rtx pool)]
      (try
        (fetch-value dbi rtx k k-type v-type ignore-key?)
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
    (let [dbi (.get-dbi this dbi-name)
          rtx (get-rtx pool)]
      (try
        (fetch-first dbi rtx k-range k-type v-type ignore-key?)
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
    (let [dbi (.get-dbi this dbi-name)
          rtx (get-rtx pool)]
      (try
        (fetch-range dbi rtx k-range k-type v-type ignore-key?)
        (catch Exception e
          (raise "Fail to get-range: " (ex-message e)
                 {:dbi    dbi-name :k-range k-range
                  :k-type k-type   :v-type  v-type}))
        (finally (.reset rtx)))))

  (range-count [this dbi-name k-range]
    (.range-count this dbi-name k-range :data))
  (range-count [this dbi-name k-range k-type]
    (assert (not (.closed? this)) "LMDB env is closed.")
    (let [dbi (.get-dbi this dbi-name)
          rtx (get-rtx pool)]
      (try
        (fetch-range-count dbi rtx k-range k-type)
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
    (let [dbi (.get-dbi this dbi-name)
          rtx (get-rtx pool)]
      (try
        (fetch-some dbi rtx pred k-range k-type v-type ignore-key?)
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
    (let [dbi (.get-dbi this dbi-name)
          rtx (get-rtx pool)]
      (try
        (fetch-range-filtered dbi rtx pred k-range k-type v-type ignore-key?)
        (catch Exception e
          (raise "Fail to range-filter: " (ex-message e)
                 {:dbi    dbi-name :k-range k-range
                  :k-type k-type   :v-type  v-type}))
        (finally (.reset rtx)))))

  (range-filter-count [this dbi-name pred k-range]
    (.range-filter-count this dbi-name pred k-range :data))
  (range-filter-count [this dbi-name pred k-range k-type]
    (assert (not (.closed? this)) "LMDB env is closed.")
    (let [dbi (.get-dbi this dbi-name)
          rtx (get-rtx pool)]
      (try
        (fetch-range-filtered-count dbi rtx pred k-range k-type)
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
      (LMDB. env dir pool (ConcurrentHashMap.)))
    (catch Exception e
      (raise
        "Fail to open LMDB database: " (ex-message e)
        {:dir dir}))))
