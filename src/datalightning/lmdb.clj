(ns datalightning.lmdb
  "Wrapping LMDB"
  (:refer-clojure :exclude [get])
  (:require [datalightning.util :as util]
            [taoensso.timbre :as log])
  (:import [org.lmdbjava Env EnvFlags Env$MapFullException Dbi DbiFlags
            PutFlags Txn CursorIterable CursorIterable$KeyVal KeyRange]
           [java.util Iterator]
           [java.util.concurrent ConcurrentHashMap]
           [java.util.concurrent.atomic AtomicLong AtomicBoolean]
           [java.nio ByteBuffer]))

(def default-env-flags [EnvFlags/MDB_NOTLS
                        EnvFlags/MDB_NORDAHEAD])

(def default-dbi-name "default")
(def default-dbi-flags [DbiFlags/MDB_CREATE])

(def ^:const +init-db-size+ 10) ; in megabytes
(def ^:const +max-dbs+ 64)
(def ^:const +max-readers+ 126)
(def ^:const +max-key-size+ 511) ; in bytes
(def ^:const +default-val-size+ 3456)

(defprotocol IBuffer
  (put-key [this data k-type]
    "put data in key buffer, k-type can be :long, :bytes, :data")
  (put-val [this data v-type]
    "put data in val buffer, v-type can be :long, :bytes, :data"))

(defn- put-buffer
  [bf x t]
  (case t
    :long  (util/put-long bf x)
    :bytes (util/put-bytes bf x)
    (util/put-data bf x)))

(defprotocol IKV
  (put [this txn] [this txn put-flags]
    "Put kv pair given in `put-key` and `put-val`")
  (get [this txn] "Get value of the key given in `put-key`")
  (del [this txn] "Delete the key given in `put-key`"))

(deftype DBI [^Dbi db ^ByteBuffer kb ^ByteBuffer vb]
  IBuffer
  (put-key [_ x t]
    (put-buffer kb x t))
  (put-val [_ x t]
    (put-buffer vb x t))

  IKV
  (put [this txn]
    (put this txn nil))
  (put [_ txn flags]
    (.flip kb)
    (.flip vb)
    (if flags
      (.put db txn kb vb (into-array PutFlags flags))
      (.put db txn kb vb (make-array PutFlags 0)))
    (.clear kb)
    (.clear vb))
  (get [_ txn]
    (.flip kb)
    (let [res (.get db txn kb)]
      (.clear kb)
      res))
  (del [_ txn]
    (.flip kb)
    (.delete db txn kb)
    (.clear kb)))

(defprotocol IRtx
  (close-rtx [this] "close the read-only transaction")
  (in-use? [this] "return true if this transaction is in use")
  (reset [this] "reset transaction so it can be reused upon renew")
  (renew [this] "renew and return previously reset transaction for reuse"))

(deftype Rtx [^Txn txn ^AtomicBoolean use]
  IRtx
  (close-rtx [_]
    (.set use false)
    (.close txn))
  (in-use? [_]
    (.get use))
  (reset [_]
    (locking use
      (.set use false)
      (.reset txn)))
  (renew [this]
    (locking use
      (.set use true)
      (.renew txn)
      this)))

(defn- new-rtx
  [^Env env ^ConcurrentHashMap rtxs ^AtomicLong cnt]
  (locking cnt
    (let [rtx (->Rtx (.txnRead env) (AtomicBoolean. false))]
      (reset rtx)
      (.put rtxs (.get cnt) rtx)
      (.addAndGet cnt 1)
      (renew rtx))))

(defprotocol IRtxPool
  (close-pool [this] "Close all transactions in the pool")
  (get-rtx [this] "Obtain a ready-to-use read-only transaction")
  (return-rtx [this rtx] "Reset the transaction and return it to the pool"))

(deftype RtxPool [^Env env ^ConcurrentHashMap rtxs ^AtomicLong cnt]
  IRtxPool
  (close-pool [_]
    (doseq [^Rtx rtx (.values rtxs)] (close-rtx rtx)))
  (get-rtx [_]
    (let [total (.get cnt)]
      (if (zero? total)
       (new-rtx env rtxs cnt)
       (loop [i (.getId ^Thread (Thread/currentThread))]
         (let [i'        (mod i total)
               ^Rtx rtxi (.get rtxs i')]
          (if-not (in-use? rtxi)
            (renew rtxi)
            (if (< total +max-readers+)
              (new-rtx env rtxs cnt)
              (recur (inc i'))))))))))

(defprotocol ILMDB
  (close [this] "Close this LMDB env")
  (open-dbi [this dbi-name] [this dbi-name key-size val-size flags]
    "Open a named dbi (i.e. sub-db) in a LMDB")
  (get-dbi [this dbi-name] "Lookup DBI (i.e. sub-db) by name")
  (transact [this txs]
    "Update db, txs is a seq of [op dbi-name k v k-type v-type] when op is :put,
     [op dbi-name k k-type] when op is :del; k-type and v-type can be :long,
     :bytes, or :data")
  (get-value
    [this dbi-name k]
    [this dbi-name k k-type]
    [this dbi-name k k-type v-type]
    "Get the value of a key, k-type and v-type can be :data, :bytes or :long")
  (get-range
    [this dbi-name k-range]
    [this dbi-name k-range k-type]
    [this dbi-name k-range k-type v-type]
    "Return a seq of kv pair in the specified key range,
     k-type and v-type can be :data (default), :long, or :bytes"))

(defn- double-db-size [^Env env]
  (.setMapSize env (* 2 (-> env .info .mapSize))))

(defn- read-buffer
  [^ByteBuffer bb v-type]
  (case v-type
    :long  (util/get-long bb)
    :bytes (util/get-bytes bb)
    (util/get-data bb)))

(defn- fetch-value
  [dbi rtx k k-type v-type]
  (put-key dbi k k-type)
  (when-let [^ByteBuffer bb (get dbi rtx)]
    (read-buffer bb v-type)))

(deftype LMDB [^Env env
               ^String dir
               ^RtxPool pool
               ^ConcurrentHashMap dbis]
  ILMDB
  (close [_]
    (close pool)
    (.close env))
  (open-dbi [this dbi-name]
    (open-dbi this dbi-name +max-key-size+ +default-val-size+ default-dbi-flags))
  (open-dbi [_ dbi-name key-size val-size flags]
    (let [kb   (ByteBuffer/allocateDirect key-size)
          vb   (ByteBuffer/allocateDirect val-size)
          db   (.openDbi env
                         ^String dbi-name
                         ^"[Lorg.lmdbjava.DbiFlags;" (into-array DbiFlags flags))
          dbi  (->DBI db kb vb)]
      (.put dbis dbi-name dbi)
      dbi))
  (get-dbi [_ dbi-name]
    (or (.get dbis dbi-name)
        (throw (ex-info (str "`open-dbi` was not called for " dbi-name) {}))))
  (transact [this txs]
    (with-open [txn (.txnWrite env)]
      (try
        (doseq [[op dbi-name k & r] txs
                :let                [dbi (get-dbi this dbi-name)]]
          (case op
            :put (let [[v kt vt] r]
                   (put-key dbi k kt)
                   (put-val dbi v vt)
                   (put dbi txn))
            :del (let [[kt] r]
                   (put-key dbi k kt)
                   (del dbi txn))))
        (catch Env$MapFullException e
          (.abort txn)
          (double-db-size env)
          (transact this txs)))
      (.commit txn)))
  (get-value [this dbi-name k]
    (get-value this dbi-name k :data :data))
  (get-value [this dbi-name k k-type]
    (get-value this dbi-name k k-type :data))
  (get-value [this dbi-name k k-type v-type]
    (.renew rtx)
    (try
      (fetch-value (get-dbi this dbi-name) rtx k k-type v-type)
      (catch Exception e
        (throw (ex-info (str "Fail to get-value: " (ex-message e))
                        {:dbi dbi-name :k k :k-type k-type :v-type v-type})))
      (finally (.reset rtx))))
  (get-range [this dbi-name k-range]
    (get-range this dbi-name k-range :data :data))
  (get-range [this dbi-name k-range k-type]
    (get-range this dbi-name k-range k-type :data))
  (get-range [this dbi-name k-range k-type v-type]
    (let [^DBI dbi (get-dbi this dbi-name)]
      (with-open [^Txn txn (.txnRead env)
                  iterable (.iterate ^Dbi (.-db dbi) txn k-range)]
        (loop [^Iterator iter (.iterator ^CursorIterable iterable)
               holder         (transient [])]
          (let [^CursorIterable$KeyVal kv (.next iter)
                k                         (-> kv (.key) (read-buffer k-type))
                v                         (-> kv (.val) (read-buffer v-type))
                holder'                   (conj! holder [k v])]
            (if (.hasNext iter)
              (recur iter holder')
              (persistent! holder'))))))))

(defn open-lmdb
  "Open an LMDB env"
  ([dir]
   (apply open-lmdb dir +init-db-size+ default-env-flags))
  ([dir size & flags]
   (let [file     (util/file dir)
         builder  (doto (Env/create)
                    (.setMapSize (* ^long size 1024 1024))
                    (.setMaxReaders +max-readers+)
                    (.setMaxDbs +max-dbs+))
         ^Env env (.open builder file (into-array EnvFlags flags))
         ^Txn rtx (.txnRead env)]
     (.reset rtx)
     (->LMDB env dir rtx (ConcurrentHashMap.)))))
