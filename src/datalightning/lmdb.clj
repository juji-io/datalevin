(ns datalightning.lmdb
  "Wrapping LMDB"
  (:refer-clojure :exclude [get])
  (:require [datalightning.util :as util]
            [taoensso.timbre :as log])
  (:import [org.lmdbjava Env EnvFlags Env$MapFullException Dbi DbiFlags
            PutFlags Txn CursorIterable CursorIterable$KeyVal KeyRange]
           [java.util Iterator]
           [java.util.concurrent ConcurrentHashMap]
           [java.nio ByteBuffer]))

(def default-env-flags [EnvFlags/MDB_NOTLS
                        EnvFlags/MDB_NORDAHEAD])

(def default-dbi-name "default")
(def default-dbi-flags [DbiFlags/MDB_CREATE])

(def ^:const +init-db-size+ 10) ; in megabytes
(def ^:const +max-dbs+ 64)
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
               ^Txn rtx ; reuse a transaction for random reads
               ^ConcurrentHashMap dbis]
  ILMDB
  (close [_]
    (.close rtx)
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
                    (.setMaxDbs +max-dbs+))
         ^Env env (.open builder file (into-array EnvFlags flags))
         ^Txn rtx (.txnRead env)]
     (.reset rtx)
     (->LMDB env dir rtx (ConcurrentHashMap.)))))
