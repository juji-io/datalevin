(ns datalightning.lmdb
  "Wrapping LMDB"
  (:refer-clojure :exclude [get])
  (:require [datalightning.util :as util]
            [taoensso.nippy :as nippy]
            [taoensso.timbre :as log])
  (:import [org.lmdbjava Env EnvFlags Env$MapFullException Dbi DbiFlags
            PutFlags Txn CursorIterable CursorIterable$KeyVal KeyRange]
           [java.util Iterator]
           [java.nio ByteBuffer]))

;; (set! *unchecked-math* :warn-on-boxed)

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

(defn- check-buffer-overflow
  [^long length ^long remaining]
  (assert (<= length remaining)
          (str "BufferOverlfow: trying to put "
               length
               " bytes while "
               remaining
               "remaining in the ByteBuffer.")))

(defn- put-long
  [^ByteBuffer bb n]
  (assert (integer? n) "put-long requires an integer")
  (check-buffer-overflow Long/BYTES (.remaining bb))
  (.putLong bb ^long (long n)))

(defn- put-bytes
  [^ByteBuffer bb ^bytes bs]
  (let [len (alength bs)]
    (assert (< 0 len) "Cannot put empty byte array into ByteBuffer")
    (check-buffer-overflow len (.remaining bb))
    (.put bb bs)))

(defn- put-data
  [^ByteBuffer b x]
  (put-bytes b (nippy/fast-freeze x)))

(defn- put-buffer
  [bf x t]
  (case t
    :long  (put-long bf x)
    :bytes (put-bytes bf x)
    (put-data bf x)))

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
  (get-dbis [this] "Return a map from dbi names to DBI (i.e. sub-db)")
  (set-dbis [this dbis] "Set a map from dbi names to DBI")
  (get-value
    [this dbi-name k]
    [this dbi-name k k-type]
    [this dbi-name k k-type v-type]
    "Get the value of a key, k-type and v-type can be :data, :bytes or :long")
  (transact [this txs]
    "Update db, txs is a seq of [op dbi-name k v k-type v-type] when op is :put,
     [op dbi-name k k-type] when op is :del; k-type and v-type can be :long,
     :bytes, or :data")
  #_(get-range
    [this dbi-name k-range]
    [this dbi-name k-range v-type]
    [this dbi-name k-range k-type v-type]
    "Return a seq of kv pair in the specified key range,
     k-type and v-type can be :data (default), :long, or :bytes"))

(defn- double-db-size [^Env env]
  (.setMapSize env (* 2 (-> env .info .mapSize))))

(defn- throw-dbi-not-open
  [dbi-name dbis]
  (throw (ex-info (str "`open-dbi` was not called for " dbi-name)
                  {:dbis (keys dbis)})))

(defn- copy-bytes
  "copy content from a ByteBuffer to a byte array, for buffer content is gone
  when txn is done"
  [^ByteBuffer bb]
  (let [n   (.remaining bb)
        arr (byte-array n)]
    (.get bb arr)
    arr))

(deftype LMDB [^Env env
               ^String dir
               ^Txn rtx ; reuse a transaction for random reads
               ^:volatile-mutable dbis]
  ILMDB
  (close [_]
    (.close rtx)
    (.close env))
  (get-dbis [_]
    dbis)
  (set-dbis [_ new-dbis]
    (set! dbis new-dbis))
  (get-value [this dbi-name k]
    (get-value this dbi-name k :data :data))
  (get-value [this dbi-name k k-type]
    (get-value this dbi-name k k-type :data))
  (get-value [this dbi-name k k-type v-type]
    (.renew rtx)
    (try
      (let [dbis            (get-dbis this)
           ^ByteBuffer raw (if-let [dbi (dbis dbi-name)]
                             (do (put-key dbi k k-type)
                                 (get dbi rtx))
                             (throw-dbi-not-open dbi-name dbis))]
       (when raw
         (case v-type
           :long  (.getLong raw)
           :bytes (copy-bytes raw)
           :data  (when-let [bs (copy-bytes raw)]
                    (nippy/fast-thaw bs)))))
      (catch Exception e
        (throw (ex-info (str "Error get-value: " (ex-message e))
                        {:dbi dbi-name :k k :k-type k-type :v-type v-type})))
      (finally (.reset rtx))))
  (transact [this txs]
    (with-open [txn (.txnWrite env)]
      (try
        (doseq [[op dbi-name k & r] txs
                :let                [dbis (get-dbis this)
                                     dbi (dbis dbi-name)]]
          (if dbi
            (case op
              :put (let [[v kt vt] r]
                     (put-key dbi k kt)
                     (put-val dbi v vt)
                     (put dbi txn))
              :del (let [[kt] r]
                     (put-key dbi k kt)
                     (del dbi txn)))
            (throw-dbi-not-open dbi-name dbis)))
        (catch Env$MapFullException e
          (.abort txn)
          (double-db-size env)
          (transact this txs)))
      (.commit txn)))
  #_(iterator [this txn dbi-name k-range]
      (if-let [^DBI dbi (dbis dbi-name)]
      (.iterate ^Dbi (.-db dbi) txn k-range)
      (throw-dbi-not-open dbi-name dbis))))

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
     (->LMDB env dir rtx {}))))

(defn open-dbi
  "Open a named dbi (i.e. sub-db) in a LMDB"
  ([lmdb name]
   (apply open-dbi
          lmdb name +max-key-size+ +default-val-size+ default-dbi-flags))
  ([lmdb name key-size val-size & flags]
   (let [env  (.-env ^LMDB lmdb)
         kb   (ByteBuffer/allocateDirect key-size)
         vb   (ByteBuffer/allocateDirect val-size)
         db   (.openDbi ^Env env
                        ^String name
                        ^"[Lorg.lmdbjava.DbiFlags;" (into-array DbiFlags flags))
         dbi  (->DBI db kb vb)
         dbis (get-dbis lmdb)]
     (set-dbis lmdb (assoc dbis name dbi))
     dbi)))
