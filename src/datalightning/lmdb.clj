(ns datalightning.lmdb
  "Wrapper of LMDBJava"
  (:refer-clojure :exclude [get count])
  (:require [datalightning.util :as util]
            [taoensso.nippy :as nippy]
            [taoensso.timbre :as log])
  (:import [org.lmdbjava Env EnvFlags Dbi DbiFlags PutFlags Txn
            Env$MapFullException]
           [java.nio ByteBuffer]))

(set! *unchecked-math* :warn-on-boxed)

(def default-env-flags [EnvFlags/MDB_NOTLS
                        EnvFlags/MDB_NORDAHEAD])

(def default-dbi-name "default")
(def default-dbi-flags [DbiFlags/MDB_CREATE])

(def ^:const +init-db-size+ 10) ; in megabytes
(def ^:const +max-dbs+ 64)
(def ^:const +max-key-size+ 511) ; in bytes
(def ^:const +default-val-size+ 3456)

(defprotocol IBuffer
  (put-key [this data] "put data in key buffer")
  (put-val [this data] "put data in val buffer")
  (put-long-key [this i] "put a long in key buffer")
  (put-long-val [this i] "put a long in val buffer"))

;; (defprotocol IDBI
;;   (open-cursor [this txn] "Open a Cursor (i.e. iterator)")
;;   (iterate [this txn key-range] "Return a Cursor based on key range"))

(defn- check-buffer-overflow
  [^long length ^long remaining]
  (assert (<= length remaining)
          (str "BufferOverlfow: trying to put "
               length
               " bytes while "
               remaining
               "remaining in the ByteBuffer.")))

(defn- put-bytes
  [^ByteBuffer bb ^bytes bs]
  (let [len (alength bs)]
    (assert (< 0 len) "Cannot put empty byte array into ByteBuffer")
    (check-buffer-overflow len (.remaining bb))
    (.put bb bs)))

(defn- put-data
  [^ByteBuffer b x]
  (if (bytes? x)
    (put-bytes b x)
    (put-bytes b (nippy/fast-freeze x))))

(defprotocol IKV
  (put [this txn] [this txn put-flags]
    "Put kv pair given in `put-key` and `put-val`")
  (get [this txn] "Get value of the key given in `put-key`")
  (del [this txn] "Delete the key given in `put-key`"))

(deftype DBI [^Dbi db ^ByteBuffer kb ^ByteBuffer vb]
  IBuffer
  (put-key [_ x]
    (put-data kb x))
  (put-val [_ x]
    (put-data vb x))
  (put-long-key [_ i]
    (check-buffer-overflow Long/BYTES (.remaining kb))
    (.putLong kb i))
  (put-long-val [_ i]
    (check-buffer-overflow Long/BYTES (.remaining vb))
    (.putLong vb i))

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
  (get-long [this dbi-name k] [this dbi-name k k-type]
    "Get value of a key as long, k-type is :bytes or :long")
  (get-bytes [this dbi-name k] [this dbi-name k k-type]
    "Get value of a key as bytes, k-type is :bytes or :long")
  (get-data [this dbi-name k] [this dbi-name k k-type]
    "Get value of a key as Clojure data, k-type is :bytes or :long")
  (get-raw [this dbi-name k] [this dbi-name k k-type]
    "Get the value of a key as ByteBuffer, k-type is :bytes or :long")
  (transact [this txs]
    "Update db, txs is a seq of [op dbi-name k v],
     op is one of :put, :put-long-key, :put-long-val, :put-long-kv,
     :del, or :del-long-key"))

(defn- double-db-size [^Env env]
  (.setMapSize env (* 2 (-> env .info .mapSize))))

(defn- perform-update
  [op k v dbi txn]
  (case op
    :put          (do (put-key dbi k)
                      (put-val dbi v)
                      (put dbi txn))
    :put-long-key (do (assert (integer? k) "key must be integer")
                      (put-long-key dbi (long k))
                      (put-val dbi v)
                      (put dbi txn))
    :put-long-val (do (assert (integer? v) "val must be integer")
                      (put-key dbi k)
                      (put-long-val dbi (long v))
                      (put dbi txn))
    :put-long-kv  (do (assert (and (integer? k) (integer? v))
                              "key and val must be integer")
                      (put-long-key dbi (long k))
                      (put-long-val dbi (long v))
                      (put dbi txn))
    :del          (do (put-key dbi k)
                      (del dbi txn))
    :del-long-key (do (assert (integer? k) "key must be integer")
                      (put-long-key dbi (long k))
                      (del dbi txn))))

(deftype LMDB [^Env env ^String dir ^Txn rtx ^:volatile-mutable dbis]
  ILMDB
  (close [_]
    (.close rtx)
    (.close env))
  (get-dbis [_]
    dbis)
  (set-dbis [_ new-dbis]
    (set! dbis new-dbis))
  (get-long [this dbi-name k]
    (get-long this dbi-name k :bytes))
  (get-long [this dbi-name k k-type]
    (if-let [^ByteBuffer bb (get-raw this dbi-name k k-type)]
      (let [res (.getLong bb)]
        (.reset rtx)
        res)
      (.reset rtx)))
  (get-bytes [this dbi-name k]
    (get-bytes this dbi-name k :bytes))
  (get-bytes [this dbi-name k k-type]
    (if-let [^ByteBuffer bb (get-raw this dbi-name k k-type)]
      (let [n   (.remaining bb)
            arr (byte-array n)]
        (.get bb arr)
        (.reset rtx)
        arr)
      (.reset rtx)))
  (get-data [this dbi-name k]
    (get-data this dbi-name k :bytes))
  (get-data [this dbi-name k k-type]
    (when-let [bs (get-bytes this dbi-name k k-type)]
      (nippy/fast-thaw bs)))
  (get-raw [this dbi-name k]
    (get-raw this dbi-name k :bytes))
  (get-raw [this dbi-name k k-type]
    (.renew rtx)
    (let [dbis (get-dbis this)]
      (if-let [dbi (dbis dbi-name)]
        (do (case k-type
              :bytes (put-key dbi k)
              :long  (put-long-key dbi k))
            (get dbi rtx))
        (do (.reset rtx)
            (throw (ex-info (str "`open-dbi` was not called for " dbi-name)
                            {:dbis (keys dbis)}))))))
  (transact [this txs]
    (with-open [txn (.txnWrite env)]
      (try
        (doseq [[op dbi-name k v] txs
                :let              [dbis (get-dbis this)
                                   dbi (dbis dbi-name)]]
          (if dbi
            (perform-update op k v dbi txn)
            (throw (ex-info (str "`open-dbi` was not called for " dbi-name)
                            {:dbis (keys dbis)}))))
        (catch Env$MapFullException e
          (.abort txn)
          (double-db-size env)
          (transact this txs)))
      (.commit txn))))

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
