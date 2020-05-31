(ns datalightning.lmdb
  "Wrapper of LMDBJava"
  (:refer-clojure :exclude [get count])
  (:require [datalightning.util :as util]
            [taoensso.nippy :as nippy]
            [taoensso.timbre :as log])
  (:import [org.lmdbjava Env EnvFlags Dbi DbiFlags PutFlags Txn]
           [java.nio ByteBuffer]))


(def default-env-flags [EnvFlags/MDB_NOTLS
                        EnvFlags/MDB_NORDAHEAD])

(def default-db-size 10) ; in megabytes

(def max-dbs 32)

(def default-dbi-name "default")
(def default-dbi-flags [DbiFlags/MDB_CREATE])

(def default-key-size 511) ; in bytes, this is the maximum already
(def default-val-size 3456)

(defprotocol IKV
  (put-key [this data] "put data in key buffer")
  (put-val [this data] "put data in val buffer")
  (put [this txn] [this txn put-flags]
    "Put kv pair given in `put-key` and `put-val`")
  (get [this txn] "Get value of the key given in `put-key`")
  (del [this txn] "Delete the key given in `put-key"))

;; (defprotocol IDBI
;;   (open-cursor [this txn] "Open a Cursor (i.e. iterator)")
;;   (iterate [this txn key-range] "Return a Cursor based on key range"))

(defn write-buffer
  "Write v to a ByteBuffer. Written as long if v is an integer, otherwise as
  a byte array"
  [^ByteBuffer b v]
  (cond
    (integer? v) (.putLong b (long v))
    (bytes? v)   (.put b ^bytes v)
    :else        (.put b ^bytes (nippy/freeze v))))

(deftype DBI [^Dbi db ^ByteBuffer kb ^ByteBuffer vb]
  IKV
  (put-key [_ d]
    (write-buffer kb d))
  (put-val [_ d]
    (write-buffer vb d))
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
  (get-long [this dbi-name k] "Get value of a key as long")
  (get-bytes [this dbi-name k] "Get value of a key as bytes")
  (get-data [this dbi-name k] "Get value of a key as Clojure data")
  (get-raw [this dbi-name k]
    "Get the value of a key as ByteBuffer, leave the read transaction open")
  (transact [this txs]
    "Update db, txs is a seq of [op dbi-name k v],
     op is one of :put or :del"))

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
    (if-let [^ByteBuffer bb (get-raw this dbi-name k)]
      (let [res (.getLong bb)]
        (.reset rtx)
        res)
      (.reset rtx)))
  (get-bytes [this dbi-name k]
    (if-let [^ByteBuffer bb (get-raw this dbi-name k)]
      (let [n   (.remaining bb)
            arr (byte-array n)]
        (.get bb arr)
        (.reset rtx)
        arr)
      (.reset rtx)))
  (get-data [this dbi-name k]
    (nippy/thaw (get-bytes this dbi-name k)))
  (get-raw [this dbi-name k]
    (.renew rtx)
    (let [dbis (get-dbis this)]
      (if-let [dbi (dbis dbi-name)]
        (do (put-key dbi k)
            (get dbi rtx))
        (do (.reset rtx)
            (throw (ex-info (str "`open-dbi` was not called for " dbi-name)
                            {:dbis (keys dbis)}))))))
  (transact [this txs]
    (with-open [txn (.txnWrite env)]
      (doseq [[op dbi-name k v] txs
              :let              [dbis (get-dbis this)
                                 dbi (dbis dbi-name)]]
        (if dbi
          (case op
            :del (do (put-key dbi k)
                     (del dbi txn))
            :put (do (put-key dbi k)
                     (put-val dbi v)
                     (put dbi txn)))
          (throw (ex-info (str "`open-dbi` was not called for " dbi-name)
                          {:dbis (keys dbis)}))))
      (.commit txn))))

(defn open-lmdb
  "Open an LMDB env"
  ([dir]
   (apply open-lmdb dir default-db-size default-env-flags))
  ([dir size & flags]
   (let [file     (util/file dir)
         builder  (doto (Env/create)
                    (.setMapSize (* size 1024 1024))
                    (.setMaxDbs max-dbs))
         ^Env env (.open builder file (into-array EnvFlags flags))
         ^Txn rtx (.txnRead env)]
     (.reset rtx)
     (->LMDB env dir rtx {}))))

(defn open-dbi
  "Open a named dbi (i.e. sub-db) in a LMDB"
  ([lmdb name]
   (apply open-dbi
          lmdb name default-key-size default-val-size default-dbi-flags))
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

(comment

  (def ^LMDB lmdb (open-lmdb "/tmp/lmdb-test7"))
  (def ^DBI db (open-dbi lmdb default-dbi-name))

  (get-dbis lmdb)

  (transact lmdb [[:put "default" 1 2]
                  [:put "default" 2 3]])

  (.getLong ^ByteBuffer (get-raw lmdb "default" 2))
  (.getLong ^ByteBuffer (get-raw lmdb "default" 1))


  (close lmdb)

  )
