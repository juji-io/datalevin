(ns datalevin.lmdb
  "Wrapping LMDB"
  (:refer-clojure :exclude [get])
  (:require [datalevin.bits :as b]
            [taoensso.timbre :as log])
  (:import [org.lmdbjava Env EnvFlags Env$MapFullException Dbi DbiFlags
            PutFlags Txn CursorIterable CursorIterable$KeyVal KeyRange]
           [java.util Iterator]
           [java.util.concurrent ConcurrentHashMap]
           [java.nio ByteBuffer]))

(def default-env-flags [EnvFlags/MDB_NOTLS
                        EnvFlags/MDB_NORDAHEAD])

(def default-dbi-flags [DbiFlags/MDB_CREATE])

(def ^:const +max-dbs+ 64)
(def ^:const +max-readers+ 126)
(def ^:const +init-db-size+ 100) ; in megabytes
(def ^:const +max-key-size+ 511) ; in bytes
(def ^:const +default-val-size+ 16384) ; 16 kilobytes

(defprotocol IBuffer
  (put-key [this data k-type]
    "put data in key buffer, k-type can be :long, :byte, :bytes, :data")
  (put-val [this data v-type]
    "put data in val buffer, v-type can be :long, :byte, :bytes, :data"))

(defn- put-buffer
  [bf x t]
  (case t
    :long  (b/put-long bf x)
    :byte  (b/put-byte bf x)
    :bytes (b/put-bytes bf x)
    (b/put-data bf x)))

(defprotocol IRtx
  (close-rtx [this] "close the read-only transaction")
  (reset [this] "reset transaction so it can be reused upon renew")
  (renew [this] "renew and return previously reset transaction for reuse"))

(deftype Rtx [^Txn txn ^:volatile-mutable use ^ByteBuffer kb]
  IBuffer
  (put-key [_ x t]
    (put-buffer kb x t))
  (put-val [_ x t]
    (throw (ex-info "put-val not allowed for read only txn buffer")))

  IRtx
  (close-rtx [_]
    (set! use false)
    (.close txn))
  (reset [this]
    (locking this
      (.reset txn)
      (set! use false)
      this))
  (renew [this]
    (locking this
      (when-not use
        (.renew txn)
        (set! use true)
        this))))

(defprotocol IRtxPool
  (close-pool [this] "Close all transactions in the pool")
  (new-rtx [this] "Create a new read-only transaction")
  (get-rtx [this] "Obtain a ready-to-use read-only transaction"))

(deftype RtxPool [^Env env ^ConcurrentHashMap rtxs ^:volatile-mutable cnt]
  IRtxPool
  (close-pool [this]
    (locking this
      (doseq [^Rtx rtx (.values rtxs)] (close-rtx rtx))
      (.clear rtxs)
      (set! cnt 0)))
  (new-rtx [this]
    (locking this
      (when (< cnt +max-readers+)
        (let [rtx (->Rtx (.txnRead env)
                         false
                         (ByteBuffer/allocateDirect +max-key-size+))]
          (.put rtxs cnt rtx)
          (set! cnt (inc cnt))
          (reset rtx)
          (renew rtx)))))
  (get-rtx [this]
    (loop [i (.getId ^Thread (Thread/currentThread))]
      (let [i'       (mod i cnt)
            ^Rtx rtx (.get rtxs i')]
        (or (renew rtx)
            (new-rtx this)
            (recur (long (inc i'))))))))

(defprotocol IKV
  (put [this txn] [this txn put-flags]
    "Put kv pair given in `put-key` and `put-val` of dbi")
  (del [this txn] "Delete the key given in `put-key` of dbi")
  (get [this rtx] "Get value of the key given in `put-key` of rtx"))

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
  (del [_ txn]
    (.flip kb)
    (.delete db txn kb)
    (.clear kb))
  (get [_ rtx]
    (let [^ByteBuffer kb (.-kb ^Rtx rtx)]
      (.flip kb)
      (let [res (.get db (.-txn ^Rtx rtx) kb)]
        (.clear kb)
        res))))

(defprotocol ILMDB
  (close [this] "Close this LMDB env")
  (open-dbi [this dbi-name] [this dbi-name key-size val-size flags]
    "Open a named dbi (i.e. sub-db) in a LMDB")
  (get-dbi [this dbi-name] "Lookup DBI (i.e. sub-db) by name")
  (transact [this txs]
    "Update db, txs is a seq of [op dbi-name k v k-type v-type put-flags]
     when op is :put; [op dbi-name k k-type] when op is :del;
     k-type and v-type can be :long, :byte, :bytes, or :data")
  (get-value
    [this dbi-name k]
    [this dbi-name k k-type]
    [this dbi-name k k-type v-type]
    "Get the value of a key, k-type and v-type can be :data (default), :byte,
     :bytes or :long")
  ;; TODO use pre-allocated bytebuffers for defining k-range
  (get-range
    [this dbi-name k-range]
    [this dbi-name k-range k-type]
    [this dbi-name k-range k-type v-type]
    [this dbi-name k-range k-type v-type ignore-key?]
    "Return a seq of kv pair in the specified key range,
     k-type and v-type can be :data (default), :long, :byte, :bytes,
     only values will be returned if ignore-key? is true"))

(defn- double-db-size [^Env env]
  (.setMapSize env (* 2 (-> env .info .mapSize))))

(defn- read-buffer
  [^ByteBuffer bb v-type]
  (case v-type
    :raw   bb
    :long  (b/get-long bb)
    :byte  (b/get-byte bb)
    :bytes (b/get-bytes bb)
    (b/get-data bb)))

(defn- fetch-value
  [dbi ^Rtx rtx k k-type v-type]
  (put-key rtx k k-type)
  (when-let [^ByteBuffer bb (get dbi rtx)]
    (read-buffer bb v-type)))

(defn- fetch-range
  [^DBI dbi ^Rtx rtx k-range k-type v-type ignore-key?]
  (with-open [iterable (.iterate ^Dbi (.-db dbi) (.-txn rtx) k-range)]
    (loop [^Iterator iter (.iterator ^CursorIterable iterable)
           holder         (transient [])]
      (let [^CursorIterable$KeyVal kv (.next iter)
            v                         (-> kv (.val) (read-buffer v-type))
            holder'                   (conj! holder
                                             (if ignore-key?
                                               v
                                               [(-> kv
                                                    (.key)
                                                    (read-buffer k-type))
                                                v]))]
        (if (.hasNext iter)
          (recur iter holder')
          (persistent! holder'))))))

(deftype LMDB [^Env env ^String dir ^RtxPool pool ^ConcurrentHashMap dbis]
  ILMDB
  (close [_]
    (close-pool pool)
    (.close env))
  (open-dbi [this dbi-name]
    (open-dbi this dbi-name +max-key-size+ +default-val-size+ default-dbi-flags))
  (open-dbi [_ dbi-name key-size val-size flags]
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
        (throw (ex-info (str "`open-dbi` was not called for " dbi-name) {}))))
  (transact [this txs]
    (try
      (with-open [txn (.txnWrite env)]
        (doseq [[op dbi-name k & r] txs
                :let                [dbi (get-dbi this dbi-name)]]
          (case op
            :put (let [[v kt vt flags] r]
                   (put-key dbi k kt)
                   (put-val dbi v vt)
                   (if flags
                     (put dbi txn flags)
                     (put dbi txn)))
            :del (let [[kt] r]
                   (put-key dbi k kt)
                   (del dbi txn))))
        (.commit txn))
      (catch Env$MapFullException e
        (double-db-size env)
        (transact this txs))))
  (get-value [this dbi-name k]
    (get-value this dbi-name k :data :data))
  (get-value [this dbi-name k k-type]
    (get-value this dbi-name k k-type :data))
  (get-value [this dbi-name k k-type v-type]
    (let [dbi (get-dbi this dbi-name)
          rtx (get-rtx pool)]
      (try
        (fetch-value dbi rtx k k-type v-type)
        (catch Exception e
          (throw (ex-info (str "Fail to get-value: " (ex-message e))
                          {:dbi dbi-name :k k :k-type k-type :v-type v-type})))
        (finally (reset rtx)))))
  (get-range [this dbi-name k-range]
    (get-range this dbi-name k-range :data :data false))
  (get-range [this dbi-name k-range k-type]
    (get-range this dbi-name k-range k-type :data false))
  (get-range [this dbi-name k-range k-type v-type]
    (get-range this dbi-name k-range k-type v-type false))
  (get-range [this dbi-name k-range k-type v-type ignore-key?]
    (let [dbi (get-dbi this dbi-name)
          rtx (get-rtx pool)]
      (try
        (fetch-range dbi rtx k-range k-type v-type ignore-key?)
        (catch Exception e
          (throw (ex-info (str "Fail to get-range: " (ex-message e))
                          {:dbi    dbi-name :k-range k-range
                           :k-type k-type   :v-type  v-type})))
        (finally (reset rtx))))))

(defn open-lmdb
  "Open an LMDB env"
  ([dir]
   (open-lmdb dir +init-db-size+ default-env-flags))
  ([dir size flags]
   (let [file          (b/file dir)
         builder       (doto (Env/create)
                         (.setMapSize (* ^long size 1024 1024))
                         (.setMaxReaders +max-readers+)
                         (.setMaxDbs +max-dbs+))
         ^Env env      (.open builder file (into-array EnvFlags flags))
         ^RtxPool pool (->RtxPool env (ConcurrentHashMap.) 0)]
     (new-rtx pool)
     (->LMDB env dir pool (ConcurrentHashMap.)))))
