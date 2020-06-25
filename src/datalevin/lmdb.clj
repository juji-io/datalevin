(ns datalevin.lmdb
  "Wrapping LMDB"
  (:refer-clojure :exclude [get iterate])
  (:require [datalevin.bits :as b]
            [datalevin.util :refer [raise]]
            [datalevin.constants :as c]
            [clojure.string :as s]
            [taoensso.timbre :as log])
  (:import [org.lmdbjava Env EnvFlags Env$MapFullException Dbi DbiFlags
            PutFlags Txn CursorIterable CursorIterable$KeyVal KeyRange]
           [java.util Iterator]
           [java.util.concurrent ConcurrentHashMap]
           [java.nio ByteBuffer]))

(def default-env-flags [EnvFlags/MDB_NOTLS
                        EnvFlags/MDB_NORDAHEAD])

(def default-dbi-flags [DbiFlags/MDB_CREATE])

(defprotocol IBuffer
  (put-key [this data k-type]
    "put data in key buffer, k-type can be :long, :byte, :bytes, :data")
  (put-val [this data v-type]
    "put data in val buffer, v-type can be :long, :byte, :bytes, :data"))

(defprotocol IRange
  (put-start-key [this data k-type]
    "put data in start-key buffer, k-type can be :long, :byte, :bytes, :data")
  (put-stop-key [this data k-type]
    "put data in stop-key buffer, k-type can be :long, :byte, :bytes, :data"))

(defprotocol IRtx
  (close-rtx [this] "close the read-only transaction")
  (reset [this] "reset transaction so it can be reused upon renew")
  (renew [this] "renew and return previously reset transaction for reuse"))

(deftype Rtx [^Txn txn
              ^:volatile-mutable use
              ^ByteBuffer kb
              ^ByteBuffer start-kb
              ^ByteBuffer stop-kb]
  IBuffer
  (put-key [_ x t]
    (b/put-buffer kb x t))
  (put-val [_ x t]
    (raise "put-val not allowed for read only txn buffer" {}))

  IRange
  (put-start-key [_ x t]
    (when x
      (.clear start-kb)
      (b/put-buffer start-kb x t)))
  (put-stop-key [_ x t]
    (when x
      (.clear stop-kb)
      (b/put-buffer stop-kb x t)))

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

(deftype RtxPool [^Env env
                  ^ConcurrentHashMap rtxs
                  ^:volatile-mutable ^long cnt]
  IRtxPool
  (close-pool [this]
    (locking this
      (doseq [^Rtx rtx (.values rtxs)] (close-rtx rtx))
      (.clear rtxs)
      (set! cnt 0)))
  (new-rtx [this]
    (locking this
      (when (< cnt c/+max-readers+)
        (let [rtx (->Rtx (.txnRead env)
                         false
                         (ByteBuffer/allocateDirect c/+max-key-size+)
                         (ByteBuffer/allocateDirect c/+max-key-size+)
                         (ByteBuffer/allocateDirect c/+max-key-size+))]
          (.put rtxs cnt rtx)
          (set! cnt (inc cnt))
          (reset rtx)
          (renew rtx)))))
  (get-rtx [this]
    (loop [i (.getId ^Thread (Thread/currentThread))]
      (let [^long i'       (mod i cnt)
            ^Rtx rtx (.get rtxs i')]
        (or (renew rtx)
            (new-rtx this)
            (recur (long (inc i'))))))))

(defprotocol IKV
  (put [this txn] [this txn put-flags]
    "Put kv pair given in `put-key` and `put-val` of dbi")
  (del [this txn] "Delete the key given in `put-key` of dbi")
  (get [this rtx] "Get value of the key given in `put-key` of rtx")
  (iterate [this rtx range-type] "Return a CursorIterable"))

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

(deftype DBI [^Dbi db ^ByteBuffer kb ^:volatile-mutable ^ByteBuffer vb]
  IBuffer
  (put-key [_ x t]
    (b/put-buffer kb x t))
  (put-val [_ x t]
    (try
      (b/put-buffer vb x t)
      (catch java.lang.AssertionError e
        (if (s/includes? (ex-message e) c/buffer-overflow)
          (let [size (b/measure-size x)]
            (set! vb (ByteBuffer/allocateDirect size))
            (b/put-buffer vb x t))
          (throw e)))))

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
        res)))
  (iterate [this rtx range-type]
    (let [^ByteBuffer start-kb (.-start-kb ^Rtx rtx)
          ^ByteBuffer stop-kb (.-stop-kb ^Rtx rtx)]
      (.flip start-kb)
      (.flip stop-kb)
      (.iterate db (.-txn ^Rtx rtx) (key-range range-type start-kb stop-kb)))))

(defprotocol ILMDB
  (close [this] "Close this LMDB env")
  (open-dbi
    [this dbi-name]
    [this dbi-name key-size]
    [this dbi-name key-size val-size]
    [this dbi-name key-size val-size flags]
    "Open a named dbi (i.e. sub-db) in the LMDB")
  (get-dbi [this dbi-name] "Lookup DBI (i.e. sub-db) by name")
  (transact [this txs]
    "Update db, txs is a seq of [op dbi-name k v k-type v-type put-flags]
     when op is :put; [op dbi-name k k-type] when op is :del;
     See `bits/put-buffer` for allowed k-type and v-type")
  (get-value
    [this dbi-name k]
    [this dbi-name k k-type]
    [this dbi-name k k-type v-type]
    "Get the value of a key, k-type and v-type can be :data (default), :byte,
     :bytes, :attr, :datom or :long")
  (get-first
    [this dbi-name k-range]
    [this dbi-name k-range k-type]
    [this dbi-name k-range k-type v-type]
    [this dbi-name k-range k-type v-type ignore-key?]
    "Return the first kv pair in the specified key range;
     k-range is a vector [range-type k1 k2], range-type can be one of
     :all, :at-least, :at-most, :closed, :closed-open, :greater-than,
     :less-than, :open, :open-closed, plus backward variants that put a
     `-back` suffix to each of the above, e.g. :all-back;
     k-type and v-type can be :data (default), :long, :byte, :bytes, :datom,
     or :attr; only the value will be returned if ignore-key? is true;
     If value is to be ignored, put :ignore as v-type")
  (get-range
    [this dbi-name k-range]
    [this dbi-name k-range k-type]
    [this dbi-name k-range k-type v-type]
    [this dbi-name k-range k-type v-type ignore-key?]
    "Return a seq of kv pair in the specified key range;
     k-range is a vector [range-type k1 k2], range-type can be one of
     :all, :at-least, :at-most, :closed, :closed-open, :greater-than,
     :less-than, :open, :open-closed, plus backward variants that put a
     `-back` suffix to each of the above, e.g. :all-back;
     k-type and v-type can be :data (default), :long, :byte, :bytes, :datom,
     or :attr; only values will be returned if ignore-key? is true;
     If value is to be ignored, put :ignore as v-type")
  (get-some
    [this pred dbi-name k-range]
    [this pred dbi-name k-range k-type]
    [this pred dbi-name k-range k-type v-type]
    "Like some, return the first logical true value of (pred x) for any x,
     a CursorIterable$KeyVal, in the specified key range, or return nil;
     k-range is a vector [range-type k1 k2], range-type can be one of
     :all, :at-least, :at-most, :closed, :closed-open, :greater-than,
     :less-than, :open, :open-closed, plus backward variants that put a
     `-back` suffix to each of the above, e.g. :all-back;
     k-type and v-type can be :data (default), :long, :byte, :bytes, :datom,
     or :attr; only values will be returned if ignore-key? is true"))

(defn- double-db-size [^Env env]
  (.setMapSize env (* 2 (-> env .info .mapSize))))

(defn- fetch-value
  [dbi ^Rtx rtx k k-type v-type]
  (put-key rtx k k-type)
  (when-let [^ByteBuffer bb (get dbi rtx)]
    (b/read-buffer bb v-type)))

(defn- fetch-first
  [^DBI dbi ^Rtx rtx [range-type k1 k2] k-type v-type ignore-key?]
  (put-start-key rtx k1 k-type)
  (put-stop-key rtx k2 k-type)
  (with-open [^CursorIterable iterable (iterate dbi rtx range-type)]
    (let [^Iterator iter            (.iterator iterable)
          ^CursorIterable$KeyVal kv (.next iter)
          v                         (when (not= v-type :ignore)
                                      (-> kv (.val) (b/read-buffer v-type)))]
      (if ignore-key?
        v
        [(-> kv (.key) (b/read-buffer k-type)) v]))))

(defn- fetch-range
  [^DBI dbi ^Rtx rtx [range-type k1 k2] k-type v-type ignore-key?]
  (put-start-key rtx k1 k-type)
  (put-stop-key rtx k2 k-type)
  (with-open [^CursorIterable iterable (iterate dbi rtx range-type)]
    (loop [^Iterator iter (.iterator iterable)
           holder         (transient [])]
      (let [^CursorIterable$KeyVal kv (.next iter)
            v                         (when (not= v-type :ignore)
                                        (-> kv (.val) (b/read-buffer v-type)))
            holder'                   (conj! holder
                                             (if ignore-key?
                                               v
                                               [(-> kv
                                                    (.key)
                                                    (b/read-buffer k-type))
                                                v]))]
        (if (.hasNext iter)
          (recur iter holder')
          (persistent! holder'))))))

(defn- fetch-some
  [pred ^DBI dbi ^Rtx rtx [range-type k1 k2] k-type v-type]
  (put-start-key rtx k1 k-type)
  (put-stop-key rtx k2 k-type)
  (with-open [^CursorIterable iterable (iterate dbi rtx range-type)]
    (loop [^Iterator iter (.iterator iterable)]
      (if-let [res (pred (.next iter))]
        res
        (when (.hasNext iter) (recur iter))))))

(deftype LMDB [^Env env ^String dir ^RtxPool pool ^ConcurrentHashMap dbis]
  ILMDB
  (close [_]
    (close-pool pool)
    (.close env))
  (open-dbi [this dbi-name]
    (open-dbi this dbi-name c/+max-key-size+ c/+default-val-size+ default-dbi-flags))
  (open-dbi [this dbi-name key-size]
    (open-dbi this dbi-name key-size c/+default-val-size+ default-dbi-flags))
  (open-dbi [this dbi-name key-size val-size]
    (open-dbi this dbi-name key-size val-size default-dbi-flags))
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
        (raise "`open-dbi` was not called for " dbi-name {})))
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
          (raise "Fail to get-value: " (ex-message e)
                 {:dbi dbi-name :k k :k-type k-type :v-type v-type}))
        (finally (reset rtx)))))
  (get-first [this dbi-name k-range]
    (get-first this dbi-name k-range :data :data false))
  (get-first [this dbi-name k-range k-type]
    (get-first this dbi-name k-range k-type :data false))
  (get-first [this dbi-name k-range k-type v-type]
    (get-first this dbi-name k-range k-type v-type false))
  (get-first [this dbi-name k-range k-type v-type ignore-key?]
    (let [dbi (get-dbi this dbi-name)
          rtx (get-rtx pool)]
      (try
        (fetch-first dbi rtx k-range k-type v-type ignore-key?)
        (catch Exception e
          (raise "Fail to get-first: " (ex-message e)
                 {:dbi    dbi-name :k-range k-range
                  :k-type k-type   :v-type  v-type}))
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
          (raise "Fail to get-range: " (ex-message e)
                 {:dbi    dbi-name :k-range k-range
                  :k-type k-type   :v-type  v-type}))
        (finally (reset rtx)))))
  (get-some [this pred dbi-name k-range]
    (get-some this pred dbi-name k-range :data :data))
  (get-some [this pred dbi-name k-range k-type]
    (get-some this pred dbi-name k-range k-type :data))
  (get-some [this pred dbi-name k-range k-type v-type]
    (let [dbi (get-dbi this dbi-name)
          rtx (get-rtx pool)]
      (try
        (fetch-some pred dbi rtx k-range k-type v-type)
        (catch Exception e
          (raise "Fail to get-some: " (ex-message e)
                 {:dbi    dbi-name :k-range k-range
                  :k-type k-type   :v-type  v-type}))
        (finally (reset rtx))))))

(defn open-lmdb
  "Open an LMDB env"
  ([dir]
   (open-lmdb dir c/+init-db-size+ default-env-flags))
  ([dir size flags]
   (let [file          (b/file dir)
         builder       (doto (Env/create)
                         (.setMapSize (* ^long size 1024 1024))
                         (.setMaxReaders c/+max-readers+)
                         (.setMaxDbs c/+max-dbs+))
         ^Env env      (.open builder file (into-array EnvFlags flags))
         ^RtxPool pool (->RtxPool env (ConcurrentHashMap.) 0)]
     (new-rtx pool)
     (->LMDB env dir pool (ConcurrentHashMap.)))))
