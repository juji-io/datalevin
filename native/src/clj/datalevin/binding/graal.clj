(ns datalevin.binding.graal
  "LMDB binding for GraalVM native image"
  (:require [datalevin.bits :as b]
            [datalevin.util :refer [raise]]
            [datalevin.constants :as c]
            [datalevin.lmdb :refer [open-lmdb IBuffer IRange IRtx IKV ILMDB]]
            [clojure.string :as s])
  (:import [clojure.lang IMapEntry]
           [java.util Iterator]
           [java.util.concurrent ConcurrentHashMap]
           [java.nio.charset StandardCharsets]
           [java.nio ByteBuffer]
           [java.lang.annotation Retention RetentionPolicy Target ElementType]
           [org.graalvm.nativeimage PinnedObject StackValue]
           [org.graalvm.nativeimage.c CContext]
           [org.graalvm.nativeimage.c.type CTypeConversion WordPointer
            CTypeConversion$CCharPointerHolder]
           [datalevin.ni Lib Lib$Directives Lib$MDB_env Lib$MDB_txn
            Lib$MDB_cursor Lib$LMDBException Lib$BadReaderLockException]
           ))

(def default-put-flags 0)

(deftype ^{Retention RetentionPolicy/RUNTIME
           CContext  {:value Lib$Directives}}
    Rtx [^Lib$MDB_txn txn
         ^:volatile-mutable ^Boolean use?
         ^PinnedObject kp
         ^PinnedObject start-kp
         ^PinnedObject stop-kp]
  IBuffer
  (put-key [_ x t]
    (let [kb ^ByteBuffer (.getObject kp)]
      (try
        (.clear kb)
        (b/put-buffer kb x t)
        (.flip kb)
        (catch Exception e
          (raise "Error putting read-only transaction key buffer: "
                 (ex-message e)
                 {:value x :type t})))))
  (put-val [_ x t]
    (raise "put-val not allowed for read only txn buffer" {}))

  IRange
  (put-start-key [_ x t]
    (when x
      (let [start-kb ^ByteBuffer (.getObject start-kp)]
        (try
          (.clear start-kb)
          (b/put-buffer start-kb x t)
          (.flip start-kb)
          (catch Exception e
            (raise "Error putting read-only transaction start key buffer: "
                   (ex-message e)
                   {:value x :type t}))))))
  (put-stop-key [_ x t]
    (when x
      (let [stop-kb ^ByteBuffer (.getObject stop-kp)]
        (try
          (.clear stop-kb)
          (b/put-buffer stop-kb x t)
          (.flip stop-kb)
          (catch Exception e
            (raise "Error putting read-only transaction stop key buffer: "
                   (ex-message e)
                   {:value x :type t}))))))

  IRtx
  (close-rtx [_]
    (Lib/mdb_txn_abort txn)
    (Lib/freeBuffer kp)
    (Lib/freeBuffer start-kp)
    (Lib/freeBuffer stop-kp)
    (set! use? false))
  (reset [this]
    (Lib/mdb_txn_reset txn)
    (set! use? false)
    this)
  (renew [this]
    (when-not use?
      (set! use? true)
      (Lib/checkRc (Lib/mdb_txn_renew txn))
      this)))

(defprotocol IRtxPool
  (close-pool [this] "Close all transactions in the pool")
  (new-rtx [this] "Create a new read-only transaction")
  (get-rtx [this] "Obtain a ready-to-use read-only transaction"))

(deftype ^{Retention RetentionPolicy/RUNTIME
           CContext  {:value Lib$Directives}}
    RtxPool [^Lib$MDB_env env
             ^ConcurrentHashMap rtxs
             ^:volatile-mutable ^long cnt]
  IRtxPool
  (close-pool [this]
    (doseq [^Rtx rtx (.values rtxs)] (.close-rtx rtx))
    (.clear rtxs)
    (set! cnt 0))
  (new-rtx [this]
    (when (< cnt c/+use-readers+)
      (let [txnPtr (Lib/allocateTxnPtr)
            _      (Lib/checkRc
                     (Lib/mdb_txn_begin env nil (Lib/MDB_RDONLY) txnPtr))
            txn    ^Lib$MDB_txn (.read txnPtr)
            rtx    (->Rtx txn
                          false
                          (Lib/allocateBuffer c/+max-key-size+)
                          (Lib/allocateBuffer c/+max-key-size+)
                          (Lib/allocateBuffer c/+max-key-size+))]
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
      (catch Lib$BadReaderLockException _
        (raise
          "Please do not open multiple LMDB connections to the same DB
           in the same process. Instead, a LMDB connection should be held onto
           and managed like a stateful resource. Refer to the documentation of
           `datalevin.lmdb/open-lmdb` for more details." {})))))

(deftype ^{Retention RetentionPolicy/RUNTIME
           CContext  {:value Lib$Directives}}
    DBI [^Lib/MDB_dbiPointer dbi
         ^String dbi-name
         ^PinnedObject kp
         ^:volatile-mutable ^PinnedObject vp
         ]
  IBuffer
  (put-key [this x t]
    (let [^ByteBuffer kb (.getObject kp)]
      (try
        (.clear kb)
        (b/put-buffer kb x t)
        (.flip kb)
        (catch Exception e
          (raise "Error putting r/w key buffer of "
                 (.dbi-name this) ": " (ex-message e)
                 {:value x :type t :dbi dbi-name})))))
  (put-val [this x t]
    (let [^ByteBuffer vb (.getObject vp)]
      (try
        (.clear vb)
        (b/put-buffer vb x t)
        (.flip vb)
        (catch Exception e
          (if (s/includes? (ex-message e) c/buffer-overflow)
            (let [size (* 2 ^long (b/measure-size x))]
              (Lib/freeBuffer vp)
              (set! vp (Lib/allocateBuffer size))
              (let [^ByteBuffer vb (.getObject vp)]
                (b/put-buffer vb x t)
                (.flip vb)))
            (raise "Error putting r/w value buffer of "
                   dbi-name ": " (ex-message e)
                   {:value x :type t :dbi dbi-name}))))))

  IKV
  (dbi-name [_]
    dbi-name)
  (put [_ txn flags]
    (if flags
      (Lib/mdb_put txn (.read dbi) )
      (.put db txn kb vb flags)
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

(deftype ^{Retention RetentionPolicy/RUNTIME
           CContext  {:value Lib$Directives}}
    LMDB [env dir dbis])

(defmethod open-lmdb :graal
  [dir]
  #_(try
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
