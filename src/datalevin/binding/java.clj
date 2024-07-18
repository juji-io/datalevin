(ns ^:no-doc datalevin.binding.java
  "LMDB binding for Java"
  (:require
   [datalevin.bits :as b]
   [datalevin.compress :as cp]
   [datalevin.buffer :as bf]
   [datalevin.spill :as sp]
   [datalevin.util :refer [raise] :as u]
   [datalevin.constants :as c]
   [datalevin.scan :as scan]
   [datalevin.lmdb :as l
    :refer [open-kv IBuffer IRange IRtx IDB IKV IList ILMDB IWriting IAdmin
            IListRandKeyValIterable IListRandKeyValIterator]]
   [clojure.string :as s]
   [clojure.java.io :as io])
  (:import
   [org.lmdbjava Env EnvFlags Env$MapFullException Stat Dbi DbiFlags
    PutFlags Txn TxnFlags Txn$BadReaderLockException CopyFlags
    Cursor CursorIterable$KeyVal GetOp SeekOp]
   [org.eclipse.collections.impl.map.mutable UnifiedMap]
   [java.util.concurrent ConcurrentLinkedQueue]
   [java.util Iterator UUID]
   [java.nio ByteBuffer BufferOverflowException]
   [java.io File InputStream OutputStream]
   [java.nio.file Files OpenOption]
   [clojure.lang MapEntry IObj]
   [datalevin.lmdb RangeContext KVTxData]
   [datalevin.spill SpillableVector]
   [datalevin.compress ICompressor]))

(extend-protocol IKV
  CursorIterable$KeyVal
  (k [this] (.key ^CursorIterable$KeyVal this))
  (v [this] (.val ^CursorIterable$KeyVal this))

  MapEntry
  (k [this] (.key ^MapEntry this))
  (v [this] (.val ^MapEntry this)))

(defn- flag
  [flag-key]
  (case flag-key
    :fixedmap   EnvFlags/MDB_FIXEDMAP
    :nosubdir   EnvFlags/MDB_NOSUBDIR
    :rdonly-env EnvFlags/MDB_RDONLY_ENV
    :writemap   EnvFlags/MDB_WRITEMAP
    :nometasync EnvFlags/MDB_NOMETASYNC
    :nosync     EnvFlags/MDB_NOSYNC
    :mapasync   EnvFlags/MDB_MAPASYNC
    :notls      EnvFlags/MDB_NOTLS
    :nolock     EnvFlags/MDB_NOLOCK
    :nordahead  EnvFlags/MDB_NORDAHEAD
    :nomeminit  EnvFlags/MDB_NOMEMINIT

    :cp-compact CopyFlags/MDB_CP_COMPACT

    :reversekey DbiFlags/MDB_REVERSEKEY
    :dupsort    DbiFlags/MDB_DUPSORT
    :integerkey DbiFlags/MDB_INTEGERKEY
    :dupfixed   DbiFlags/MDB_DUPFIXED
    :integerdup DbiFlags/MDB_INTEGERDUP
    :reversedup DbiFlags/MDB_REVERSEDUP
    :create     DbiFlags/MDB_CREATE

    :nooverwrite PutFlags/MDB_NOOVERWRITE
    :nodupdata   PutFlags/MDB_NODUPDATA
    :current     PutFlags/MDB_CURRENT
    :reserve     PutFlags/MDB_RESERVE
    :append      PutFlags/MDB_APPEND
    :appenddup   PutFlags/MDB_APPENDDUP
    :multiple    PutFlags/MDB_MULTIPLE

    :rdonly-txn TxnFlags/MDB_RDONLY_TXN))

(defn- flag-type
  [type-key]
  (case type-key
    :env  EnvFlags
    :copy CopyFlags
    :dbi  DbiFlags
    :put  PutFlags
    :txn  TxnFlags))

(defn- kv-flags
  [type flags]
  (let [t (flag-type type)]
    (if (seq flags)
      (into-array t (mapv flag flags))
      (make-array t 0))))

(deftype Rtx [lmdb
              ^Txn txn
              ^ByteBuffer kb
              ^ByteBuffer start-kb
              ^ByteBuffer stop-kb
              ^ByteBuffer start-vb
              ^ByteBuffer stop-vb
              aborted?]
  IBuffer
  (put-key [_ x t]
    (try
      (b/put-bf kb x t)
      (catch BufferOverflowException _
        (raise "Key cannot be larger than 511 bytes." {:input x}))
      (catch Exception e
        (raise "Error putting read-only transaction key buffer: "
               e {:value x :type t}))))
  (put-val [_ x t]
    (raise "put-val not allowed for read only txn buffer" {}))

  IRange
  (range-info [_ range-type k1 k2 kt]
    (b/put-bf start-kb k1 kt)
    (b/put-bf stop-kb k2 kt)
    (l/range-table range-type start-kb stop-kb))

  (list-range-info [_ k-range-type k1 k2 kt v-range-type v1 v2 vt]
    (b/put-bf start-kb k1 kt)
    (b/put-bf stop-kb k2 kt)
    (b/put-bf start-vb v1 vt)
    (b/put-bf stop-vb v2 vt)
    [(l/range-table k-range-type start-kb stop-kb)
     (l/range-table v-range-type start-vb stop-vb)])

  IRtx
  (read-only? [_] (.isReadOnly txn))
  (get-txn [_] txn)
  (close-rtx [_] (.close txn))
  (reset [this] (.reset txn) this)
  (renew [this] (.renew txn) this))

(defn- stat-map [^Stat stat]
  {:psize          (.-pageSize stat)
   :depth          (.-depth stat)
   :branch-pages   (.-branchPages stat)
   :leaf-pages     (.-leafPages stat)
   :overflow-pages (.-overflowPages stat)
   :entries        (.-entries stat)})

(declare ->KeyIterable ->ListIterable ->ListRandKeyValIterable)

(deftype DBI [^Dbi db
              ^ConcurrentLinkedQueue curs
              ^ByteBuffer kb
              ^:unsynchronized-mutable ^ByteBuffer vb
              ^ICompressor kc
              ^ICompressor vc
              ^boolean dupsort?
              ^boolean validate-data?]
  IBuffer
  (put-key [this x t]
    (or (not validate-data?)
        (b/valid-data? x t)
        (raise "Invalid data, expecting" t " got " x {:input x}))
    (try
      (b/put-bf kb x t)
      (catch BufferOverflowException _
        (raise "Key cannot be larger than 511 bytes." {:input x}))
      (catch Exception e
        (raise "Error putting r/w key buffer of "
               (.dbi-name this) "with value" x ": " e {:type t}))))
  (put-val [this x t]
    (or (not validate-data?)
        (b/valid-data? x t)
        (raise "Invalid data, expecting " t " got " x {:input x}))
    (try
      (b/put-bf vb x t)
      (catch BufferOverflowException _
        ;; TODO reset the val-size in kv-info
        (let [size (* ^long c/+buffer-grow-factor+ ^long (b/measure-size x))]
          (set! vb (bf/allocate-buffer size))
          (b/put-bf vb x t)))
      (catch Exception e
        (raise "Error putting r/w value buffer of "
               (.dbi-name this) ": " e {:value x :type t}))))

  IDB
  (dbi [_] db)
  (dbi-name [_] (b/text-ba->str (.getName db)))
  (put [_ txn flags]
    (if flags
      (.put db txn kb vb (kv-flags :put flags))
      (.put db txn kb vb (kv-flags :put c/default-put-flags))))
  (put [this txn] (.put this txn nil))
  (del [_ txn all?] (if all? (.delete db txn kb) (.delete db txn kb vb)))
  (del [this txn] (.del this txn true))
  (get-kv [_ rtx]
    (let [^ByteBuffer kb (.-kb ^Rtx rtx)]
      (.get db (.-txn ^Rtx rtx) kb)))
  (iterate-key [this rtx cur [range-type k1 k2] k-type]
    (let [ctx (l/range-info rtx range-type k1 k2 k-type)]
      (->KeyIterable this cur rtx ctx)))
  (iterate-list [this rtx cur [k-range-type k1 k2] k-type
                 [v-range-type v1 v2] v-type]
    (let [ctx (l/list-range-info rtx k-range-type k1 k2 k-type
                                 v-range-type v1 v2 v-type)]
      (->ListIterable this cur rtx ctx)))
  (iterate-list-val [this rtx cur [v-range-type v1 v2] v-type]
    (let [ctx (l/range-info rtx v-range-type v1 v2 v-type)]
      (->ListRandKeyValIterable this cur rtx ctx)))
  (iterate-kv [this rtx cur k-range k-type v-type]
    (if dupsort?
      (.iterate-list this rtx cur k-range k-type [:all] v-type)
      (.iterate-key this rtx cur k-range k-type)))
  (get-cursor [_ rtx]
    (let [txn ^Txn (.-txn ^Rtx rtx)]
      (or (when (.isReadOnly txn)
            (when-let [^Cursor cur (.poll curs)]
              (.renew cur txn)
              cur))
          (.openCursor db txn))))
  (close-cursor [_ cur] (.close ^Cursor cur))
  (return-cursor [_ cur] (.add curs cur)))

(deftype KeyIterable [^DBI db
                      ^Cursor cur
                      ^Rtx rtx
                      ^RangeContext ctx]
  Iterable
  (iterator [_]
    (let [forward?       (.-forward? ctx)
          include-start? (.-include-start? ctx)
          include-stop?  (.-include-stop? ctx)
          ^ByteBuffer sk (.-start-bf ctx)
          ^ByteBuffer ek (.-stop-bf ctx)
          started?       (volatile! false)
          ^ByteBuffer k  (.key cur)
          ^ByteBuffer v  (.val cur)]
      (letfn [(init []
                (if sk
                  (if (.get cur sk GetOp/MDB_SET_RANGE)
                    (if include-start?
                      (continue?)
                      (if (zero? (bf/compare-buffer k sk))
                        (check SeekOp/MDB_NEXT_NODUP)
                        (continue?)))
                    false)
                  (check SeekOp/MDB_FIRST)))
              (init-back []
                (if sk
                  (if (.get cur sk GetOp/MDB_SET_RANGE)
                    (if include-start?
                      (if (zero? (bf/compare-buffer k sk))
                        (continue-back?)
                        (check-back SeekOp/MDB_PREV_NODUP))
                      (check-back SeekOp/MDB_PREV_NODUP))
                    (check-back SeekOp/MDB_LAST))
                  (check-back SeekOp/MDB_LAST)))
              (continue? []
                (if ek
                  (let [r (bf/compare-buffer k ek)]
                    (if (= r 0)
                      include-stop?
                      (if (> r 0) false true)))
                  true))
              (continue-back? []
                (if ek
                  (let [r (bf/compare-buffer k ek)]
                    (if (= r 0)
                      include-stop?
                      (if (> r 0) true false)))
                  true))
              (check [op] (if (.seek cur op) (continue?) false))
              (check-back [op] (if (.seek cur op) (continue-back?) false))
              (advance []
                (if forward?
                  (check SeekOp/MDB_NEXT_NODUP)
                  (check-back SeekOp/MDB_PREV_NODUP)))
              (init-k []
                (vreset! started? true)
                (if forward? (init) (init-back)))]
        (reify
          Iterator
          (hasNext [_] (if @started? (advance) (init-k)))
          (next [_] (MapEntry. k v)))))))

(deftype ListIterable [^DBI db
                       ^Cursor cur
                       ^Rtx rtx
                       ctx]
  Iterable
  (iterator [_]
    (let [[^RangeContext kctx ^RangeContext vctx]
          ctx
          forward-key?       (.-forward? kctx)
          include-start-key? (.-include-start? kctx)
          include-stop-key?  (.-include-stop? kctx)
          ^ByteBuffer sk     (.-start-bf kctx)
          ^ByteBuffer ek     (.-stop-bf kctx)
          forward-val?       (.-forward? vctx)
          include-start-val? (.-include-start? vctx)
          include-stop-val?  (.-include-stop? vctx)
          ^ByteBuffer sv     (.-start-bf vctx)
          ^ByteBuffer ev     (.-stop-bf vctx)
          key-ended?         (volatile! false)
          started?           (volatile! false)
          k                  (.key cur)
          v                  (.val cur)]
      (letfn [(init-key []
                (if sk
                  (if (.get cur sk GetOp/MDB_SET_RANGE)
                    (if include-start-key?
                      (key-continue?)
                      (if (zero? (bf/compare-buffer k sk))
                        (check-key SeekOp/MDB_NEXT_NODUP)
                        (key-continue?)))
                    false)
                  (check-key SeekOp/MDB_FIRST)))
              (init-key-back []
                (if sk
                  (if (.get cur sk GetOp/MDB_SET_RANGE)
                    (if include-start-key?
                      (if (zero? (bf/compare-buffer k sk))
                        (key-continue-back?)
                        (check-key-back SeekOp/MDB_PREV_NODUP))
                      (check-key-back SeekOp/MDB_PREV_NODUP))
                    (check-key-back SeekOp/MDB_LAST))
                  (check-key-back SeekOp/MDB_LAST)))
              (init-val []
                (if sv
                  (if (.get cur (.key cur) sv SeekOp/MDB_GET_BOTH_RANGE)
                    (if include-start-val?
                      (val-continue?)
                      (if (zero? (bf/compare-buffer v sv))
                        (check-val SeekOp/MDB_NEXT_DUP)
                        (val-continue?)))
                    false)
                  (check-val SeekOp/MDB_FIRST_DUP)))
              (init-val-back []
                (if sv
                  (if (.get cur (.key cur) sv SeekOp/MDB_GET_BOTH_RANGE)
                    (if include-start-val?
                      (if (zero? (bf/compare-buffer v sv))
                        (val-continue-back?)
                        (check-val-back SeekOp/MDB_PREV_DUP))
                      (check-val-back SeekOp/MDB_PREV_DUP))
                    (check-val-back SeekOp/MDB_LAST_DUP))
                  (check-val-back SeekOp/MDB_LAST_DUP)))
              (key-end [] (vreset! key-ended? true) false)
              (val-end [] (if @key-ended? false (advance-key)))
              (key-continue? []
                (if ek
                  (let [r (bf/compare-buffer k ek)]
                    (if (= r 0)
                      (do (vreset! key-ended? true) include-stop-key?)
                      (if (> r 0) (key-end) true)))
                  true))
              (key-continue-back? []
                (if ek
                  (let [r (bf/compare-buffer k ek)]
                    (if (= r 0)
                      (do (vreset! key-ended? true) include-stop-key?)
                      (if (> r 0) true (key-end))))
                  true))
              (check-key [op]
                (if (.seek cur op) (key-continue?) (key-end)))
              (check-key-back [op]
                (if (.seek cur op) (key-continue-back?) (key-end)))
              (advance-key []
                (or (and (if forward-key?
                           (check-key SeekOp/MDB_NEXT_NODUP)
                           (check-key-back SeekOp/MDB_PREV_NODUP))
                         (if forward-val? (init-val) (init-val-back)))
                    (if @key-ended? false (recur))))
              (init-kv []
                (vreset! started? true)
                (or (and (if forward-key? (init-key) (init-key-back))
                         (if forward-val? (init-val) (init-val-back)))
                    (advance-key)))
              (val-continue? []
                (if ev
                  (let [r (bf/compare-buffer v ev)]
                    (if (= r 0)
                      (if include-stop-val? true (val-end))
                      (if (> r 0) (val-end) true)))
                  true))
              (val-continue-back? []
                (if ev
                  (let [r (bf/compare-buffer v ev)]
                    (if (= r 0)
                      (if include-stop-val? true (val-end))
                      (if (> r 0) true (val-end))))
                  true))
              (check-val [op]
                (if (.seek cur op) (val-continue?) (val-end)))
              (check-val-back [op]
                (if (.seek cur op) (val-continue-back?) (val-end)))
              (advance-val [] (check-val SeekOp/MDB_NEXT_DUP))
              (advance-val-back [] (check-val-back SeekOp/MDB_PREV_DUP))]
        (reify
          Iterator
          (hasNext [_]
            (if @started?
              (if forward-val? (advance-val) (advance-val-back))
              (init-kv)))
          (next [_] (MapEntry. k v)))))))

(deftype ListRandKeyValIterable [^DBI db
                                 ^Cursor cur
                                 ^Rtx rtx
                                 ^RangeContext ctx]
  IListRandKeyValIterable
  (val-iterator [_]
    (let [forward-val?       (.-forward? ctx)
          include-start-val? (.-include-start? ctx)
          include-stop-val?  (.-include-stop? ctx)
          ^ByteBuffer sv     (.-start-bf ctx)
          ^ByteBuffer ev     (.-stop-bf ctx)
          v                  (.val cur)]
      (assert forward-val?
              "Backward iterate is not supported in ListRandKeyValIterable")
      (letfn [(init-val []
                (if sv
                  (if (.get cur (.-kb rtx) sv SeekOp/MDB_GET_BOTH_RANGE)
                    (if include-start-val?
                      (val-continue?)
                      (if (zero? (bf/compare-buffer v sv))
                        (check-val SeekOp/MDB_NEXT_DUP)
                        (val-continue?)))
                    false)
                  (if (.get cur (.-kb rtx) GetOp/MDB_SET_RANGE)
                    (check-val SeekOp/MDB_FIRST_DUP)
                    false)))
              (val-continue? []
                (if ev
                  (let [r (bf/compare-buffer v ev)]
                    (if (= r 0)
                      (if include-stop-val? true false)
                      (if (> r 0) false true)))
                  true))
              (check-val [op]
                (if (.seek cur op) (val-continue?) false))
              (advance-val []
                (check-val SeekOp/MDB_NEXT_DUP))]
        (reify
          IListRandKeyValIterator
          (seek-key [_ k kt]
            (l/put-key rtx k kt)
            (init-val))
          (has-next-val [_] (advance-val))
          (next-val [_] v))))))

(defn- up-db-size [^Env env]
  (.setMapSize env
               (* ^long c/+buffer-grow-factor+ ^long (-> env .info .mapSize))))

(defn- put-tx
  [^DBI dbi txn ^KVTxData tx]
  (case (.-op tx)
    :put      (do (.put-key dbi (.-k tx) (.-kt tx))
                  (.put-val dbi (.-v tx) (.-vt tx))
                  (if-let [f (.-flags tx)]
                    (.put dbi txn f)
                    (.put dbi txn)))
    :del      (do (.put-key dbi (.-k tx) (.-kt tx))
                  (.del dbi txn))
    :put-list (let [vs (.-v tx)]
                (.put-key dbi (.-k tx) (.-kt tx))
                (doseq [v vs]
                  (.put-val dbi v (.-vt tx))
                  (.put dbi txn)))
    :del-list (let [vs (.-v tx)]
                (.put-key dbi (.-k tx) (.-kt tx))
                (doseq [v vs]
                  (.put-val dbi v (.-vt tx))
                  (.del dbi txn false)))
    (raise "Unknown kv transact operator: " (.-op tx) {})))

(defn- transact1*
  [txs ^DBI dbi txn kt vt]
  (doseq [t txs]
    (put-tx dbi txn (l/->kv-tx-data t kt vt))))

(defn- transact*
  [txs ^UnifiedMap dbis txn]
  (doseq [t txs]
    (let [^KVTxData tx (l/->kv-tx-data t)
          dbi-name     (.-dbi-name tx)
          ^DBI dbi     (or (.get dbis dbi-name)
                           (raise dbi-name " is not open" {}))]
      (put-tx dbi txn tx))))

(defn- get-list*
  [lmdb ^Rtx rtx ^Cursor cur k kt vt]
  (.put-key rtx k kt)
  (when (.get cur (.-kb rtx) GetOp/MDB_SET)
    (let [^SpillableVector holder
          (sp/new-spillable-vector nil (:spill-opts (l/opts lmdb)))]
      (.seek cur SeekOp/MDB_FIRST_DUP)
      (dotimes [_ (.count cur)]
        (.cons holder (b/read-buffer (.val cur) vt))
        (.seek cur SeekOp/MDB_NEXT_DUP))
      holder)))

(defn- visit-list*
  [^Rtx rtx ^Cursor cur visitor raw-pred? k kt vt]
  (let [kv (reify IKV
             (k [_] (.key cur))
             (v [_] (.val cur)))
        vs #(if raw-pred?
              (visitor kv)
              (visitor (b/read-buffer (l/k kv) kt)
                       (when vt (b/read-buffer (l/v kv) vt))))]
    (.put-key rtx k kt)
    (when (.get cur (.-kb rtx) GetOp/MDB_SET)
      (.seek cur SeekOp/MDB_FIRST_DUP)
      (dotimes [_ (.count cur)]
        (vs)
        (.seek cur SeekOp/MDB_NEXT_DUP)))))

(defn- list-count*
  [^Rtx rtx ^Cursor cur k kt]
  (.put-key rtx k kt)
  (if (.get cur (.-kb rtx) GetOp/MDB_SET)
    (.count cur)
    0))

(defn- key-range-list-count*
  [^DBI dbi ^Rtx rtx ^Cursor cur k-range k-type cap]
  (let [^Iterable iterable (.iterate-key dbi rtx cur k-range k-type)
        ^Iterator iter     (.iterator iterable)]
    (if cap
      (loop [c 0]
        (if (<= ^long cap c)
          c
          (if (.hasNext iter)
            (let [n (.count cur)]
              (.next iter)
              (recur (+ c n)))
            c)))
      (loop [c 0]
        (if (.hasNext iter)
          (let [n (.count cur)]
            (.next iter)
            (recur (+ c n)))
          c)))))

(defn- in-list?*
  [^Rtx rtx ^Cursor cur k kt v vt]
  (l/list-range-info rtx :at-least k nil kt :at-least v nil vt)
  (.get cur (.-start-kb rtx) (.-start-vb rtx) SeekOp/MDB_GET_BOTH))

(declare ->LMDB reset-write-txn)

(deftype LMDB [^Env env
               info
               ^ConcurrentLinkedQueue pool
               ^UnifiedMap dbis
               ^ByteBuffer kb-w
               ^ByteBuffer start-kb-w
               ^ByteBuffer stop-kb-w
               ^ByteBuffer start-vb-w
               ^ByteBuffer stop-vb-w
               write-txn
               writing?
               ^:unsynchronized-mutable meta]
  IWriting
  (writing? [_] writing?)

  (write-txn [_] write-txn)

  (mark-write [_]
    (->LMDB
      env info pool dbis kb-w start-kb-w stop-kb-w
      start-vb-w stop-vb-w write-txn true meta))

  IObj
  (withMeta [this m] (set! meta m) this)
  (meta [_] meta)

  ILMDB
  (close-kv [_]
    (when-not (.isClosed env)
      (let [^Iterator iter (.iterator pool)]
        (loop []
          (when (.hasNext iter)
            (.close-rtx ^Rtx (.next iter))
            (.remove iter)
            (recur))))
      (.sync env true)
      (.close env))
    (when (@info :temp?) (u/delete-files (@info :dir)))
    nil)

  (closed-kv? [_] (.isClosed env))

  (check-ready [this] (assert (not (.closed-kv? this)) "LMDB env is closed."))

  (dir [_] (@info :dir))

  (opts [_] @info)

  (dbi-opts [_ dbi-name] (get-in @info [:dbis dbi-name]))

  (open-dbi [this dbi-name]
    (.open-dbi this dbi-name nil))
  (open-dbi [this dbi-name {:keys [key-size val-size flags validate-data?
                                   dupsort?]
                            :or   {key-size       c/+max-key-size+
                                   val-size       c/*init-val-size*
                                   flags          c/default-dbi-flags
                                   dupsort?       false
                                   validate-data? false}}]
    (.check-ready this)
    (assert (< ^long key-size 512) "Key size cannot be greater than 511 bytes")
    (let [{info-dbis :dbis max-dbis :max-dbs} @info]
      (if (< (count info-dbis) ^long max-dbis)
        (let [opts (merge (get info-dbis dbi-name)
                          {:key-size       key-size
                           :val-size       val-size
                           :flags          flags
                           :dupsort?       dupsort?
                           :validate-data? validate-data?})
              kb   (bf/allocate-buffer key-size)
              vb   (bf/allocate-buffer val-size)
              db   (.openDbi env ^String dbi-name
                             ^"[Lorg.lmdbjava.DbiFlags;"
                             (kv-flags :dbi (if dupsort?
                                              (conj flags :dupsort)
                                              flags)))
              kc   (:key-compress opts)
              dbi  (DBI. db (ConcurrentLinkedQueue.) kb vb
                         (when kc (cp/key-compressor (:lens kc) (:codes kc)))
                         (cp/get-dict-less-compressor)
                         dupsort? validate-data?)]
          (when (not= dbi-name c/kv-info)
            (vswap! info assoc-in [:dbis dbi-name] opts)
            (l/transact-kv this [(l/kv-tx :put c/kv-info [:dbis dbi-name] opts
                                          [:keyword :string])]))
          (.put dbis dbi-name dbi)
          dbi)
        (u/raise (str "Reached maximal number of DBI: " max-dbis) {}))))

  (get-dbi [this dbi-name]
    (.get-dbi this dbi-name true))
  (get-dbi [this dbi-name create?]
    (or (.get dbis dbi-name)
        (if create?
          (.open-dbi this dbi-name)
          (u/raise (str "DBI " dbi-name " is not open") {}))))

  (clear-dbi [this dbi-name]
    (.check-ready this)
    (let [^DBI dbi (.get-dbi this dbi-name)]
      (try
        (with-open [txn (.txnWrite env)]
          (.drop ^Dbi (.-db dbi) txn)
          (.commit txn))
        (catch Env$MapFullException _
          (.setMapSize env (* ^long c/+buffer-grow-factor+
                              ^long (-> env .info .mapSize)))
          (.clear-dbi this dbi-name))
        (catch Exception e
          (raise "Fail to clear DBI: " dbi-name " " e {})))))

  (drop-dbi [this dbi-name]
    (.check-ready this)
    (try
      (let [^DBI dbi (.get-dbi this dbi-name)]
        (with-open [txn (.txnWrite env)]
          (.drop ^Dbi (.-db dbi) txn true)
          (.commit txn))
        (vswap! info update :dbis dissoc dbi-name)
        (l/transact-kv this c/kv-info
                       [[:del [:dbis dbi-name]]] [:keyword :string])
        (.remove dbis dbi-name)
        nil)
      (catch Exception e (raise "Fail to drop DBI: " dbi-name e {}))))

  (list-dbis [_] (keys (@info :dbis)))

  (copy [this dest]
    (.copy this dest false))
  (copy [_ dest compact?]
    (let [d (u/file dest)]
      (if (u/empty-dir? d)
        (.copy env d (kv-flags :copy (if compact? #{:cp-compact} #{})))
        (raise "Destination directory is not empty." {}))))

  (get-rtx [this]
    (try
      (or (when-let [^Rtx rtx (.poll pool)]
            (.renew rtx))
          (locking env
            (->Rtx this
                   (.txnRead env)
                   (bf/allocate-buffer c/+max-key-size+)
                   (bf/allocate-buffer c/+max-key-size+)
                   (bf/allocate-buffer c/+max-key-size+)
                   (bf/allocate-buffer c/+max-key-size+)
                   (bf/allocate-buffer c/+max-key-size+)
                   (volatile! false))))
      (catch Txn$BadReaderLockException _
        (raise
          "Please do not open multiple LMDB connections to the same DB
           in the same process. Instead, a LMDB connection should be held onto
           and managed like a stateful resource. Refer to the documentation of
           `datalevin.core/open-kv` for more details." {})))
    )

  (return-rtx [_ rtx]
    (.reset ^Rtx rtx)
    (.add pool rtx))

  (stat [_]
    (try
      (stat-map (.stat env))
      (catch Exception e
        (raise "Fail to get statistics: " e {}))))
  (stat [this dbi-name]
    (if dbi-name
      (let [^Rtx rtx (.get-rtx this)]
        (try
          (let [^DBI dbi (.get-dbi this dbi-name)
                ^Dbi db  (.-db dbi)
                ^Txn txn (.-txn rtx)]
            (stat-map (.stat db txn)))
          (catch Exception e
            (raise "Fail to get stat: " e {:dbi dbi-name}))
          (finally (.return-rtx this rtx))))
      (l/stat this)))

  (entries [this dbi-name]
    (let [^DBI dbi (.get-dbi this dbi-name)
          ^Rtx rtx (.get-rtx this)]
      (try
        (.-entries ^Stat (.stat ^Dbi (.-db dbi) (.-txn rtx)))
        (catch Exception e
          (raise "Fail to get entries: " e {:dbi dbi-name}))
        (finally (.return-rtx this rtx)))))

  (open-transact-kv [this]
    (.check-ready this)
    (try
      (reset-write-txn this)
      (.mark-write this)
      (catch Exception e
        (raise "Fail to open read/write transaction in LMDB: " e {}))))

  (close-transact-kv [_]
    (if-let [^Rtx wtxn @write-txn]
      (when-let [^Txn txn (.-txn wtxn)]
        (let [aborted? @(.-aborted? wtxn)]
          (when-not aborted?
            (try
              (.commit txn)
              (catch Env$MapFullException _
                (vreset! write-txn nil)
                (.close txn)
                (up-db-size env)
                (raise "DB resized" {:resized true}))
              (catch Exception e
                (raise "Fail to commit read/write transaction in LMDB: "
                       e {}))))
          (vreset! write-txn nil)
          (.close txn)
          (if aborted? :aborted :committed)))
      (raise "Calling `close-transact-kv` without opening" {})))

  (abort-transact-kv [_]
    (when-let [^Rtx wtxn @write-txn]
      (vreset! (.-aborted? wtxn) true)
      (vreset! write-txn wtxn)
      nil))

  (transact-kv [this txs] (.transact-kv this nil txs))
  (transact-kv [this dbi-name txs]
    (.transact-kv this dbi-name txs :data :data))
  (transact-kv [this dbi-name txs k-type]
    (.transact-kv this dbi-name txs k-type :data))
  (transact-kv [this dbi-name txs k-type v-type]
    (.check-ready this)
    (locking write-txn
      (let [^Rtx rtx  @write-txn
            one-shot? (nil? rtx)
            ^DBI dbi  (when dbi-name
                        (or (.get dbis dbi-name)
                            (raise dbi-name " is not open" {})))]
        (try
          (if one-shot?
            (with-open [txn (.txnWrite env)]
              (if dbi
                (transact1* txs dbi txn k-type v-type)
                (transact* txs dbis txn))
              (.commit txn))
            (let [txn (.-txn rtx)]
              (if dbi
                (transact1* txs dbi txn k-type v-type)
                (transact* txs dbis txn))))
          :transacted
          (catch Env$MapFullException _
            (when-not one-shot? (.close ^Txn (.-txn rtx)))
            (up-db-size env)
            (if one-shot?
              (.transact-kv this dbi-name txs k-type v-type)
              (do (reset-write-txn this)
                  (raise "DB resized" {:resized true}))))
          (catch Exception e (raise "Fail to transact to LMDB: " e {}))))))

  (get-value [this dbi-name k]
    (.get-value this dbi-name k :data :data true))
  (get-value [this dbi-name k k-type]
    (.get-value this dbi-name k k-type :data true))
  (get-value [this dbi-name k k-type v-type]
    (.get-value this dbi-name k k-type v-type true))
  (get-value [this dbi-name k k-type v-type ignore-key?]
    (scan/get-value this dbi-name k k-type v-type ignore-key?))

  (get-first [this dbi-name k-range]
    (.get-first this dbi-name k-range :data :data false))
  (get-first [this dbi-name k-range k-type]
    (.get-first this dbi-name k-range k-type :data false))
  (get-first [this dbi-name k-range k-type v-type]
    (.get-first this dbi-name k-range k-type v-type false))
  (get-first [this dbi-name k-range k-type v-type ignore-key?]
    (scan/get-first this dbi-name k-range k-type v-type ignore-key?))

  (get-range [this dbi-name k-range]
    (.get-range this dbi-name k-range :data :data false))
  (get-range [this dbi-name k-range k-type]
    (.get-range this dbi-name k-range k-type :data false))
  (get-range [this dbi-name k-range k-type v-type]
    (.get-range this dbi-name k-range k-type v-type false))
  (get-range [this dbi-name k-range k-type v-type ignore-key?]
    (scan/get-range this dbi-name k-range k-type v-type ignore-key?))

  (key-range [this dbi-name k-range]
    (.key-range this dbi-name k-range :data))
  (key-range [this dbi-name k-range k-type]
    (scan/key-range this dbi-name k-range k-type))

  (visit-key-range [this dbi-name visitor k-range]
    (.visit-key-range this dbi-name visitor k-range :data true))
  (visit-key-range [this dbi-name visitor k-range k-type]
    (.visit-key-range this dbi-name visitor k-range k-type true))
  (visit-key-range [this dbi-name visitor k-range k-type raw-pred?]
    (scan/visit-key-range this dbi-name visitor k-range k-type raw-pred?))

  (key-range-count [this dbi-name k-range]
    (.key-range-count this dbi-name k-range :data))
  (key-range-count [this dbi-name k-range k-type]
    (.key-range-count this dbi-name k-range k-type nil))
  (key-range-count [this dbi-name k-range k-type cap]
    (scan/key-range-count this dbi-name k-range k-type cap))

  (range-seq [this dbi-name k-range]
    (.range-seq this dbi-name k-range :data :data false nil))
  (range-seq [this dbi-name k-range k-type]
    (.range-seq this dbi-name k-range k-type :data false nil))
  (range-seq [this dbi-name k-range k-type v-type]
    (.range-seq this dbi-name k-range k-type v-type false nil))
  (range-seq [this dbi-name k-range k-type v-type ignore-key?]
    (.range-seq this dbi-name k-range k-type v-type  ignore-key? nil))
  (range-seq [this dbi-name k-range k-type v-type ignore-key? opts]
    (scan/range-seq this dbi-name k-range k-type v-type ignore-key? opts))

  (range-count [this dbi-name k-range]
    (.range-count this dbi-name k-range :data))
  (range-count [this dbi-name k-range k-type]
    (scan/range-count this dbi-name k-range k-type))

  (get-some [this dbi-name pred k-range]
    (.get-some this dbi-name pred k-range :data :data false true))
  (get-some [this dbi-name pred k-range k-type]
    (.get-some this dbi-name pred k-range k-type :data false true))
  (get-some [this dbi-name pred k-range k-type v-type]
    (.get-some this dbi-name pred k-range k-type v-type false true))
  (get-some [this dbi-name pred k-range k-type v-type ignore-key?]
    (.get-some this dbi-name pred k-range k-type v-type ignore-key? true))
  (get-some [this dbi-name pred k-range k-type v-type ignore-key? raw-pred?]
    (scan/get-some this dbi-name pred k-range k-type v-type ignore-key?
                   raw-pred?))

  (range-filter [this dbi-name pred k-range]
    (.range-filter this dbi-name pred k-range :data :data false true))
  (range-filter [this dbi-name pred k-range k-type]
    (.range-filter this dbi-name pred k-range k-type :data false true))
  (range-filter [this dbi-name pred k-range k-type v-type]
    (.range-filter this dbi-name pred k-range k-type v-type false true))
  (range-filter [this dbi-name pred k-range k-type v-type ignore-key?]
    (.range-filter this dbi-name pred k-range k-type v-type ignore-key? true))
  (range-filter [this dbi-name pred k-range k-type v-type ignore-key? raw-pred?]
    (scan/range-filter this dbi-name pred k-range k-type v-type ignore-key?
                       raw-pred?))

  (range-keep [this dbi-name pred k-range]
    (.range-keep this dbi-name pred k-range :data :data true))
  (range-keep [this dbi-name pred k-range k-type]
    (.range-keep this dbi-name pred k-range k-type :data true))
  (range-keep [this dbi-name pred k-range k-type v-type]
    (.range-keep this dbi-name pred k-range k-type v-type true))
  (range-keep [this dbi-name pred k-range k-type v-type raw-pred?]
    (scan/range-keep this dbi-name pred k-range k-type v-type raw-pred?))

  (range-some [this dbi-name pred k-range]
    (.range-some this dbi-name pred k-range :data :data true))
  (range-some [this dbi-name pred k-range k-type]
    (.range-some this dbi-name pred k-range k-type :data true))
  (range-some [this dbi-name pred k-range k-type v-type]
    (.range-some this dbi-name pred k-range k-type v-type true))
  (range-some [this dbi-name pred k-range k-type v-type raw-pred?]
    (scan/range-some this dbi-name pred k-range k-type v-type raw-pred?))

  (range-filter-count [this dbi-name pred k-range]
    (.range-filter-count this dbi-name pred k-range :data :data true))
  (range-filter-count [this dbi-name pred k-range k-type]
    (.range-filter-count this dbi-name pred k-range k-type :data true))
  (range-filter-count [this dbi-name pred k-range k-type v-type]
    (.range-filter-count this dbi-name pred k-range k-type v-type true))
  (range-filter-count [this dbi-name pred k-range k-type v-type raw-pred?]
    (scan/range-filter-count this dbi-name pred k-range k-type v-type
                             raw-pred?))

  (visit [this dbi-name visitor k-range]
    (.visit this dbi-name visitor k-range :data :data true))
  (visit [this dbi-name visitor k-range k-type]
    (scan/visit this dbi-name visitor k-range k-type :data true))
  (visit [this dbi-name visitor k-range k-type v-type]
    (scan/visit this dbi-name visitor k-range k-type v-type true))
  (visit [this dbi-name visitor k-range k-type v-type raw-pred?]
    (scan/visit this dbi-name visitor k-range k-type v-type raw-pred?))

  (open-list-dbi [this dbi-name {:keys [key-size val-size]
                                 :or   {key-size c/+max-key-size+
                                        val-size c/+max-key-size+}}]
    (.check-ready this)
    (assert (and (>= c/+max-key-size+ ^long key-size)
                 (>= c/+max-key-size+ ^long val-size))
            "Data size cannot be larger than 511 bytes")
    (.open-dbi this dbi-name
               {:key-size key-size :val-size val-size :dupsort? true}))
  (open-list-dbi [lmdb dbi-name]
    (.open-list-dbi lmdb dbi-name nil))

  IList
  (put-list-items [this dbi-name k vs kt vt]
    (.transact-kv this [(l/kv-tx :put-list dbi-name k vs kt vt)]))

  (del-list-items [this dbi-name k kt]
    (.transact-kv this [(l/kv-tx :del dbi-name k kt)]))
  (del-list-items [this dbi-name k vs kt vt]
    (.transact-kv this [(l/kv-tx :del-list dbi-name k vs kt vt)]))

  (get-list [this dbi-name k kt vt]
    (.check-ready this)
    (when k
      (let [lmdb this]
        (scan/scan
          (get-list* this rtx cur k kt vt)
          (raise "Fail to get a list: " e {:dbi dbi-name :key k})))))

  (visit-list [this dbi-name visitor k kt]
    (.visit-list this dbi-name visitor k kt :data true))
  (visit-list [this dbi-name visitor k kt vt]
    (.visit-list this dbi-name visitor k kt vt true))
  (visit-list [this dbi-name visitor k kt vt raw-pred?]
    (.check-ready this)
    (when k
      (let [lmdb this]
        (scan/scan
          (visit-list* rtx cur visitor raw-pred? k kt vt)
          (raise "Fail to visit list: " e {:dbi dbi-name :k k})))))

  (list-count [lmdb dbi-name k kt]
    (.check-ready lmdb)
    (if k
      (scan/scan
        (list-count* rtx cur k kt)
        (raise "Fail to count list: " e {:dbi dbi-name :k k}))
      0))

  (key-range-list-count [lmdb dbi-name k-range k-type]
    (.key-range-list-count lmdb dbi-name k-range k-type nil))
  (key-range-list-count [lmdb dbi-name k-range k-type cap]
    (.check-ready lmdb)
    (scan/scan
      (key-range-list-count* dbi rtx cur k-range k-type cap)
      (raise "Fail to count list in key range: " e
             {:dbi dbi-name :k-range k-range})))

  (in-list? [this dbi-name k v kt vt]
    (.check-ready this)
    (if (and k v)
      (let [lmdb this]
        (scan/scan
          (in-list?* rtx cur k kt v vt)
          (raise "Fail to test if an item is in list: "
                 e {:dbi dbi-name :k k :v v})))
      false))

  (list-range [this dbi-name k-range kt v-range vt]
    (scan/list-range this dbi-name k-range kt v-range vt))

  (list-range-count [this dbi-name k-range kt v-range vt]
    (.list-range-count this dbi-name k-range kt v-range vt nil))
  (list-range-count [this dbi-name k-range kt v-range vt cap]
    (scan/list-range-count this dbi-name k-range kt v-range vt cap))

  (list-range-first [this dbi-name k-range kt v-range vt]
    (scan/list-range-first this dbi-name k-range kt v-range vt))

  (list-range-filter [this dbi-name pred k-range kt v-range vt]
    (.list-range-filter this dbi-name pred k-range kt v-range vt true))
  (list-range-filter [this dbi-name pred k-range kt v-range vt raw-pred?]
    (scan/list-range-filter this dbi-name pred k-range kt v-range vt raw-pred?))

  (list-range-keep [this dbi-name pred k-range kt v-range vt]
    (.list-range-keep this dbi-name pred k-range kt v-range vt true))
  (list-range-keep [this dbi-name pred k-range kt v-range vt raw-pred?]
    (scan/list-range-keep this dbi-name pred k-range kt v-range vt raw-pred?))

  (list-range-some [this list-name pred k-range k-type v-range v-type]
    (.list-range-some this list-name pred k-range k-type v-range v-type true))
  (list-range-some [this dbi-name pred k-range kt v-range vt raw-pred?]
    (scan/list-range-some this dbi-name pred k-range kt v-range vt raw-pred?))

  (list-range-filter-count
    [this list-name pred k-range k-type v-range v-type]
    (.list-range-filter-count this list-name pred k-range k-type v-range
                              v-type true nil))
  (list-range-filter-count
    [this dbi-name pred k-range kt v-range vt raw-pred?]
    (.list-range-filter-count this dbi-name pred k-range kt v-range
                              vt raw-pred? nil))
  (list-range-filter-count
    [this dbi-name pred k-range kt v-range vt raw-pred? cap]
    (scan/list-range-filter-count this dbi-name pred k-range kt v-range
                                  vt raw-pred? cap))

  (visit-list-range
    [this list-name visitor k-range k-type v-range v-type]
    (.visit-list-range this list-name visitor k-range k-type v-range
                       v-type true))
  (visit-list-range
    [this dbi-name visitor k-range kt v-range vt raw-pred?]
    (scan/visit-list-range this dbi-name visitor k-range kt v-range
                           vt raw-pred?))

  (operate-list-val-range
    [this dbi-name operator v-range vt]
    (scan/operate-list-val-range this dbi-name operator v-range vt))

  IAdmin
  (re-index [this opts] (l/re-index* this opts)))

(defn- reset-write-txn
  [^LMDB lmdb]
  (let [kb-w       ^ByteBuffer (.-kb-w lmdb)
        start-kb-w ^ByteBuffer (.-start-kb-w lmdb)
        stop-kb-w  ^ByteBuffer (.-stop-kb-w lmdb)
        start-vb-w ^ByteBuffer (.-start-vb-w lmdb)
        stop-vb-w  ^ByteBuffer (.-stop-vb-w lmdb)]
    (.clear kb-w)
    (.clear start-kb-w)
    (.clear stop-kb-w)
    (.clear start-vb-w)
    (.clear stop-vb-w)
    (vreset! (.-write-txn lmdb) (->Rtx lmdb
                                       (.txnWrite ^Env (.-env lmdb))
                                       kb-w
                                       start-kb-w
                                       stop-kb-w
                                       start-vb-w
                                       stop-vb-w
                                       (volatile! false)))))

(defn- init-info
  [^LMDB lmdb new-info]
  (l/transact-kv lmdb c/kv-info (map (fn [[k v]] [:put k v]) new-info))
  (let [dbis (into {}
                   (map (fn [[[_ dbi-name] opts]] [dbi-name opts]))
                   (l/get-range lmdb c/kv-info
                                [:closed
                                 [:dbis :db.value/sysMin]
                                 [:dbis :db.value/sysMax]]
                                [:keyword :string]))
        info (into {}
                   (l/range-filter lmdb c/kv-info
                                   (fn [kv]
                                     (let [b (b/read-buffer (l/k kv) :byte)]
                                       (not= b c/type-hete-tuple)))
                                   [:all]))]
    (vreset! (.-info lmdb) (assoc info :dbis dbis))))

(defmethod open-kv :java
  ([dir] (open-kv dir {}))
  ([dir {:keys [mapsize max-readers max-dbs flags temp? compress?]
         :or   {max-readers c/*max-readers*
                max-dbs     c/*max-dbs*
                mapsize     c/*init-db-size*
                flags       c/default-env-flags
                temp?       false
                compress?   true}
         :as   opts}]
   (assert (string? dir) "directory should be a string.")
   (try
     (let [^File file (u/file dir)
           mapsize    (* (long (if (u/empty-dir? file)
                                 mapsize
                                 (c/pick-mapsize dir)))
                         1024 1024)
           builder    (doto (Env/create)
                        (.setMapSize mapsize)
                        (.setMaxReaders max-readers)
                        (.setMaxDbs max-dbs))
           flags      (if temp?
                        (conj flags :nosync)
                        flags)
           ^Env env   (.open builder file (kv-flags :env flags))
           info       (merge opts {:dir         dir
                                   :max-readers max-readers
                                   :max-dbs     max-dbs
                                   :flags       flags
                                   :temp?       temp?
                                   :compress?   compress?})
           lmdb       (->LMDB env
                              (volatile! info)
                              (ConcurrentLinkedQueue.)
                              (UnifiedMap.)
                              (bf/allocate-buffer c/+max-key-size+)
                              (bf/allocate-buffer c/+max-key-size+)
                              (bf/allocate-buffer c/+max-key-size+)
                              (bf/allocate-buffer c/+max-key-size+)
                              (bf/allocate-buffer c/+max-key-size+)
                              (volatile! nil)
                              false
                              nil)]
       (l/open-dbi lmdb c/kv-info)
       (if temp?
         (u/delete-on-exit file)
         (do (init-info lmdb info)
             (.addShutdownHook (Runtime/getRuntime)
                               (Thread. #(l/close-kv lmdb)))))
       lmdb)
     (catch Exception e (raise "Fail to open database: " e {:dir dir})))))

(defn access-lmdb
  []
  (let [os-arch  (System/getProperty "os.arch")
        amd64?   (#{"x64" "amd64" "x86_64"} os-arch)
        aarch64? (= "aarch64" os-arch)
        os-name  (s/lower-case (System/getProperty "os.name"))
        linux?   (s/starts-with? os-name "linux")
        windows? (s/starts-with? os-name "windows")
        osx?     (s/starts-with? os-name "mac os x")
        freebsd? (s/starts-with? os-name "freebsd")

        [lib-name platform]
        (cond
          amd64?   (cond
                     linux?   ["liblmdb.so" "ubuntu-latest-amd64-shared"]
                     windows? ["liblmdb.dll" "x86_64-windows-gnu"]
                     osx?     ["liblmdb.dylib" "macos-latest-amd64-shared"]
                     freebsd? ["liblmdb.so" "freebsd-os-provided-local-lib"]
                     :else    (u/raise "Unsupported OS " os-name " on amd64"
                                       {}))
          aarch64? (cond
                     osx?     ["liblmdb.dylib" "macos-latest-aarch64-shared"]
                     linux?   ["liblmdb.so" "aarch64-linux-gnu"]
                     freebsd? ["liblmdb.so" "freebsd-os-provided-local-lib"]
                     :else    (u/raise "Unsupported OS " os-name " on aarch64"
                                       {}))
          :else    (u/raise "Unsupported architecture " os-arch {}))]
    (if (= platform "freebsd-os-provided-local-lib")
      (let [fpath (str "/usr/local/lib/" lib-name)]
        (if (.exists (File. fpath))
          (System/setProperty "lmdbjava.native.lib" fpath)
          (u/raise "liblmdb.so not found at " fpath
                   ", have you installed the package?" {})))
      ;; extract lmdb binary from jar
      (let [resource        (str "dtlvnative/" platform "/" lib-name)
            ^String dir     (u/tmp-dir (str "lmdbjava-native-lib-"
                                            (UUID/randomUUID)))
            ^File file      (File. dir ^String lib-name)
            path            (.toPath file)
            fpath           (.getAbsolutePath file)
            ^ClassLoader cl (.getContextClassLoader (Thread/currentThread))]
        (try
          (u/create-dirs dir)
          (.deleteOnExit file)
          (System/setProperty "lmdbjava.native.lib" fpath)
          (with-open [^InputStream in   (.getResourceAsStream cl resource)
                      ^OutputStream out (Files/newOutputStream
                                          path (into-array OpenOption []))]
            (io/copy in out))
          (catch Exception e
            (u/raise "Failed to extract LMDB library: " e
                     {:resource resource :path fpath})))))))

(access-lmdb)
