(ns ^:no-doc datalevin.binding.graal
  "LMDB binding for GraalVM native image"
  (:require
   [datalevin.bits :as b]
   [datalevin.util :refer [raise] :as u]
   [datalevin.constants :as c]
   [datalevin.scan :as scan]
   [datalevin.spill :as sp]
   [datalevin.lmdb :as l :refer [open-kv IBuffer IRange IRtx IDB IKV
                                 IList ILMDB IWriting]])
  (:import
   [java.util Iterator]
   [java.util.concurrent ConcurrentHashMap ConcurrentLinkedQueue]
   [java.nio BufferOverflowException]
   [java.lang AutoCloseable]
   [clojure.lang IPersistentVector MapEntry]
   [datalevin.ni BufVal Lib Env Txn Dbi Cursor Stat Info
    Lib$BadReaderLockException Lib$MDB_cursor_op Lib$MDB_envinfo
    Lib$MDB_stat Lib$MapFullException]
   [datalevin.spill SpillableVector])
  (:gen-class :name datalevin.binding.graal))

(defprotocol IFlag
  (value [this flag-key]))

(deftype Flag []
  IFlag
  (value  [_ flag-key]
    (case flag-key
      :fixedmap   (Lib/MDB_FIXEDMAP)
      :nosubdir   (Lib/MDB_NOSUBDIR)
      :rdonly-env (Lib/MDB_RDONLY)
      :writemap   (Lib/MDB_WRITEMAP)
      :nometasync (Lib/MDB_NOMETASYNC)
      :nosync     (Lib/MDB_NOSYNC)
      :mapasync   (Lib/MDB_MAPASYNC)
      :notls      (Lib/MDB_NOTLS)
      :nolock     (Lib/MDB_NOLOCK)
      :nordahead  (Lib/MDB_NORDAHEAD)
      :nomeminit  (Lib/MDB_NOMEMINIT)

      :cp-compact (Lib/MDB_CP_COMPACT)

      :reversekey (Lib/MDB_REVERSEKEY)
      :dupsort    (Lib/MDB_DUPSORT)
      :integerkey (Lib/MDB_INTEGERKEY)
      :dupfixed   (Lib/MDB_DUPFIXED)
      :integerdup (Lib/MDB_INTEGERDUP)
      :reversedup (Lib/MDB_REVERSEDUP)
      :create     (Lib/MDB_CREATE)

      :nooverwrite (Lib/MDB_NOOVERWRITE)
      :nodupdata   (Lib/MDB_NODUPDATA)
      :current     (Lib/MDB_CURRENT)
      :reserve     (Lib/MDB_RESERVE)
      :append      (Lib/MDB_APPEND)
      :appenddup   (Lib/MDB_APPENDDUP)
      :multiple    (Lib/MDB_MULTIPLE)

      :rdonly-txn (Lib/MDB_RDONLY))))

(defn- kv-flags
  [flags]
  (if (seq flags)
    (let [flag (Flag.)]
      (reduce (fn [r f] (bit-or ^int r ^int f))
              0
              (map #(value ^Flag flag %) (set flags))))
    (int 0)))

(defn- put-bufval
  [^BufVal vp k kt]
  (when-some [x k]
    (let [bf (.inBuf vp)]
      (.clear vp)
      (b/put-buffer bf x kt)
      (.flip vp))))

(deftype Rtx [lmdb
              ^Txn txn
              ^BufVal kp
              ^BufVal vp
              ^BufVal start-kp
              ^BufVal stop-kp
              ^BufVal start-vp
              ^BufVal stop-vp
              aborted?]

  IBuffer
  (put-key [_ x t]
    (try
      (put-bufval kp x t)
      (catch BufferOverflowException _
        (raise "Key cannot be larger than 511 bytes." {:input x}))
      (catch Exception e
        (raise "Error putting read-only transaction key buffer: "
               (ex-message e)
               {:value x :type t}))))
  (put-val [_ x t]
    (raise "put-val not allowed for read only txn buffer" {}))

  IRange
  (range-info [_ range-type k1 k2 kt]
    (put-bufval start-kp k1 kt)
    (put-bufval stop-kp k2 kt)
    (l/range-table range-type k1 k2 start-kp stop-kp))

  (list-range-info [_ k-range-type k1 k2 kt v-range-type v1 v2 vt]
    (put-bufval start-kp k1 kt)
    (put-bufval stop-kp k2 kt)
    (put-bufval start-vp v1 vt)
    (put-bufval stop-vp v2 vt)
    (into (l/range-table k-range-type k1 k2 start-kp stop-kp)
          (l/range-table v-range-type v1 v2 start-vp stop-vp)))

  IRtx
  (read-only? [_] (.isReadOnly txn))
  (get-txn [_] txn)
  (close-rtx [_]
    (.close txn)
    (.close kp)
    (.close vp)
    (.close start-kp)
    (.close stop-kp)
    (.close start-vp)
    (.close stop-vp))
  (reset [this] (.reset txn) this)
  (renew [this] (.renew txn) this))

(deftype KV [^BufVal kp ^BufVal vp]
  IKV
  (k [_] (.outBuf kp))
  (v [_] (.outBuf vp)))

(declare ->KeyIterable ->ListIterable)

(deftype DBI [^Dbi db
              ^ConcurrentLinkedQueue curs
              ^BufVal kp
              ^:volatile-mutable ^BufVal vp
              ^boolean dupsort?
              ^boolean validate-data?]
  IBuffer
  (put-key [this x t]
    (or (not validate-data?)
        (b/valid-data? x t)
        (raise "Invalid data, expecting " t {:input x}))
    (let [dbi-name (.dbi-name this)]
      (try
        (put-bufval kp x t)
        (catch BufferOverflowException _
          (raise "Key cannot be larger than 511 bytes." {:input x}))
        (catch Exception e
          (raise "Error putting r/w key buffer of "
                 dbi-name ": " (ex-message e)
                 {:value x :type t :dbi dbi-name})))))
  (put-val [this x t]
    (or (not validate-data?)
        (b/valid-data? x t)
        (raise "Invalid data, expecting " t {:input x}))
    (let [dbi-name (.dbi-name this)]
      (try
        (put-bufval vp x t)
        (catch BufferOverflowException _
          (let [size (* ^long c/+buffer-grow-factor+
                        ^long (b/measure-size x))]
            (.close vp)
            (set! vp (BufVal/create size))
            (put-bufval vp x t)))
        (catch Exception e
          (raise "Error putting r/w value buffer of "
                 dbi-name ": " e
                 {:value x :type t :dbi dbi-name})))))

  IDB
  (dbi [_] db)
  (dbi-name [_] (.getName db))
  (put [this txn] (.put this txn nil))
  (put [_ txn flags] (.put db txn kp vp (kv-flags flags)))
  (del [this txn] (.del this txn true))
  (del [_ txn all?] (if all? (.del db txn kp nil) (.del db txn kp vp)))
  (get-kv [_ rtx]
    (let [^BufVal kp (.-kp ^Rtx rtx)
          ^BufVal vp (.-vp ^Rtx rtx)
          rc         (Lib/mdb_get (.get ^Txn (.-txn ^Rtx rtx))
                                  (.get db) (.ptr kp) (.ptr vp))]
      (Lib/checkRc rc)
      (when-not (= rc (Lib/MDB_NOTFOUND))
        (.outBuf vp))))
  (iterate-key [this rtx [range-type k1 k2] k-type]
    (let [cur (.get-cursor this rtx)
          ctx (l/range-info rtx range-type k1 k2 k-type)]
      (->KeyIterable this cur rtx ctx)))
  (iterate-list [this rtx [k-range-type k1 k2] k-type
                 [v-range-type v1 v2] v-type]
    (let [cur (.get-cursor this rtx)
          ctx (l/list-range-info rtx k-range-type k1 k2 k-type
                                 v-range-type v1 v2 v-type)]
      (->ListIterable this cur rtx ctx)))
  (iterate-kv [this rtx k-range k-type v-type]
    (if dupsort?
      (.iterate-list this rtx k-range k-type [:all] v-type)
      (.iterate-key this rtx k-range k-type)))
  (get-cursor [_ rtx]
    (let [^Rtx rtx rtx
          ^Txn txn (.-txn rtx)]
      (or (when (.isReadOnly txn)
            (when-let [^Cursor cur (.poll curs)]
              (.renew cur txn)
              cur))
          (Cursor/create txn db (.-kp rtx) (.-vp rtx)))))
  (close-cursor [_ cur] (.close ^Cursor cur))
  (return-cursor [_ cur] (.add curs cur)))

(deftype KeyIterable [^DBI db
                      ^Cursor cur
                      ^Rtx rtx
                      ctx]
  AutoCloseable
  (close [_]
    (if (.isReadOnly ^Txn (.-txn rtx))
      (.return-cursor db cur)
      (.close cur)))

  Iterable
  (iterator [_]
    (let [[forward? include-start? include-stop? ^BufVal sk ^BufVal ek] ctx

          started?  (volatile! false)
          ^BufVal k (.key cur)
          ^BufVal v (.val cur)]
      (letfn [(init []
                (if sk
                  (if (.get cur sk Lib$MDB_cursor_op/MDB_SET_RANGE)
                    (if include-start?
                      (continue?)
                      (if (zero? (b/compare-buffer (.outBuf k) (.outBuf sk)))
                        (check Lib$MDB_cursor_op/MDB_NEXT_NODUP)
                        (continue?)))
                    false)
                  (check Lib$MDB_cursor_op/MDB_FIRST)))
              (init-back []
                (if sk
                  (if (.get cur sk Lib$MDB_cursor_op/MDB_SET_RANGE)
                    (if include-start?
                      (if (zero? (b/compare-buffer (.outBuf k) (.outBuf sk)))
                        (continue-back?)
                        (check-back Lib$MDB_cursor_op/MDB_PREV_NODUP))
                      (check-back Lib$MDB_cursor_op/MDB_PREV_NODUP))
                    (check-back Lib$MDB_cursor_op/MDB_LAST))
                  (check-back Lib$MDB_cursor_op/MDB_LAST)))
              (continue? []
                (if ek
                  (let [r (b/compare-buffer (.outBuf k) (.outBuf ek))]
                    (if (= r 0)
                      include-stop?
                      (if (> r 0) false true)))
                  true))
              (continue-back? []
                (if ek
                  (let [r (b/compare-buffer (.outBuf k) (.outBuf ek))]
                    (if (= r 0)
                      include-stop?
                      (if (> r 0) true false)))
                  true))
              (check [op] (if (.seek cur op) (continue?) false))
              (check-back [op] (if (.seek cur op) (continue-back?) false))
              (advance []
                (if forward?
                  (check Lib$MDB_cursor_op/MDB_NEXT_NODUP)
                  (check-back Lib$MDB_cursor_op/MDB_PREV_NODUP)))
              (init-k []
                (vreset! started? true)
                (if forward? (init) (init-back)))]
        (reify
          Iterator
          (hasNext [_]
            (if (not @started?) (init-k) (advance)))
          (next [_] (KV. k v)))))))

(deftype ListIterable [^DBI db
                       ^Cursor cur
                       ^Rtx rtx
                       ctx]
  AutoCloseable
  (close [_]
    (if (.isReadOnly ^Txn (.-txn rtx))
      (.return-cursor db cur)
      (.close cur)))

  Iterable
  (iterator [_]
    (let [[forward-key? include-start-key? include-stop-key?
           ^BufVal sk ^BufVal ek
           forward-val? include-start-val? include-stop-val?
           ^BufVal sv ^BufVal ev] ctx

          key-ended? (volatile! false)
          started?   (volatile! false)
          k          (.key cur)
          v          (.val cur)]
      (letfn [(init-key []
                (if sk
                  (if (.get cur sk Lib$MDB_cursor_op/MDB_SET_RANGE)
                    (if include-start-key?
                      (key-continue?)
                      (if (zero? (b/compare-buffer (.outBuf k) (.outBuf sk)))
                        (check-key Lib$MDB_cursor_op/MDB_NEXT_NODUP)
                        (key-continue?)))
                    false)
                  (check-key Lib$MDB_cursor_op/MDB_FIRST)))
              (init-key-back []
                (if sk
                  (if (.get cur sk Lib$MDB_cursor_op/MDB_SET_RANGE)
                    (if include-start-key?
                      (if (zero? (b/compare-buffer (.outBuf k) (.outBuf sk)))
                        (key-continue-back?)
                        (check-key-back Lib$MDB_cursor_op/MDB_PREV_NODUP))
                      (check-key-back Lib$MDB_cursor_op/MDB_PREV_NODUP))
                    (check-key-back Lib$MDB_cursor_op/MDB_LAST))
                  (check-key-back Lib$MDB_cursor_op/MDB_LAST)))
              (init-val []
                (if sv
                  (if (.get cur k sv Lib$MDB_cursor_op/MDB_GET_BOTH_RANGE)
                    (if include-start-val?
                      (val-continue?)
                      (if (zero? (b/compare-buffer (.outBuf v) (.outBuf sv)))
                        (check-val Lib$MDB_cursor_op/MDB_NEXT_DUP)
                        (val-continue?)))
                    false)
                  (check-val Lib$MDB_cursor_op/MDB_FIRST_DUP)))
              (init-val-back []
                (if sv
                  (if (.get cur k sv Lib$MDB_cursor_op/MDB_GET_BOTH_RANGE)
                    (if include-start-val?
                      (if (zero? (b/compare-buffer (.outBuf v) (.outBuf sv)))
                        (val-continue-back?)
                        (check-val-back Lib$MDB_cursor_op/MDB_PREV_DUP))
                      (check-val-back Lib$MDB_cursor_op/MDB_PREV_DUP))
                    (check-val-back Lib$MDB_cursor_op/MDB_LAST_DUP))
                  (check-val-back Lib$MDB_cursor_op/MDB_LAST_DUP)))
              (key-end [] (vreset! key-ended? true) false)
              (val-end [] (if @key-ended? false (advance-key)))
              (key-continue? []
                (if ek
                  (let [r (b/compare-buffer (.outBuf k) (.outBuf ek))]
                    (if (= r 0)
                      (do (vreset! key-ended? true)
                          include-stop-key?)
                      (if (> r 0) (key-end) true)))
                  true))
              (key-continue-back? []
                (if ek
                  (let [r (b/compare-buffer (.outBuf k) (.outBuf ek))]
                    (if (= r 0)
                      (do (vreset! key-ended? true)
                          include-stop-key?)
                      (if (> r 0) true (key-end))))
                  true))
              (check-key [op]
                (if (.seek cur op) (key-continue?) (key-end)))
              (check-key-back [op]
                (if (.seek cur op) (key-continue-back?) (key-end)))
              (advance-key []
                (or (and (if forward-key?
                           (check-key Lib$MDB_cursor_op/MDB_NEXT_NODUP)
                           (check-key-back Lib$MDB_cursor_op/MDB_PREV_NODUP))
                         (if forward-val? (init-val) (init-val-back)))
                    (if @key-ended? false (recur))))
              (init-kv []
                (vreset! started? true)
                (or (and (if forward-key? (init-key) (init-key-back))
                         (if forward-val? (init-val) (init-val-back)))
                    (advance-key)))
              (val-continue? []
                (if ev
                  (let [r (b/compare-buffer (.outBuf v) (.outBuf ev))]
                    (if (= r 0)
                      (if include-stop-val? true (val-end))
                      (if (> r 0) (val-end) true)))
                  true))
              (val-continue-back? []
                (if ev
                  (let [r (b/compare-buffer (.outBuf v) (.outBuf ev))]
                    (if (= r 0)
                      (if include-stop-val? true (val-end))
                      (if (> r 0) true (val-end))))
                  true))
              (check-val [op]
                (if (.seek cur op) (val-continue?) (val-end)))
              (check-val-back [op]
                (if (.seek cur op) (val-continue-back?) (val-end)))
              (advance-val []
                (check-val Lib$MDB_cursor_op/MDB_NEXT_DUP))
              (advance-val-back []
                (check-val-back Lib$MDB_cursor_op/MDB_PREV_DUP))]
        (reify
          Iterator
          (hasNext [_]
            (if (not @started?)
              (init-kv)
              (if forward-val? (advance-val) (advance-val-back))))
          (next [_] (KV. k v)))))))

(defn- stat-map [^Stat stat]
  {:psize          (.ms_psize ^Lib$MDB_stat (.get stat))
   :depth          (.ms_depth ^Lib$MDB_stat (.get stat))
   :branch-pages   (.ms_branch_pages ^Lib$MDB_stat (.get stat))
   :leaf-pages     (.ms_leaf_pages ^Lib$MDB_stat (.get stat))
   :overflow-pages (.ms_overflow_pages ^Lib$MDB_stat (.get stat))
   :entries        (.ms_entries ^Lib$MDB_stat (.get stat))})

(defn- transact*
  [txs ^ConcurrentHashMap dbis ^Txn txn]
  (doseq [^IPersistentVector tx txs]
    (let [cnt      (.length tx)
          op       (.nth tx 0)
          dbi-name (.nth tx 1)
          k        (.nth tx 2)
          ^DBI dbi (or (.get dbis dbi-name)
                       (raise dbi-name " is not open" {}))]
      (case op
        :put      (let [v     (.nth tx 3)
                        kt    (when (< 4 cnt) (.nth tx 4))
                        vt    (when (< 5 cnt) (.nth tx 5))
                        flags (when (< 6 cnt) (.nth tx 6))]
                    (.put-key dbi k kt)
                    (.put-val dbi v vt)
                    (if flags
                      (.put dbi txn flags)
                      (.put dbi txn)))
        :del      (let [kt (when (< 3 cnt) (.nth tx 3)) ]
                    (.put-key dbi k kt)
                    (.del dbi txn))
        :put-list (let [vs (.nth tx 3)
                        kt (when (< 4 cnt) (.nth tx 4))
                        vt (when (< 5 cnt) (.nth tx 5))]
                    (.put-key dbi k kt)
                    (doseq [v vs]
                      (.put-val dbi v vt)
                      (.put dbi txn)))
        :del-list (let [vs (.nth tx 3)
                        kt (when (< 4 cnt) (.nth tx 4))
                        vt (when (< 5 cnt) (.nth tx 5))]
                    (.put-key dbi k kt)
                    (doseq [v vs]
                      (.put-val dbi v vt)
                      (.del dbi txn false)))
        (raise "Unknown kv operator: " op {})))))

(defn- get-list*
  [lmdb ^Rtx rtx ^Cursor cur k kt vt]
  (.put-key rtx k kt)
  (when (.get cur ^BufVal (.-kp rtx) Lib$MDB_cursor_op/MDB_SET)
    (let [^SpillableVector holder
          (sp/new-spillable-vector nil (:spill-opts (l/opts lmdb)))]
      (.seek cur Lib$MDB_cursor_op/MDB_FIRST_DUP)
      (.cons holder (b/read-buffer (.outBuf (.val cur)) vt))
      (dotimes [_ (dec (.count cur))]
        (.seek cur Lib$MDB_cursor_op/MDB_NEXT_DUP)
        (.cons holder (b/read-buffer (.outBuf (.val cur)) vt)))
      holder)))

(defn- visit-list*
  [^Rtx rtx ^Cursor cur k kt visitor]
  (let [kv (reify IKV
             (k [_] (.outBuf (.key cur)))
             (v [_] (.outBuf (.val cur))))]
    (.put-key rtx k kt)
    (when (.get cur ^BufVal (.-kp rtx) Lib$MDB_cursor_op/MDB_SET)
      (.seek cur Lib$MDB_cursor_op/MDB_FIRST_DUP)
      (visitor kv)
      (dotimes [_ (dec (.count cur))]
        (.seek cur Lib$MDB_cursor_op/MDB_NEXT_DUP)
        (visitor kv)))))

(defn- list-count*
  [^Rtx rtx ^Cursor cur k kt]
  (.put-key rtx k kt)
  (if (.get cur ^BufVal (.-kp rtx) Lib$MDB_cursor_op/MDB_SET)
    (.count cur)
    0))

(defn- in-list?*
  [^Rtx rtx ^Cursor cur k kt v vt]
  (l/list-range-info rtx :at-least k nil kt :at-least v nil vt)
  (.get cur ^BufVal (.-start-kp rtx) ^BufVal (.-start-vp rtx)
        Lib$MDB_cursor_op/MDB_GET_BOTH))

(declare reset-write-txn ->LMDB)

(deftype LMDB [^Env env
               ^String dir
               temp?
               opts
               ^ConcurrentLinkedQueue pool
               ^ConcurrentHashMap dbis
               ^:volatile-mutable closed?
               ^BufVal kp-w
               ^BufVal vp-w
               ^BufVal start-kp-w
               ^BufVal stop-kp-w
               ^BufVal start-vp-w
               ^BufVal stop-vp-w
               write-txn
               writing?]

  IWriting
  (writing? [_] writing?)

  (write-txn [_] write-txn)

  (mark-write [_]
    (->LMDB
      env dir temp? opts pool dbis closed? kp-w vp-w start-kp-w
      stop-kp-w start-vp-w stop-vp-w write-txn true))

  ILMDB
  (close-kv [_]
    (when-not closed?
      (loop [^Iterator iter (.iterator pool)]
        (when (.hasNext iter)
          (.close-rtx ^Rtx (.next iter))
          (.remove iter)
          (recur iter)))
      (doseq [^DBI dbi (.values dbis)]
        (loop [^Iterator iter (.iterator ^ConcurrentLinkedQueue (.-curs dbi))]
          (when (.hasNext iter)
            (.close ^Cursor (.next iter))
            (.remove iter)
            (recur iter)))
        (.close ^Dbi (.-db dbi)))
      (when-not (.isClosed env) (.sync env))
      (.close env)
      (set! closed? true)
      (when temp? (u/delete-files dir))
      nil))

  (closed-kv? [_] closed?)

  (dir [_] dir)

  (opts [_] opts)

  (open-dbi [this dbi-name]
    (.open-dbi this dbi-name nil))
  (open-dbi [_ dbi-name {:keys [key-size val-size flags validate-data?
                                dupsort?]
                         :or   {key-size       c/+max-key-size+
                                val-size       c/+default-val-size+
                                flags          c/default-dbi-flags
                                dupsort?       false
                                validate-data? false}}]
    (assert (not closed?) "LMDB env is closed.")
    (assert (< ^long key-size 512) "Key size cannot be greater than 511 bytes")
    (let [kp  (BufVal/create key-size)
          vp  (BufVal/create val-size)
          dbi (Dbi/create env dbi-name
                          (kv-flags (if dupsort? (conj flags :dupsort) flags)))
          db  (DBI. dbi (ConcurrentLinkedQueue.) kp vp
                    dupsort? validate-data?)]
      (.put dbis dbi-name db)
      db))

  (get-dbi [this dbi-name]
    (.get-dbi this dbi-name true))
  (get-dbi [this dbi-name create?]
    (or (.get dbis dbi-name)
        (if create?
          (.open-dbi this dbi-name)
          (.open-dbi this dbi-name {:key-size c/+max-key-size+
                                    :val-size c/+default-val-size+
                                    :flags    c/read-dbi-flags}))))

  (clear-dbi [this dbi-name]
    (assert (not closed?) "LMDB env is closed.")
    (try
      (let [^Dbi dbi (.-db ^DBI (.get-dbi this dbi-name))
            ^Txn txn (Txn/create env)]
        (Lib/checkRc (Lib/mdb_drop (.get txn) (.get dbi) 0))
        (.commit txn))
      (catch Exception e
        (raise "Fail to clear DBI: " dbi-name " " e {}))))

  (drop-dbi [this dbi-name]
    (assert (not closed?) "LMDB env is closed.")
    (try
      (let [^Dbi dbi (.-db ^DBI (.get-dbi this dbi-name))
            ^Txn txn (Txn/create env)]
        (Lib/checkRc (Lib/mdb_drop (.get txn) (.get dbi) 1))
        (.commit txn)
        (.remove dbis dbi-name)
        nil)
      (catch Exception e
        (raise "Fail to drop DBI: " dbi-name e {}))))

  (get-rtx [this]
    (try
      (or (when-let [^Rtx rtx (.poll pool)]
            (.renew rtx))
          (Rtx. this
                (Txn/createReadOnly env)
                (BufVal/create c/+max-key-size+)
                (BufVal/create 0)
                (BufVal/create c/+max-key-size+)
                (BufVal/create c/+max-key-size+)
                (BufVal/create c/+max-key-size+)
                (BufVal/create c/+max-key-size+)
                (volatile! false)))
      (catch Lib$BadReaderLockException _
        (raise
          "Please do not open multiple LMDB connections to the same DB
           in the same process. Instead, a LMDB connection should be held onto
           and managed like a stateful resource. Refer to the documentation of
           `datalevin.core/open-kv` for more details." {}))))

  (return-rtx [this rtx]
    (.reset ^Rtx rtx)
    (.add pool rtx))

  (list-dbis [this]
    (assert (not closed?) "LMDB env is closed.")
    (let [^Dbi main (Dbi/create env 0)
          ^DBI dbi  (->DBI main
                           (ConcurrentLinkedQueue.)
                           (BufVal/create c/+max-key-size+)
                           (BufVal/create c/+max-key-size+)
                           false
                           false)
          ^Rtx rtx  (.get-rtx this)]
      (try
        (with-open [^Cursor cursor (.get-cursor dbi rtx)]
          (with-open [^AutoCloseable iterable
                      (->KeyIterable dbi cursor rtx
                                     (l/range-info rtx :all nil nil nil))]
            (loop [^Iterator iter (.iterator ^Iterable iterable)
                   holder         (transient [])]
              (if (.hasNext iter)
                (let [kv      (.next iter)
                      holder' (conj! holder
                                     (-> kv l/k b/get-bytes b/text-ba->str))]
                  (recur iter holder'))
                (persistent! holder)))))
        (catch Exception e
          (raise "Fail to list DBIs: " e {}))
        (finally (.return-rtx this rtx)))))

  (copy [this dest]
    (.copy this dest false))
  (copy [_ dest compact?]
    (assert (not closed?) "LMDB env is closed.")
    (if (-> dest u/file u/empty-dir?)
      (.copy env dest (if compact? true false))
      (raise "Destination directory is not empty." {})))

  (stat [this]
    (assert (not closed?) "LMDB env is closed.")
    (try
      (let [stat ^Stat (Stat/create env)
            m    (stat-map stat)]
        (.close stat)
        m)
      (catch Exception e
        (raise "Fail to get statistics: " e {}))))
  (stat [this dbi-name]
    (assert (not closed?) "LMDB env is closed.")
    (if dbi-name
      (let [^Rtx rtx (.get-rtx this)]
        (try
          (let [^DBI dbi   (.get-dbi this dbi-name false)
                ^Dbi db    (.-db dbi)
                ^Txn txn   (.-txn rtx)
                ^Stat stat (Stat/create txn db)
                m          (stat-map stat)]
            (.close stat)
            m)
          (catch Exception e
            (raise "Fail to get statistics: " e
                   {:dbi dbi-name}))
          (finally (.return-rtx this rtx))))
      (l/stat this)))

  (entries [this dbi-name]
    (assert (not closed?) "LMDB env is closed.")
    (let [^DBI dbi (.get-dbi this dbi-name false)
          ^Rtx rtx (.get-rtx this)]
      (try
        (let [^Dbi db    (.-db dbi)
              ^Txn txn   (.-txn rtx)
              ^Stat stat (Stat/create txn db)
              entries    (.ms_entries ^Lib$MDB_stat (.get stat))]
          (.close stat)
          entries)
        (catch Exception e
          (raise "Fail to get entries: " (ex-message e) {:dbi dbi-name}))
        (finally (.return-rtx this rtx)))))

  (open-transact-kv [this]
    (assert (not closed?) "LMDB env is closed.")
    (try
      (reset-write-txn this)
      (.mark-write this)
      (catch Exception e
        (raise "Fail to open read/write transaction in LMDB: "
               (ex-message e) {}))))

  (close-transact-kv [_]
    (when-let [^Rtx wtxn @write-txn]
      (when-let [^Txn txn (.-txn wtxn)]
        (try
          (let [aborted? @(.-aborted? wtxn)]
            (if aborted? (.close txn) (.commit txn))
            (vreset! write-txn nil)
            (if aborted? :aborted :committed))
          (catch Exception e
            (.close txn)
            (vreset! write-txn nil)
            (raise "Fail to commit read/write transaction in LMDB: "
                   (ex-message e) {}))))))

  (abort-transact-kv [_]
    (when-let [^Rtx wtxn @write-txn]
      (vreset! (.-aborted? wtxn) true)
      (vreset! write-txn wtxn)
      nil))

  (transact-kv [this txs]
    (assert (not closed?) "LMDB env is closed.")
    (locking write-txn
      (let [^Rtx rtx  @write-txn
            one-shot? (nil? rtx)
            ^Txn txn  (if one-shot? (Txn/create env) (.-txn rtx))]
        (try
          (transact* txs dbis txn)
          (when one-shot? (.commit txn))
          :transacted
          (catch Lib$MapFullException _
            (.close txn)
            (let [^Info info (Info/create env)]
              (.setMapSize env (* ^long c/+buffer-grow-factor+
                                  (.me_mapsize ^Lib$MDB_envinfo (.get info))))
              (.close info))
            (if one-shot?
              (.transact-kv this txs)
              (do (reset-write-txn this)
                  (raise "DB needs resize" {:resized true}))))
          (catch Exception e
            (when one-shot? (.close txn))
            (raise "Fail to transact to LMDB: " (ex-message e) {}))))))

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

  (range-seq [this dbi-name k-range]
    (.range-seq this dbi-name k-range :data :data false nil))
  (range-seq [this dbi-name k-range k-type]
    (.range-seq this dbi-name k-range k-type :data false nil))
  (range-seq [this dbi-name k-range k-type v-type]
    (.range-seq this dbi-name k-range k-type v-type false nil))
  (range-seq [this dbi-name k-range k-type v-type ignore-key?]
    (.range-seq this dbi-name k-range k-type v-type ignore-key? nil))
  (range-seq [this dbi-name k-range k-type v-type ignore-key? opts]
    (scan/range-seq this dbi-name k-range k-type v-type ignore-key? opts))

  (range-count [this dbi-name k-range]
    (.range-count this dbi-name k-range :data))
  (range-count [this dbi-name k-range k-type]
    (scan/range-count this dbi-name k-range k-type))

  (get-some [this dbi-name pred k-range]
    (.get-some this dbi-name pred k-range :data :data false))
  (get-some [this dbi-name pred k-range k-type]
    (.get-some this dbi-name pred k-range k-type :data false))
  (get-some [this dbi-name pred k-range k-type v-type]
    (.get-some this dbi-name pred k-range k-type v-type false))
  (get-some [this dbi-name pred k-range k-type v-type ignore-key?]
    (scan/get-some this dbi-name pred k-range k-type v-type ignore-key?))

  (range-filter [this dbi-name pred k-range]
    (.range-filter this dbi-name pred k-range :data :data false))
  (range-filter [this dbi-name pred k-range k-type]
    (.range-filter this dbi-name pred k-range k-type :data false))
  (range-filter [this dbi-name pred k-range k-type v-type]
    (.range-filter this dbi-name pred k-range k-type v-type false))
  (range-filter [this dbi-name pred k-range k-type v-type ignore-key?]
    (scan/range-filter this dbi-name pred k-range k-type v-type ignore-key?))

  (range-filter-count [this dbi-name pred k-range]
    (.range-filter-count this dbi-name pred k-range :data))
  (range-filter-count [this dbi-name pred k-range k-type]
    (scan/range-filter-count this dbi-name pred k-range k-type))

  (visit [this dbi-name visitor k-range]
    (.visit this dbi-name visitor k-range :data))
  (visit [this dbi-name visitor k-range k-type]
    (scan/visit this dbi-name visitor k-range k-type))

  (open-list-dbi [this dbi-name {:keys [key-size val-size]
                                 :or   {key-size c/+max-key-size+
                                        val-size c/+max-key-size+}}]
    (assert (and (>= c/+max-key-size+ ^long key-size)
                 (>= c/+max-key-size+ ^long val-size))
            "Data size cannot be larger than 511 bytes")
    (.open-dbi this dbi-name
               {:key-size key-size :val-size val-size :dupsort? true}))
  (open-list-dbi [lmdb dbi-name]
    (.open-list-dbi lmdb dbi-name nil))

  IList
  (put-list-items [this dbi-name k vs kt vt]
    (.transact-kv this [[:put-list dbi-name k vs kt vt]]))

  (del-list-items [this dbi-name k kt]
    (.transact-kv this [[:del dbi-name k kt]]))
  (del-list-items [this dbi-name k vs kt vt]
    (.transact-kv this [[:del-list dbi-name k vs kt vt]]))

  (get-list [this dbi-name k kt vt]
    (when k
      (let [lmdb this]
        (scan/scan
          (get-list* this rtx cur k kt vt)
          (raise "Fail to get a list: " e {:dbi dbi-name :key k})))))

  (visit-list [this dbi-name visitor k kt]
    (when k
      (let [lmdb this]
        (scan/scan
          (visit-list* rtx cur k kt visitor)
          (raise "Fail to visit list: " e {:dbi dbi-name :k k})))))

  (list-count [this dbi-name k kt]
    (if k
      (let [lmdb this]
        (scan/scan
          (list-count* rtx cur k kt)
          (raise "Fail to count list: " e {:dbi dbi-name :k k})))
      0))

  (in-list? [this dbi-name k v kt vt]
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
    (scan/list-range-count this dbi-name k-range kt v-range vt))

  (list-range-filter [this dbi-name pred k-range kt v-range vt]
    (scan/list-range-filter this dbi-name pred k-range kt v-range vt))

  (list-range-filter-count [this dbi-name pred k-range kt v-range vt]
    (scan/list-range-filter-count this dbi-name pred k-range kt v-range vt))

  (visit-list-range [this dbi-name visitor k-range kt v-range vt]
    (scan/visit-list-range this dbi-name visitor k-range kt v-range vt))
  )

(defn- reset-write-txn
  [^LMDB lmdb]
  (let [kp-w       ^BufVal (.-kp-w lmdb)
        vp-w       ^BufVal (.-vp-w lmdb)
        start-kp-w ^BufVal (.-start-kp-w lmdb)
        stop-kp-w  ^BufVal (.-stop-kp-w lmdb)
        start-vp-w ^BufVal (.-start-vp-w lmdb)
        stop-vp-w  ^BufVal (.-stop-vp-w lmdb)]
    (.clear kp-w)
    (.clear vp-w)
    (.clear start-kp-w)
    (.clear stop-kp-w)
    (.clear start-vp-w)
    (.clear stop-vp-w)
    (vreset! (.-write-txn lmdb) (Rtx. lmdb
                                      (Txn/create (.-env lmdb))
                                      kp-w
                                      vp-w
                                      start-kp-w
                                      stop-kp-w
                                      start-vp-w
                                      stop-vp-w
                                      (volatile! false)))))

(defmethod open-kv :graal
  ([dir]
   (open-kv dir {}))
  ([dir {:keys [mapsize flags temp?]
         :or   {mapsize c/+init-db-size+
                flags   c/default-env-flags
                temp?   false}
         :as   opts}]
   (try
     (let [file     (u/file dir)
           ^Env env (Env/create
                      dir
                      (* ^long mapsize 1024 1024)
                      c/+max-readers+
                      c/+max-dbs+
                      (kv-flags flags))
           lmdb     (->LMDB env
                            dir
                            temp?
                            opts
                            (ConcurrentLinkedQueue.)
                            (ConcurrentHashMap.)
                            false
                            (BufVal/create c/+max-key-size+)
                            (BufVal/create 0)
                            (BufVal/create c/+max-key-size+)
                            (BufVal/create c/+max-key-size+)
                            (BufVal/create c/+max-key-size+)
                            (BufVal/create c/+max-key-size+)
                            (volatile! nil)
                            false)]
       (when temp? (u/delete-on-exit file))
       lmdb)
     (catch Exception e
       (raise "Fail to open database: " e {:dir dir})))))
