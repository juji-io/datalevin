(ns ^:no-doc datalevin.binding.graal
  "LMDB binding for GraalVM native image"
  (:require
   [datalevin.bits :as b]
   [datalevin.util :refer [raise] :as u]
   [datalevin.constants :as c]
   [datalevin.scan :as scan]
   [datalevin.lmdb :as l :refer [open-kv open-inverted-list IBuffer IRange
                                 IRtx IDB IKV IInvertedList ILMDB IWriting]]
   [clojure.stacktrace :as st])
  (:import
   [java.util Iterator]
   [java.util.concurrent ConcurrentHashMap ConcurrentLinkedQueue]
   [java.nio ByteBuffer BufferOverflowException]
   [java.lang AutoCloseable]
   [java.io File]
   [java.lang.annotation Retention RetentionPolicy]
   [clojure.lang IPersistentVector]
   [org.graalvm.word WordFactory]
   [datalevin.ni BufVal Lib Env Txn Dbi Cursor Stat Info
    Lib$BadReaderLockException Lib$MDB_cursor_op Lib$MDB_envinfo Lib$MDB_stat
    Lib$MapFullException])
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
              (map #(value ^Flag flag %) flags)))
    (int 0)))

(deftype Rtx [lmdb
         ^Txn txn
         ^BufVal kp
         ^BufVal vp
         ^BufVal start-kp
         ^BufVal stop-kp
         aborted?]

  IBuffer
  (put-key [_ x t]
    (try
      (let [^ByteBuffer kb (.inBuf kp)]
        (.clear ^BufVal kp)
        (b/put-buffer kb x t)
        (.flip ^BufVal kp))
      (catch Exception e
        (raise "Error putting read-only transaction key buffer: "
               (ex-message e)
               {:value x :type t}))))
  (put-val [_ x t]
    (raise "put-val not allowed for read only txn buffer" {}))

  IRange
  (range-info [this range-type k1 k2]
    (let [chk1 #(if k1
                  %1
                  (raise "Missing start/end key for range type " %2 {}))
          chk2 #(if (and k1 k2)
                  %1
                  (raise "Missing start/end key for range type " %2 {}))
          v1   (.-start-kp this)
          v2   (.-stop-kp this)]
      (case range-type
        :all               [true false false false false nil nil]
        :all-back          [false false false false false nil nil]
        :at-least          (chk1 [true true true false false v1 nil] :at-least)
        :at-most-back      (chk1 [false true true false false v1 nil]
                                 :at-most-back)
        :at-most           (chk1 [true false false true true nil v1] :at-most)
        :at-least-back     (chk1 [false false false true true nil v1]
                                 :at-least-back)
        :closed            (chk2 [true true true true true v1 v2] :closed)
        :closed-back       (chk2 [false true true true true v1 v2] :closed-back)
        :closed-open       (chk2 [true true true true false v1 v2] :closed-open)
        :closed-open-back  (chk2 [false true true true false v1 v2]
                                 :closed-open-back)
        :greater-than      (chk1 [true true false false false v1 nil]
                                 :greater-than)
        :less-than-back    (chk1 [false true false false false v1 nil]
                                 :less-than-back)
        :less-than         (chk1 [true false false true false nil v1] :less-than)
        :greater-than-back (chk1 [false false false true false nil v1]
                                 :greater-than-back)
        :open              (chk2 [true true false true false v1 v2] :open)
        :open-back         (chk2 [false true false true false v1 v2] :open-back)
        :open-closed       (chk2 [true true false true true v1 v2] :open-closed)
        :open-closed-back  (chk2 [false true false true true v1 v2]
                                 :open-closed-back)
        (raise "Unknown range type" range-type {}))))
  (put-start-key [_ x t]
    (try
      (when x
        (let [^ByteBuffer start-kb (.inBuf start-kp)]
          (.clear ^BufVal start-kp)
          (b/put-buffer start-kb x t)
          (.flip ^BufVal start-kp)))
      (catch Exception e
        (raise "Error putting read-only transaction start key buffer: "
               (ex-message e)
               {:value x :type t}))))
  (put-stop-key [_ x t]
    (try
      (when x
        (let [^ByteBuffer stop-kb (.inBuf stop-kp)]
          (.clear ^BufVal stop-kp)
          (b/put-buffer stop-kb x t)
          (.flip ^BufVal stop-kp)))
      (catch Exception e
        (raise "Error putting read-only transaction stop key buffer: "
               (ex-message e)
               {:value x :type t}))))

  IRtx
  (close-rtx [_]
    (.close txn)
    (.close kp)
    (.close vp)
    (.close start-kp)
    (.close stop-kp))
  (reset [this]
    (.reset txn)
    this)
  (renew [this]
    (.renew txn)
    this))

(deftype KV [^BufVal kp ^BufVal vp]
  IKV
  (k [this] (.outBuf kp))
  (v [this] (.outBuf vp)))

(defn has?
  [rc]
  (if (= ^int rc (Lib/MDB_NOTFOUND))
    false
    (do (Lib/checkRc ^int rc) true)))

(declare ->CursorIterable)

(deftype DBI [^Dbi db
              ^ConcurrentLinkedQueue curs
              ^BufVal kp
              ^:volatile-mutable ^BufVal vp
              ^boolean validate-data?]
  IBuffer
  (put-key [this x t]
    (or (not validate-data?)
        (b/valid-data? x t)
        (raise "Invalid data, expecting " t {:input x}))
    (let [^ByteBuffer kb (.inBuf kp)
          dbi-name       (.dbi-name this)]
      (try
        (.clear ^BufVal kp)
        (b/put-buffer kb x t)
        (.flip ^BufVal kp)
        (catch Exception e
          (raise "Error putting r/w key buffer of "
                 dbi-name ": " (ex-message e)
                 {:value x :type t :dbi dbi-name})))))
  (put-val [this x t]
    (or (not validate-data?)
        (b/valid-data? x t)
        (raise "Invalid data, expecting " t {:input x}))
    (let [^ByteBuffer vb (.inBuf vp)
          dbi-name       (.dbi-name this)]
      (try
        (.clear ^BufVal vp)
        (b/put-buffer vb x t)
        (.flip ^BufVal vp)
        (catch BufferOverflowException _
          (let [size (* ^long c/+buffer-grow-factor+ ^long (b/measure-size x))]
            (.close vp)
            (set! vp (BufVal/create size))
            (let [^ByteBuffer vb (.inBuf vp)]
              (b/put-buffer vb x t)
              (.flip ^BufVal vp))))
        (catch Exception e
          ;; (st/print-stack-trace e)
          (raise "Error putting r/w value buffer of "
                 dbi-name ": " (ex-message e)
                 {:value x :type t :dbi dbi-name})))))

  IDB
  (dbi [_]
    db)

  (dbi-name [_]
    (.getName db))

  (put [this txn]
    (.put this txn nil))
  (put [_ txn flags]
    (let [i (.get db)]
      (Lib/checkRc
        (Lib/mdb_put (.get ^Txn txn) i (.getVal kp) (.getVal vp)
                     (kv-flags flags)))))

  (del [_ txn all?]
    (Lib/checkRc
      (Lib/mdb_del (.get ^Txn txn) (.get db) (.getVal kp)
                   (if all? (WordFactory/nullPointer) (.getVal vp)))))
  (del [this txn]
    (.del this txn true))

  (get-kv [_ rtx]
    (let [^BufVal kp (.-kp ^Rtx rtx)
          ^BufVal vp (.-vp ^Rtx rtx)
          rc         (Lib/mdb_get (.get ^Txn (.-txn ^Rtx rtx))
                                  (.get db) (.getVal kp) (.getVal vp))]
      (Lib/checkRc rc)
      (when-not (= rc (Lib/MDB_NOTFOUND))
        (.outBuf vp))))

  (iterate-kv [this rtx [f? sk? is? ek? ie? sk ek]]
    (let [txn (.-txn ^Rtx rtx)
          cur (.get-cursor this txn)]
      (->CursorIterable cur this rtx f? sk? is? ek? ie? sk ek)))

  (get-cursor [_ txn]
    (or (when (.isReadOnly ^Txn txn)
          (when-let [^Cursor cur (.poll curs)]
            (.renew cur txn)
            cur))
        (Cursor/create txn db)))

  (return-cursor [_ cur]
    (.add curs cur)))

(deftype CursorIterable [^Cursor cursor
                    ^DBI db
                    ^Rtx rtx
                    forward?
                    start-key?
                    include-start?
                    stop-key?
                    include-stop?
                    ^BufVal sk
                    ^BufVal ek]
  AutoCloseable
  (close [_]
    (if (.isReadOnly ^Txn (.-txn rtx))
      (.return-cursor db cursor)
      (.close cursor)))

  Iterable
  (iterator [this]
    (let [started?     (volatile! false)
          ended?       (volatile! false)
          ^BufVal k    (.-kp rtx)
          ^BufVal v    (.-vp rtx)
          ^Txn txn     (.-txn rtx)
          ^Dbi dbi     (.dbi db)
          i            (.get dbi)
          op-get       Lib$MDB_cursor_op/MDB_GET_CURRENT
          op-next      Lib$MDB_cursor_op/MDB_NEXT
          op-prev      Lib$MDB_cursor_op/MDB_PREV
          op-set-range Lib$MDB_cursor_op/MDB_SET_RANGE
          op-set       Lib$MDB_cursor_op/MDB_SET
          op-first     Lib$MDB_cursor_op/MDB_FIRST
          op-last      Lib$MDB_cursor_op/MDB_LAST
          get-cur      #(Lib/checkRc
                          (Lib/mdb_cursor_get
                            (.get cursor) (.getVal k) (.getVal v) op-get))]
      (reify
        Iterator
        (hasNext [_]
          (let [end       #(do (vreset! ended? true) false)
                continue? #(if stop-key?
                             (let [_ (get-cur)
                                   r (Lib/mdb_cmp (.get txn) i (.getVal k)
                                                  (.getVal ek))]
                               (if (= r 0)
                                 (do (vreset! ended? true)
                                     include-stop?)
                                 (if (> r 0)
                                   (if forward? (end) true)
                                   (if forward? true (end)))))
                             true)
                check     #(if (has?
                                 (Lib/mdb_cursor_get
                                   (.get cursor) (.getVal k) (.getVal v) %))
                             (continue?)
                             false)]
            (if @ended?
              false
              (if @started?
                (if forward?
                  (check op-next)
                  (check op-prev))
                (do
                  (vreset! started? true)
                  (if start-key?
                    (if (has? (Lib/mdb_cursor_get
                                (.get cursor) (.getVal sk) (.getVal v) op-set))
                      (if include-start?
                        (continue?)
                        (if forward?
                          (check op-next)
                          (check op-prev)))
                      (if (has? (Lib/mdb_cursor_get
                                  (.get cursor) (.getVal sk) (.getVal v)
                                  op-set-range))
                        (if forward?
                          (continue?)
                          (check op-prev))
                        (if forward?
                          false
                          (check op-last))))
                    (if forward?
                      (check op-first)
                      (check op-last))))))))
        (next [_]
          (get-cur)
          (->KV k v))))))

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
               write-txn
               writing?]

  IWriting
  (writing? [_] writing?)

  (write-txn [_] write-txn)

  (mark-write [_]
    (->LMDB
      env dir temp? opts pool dbis closed? kp-w vp-w start-kp-w stop-kp-w
      write-txn true))

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
  (open-dbi [_ dbi-name {:keys [key-size val-size flags validate-data?]
                         :or   {key-size       c/+max-key-size+
                                val-size       c/+default-val-size+
                                flags          c/default-dbi-flags
                                validate-data? false}}]
    (assert (not closed?) "LMDB env is closed.")
    (assert (< ^long key-size 512) "Key size cannot be greater than 511 bytes")
    (let [kp  (BufVal/create key-size)
          vp  (BufVal/create val-size)
          dbi (Dbi/create env dbi-name (kv-flags flags))
          db  (DBI. dbi (ConcurrentLinkedQueue.) kp vp validate-data?)]
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
        (raise "Fail to clear DBI: " dbi-name " " (ex-message e) {}))))

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
        (raise "Fail to drop DBI: " dbi-name (ex-message e) {}))))

  (get-rtx [this]
    (try
      (or (when-let [^Rtx rtx (.poll pool)]
            (.renew rtx))
          (Rtx. this
                (Txn/createReadOnly env)
                (BufVal/create c/+max-key-size+)
                (BufVal/create 1)
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
                           false)
          ^Rtx rtx  (.get-rtx this)]
      (try
        (with-open [^Cursor cursor (.get-cursor dbi (.-txn rtx))]
          (with-open [^AutoCloseable iterable (->CursorIterable
                                                cursor dbi rtx true false false
                                                false false nil nil)]
            (loop [^Iterator iter (.iterator ^Iterable iterable)
                   holder         (transient [])]
              (if (.hasNext iter)
                (let [kv      (.next iter)
                      holder' (conj! holder
                                     (-> kv l/k b/get-bytes b/text-ba->str))]
                  (recur iter holder'))
                (persistent! holder)))))
        (catch Exception e
          (raise "Fail to list DBIs: " (ex-message e) {}))
        (finally (.return-rtx this rtx)))))

  (copy [this dest]
    (.copy this dest false))
  (copy [this dest compact?]
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
        (raise "Fail to get statistics: " (ex-message e) {}))))
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
            (raise "Fail to get statistics: " (ex-message e)
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

  (close-transact-kv [this]
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

  (abort-transact-kv [this]
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
            (if @write-txn
              (do (reset-write-txn this)
                  (raise "DB needs resize" {:resized true}))
              (.transact-kv this txs)))
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

  (open-inverted-list [this dbi-name {:keys [key-size val-size]
                                      :or   {key-size c/+max-key-size+
                                             val-size c/+max-key-size+}}]
    (assert (and (>= c/+max-key-size+ ^long key-size)
                 (>= c/+max-key-size+ ^long val-size))
            "Data size cannot be larger than 511 bytes")
    (.open-dbi this dbi-name {:key-size key-size :val-size val-size
                              :flags    (conj c/default-dbi-flags :dupsort)}))
  (open-inverted-list [lmdb dbi-name]
    (.open-inverted-list lmdb dbi-name nil))

  IInvertedList
  (put-list-items [this dbi-name k vs kt vt]
    (try
      (let [^DBI dbi (.get-dbi this dbi-name false)
            ^Txn txn (Txn/create env)]
        (.put-key dbi k kt)
        (doseq [v vs]
          (.put-val dbi v vt)
          (.put dbi txn))
        (.commit txn)
        :transacted)
      (catch Exception e
        (raise "Fail to put an inverted list: " (ex-message e) {}))))

  (del-list-items [this dbi-name k kt]
    (try
      (let [^DBI dbi (.get-dbi this dbi-name false)
            ^Txn txn (Txn/create env)]
        (.put-key dbi k kt)
        (.del dbi txn)
        (.commit txn)
        :transacted)
      (catch Exception e
        (raise "Fail to delete an inverted list: " (ex-message e) {}))))
  (del-list-items [this dbi-name k vs kt vt]
    (try
      (let [^DBI dbi (.get-dbi this dbi-name false)
            ^Txn txn (Txn/create env)]
        (.put-key dbi k kt)
        (doseq [v vs]
          (.put-val dbi v vt)
          (.del dbi txn false))
        (.commit txn)
        :transacted)
      (catch Exception e
        (raise "Fail to delete items from an inverted list: "
               (ex-message e) {}))))

  (get-list [this dbi-name k kt vt]
    (when k
      (let [^DBI dbi    (.get-dbi this dbi-name false)
            ^Rtx rtx    (.get-rtx this)
            txn         (.-txn rtx)
            ^Cursor cur (.get-cursor dbi txn) ]
        (try
          (.put-start-key rtx k kt)
          (let [rc (Lib/mdb_cursor_get
                     (.get cur)
                     (.getVal ^BufVal (.-start-kp rtx))
                     (.getVal ^BufVal (.-stop-kp rtx))
                     Lib$MDB_cursor_op/MDB_SET)]
            (when (has? rc)
              (let [^BufVal k (.-kp rtx)
                    ^BufVal v (.-vp rtx)
                    holder    (transient [])]
                (Lib/mdb_cursor_get (.get cur) (.getVal k) (.getVal v)
                                    Lib$MDB_cursor_op/MDB_FIRST_DUP)
                (conj! holder (b/read-buffer (.outBuf v) vt))
                (dotimes [_ (dec (.count cur))]
                  (Lib/mdb_cursor_get (.get cur) (.getVal k) (.getVal v)
                                      Lib$MDB_cursor_op/MDB_NEXT_DUP)
                  (conj! holder (b/read-buffer (.outBuf v) vt)))
                (persistent! holder))))
          (catch Exception e
            (raise "Fail to get inverted list: " (ex-message e)
                   {:dbi dbi-name}))
          (finally (.return-rtx this rtx)
                   (.return-cursor dbi cur))))))

  (visit-list [this dbi-name visitor k kt]
    (when k
      (let [^DBI dbi    (.get-dbi this dbi-name false)
            ^Rtx rtx    (.get-rtx this)
            txn         (.-txn rtx)
            ^Cursor cur (.get-cursor dbi txn)]
        (try
          (.put-start-key rtx k kt)
          (let [rc (Lib/mdb_cursor_get
                     (.get cur)
                     (.getVal ^BufVal (.-start-kp rtx))
                     (.getVal ^BufVal (.-stop-kp rtx))
                     Lib$MDB_cursor_op/MDB_SET)]
            (when (has? rc)
              (let [k ^BufVal (.-kp rtx)
                    v ^BufVal (.-vp rtx)]
                (Lib/mdb_cursor_get (.get cur) (.getVal k) (.getVal v)
                                    Lib$MDB_cursor_op/MDB_FIRST_DUP)
                (visitor (->KV k v))
                (dotimes [_ (dec (.count cur))]
                  (Lib/mdb_cursor_get (.get cur) (.getVal k) (.getVal v)
                                      Lib$MDB_cursor_op/MDB_NEXT_DUP)
                  (visitor (->KV k v))))))
          (catch Exception e
            (raise "Fail to visit inverted list: " (ex-message e)
                   {:dbi dbi-name}))
          (finally (.return-rtx this rtx)
                   (.return-cursor dbi cur))))))

  (list-count [this dbi-name k kt]
    (if k
      (let [^DBI dbi    (.get-dbi this dbi-name false)
            ^Rtx rtx    (.get-rtx this)
            txn         (.-txn rtx)
            ^Cursor cur (.get-cursor dbi txn)]
        (try
          (.put-start-key rtx k kt)
          (let [rc (Lib/mdb_cursor_get
                     (.get cur)
                     (.getVal ^BufVal (.-start-kp rtx))
                     (.getVal ^BufVal (.-stop-kp rtx))
                     Lib$MDB_cursor_op/MDB_SET)]
            (if (has? rc)
              (.count cur)
              0))
          (catch Exception e
            (raise "Fail to get count of inverted list: " (ex-message e)
                   {:dbi dbi-name}))
          (finally (.return-rtx this rtx)
                   (.return-cursor dbi cur))))
      0))

  (filter-list [this dbi-name k pred kt vt]
    (let [^DBI dbi    (.get-dbi this dbi-name false)
          ^Rtx rtx    (.get-rtx this)
          txn         (.-txn rtx)
          ^Cursor cur (.get-cursor dbi txn)]
      (try
        (.put-start-key rtx k kt)
        (let [rc (Lib/mdb_cursor_get
                   (.get cur)
                   (.getVal ^BufVal (.-start-kp rtx))
                   (.getVal ^BufVal (.-stop-kp rtx))
                   Lib$MDB_cursor_op/MDB_SET)]
          (if (has? rc)
            (let [k      ^BufVal (.-kp rtx)
                  v      ^BufVal (.-vp rtx)
                  holder (transient [])
                  work   #(let [kv (->KV k v)]
                            (when (pred kv)
                              (conj! holder (b/read-buffer (.outBuf v) vt))))]
              (Lib/mdb_cursor_get (.get cur) (.getVal k) (.getVal v)
                                  Lib$MDB_cursor_op/MDB_FIRST_DUP)
              (work)
              (dotimes [_ (dec (.count cur))]
                (Lib/mdb_cursor_get (.get cur) (.getVal k) (.getVal v)
                                    Lib$MDB_cursor_op/MDB_NEXT_DUP)
                (work))
              (persistent! holder))
            []))
        (catch Exception e
          (raise "Fail to get count of inverted list: " (ex-message e)
                 {:dbi dbi-name}))
        (finally (.return-rtx this rtx)
                 (.return-cursor dbi cur)))))

  (filter-list-count [this dbi-name k pred kt]
    (let [^DBI dbi    (.get-dbi this dbi-name false)
          ^Rtx rtx    (.get-rtx this)
          txn         (.-txn rtx)
          ^Cursor cur (.get-cursor dbi txn)]
      (try
        (.put-start-key rtx k kt)
        (let [rc (Lib/mdb_cursor_get
                   (.get cur)
                   (.getVal ^BufVal (.-start-kp rtx))
                   (.getVal ^BufVal (.-stop-kp rtx))
                   Lib$MDB_cursor_op/MDB_SET)]
          (if (has? rc)
            (let [k    ^BufVal (.-kp rtx)
                  v    ^BufVal (.-vp rtx)
                  c    (volatile! 0)
                  work #(when (pred (->KV k v))
                          (vreset! c (inc ^long @c)))]
              (Lib/mdb_cursor_get (.get cur) (.getVal k) (.getVal v)
                                  Lib$MDB_cursor_op/MDB_FIRST_DUP)
              (work)
              (dotimes [_ (dec (.count cur))]
                (Lib/mdb_cursor_get (.get cur) (.getVal k) (.getVal v)
                                    Lib$MDB_cursor_op/MDB_NEXT_DUP)
                (work))
              @c)
            0))
        (catch Exception e
          (raise "Fail to get count of inverted list: " (ex-message e)
                 {:dbi dbi-name}))
        (finally (.return-rtx this rtx)
                 (.return-cursor dbi cur)))))

  (in-list? [this dbi-name k v kt vt]
    (if (and k v)
      (let [^DBI dbi    (.get-dbi this dbi-name false)
            ^Rtx rtx    (.get-rtx this)
            txn         (.-txn rtx)
            ^Cursor cur (.get-cursor dbi txn)]
        (try
          (.put-start-key rtx k kt)
          (.put-stop-key rtx v vt)
          (has? (Lib/mdb_cursor_get
                  (.get cur)
                  (.getVal ^BufVal (.-start-kp rtx))
                  (.getVal ^BufVal (.-stop-kp rtx))
                  Lib$MDB_cursor_op/MDB_GET_BOTH))
          (catch Exception e
            (raise "Fail to test if an item is in an inverted list: "
                   (ex-message e) {:dbi dbi-name}))
          (finally (.return-rtx this rtx)
                   (.return-cursor dbi cur))))
      false)))

(defn- reset-write-txn
  [^LMDB lmdb]
  (let [kp-w       ^BufVal (.-kp-w lmdb)
        start-kp-w ^BufVal (.-start-kp-w lmdb)
        stop-kp-w  ^BufVal (.-stop-kp-w lmdb)]
    (.clear kp-w)
    (.clear start-kp-w)
    (.clear stop-kp-w)
    (vreset! (.-write-txn lmdb) (Rtx. lmdb
                                      (Txn/create (.-env lmdb))
                                      kp-w
                                      (.-vp-w lmdb)
                                      start-kp-w
                                      stop-kp-w
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
                            (BufVal/create 1)
                            (BufVal/create c/+max-key-size+)
                            (BufVal/create c/+max-key-size+)
                            (volatile! nil)
                            false)]
       (when temp? (u/delete-on-exit file))
       lmdb)
     (catch Exception e
       (raise
         "Fail to open database: " (ex-message e)
         {:dir dir})))))
