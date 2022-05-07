(ns datalevin.binding.graal
  "LMDB binding for GraalVM native image"
  (:require [datalevin.bits :as b]
            [datalevin.util :refer [raise] :as u]
            [datalevin.constants :as c]
            [datalevin.scan :as scan]
            [datalevin.lmdb :as l
             :refer [open-kv open-list IBuffer IRange IRtx
                     IDB IKV IInvertedList ILMDB]])
  (:import [java.util Iterator]
           [java.util.concurrent ConcurrentHashMap ConcurrentLinkedQueue]
           [java.nio ByteBuffer BufferOverflowException]
           [java.lang AutoCloseable]
           [java.lang.annotation Retention RetentionPolicy]
           [org.graalvm.nativeimage.c CContext]
           [org.graalvm.word WordFactory]
           [datalevin.ni BufVal Lib Env Txn Dbi Cursor Stat Info
            Lib$Directives Lib$BadReaderLockException
            Lib$MDB_cursor_op Lib$MDB_envinfo Lib$MDB_stat
            Lib$MapFullException]))

(defprotocol IFlag
  (value [this flag-key]))

(deftype ^{Retention RetentionPolicy/RUNTIME
           CContext  {:value Lib$Directives}}
    Flag []
  IFlag
  (value [_ flag-key]
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
    (let [flag (->Flag)]
      (reduce (fn [r f] (bit-or ^int r ^int f))
              0
              (map #(value flag %) flags)))
    (int 0)))

(deftype ^{Retention RetentionPolicy/RUNTIME
           CContext  {:value Lib$Directives}}
    Rtx [lmdb
         ^Txn txn
         ^BufVal kp
         ^BufVal vp
         ^BufVal start-kp
         ^BufVal stop-kp]

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

(deftype ^{Retention RetentionPolicy/RUNTIME
           CContext  {:value Lib$Directives}}
    KV [^BufVal kp ^BufVal vp]
  IKV
  (k [this] (.outBuf kp))
  (v [this] (.outBuf vp)))

(defn has?
  [rc]
  (if (= ^int rc (Lib/MDB_NOTFOUND))
    false
    (do (Lib/checkRc ^int rc) true)))

(declare ->CursorIterable)

(deftype ^{Retention RetentionPolicy/RUNTIME
           CContext  {:value Lib$Directives}}
    DBI [^Dbi db
         ^ConcurrentLinkedQueue curs
         ^BufVal kp
         ^:volatile-mutable ^BufVal vp
         dupsort?]
  IBuffer
  (put-key [this x t]
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

(deftype ^{Retention RetentionPolicy/RUNTIME
           CContext  {:value Lib$Directives}}
    CursorIterable [^Cursor cursor
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
    (let [started?      (volatile! false)
          ended?        (volatile! false)
          ^BufVal k     (.-kp rtx)
          ^BufVal v     (.-vp rtx)
          ^Txn txn      (.-txn rtx)
          dupsort?      (.-dupsort? db)
          ^Dbi dbi      (.dbi db)
          i             (.get dbi)
          op-get        Lib$MDB_cursor_op/MDB_GET_CURRENT
          op-next       Lib$MDB_cursor_op/MDB_NEXT
          op-prev       Lib$MDB_cursor_op/MDB_PREV
          op-set-range  Lib$MDB_cursor_op/MDB_SET_RANGE
          op-set        Lib$MDB_cursor_op/MDB_SET
          op-first      Lib$MDB_cursor_op/MDB_FIRST
          op-last       Lib$MDB_cursor_op/MDB_LAST
          op-first-dup  Lib$MDB_cursor_op/MDB_FIRST_DUP
          op-last-dup   Lib$MDB_cursor_op/MDB_LAST_DUP
          op-next-dup   Lib$MDB_cursor_op/MDB_NEXT_DUP
          op-prev-dup   Lib$MDB_cursor_op/MDB_PREV_DUP
          op-next-nodup Lib$MDB_cursor_op/MDB_NEXT_NODUP
          op-prev-nodup Lib$MDB_cursor_op/MDB_PREV_NODUP
          get-cur       #(Lib/checkRc
                           (Lib/mdb_cursor_get
                             (.get cursor) (.getVal k) (.getVal v) op-get))]
      (reify
        Iterator
        (hasNext [this]
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
                             false)
                check-dup #(if (has?
                                 (Lib/mdb_cursor_get
                                   (.get cursor) (.getVal k) (.getVal v) %))
                             true
                             (if @ended?
                               false
                               (if forward?
                                 (check op-next-nodup)
                                 (check op-prev-nodup))))
                seek      #(if (has? (Lib/mdb_cursor_get
                                       (.get cursor) (.getVal sk) (.getVal v)
                                       op-set-range))
                             (if forward? (continue?) (check op-prev))
                             (if forward? false (check op-last)))
                start     #(if forward? (check op-first) (check op-last))]
            (if dupsort?
              (if @ended?
                (if forward? (check-dup op-next-dup) (check-dup op-prev-dup))
                (if @started?
                  (if forward? (check-dup op-next-dup) (check-dup op-prev-dup))
                  (do
                    (vreset! started? true)
                    (if start-key?
                      (if (has?
                            (Lib/mdb_cursor_get
                              (.get cursor) (.getVal sk) (.getVal v) op-set))
                        (if include-start?
                          (if forward?
                            (check-dup op-first-dup)
                            (check-dup op-last-dup))
                          (if forward?
                            (check-dup op-next-nodup)
                            (check-dup op-prev-nodup)))
                        (seek))
                      (start)))))
              (if @ended?
                false
                (if @started?
                  (if forward? (check op-next) (check op-prev))
                  (do
                    (vreset! started? true)
                    (if start-key?
                      (if (has?
                            (Lib/mdb_cursor_get
                              (.get cursor) (.getVal sk) (.getVal v) op-set))
                        (if include-start?
                          (continue?)
                          (if forward? (check op-next) (check op-prev)))
                        (seek))
                      (start))))))))
        (next [this]
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
  (doseq [[op dbi-name k & r] txs
          :when               op
          :let                [^DBI dbi (or (.get dbis dbi-name)
                                            (raise dbi-name " is not open"
                                                   {}))]]
    (case op
      :put      (let [[v kt vt flags] r]
                  (.put-key dbi k kt)
                  (.put-val dbi v vt)
                  (if flags
                    (.put dbi txn flags)
                    (.put dbi txn)))
      :del      (let [[kt] r]
                  (.put-key dbi k kt)
                  (.del dbi txn))
      :put-list (let [[vs kt vt] r]
                  (.put-key dbi k kt)
                  (doseq [v vs]
                    (.put-val dbi v vt)
                    (.put dbi txn)))
      :del-list (let [[vs kt vt] r]
                  (.put-key dbi k kt)
                  (doseq [v vs]
                    (.put-val dbi v vt)
                    (.del dbi txn false)))
      (raise "Unknown kv operator: " op {}))))

(deftype ^{Retention RetentionPolicy/RUNTIME
           CContext  {:value Lib$Directives}}
    LMDB [^Env env
          ^String dir
          ^ConcurrentLinkedQueue pool
          ^ConcurrentHashMap dbis
          ^:volatile-mutable closed?
          write-txn]
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
      nil))

  (closed-kv? [_]
    closed?)

  (dir [_]
    dir)

  (open-dbi [this dbi-name]
    (.open-dbi
      this dbi-name c/+max-key-size+ c/+default-val-size+ c/default-dbi-flags))
  (open-dbi [this dbi-name key-size]
    (.open-dbi this dbi-name key-size c/+default-val-size+ c/default-dbi-flags))
  (open-dbi [this dbi-name key-size val-size]
    (.open-dbi this dbi-name key-size val-size c/default-dbi-flags))
  (open-dbi [_ dbi-name key-size val-size flags]
    (assert (not closed?) "LMDB env is closed.")
    (let [kp  (BufVal/create key-size)
          vp  (BufVal/create val-size)
          dbi (Dbi/create env dbi-name (kv-flags flags))
          db  (->DBI dbi (ConcurrentLinkedQueue.) kp vp
                     (some #{:dupsort} flags))]
      (.put dbis dbi-name db)
      db))

  (get-dbi [this dbi-name]
    (.get-dbi this dbi-name true))
  (get-dbi [this dbi-name create?]
    (or (.get dbis dbi-name)
        (if create?
          (.open-dbi this dbi-name)
          (.open-dbi this dbi-name c/+max-key-size+ c/+default-val-size+
                     c/read-dbi-flags))))

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
          (->Rtx this
                 (Txn/createReadOnly env)
                 (BufVal/create c/+max-key-size+)
                 (BufVal/create 1)
                 (BufVal/create c/+max-key-size+)
                 (BufVal/create c/+max-key-size+)))
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
                (recur iter
                       (conj! holder
                              (-> (.next iter) l/k b/get-bytes b/text-ba->str)))
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
      (vreset! write-txn (->Rtx this
                                (Txn/create env)
                                (BufVal/create c/+max-key-size+)
                                (BufVal/create 1)
                                (BufVal/create c/+max-key-size+)
                                (BufVal/create c/+max-key-size+)))
      (catch Exception e
        (raise "Fail to open read/write transaction in LMDB: "
               (ex-message e) {}))))

  (close-transact-kv [this]
    (when-let [^Rtx wtxn @write-txn]
      (when-let [^Txn txn (.-txn wtxn)]
        (try
          (.commit txn)
          (vreset! write-txn nil)
          :committed
          (catch Exception e
            (.close txn)
            (vreset! write-txn nil)
            (raise "Fail to commit read/write transaction in LMDB: "
                   (ex-message e) {}))))))

  (write-txn [this]
    write-txn)

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
              (raise "Map is resized" {:resized true})
              (.transact-kv this txs)))
          (catch Exception e
            (when one-shot? (.close txn))
            (raise "Fail to transact to LMDB: " (ex-message e) {}))))))

  (get-value [this dbi-name k]
    (.get-value this dbi-name k :data :data true false))
  (get-value [this dbi-name k k-type]
    (.get-value this dbi-name k k-type :data true false))
  (get-value [this dbi-name k k-type v-type]
    (.get-value this dbi-name k k-type v-type true false))
  (get-value [this dbi-name k k-type v-type ignore-key?]
    (.get-value this dbi-name k k-type v-type ignore-key? false))
  (get-value [this dbi-name k k-type v-type ignore-key? writing?]
    (scan/get-value this dbi-name k k-type v-type ignore-key? writing?))

  (get-first [this dbi-name k-range]
    (.get-first this dbi-name k-range :data :data false false))
  (get-first [this dbi-name k-range k-type]
    (.get-first this dbi-name k-range k-type :data false false))
  (get-first [this dbi-name k-range k-type v-type]
    (.get-first this dbi-name k-range k-type v-type false false))
  (get-first [this dbi-name k-range k-type v-type ignore-key?]
    (.get-first this dbi-name k-range k-type v-type ignore-key? false))
  (get-first [this dbi-name k-range k-type v-type ignore-key? writing?]
    (scan/get-first this dbi-name k-range k-type v-type ignore-key? writing?))

  (get-range [this dbi-name k-range]
    (.get-range this dbi-name k-range :data :data false false))
  (get-range [this dbi-name k-range k-type]
    (.get-range this dbi-name k-range k-type :data false false))
  (get-range [this dbi-name k-range k-type v-type]
    (.get-range this dbi-name k-range k-type v-type false false))
  (get-range [this dbi-name k-range k-type v-type ignore-key?]
    (.get-range this dbi-name k-range k-type v-type ignore-key? false))
  (get-range [this dbi-name k-range k-type v-type ignore-key? writing?]
    (scan/get-range this dbi-name k-range k-type v-type ignore-key? writing?))

  (range-count [this dbi-name k-range]
    (.range-count this dbi-name k-range :data false))
  (range-count [this dbi-name k-range k-type]
    (.range-count this dbi-name k-range k-type false))
  (range-count [this dbi-name k-range k-type writing?]
    (scan/range-count this dbi-name k-range k-type writing?))

  (get-some [this dbi-name pred k-range]
    (.get-some this dbi-name pred k-range :data :data false false))
  (get-some [this dbi-name pred k-range k-type]
    (.get-some this dbi-name pred k-range k-type :data false false))
  (get-some [this dbi-name pred k-range k-type v-type]
    (.get-some this dbi-name pred k-range k-type v-type false false))
  (get-some [this dbi-name pred k-range k-type v-type ignore-key?]
    (.get-some this dbi-name pred k-range k-type v-type ignore-key? false))
  (get-some [this dbi-name pred k-range k-type v-type ignore-key? writing?]
    (scan/get-some this dbi-name pred k-range k-type v-type ignore-key? writing?))

  (range-filter [this dbi-name pred k-range]
    (.range-filter this dbi-name pred k-range :data :data false false))
  (range-filter [this dbi-name pred k-range k-type]
    (.range-filter this dbi-name pred k-range k-type :data false false))
  (range-filter [this dbi-name pred k-range k-type v-type]
    (.range-filter this dbi-name pred k-range k-type v-type false false))
  (range-filter [this dbi-name pred k-range k-type v-type ignore-key?]
    (.range-filter this dbi-name pred k-range k-type v-type ignore-key? false))
  (range-filter [this dbi-name pred k-range k-type v-type ignore-key? writing?]
    (scan/range-filter this dbi-name pred k-range k-type v-type ignore-key? writing?))

  (range-filter-count [this dbi-name pred k-range]
    (.range-filter-count this dbi-name pred k-range :data false))
  (range-filter-count [this dbi-name pred k-range k-type]
    (.range-filter-count this dbi-name pred k-range k-type false))
  (range-filter-count [this dbi-name pred k-range k-type writing?]
    (scan/range-filter-count this dbi-name pred k-range k-type writing?))

  (visit [this dbi-name visitor k-range]
    (.visit this dbi-name visitor k-range :data false))
  (visit [this dbi-name visitor k-range k-type]
    (.visit this dbi-name visitor k-range k-type false))
  (visit [this dbi-name visitor k-range k-type writing?]
    (scan/visit this dbi-name visitor k-range k-type writing?))

  (open-list [this dbi-name key-size item-size]
    (assert (and (>= c/+max-key-size+ ^long key-size)
                 (>= c/+max-key-size+ ^long item-size))
            "Data size cannot be larger than 511 bytes")
    (.open-dbi this dbi-name key-size item-size
               (conj c/default-dbi-flags :dupsort)))
  (open-list [lmdb dbi-name item-size]
    (.open-list lmdb dbi-name c/+max-key-size+ item-size))
  (open-list [lmdb dbi-name]
    (.open-list lmdb dbi-name c/+max-key-size+ c/+max-key-size+))

  IInvertedList
  (put-list-items [this dbi-name k vs kt vt]
    (.transact-kv this [[:put-list dbi-name k vs kt vt]]))

  (del-list-items [this dbi-name k kt]
    (.transact-kv this [[:del dbi-name k kt]]))
  (del-list-items [this dbi-name k vs kt vt]
    (.transact-kv this [[:del-list dbi-name k vs kt vt]]))

  (get-list [this dbi-name k kt vt]
    (.get-list this dbi-name k kt vt false))
  (get-list [this dbi-name k kt vt writing?]
    (when k
      (let [^DBI dbi    (.get-dbi this dbi-name false)
            ^Rtx rtx    (if writing?
                          @(.write-txn this)
                          (.get-rtx this))
            ^Txn txn    (.-txn rtx)
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
          (finally
            (if (.isReadOnly txn)
              (.return-cursor dbi cur)
              (.close cur))
            (.return-rtx this rtx))))))

  (visit-list [this dbi-name visitor k kt]
    (.visit-list this dbi-name visitor k kt false))
  (visit-list [this dbi-name visitor k kt writing?]
    (when k
      (let [^DBI dbi    (.get-dbi this dbi-name false)
            ^Rtx rtx    (if writing?
                          @(.write-txn this)
                          (.get-rtx this))
            ^Txn txn    (.-txn rtx)
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
          (finally
            (if (.isReadOnly txn)
              (.return-cursor dbi cur)
              (.close cur))
            (.return-rtx this rtx))))))

  (list-count [this dbi-name k kt]
    (.list-count this dbi-name k kt false))
  (list-count [this dbi-name k kt writing?]
    (if k
      (let [^DBI dbi    (.get-dbi this dbi-name false)
            ^Rtx rtx    (if writing?
                          @(.write-txn this)
                          (.get-rtx this))
            ^Txn txn    (.-txn rtx)
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
          (finally
            (if (.isReadOnly txn)
              (.return-cursor dbi cur)
              (.close cur))
            (.return-rtx this rtx))))
      0))

  (filter-list [this dbi-name k pred k-type v-type]
    (.filter-list this dbi-name k pred k-type v-type false))
  (filter-list [this dbi-name k pred kt vt writing?]
    (let [^DBI dbi    (.get-dbi this dbi-name false)
          ^Rtx rtx    (if writing?
                        @(.write-txn this)
                        (.get-rtx this))
          ^Txn txn    (.-txn rtx)
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
        (finally
          (if (.isReadOnly txn)
            (.return-cursor dbi cur)
            (.close cur))
          (.return-rtx this rtx)))))

  (filter-list-count [this dbi-name k pred k-type]
    (.filter-list-count this dbi-name k pred k-type false))
  (filter-list-count [this dbi-name k pred kt writing?]
    (let [^DBI dbi    (.get-dbi this dbi-name false)
          ^Rtx rtx    (if writing?
                        @(.write-txn this)
                        (.get-rtx this))
          ^Txn txn    (.-txn rtx)
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
        (finally
          (if (.isReadOnly txn)
            (.return-cursor dbi cur)
            (.close cur))
          (.return-rtx this rtx)))))

  (in-list? [this dbi-name k v kt vt]
    (.in-list? this dbi-name k v kt vt false))
  (in-list? [this dbi-name k v kt vt writing?]
    (if (and k v)
      (let [^DBI dbi    (.get-dbi this dbi-name false)
            ^Rtx rtx    (if writing?
                          @(.write-txn this)
                          (.get-rtx this))
            ^Txn txn    (.-txn rtx)
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
          (finally
            (if (.isReadOnly txn)
              (.return-cursor dbi cur)
              (.close cur))
            (.return-rtx this rtx))))
      false)))

(defmethod open-kv :graal
  ([dir]
   (open-kv dir {}))
  ([dir {:keys [mapsize flags]
         :or   {mapsize c/+init-db-size+
                flags   c/default-env-flags}}]
   (try
     (u/file dir)
     (let [^Env env (Env/create
                      dir
                      (* ^long mapsize 1024 1024)
                      c/+max-readers+
                      c/+max-dbs+
                      (kv-flags flags))]
       (->LMDB env
               dir
               (ConcurrentLinkedQueue.)
               (ConcurrentHashMap.)
               false
               (volatile! nil)))
     (catch Exception e
       (raise
         "Fail to open database: " (ex-message e)
         {:dir dir})))))
