(ns datalevin.binding.graal
  "LMDB binding for GraalVM native image"
  (:require [datalevin.bits :as b]
            [datalevin.util :refer [raise] :as u]
            [datalevin.constants :as c]
            [datalevin.scan :as scan]
            [datalevin.lmdb :as lmdb
             :refer [open-kv IBuffer IRange IRtx IRtxPool IDB IKV ILMDB]]
            [clojure.string :as s])
  (:import [java.util Iterator]
           [java.util.concurrent ConcurrentHashMap]
           [java.nio ByteBuffer BufferOverflowException]
           [java.lang AutoCloseable]
           [java.lang.annotation Retention RetentionPolicy]
           [org.graalvm.nativeimage.c CContext]
           [datalevin.ni BufVal Lib Env Txn Dbi Cursor Stat Info
            Lib$Directives Lib$BadReaderLockException
            Lib$MDB_cursor_op Lib$MDB_envinfo Lib$MDB_stat
            Lib$MapFullException]))

(deftype ^{Retention RetentionPolicy/RUNTIME
           CContext  {:value Lib$Directives}}
    Rtx [^Txn txn
         ^:volatile-mutable ^Boolean use?
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
    (.close stop-kp)
    (set! use? false))
  (reset [this]
    (.reset txn)
    (set! use? false)
    this)
  (renew [this]
    (when-not use?
      (set! use? true)
      (.renew txn)
      this)))

(deftype ^{Retention RetentionPolicy/RUNTIME
           CContext  {:value Lib$Directives}}
    RtxPool [^Env env
             ^ConcurrentHashMap rtxs
             ^:volatile-mutable ^long cnt]
  IRtxPool
  (close-pool [this]
    (doseq [^Rtx rtx (.values rtxs)] (.close-rtx rtx))
    (.clear rtxs)
    (set! cnt 0))
  (new-rtx [this]
    (when (< cnt c/+use-readers+)
      (let [txn (Txn/createReadOnly env)
            rtx (->Rtx txn
                       false
                       (BufVal/create c/+max-key-size+)
                       (BufVal/create 1)
                       (BufVal/create c/+max-key-size+)
                       (BufVal/create c/+max-key-size+))]
        (.put rtxs cnt rtx)
        (set! cnt (inc cnt))
        (.reset ^Rtx rtx)
        (.renew ^Rtx rtx))))
  (get-rtx [this]
    (try
      (locking this
        (if (zero? cnt)
          (.new-rtx this)
          (loop [i (.getId ^Thread (Thread/currentThread))]
            (let [^long i' (mod i cnt)
                  ^Rtx rtx (.get rtxs i')]
              (or (.renew rtx)
                  (.new-rtx this)
                  (recur (long (inc i'))))))))
      (catch Lib$BadReaderLockException _
        (raise
          "Please do not open multiple LMDB connections to the same DB
           in the same process. Instead, a LMDB connection should be held onto
           and managed like a stateful resource. Refer to the documentation of
           `datalevin.lmdb/open-kv` for more details." {})))))

(deftype ^{Retention RetentionPolicy/RUNTIME
           CContext  {:value Lib$Directives}}
    KV [^BufVal kp ^BufVal vp]
  IKV
  (k [this] (.outBuf kp))
  (v [this] (.outBuf vp)))

(deftype ^{Retention RetentionPolicy/RUNTIME
           CContext  {:value Lib$Directives}}
    CursorIterable [^Cursor cursor
                    ^Dbi dbi
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
    (.close cursor))

  Iterable
  (iterator [this]
    (let [started?     (volatile! false)
          ended?       (volatile! false)
          ^BufVal k    (.-kp rtx)
          ^BufVal v    (.-vp rtx)
          ^Txn txn     (.-txn rtx)
          i            (.get ^Dbi dbi)
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
        (hasNext [this]
          (let [has?      #(if (= ^int % (Lib/MDB_NOTFOUND))
                             false
                             (do (Lib/checkRc ^int %) true))
                end       #(do (vreset! ended? true) false)
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
        (next [this]
          (get-cur)
          (->KV k v))))))

(deftype ^{Retention RetentionPolicy/RUNTIME
           CContext  {:value Lib$Directives}}
    DBI [^Dbi db
         ^BufVal kp
         ^:volatile-mutable ^BufVal vp]
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
          (let [size (* 2 ^long (b/measure-size x))]
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
  (dbi-name [_]
    (.getName db))

  (put [this txn]
    (.put this txn nil))
  (put [_ txn append?]
    (let [i (.get db)]
      (Lib/checkRc
        (if append?
          (Lib/mdb_put (.get ^Txn txn) i (.getVal kp) (.getVal vp) (Lib/MDB_APPEND))
          (Lib/mdb_put (.get ^Txn txn) i (.getVal kp) (.getVal vp) 0)))))

  (del [_ txn]
    (Lib/checkRc
      (Lib/mdb_del (.get ^Txn txn) (.get db) (.getVal kp) (.getVal vp))))

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
          cur (Cursor/create txn db)]
      (->CursorIterable cur db rtx f? sk? is? ek? ie? sk ek))))

(defn- stat-map [^Stat stat]
  {:psize          (.ms_psize ^Lib$MDB_stat (.get stat))
   :depth          (.ms_depth ^Lib$MDB_stat (.get stat))
   :branch-pages   (.ms_branch_pages ^Lib$MDB_stat (.get stat))
   :leaf-pages     (.ms_leaf_pages ^Lib$MDB_stat (.get stat))
   :overflow-pages (.ms_overflow_pages ^Lib$MDB_stat (.get stat))
   :entries        (.ms_entries ^Lib$MDB_stat (.get stat))})

(deftype ^{Retention RetentionPolicy/RUNTIME
           CContext  {:value Lib$Directives}}
    LMDB [^Env env
          ^String dir
          ^RtxPool pool
          ^ConcurrentHashMap dbis
          ^:volatile-mutable closed?]
  ILMDB
  (close-kv [_]
    (when-not closed?
      (.close-pool pool)
      (doseq [^DBI dbi (.values dbis)] (.close ^Dbi (.-db dbi)))
      (.close env)
      (set! closed? true)))

  (closed-kv? [_]
    closed?)

  (dir [_]
    dir)

  (open-dbi [this dbi-name]
    (.open-dbi
      this dbi-name c/+max-key-size+ c/+default-val-size+ (Lib/MDB_CREATE)))
  (open-dbi [this dbi-name key-size]
    (.open-dbi this dbi-name key-size c/+default-val-size+ (Lib/MDB_CREATE)))
  (open-dbi [this dbi-name key-size val-size]
    (.open-dbi this dbi-name key-size val-size (Lib/MDB_CREATE)))
  (open-dbi [_ dbi-name key-size val-size flags]
    (assert (not closed?) "LMDB env is closed.")
    (let [kp  (BufVal/create key-size)
          vp  (BufVal/create val-size)
          dbi (Dbi/create env dbi-name flags)
          db  (->DBI dbi kp vp)]
      (.put dbis dbi-name db)
      db))

  (get-dbi [this dbi-name]
    (.get-dbi this dbi-name true))
  (get-dbi [this dbi-name create?]
    (or (.get dbis dbi-name)
        (if create?
          (.open-dbi this dbi-name)
          (.open-dbi this dbi-name c/+max-key-size+ c/+default-val-size+ 0))))

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
        (.remove dbis dbi-name))
      (catch Exception e
        (raise "Fail to drop DBI: " dbi-name (ex-message e) {}))))

  (list-dbis [this]
    (assert (not closed?) "LMDB env is closed.")
    (let [^Dbi main (Dbi/create env 0)
          ^Rtx rtx  (.get-rtx pool)]
      (try
        (let [^Cursor cursor (Cursor/create (.-txn rtx) main)]
          (with-open [^AutoCloseable iterable (->CursorIterable
                                                cursor main rtx true false false
                                                false false nil nil)]
            (loop [^Iterator iter (.iterator ^Iterable iterable)
                   holder         (transient [])]
              (if (.hasNext iter)
                (let [kv      (.next iter)
                      holder' (conj! holder
                                     (-> kv lmdb/k b/get-bytes b/text-ba->str))]
                  (recur iter holder'))
                (persistent! holder)))))
        (catch Exception e
          (raise "Fail to list DBIs: " (ex-message e) {}))
        (finally (.reset rtx)))))

  (copy [this dest]
    (.copy this dest false))
  (copy [this dest compact?]
    (assert (not closed?) "LMDB env is closed.")
    (if (-> dest u/file u/empty-dir?)
      (.copy env dest (if compact? true false))
      (raise "Destination directory is not empty.")))

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
    (let [^Rtx rtx (.get-rtx pool)]
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
        (finally (.reset rtx)))))

  (entries [this dbi-name]
    (assert (not closed?) "LMDB env is closed.")
    (let [^DBI dbi (.get-dbi this dbi-name false)
          ^Rtx rtx (.get-rtx pool)]
      (try
        (let [^Dbi db    (.-db dbi)
              ^Txn txn   (.-txn rtx)
              ^Stat stat (Stat/create txn db)
              entries    (.ms_entries ^Lib$MDB_stat (.get stat))]
          (.close stat)
          entries)
        (catch Exception e
          (raise "Fail to get entries: " (ex-message e) {:dbi dbi-name}))
        (finally (.reset rtx)))))

  (get-txn [this]
    (.get-rtx pool))

  (transact-kv [this txs]
    (assert (not closed?) "LMDB env is closed.")
    (let [^Txn txn (Txn/create env)]
      (try
        (doseq [[op dbi-name k & r] txs
                :let                [^DBI dbi (or (.get dbis dbi-name)
                                                  (raise dbi-name
                                                         " is not open" {}))]]
          (case op
            :put (let [[v kt vt flags] r]
                   (.put-key dbi k kt)
                   (.put-val dbi v vt)
                   (if flags
                     (.put dbi txn flags)
                     (.put dbi txn)))
            :del (let [[kt] r]
                   (.put-key dbi k kt)
                   (.del dbi txn))))
        (.commit txn)
        (catch Lib$MapFullException _
          (.close txn)
          (let [^Info info (Info/create env)]
            (.setMapSize env (* 10 (.me_mapsize ^Lib$MDB_envinfo (.get info))))
            (.close info)
            (.transact-kv this txs)))
        (catch Exception e
          (.close txn)
          (raise "Fail to transact to LMDB: " (ex-message e) {:txs txs})))))

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
    (scan/range-filter-count this dbi-name pred k-range k-type)))

(defmethod open-kv :graal
  [dir]
  (try
    (u/file dir)
    (let [^Env env (Env/create
                     dir
                     (* ^long c/+init-db-size+ 1024 1024)
                     c/+max-readers+
                     c/+max-dbs+)]
      (->LMDB env
              dir
              (->RtxPool env (ConcurrentHashMap.) 0)
              (ConcurrentHashMap.)
              false))
    (catch Exception e
      (raise
        "Fail to open LMDB database: " (ex-message e)
        {:dir dir}))))
