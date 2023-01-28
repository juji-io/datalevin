(ns ^:no-doc datalevin.scan
  "Index scan routines common to all bindings"
  (:require
   [datalevin.bits :as b]
   [datalevin.spill :as sp]
   [datalevin.constants :as c]
   [datalevin.util :as u :refer [raise]]
   [datalevin.lmdb :as l]
   [clojure.stacktrace :as st])
  (:import
   [datalevin.spill SpillableVector]
   [clojure.lang Seqable IReduceInit]
   [java.nio ByteBuffer]
   [java.util Iterator]
   [java.lang AutoCloseable Iterable]))

(defmacro scan
  ([call error]
   `(scan ~call ~error false))
  ([call error keep-rtx?]
   `(let [~'dbi (l/get-dbi ~'lmdb ~'dbi-name false)
          ~'rtx (if (l/writing? ~'lmdb)
                  @(l/write-txn ~'lmdb)
                  (l/get-rtx ~'lmdb))]
      (try
        ~call
        (catch Exception ~'e
          ;; (st/print-stack-trace ~'e)
          ~error)
        (finally
          (when-not (or (l/writing? ~'lmdb) ~keep-rtx?)
            (l/return-rtx ~'lmdb ~'rtx)))))))

(defmacro scan-list
  ([call error]
   `(scan-list ~call ~error false))
  ([call error keep-rtx?]
   `(let [~'dbi (l/get-dbi ~'lmdb ~'dbi-name false)
          ~'rtx (if (l/writing? ~'lmdb)
                  @(l/write-txn ~'lmdb)
                  (l/get-rtx ~'lmdb))
          ~'txn (l/get-txn ~'rtx)
          ~'cur (l/get-cursor ~'dbi ~'txn)]
      (try
        ~call
        (catch Exception ~'e
          ;; (st/print-stack-trace ~'e)
          ~error)
        (finally
          (if (l/read-only? ~'rtx)
            (l/return-cursor ~'dbi ~'cur)
            (l/close-cursor ~'dbi ~'cur))
          (when-not (or (l/writing? ~'lmdb) ~keep-rtx?)
            (l/return-rtx ~'lmdb ~'rtx)))))))

(defn- read-key
  ([kv k-type v]
   (read-key kv k-type v false))
  ([kv k-type v rewind?]
   (if (and v (not= v c/normal) (c/index-types k-type))
     b/overflown-key
     (b/read-buffer (if rewind?
                      (.rewind ^ByteBuffer (l/k kv))
                      (l/k kv))
                    k-type))))

(defn get-value
  [lmdb dbi-name k k-type v-type ignore-key?]
  (scan
    (do
      (l/put-key rtx k k-type)
      (when-let [^ByteBuffer bb (l/get-kv dbi rtx)]
        (if ignore-key?
          (b/read-buffer bb v-type)
          [(b/expected-return k k-type) (b/read-buffer bb v-type)])))
    (raise "Fail to get-value: " e
           {:dbi dbi-name :k k :k-type k-type :v-type v-type})))

(defn get-first
  [lmdb dbi-name k-range k-type v-type ignore-key?]
  (scan
    (with-open [^AutoCloseable iterable (l/iterate-kv dbi rtx k-range k-type)]
      (let [^Iterator iter (.iterator ^Iterable iterable)]
        (when (.hasNext iter)
          (let [kv (.next iter)
                v  (when (not= v-type :ignore)
                     (b/read-buffer (l/v kv) v-type))]
            (if ignore-key?
              (if v v true)
              [(read-key kv k-type v) v])))))
    (raise "Fail to get-first: " e
           {:dbi    dbi-name :k-range k-range
            :k-type k-type   :v-type  v-type})))

(defn get-range
  [lmdb dbi-name k-range k-type v-type ignore-key?]
  (scan
    (do
      (assert (not (and (= v-type :ignore) ignore-key?))
              "Cannot ignore both key and value")
      (with-open [^AutoCloseable iterable (l/iterate-kv dbi rtx k-range k-type)]
        (let [^SpillableVector holder
              (sp/new-spillable-vector nil (:spill-opts (l/opts lmdb)))]
          (loop [^Iterator iter (.iterator ^Iterable iterable)]
            (if (.hasNext iter)
              (let [kv (.next iter)
                    v  (when (not= v-type :ignore)
                         (b/read-buffer (l/v kv) v-type))]
                (.cons holder (if ignore-key?
                                v
                                [(read-key kv k-type v) v]))
                (recur iter))
              holder)))))
    (raise "Fail to get-range: " e
           {:dbi    dbi-name :k-range k-range
            :k-type k-type   :v-type  v-type})))

(defn- range-seq*
  [lmdb dbi rtx k-range k-type v-type ignore-key?
   {:keys [batch-size] :or {batch-size 100}}]
  (assert (not (and (= v-type :ignore) ignore-key?))
          "Cannot ignore both key and value")
  (let [^Iterable itb  (l/iterate-kv dbi rtx k-range k-type)
        ^Iterator iter (.iterator itb)
        item           (fn [kv]
                         (let [v (when (not= v-type :ignore)
                                   (b/read-buffer (l/v kv) v-type))]
                           (if ignore-key?
                             (if v v true)
                             [(read-key kv k-type v) v])))
        fetch          (fn [^long k]
                         (let [holder (transient [])]
                           (loop [i 0]
                             (if (.hasNext iter)
                               (let [kv (.next iter)]
                                 (conj! holder (item kv))
                                 (if (< i k)
                                   (recur (inc i))
                                   {:batch  (persistent! holder)
                                    :next-k k}))
                               {:batch  (persistent! holder)
                                :next-k nil}))))]
    (reify
      Seqable
      (seq [_]
        (u/lazy-concat
          ((fn next [ret]
             (when (clojure.core/seq (:batch ret))
               (cons (:batch ret)
                     (when-some [k (:next-k ret)]
                       (lazy-seq (next (fetch k)))))))
           (fetch batch-size))))

      IReduceInit
      (reduce [_ rf init]
        (loop [acc init
               ret (fetch batch-size)]
          (if (clojure.core/seq (:batch ret))
            (let [acc (rf acc (:batch ret))]
              (if (reduced? acc)
                @acc
                (if-some [k (:next-k ret)]
                  (recur acc (fetch k))
                  acc)))
            acc)))

      AutoCloseable
      (close [_]
        (.close ^AutoCloseable itb)
        (l/return-rtx lmdb rtx))

      Object
      (toString [this] (str (apply list this))))))

(defn range-seq
  ([lmdb dbi-name k-range k-type v-type ignore-key?]
   (range-seq lmdb dbi-name k-range k-type v-type ignore-key? nil))
  ([lmdb dbi-name k-range k-type v-type ignore-key? opts]
   (scan
     (range-seq* lmdb dbi rtx k-range k-type v-type ignore-key? opts)
     (raise "Fail in range-seq: " e
            {:dbi    dbi-name :k-range k-range
             :k-type k-type   :v-type  v-type})
     true)))

(defn range-count*
  [iterable]
  (loop [^Iterator iter (.iterator ^Iterable iterable)
         c              0]
    (if (.hasNext iter)
      (do (.next iter) (recur iter (inc c)))
      c)))

(defn range-count
  [lmdb dbi-name k-range k-type]
  (scan
    (with-open [^AutoCloseable iterable (l/iterate-kv dbi rtx k-range k-type)]
      (range-count* iterable))
    (raise "Fail to range-count: " e
           {:dbi dbi-name :k-range k-range :k-type k-type})))

(defn get-some
  [lmdb dbi-name pred k-range k-type v-type ignore-key?]
  (scan
    (do
      (assert (not (and (= v-type :ignore) ignore-key?))
              "Cannot ignore both key and value")
      (with-open [^AutoCloseable iterable (l/iterate-kv dbi rtx k-range k-type)]
        (loop [^Iterator iter (.iterator ^Iterable iterable)]
          (when (.hasNext iter)
            (let [kv (.next iter)]
              (if (pred kv)
                (let [v (when (not= v-type :ignore)
                          (b/read-buffer (.rewind ^ByteBuffer (l/v kv))
                                         v-type))]
                  (if ignore-key?
                    v
                    [(read-key kv k-type v true) v]))
                (recur iter)))))))
    (raise "Fail to get-some: " e
           {:dbi    dbi-name :k-range k-range
            :k-type k-type   :v-type  v-type})))

(defn range-filter
  [lmdb dbi-name pred k-range k-type v-type ignore-key?]
  (scan
    (do
      (assert (not (and (= v-type :ignore) ignore-key?))
              "Cannot ignore both key and value")
      (let [^SpillableVector holder
            (sp/new-spillable-vector nil (:spill-opts (l/opts lmdb)))]
        (with-open [^AutoCloseable iterable (l/iterate-kv dbi rtx k-range k-type)]
          (loop [^Iterator iter (.iterator ^Iterable iterable)]
            (if (.hasNext iter)
              (let [kv (.next iter)]
                (if (pred kv)
                  (let [v (when (not= v-type :ignore)
                            (b/read-buffer
                              (.rewind ^ByteBuffer (l/v kv)) v-type))]
                    (.cons holder (if ignore-key?
                                    v
                                    [(read-key kv k-type v true) v]))
                    (recur iter))
                  (recur iter)))
              holder)))))
    (raise "Fail to range-filter: " e
           {:dbi    dbi-name :k-range k-range
            :k-type k-type   :v-type  v-type})))

(defn- filter-count*
  [iterable pred]
  (loop [^Iterator iter (.iterator ^Iterable iterable)
         c              0]
    (if (.hasNext iter)
      (let [kv (.next iter)]
        (if (pred kv)
          (recur iter (inc c))
          (recur iter c)))
      c)))

(defn range-filter-count
  [lmdb dbi-name pred k-range k-type]
  (scan
    (with-open [^AutoCloseable iterable (l/iterate-kv dbi rtx k-range k-type)]
      (filter-count* iterable pred))
    (raise "Fail to range-filter-count: " e
           {:dbi dbi-name :k-range k-range :k-type k-type})))

(defn- visit*
  [iterable visitor]
  (loop [^Iterator iter (.iterator ^Iterable iterable)]
    (when (.hasNext iter)
      (when-not (= (visitor (.next iter)) :datalevin/terminate-visit)
        (recur iter)))))

(defn visit
  [lmdb dbi-name visitor k-range k-type]
  (scan
    (with-open [^AutoCloseable iterable (l/iterate-kv dbi rtx k-range k-type)]
      (visit* iterable visitor))
    (raise "Fail to visit: " e
           {:dbi dbi-name :k-range k-range :k-type k-type})))

(defn list-range
  [lmdb dbi-name k-range k-type v-range v-type]
  (scan-list
    (let [^SpillableVector holder
          (sp/new-spillable-vector nil (:spill-opts (l/opts lmdb)))]
      (with-open [^AutoCloseable iterable
                  (l/iterate-list dbi rtx k-range k-type v-range v-type)]
        (loop [^Iterator iter (.iterator ^Iterable iterable)]
          (if (.hasNext iter)
            (let [kv (.next iter)]
              (.cons holder [(b/read-buffer (l/k kv) k-type)
                             (b/read-buffer (l/v kv) v-type)])
              (recur iter))
            holder))))
    (raise "Fail to get list range: " e
           {:dbi dbi-name :key-range k-range :val-range v-range})) )

(defn list-range-count
  [lmdb dbi-name k-range k-type v-range v-type]
  (scan-list
    (with-open [^AutoCloseable iterable
                (l/iterate-list dbi rtx k-range k-type v-range v-type)]
      (range-count* iterable))
    (raise "Fail to get list range count: " e
           {:dbi dbi-name :key-range k-range :val-range v-range})) )

(defn list-range-filter
  [lmdb dbi-name pred k-range k-type v-range v-type]
  (scan-list
    (let [^SpillableVector holder
          (sp/new-spillable-vector nil (:spill-opts (l/opts lmdb)))]
      (with-open [^AutoCloseable iterable
                  (l/iterate-list dbi rtx k-range k-type v-range v-type)]
        (loop [^Iterator iter (.iterator ^Iterable iterable)]
          (if (.hasNext iter)
            (let [kv (.next iter)]
              (if (pred kv)
                (let [k (.rewind ^ByteBuffer (l/k kv))
                      v (.rewind ^ByteBuffer (l/v kv))]
                  (.cons holder [(b/read-buffer k k-type)
                                 (b/read-buffer v v-type)])
                  (recur iter))
                (recur iter)))
            holder))))
    (raise "Fail to filter list range: " e
           {:dbi dbi-name :key-range k-range :val-range v-range})) )

(defn list-range-filter-count
  [lmdb dbi-name pred k-range k-type v-range v-type]
  (scan-list
    (with-open [^AutoCloseable iterable
                (l/iterate-list dbi rtx k-range k-type v-range v-type)]
      (filter-count* iterable pred))
    (raise "Fail to count filtered list range: " e
           {:dbi dbi-name :key-range k-range :val-range v-range})))

(defn visit-list-range
  [lmdb dbi-name visitor k-range k-type v-range v-type]
  (scan-list
    (with-open [^AutoCloseable iterable
                (l/iterate-list dbi rtx k-range k-type v-range v-type)]
      (visit* iterable visitor))
    (raise "Fail to visit list range: " e
           {:dbi dbi-name :key-range k-range :val-range v-range})) )
