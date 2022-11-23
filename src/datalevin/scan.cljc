(ns ^:no-doc datalevin.scan
  "Index scan routines common to all bindings"
  (:require [datalevin.bits :as b]
            [datalevin.constants :as c]
            [datalevin.util :refer [raise]]
            [datalevin.lmdb :as l]
            [clojure.stacktrace :as st])
  (:import [java.nio ByteBuffer]
           [java.util Iterator]
           [java.lang AutoCloseable]))

(defn- fetch-value
  [dbi rtx k k-type v-type ignore-key?]
  (l/put-key rtx k k-type)
  (when-let [^ByteBuffer bb (l/get-kv dbi rtx)]
    (if ignore-key?
      (b/read-buffer bb v-type)
      [(b/expected-return k k-type) (b/read-buffer bb v-type)])))

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

(defn- fetch-first
  [dbi rtx [range-type k1 k2] k-type v-type ignore-key?]
  (let [info (l/range-info rtx range-type k1 k2)]
    (when k1 (l/put-start-key rtx k1 k-type))
    (when k2 (l/put-stop-key rtx k2 k-type))
    (with-open [^AutoCloseable iterable (l/iterate-kv dbi rtx info)]
      (let [^Iterator iter (.iterator ^Iterable iterable)]
        (when (.hasNext iter)
          (let [kv (.next iter)
                v  (when (not= v-type :ignore)
                     (b/read-buffer (l/v kv) v-type))]
            (if ignore-key?
              (if v v true)
              [(read-key kv k-type v) v])))))))

(defn- fetch-range
  [dbi rtx [range-type k1 k2] k-type v-type ignore-key?]
  (assert (not (and (= v-type :ignore) ignore-key?))
          "Cannot ignore both key and value")
  (let [info (l/range-info rtx range-type k1 k2)]
    (when k1 (l/put-start-key rtx k1 k-type))
    (when k2 (l/put-stop-key rtx k2 k-type))
    (with-open [^AutoCloseable iterable (l/iterate-kv dbi rtx info)]
      (loop [^Iterator iter (.iterator ^Iterable iterable)
             holder         (transient [])]
        (if (.hasNext iter)
          (let [kv      (.next iter)
                v       (when (not= v-type :ignore)
                          (b/read-buffer (l/v kv) v-type))
                holder' (conj! holder
                               (if ignore-key?
                                 v
                                 [(read-key kv k-type v) v]))]
            (recur iter holder'))
          (persistent! holder))))))

(defn- fetch-range-count
  [dbi rtx [range-type k1 k2] k-type]
  (let [info (l/range-info rtx range-type k1 k2)]
    (when k1 (l/put-start-key rtx k1 k-type))
    (when k2 (l/put-stop-key rtx k2 k-type))
    (with-open [^AutoCloseable iterable (l/iterate-kv dbi rtx info)]
      (loop [^Iterator iter (.iterator ^Iterable iterable)
             c              0]
        (if (.hasNext iter)
          (do (.next iter) (recur iter (inc c)))
          c)))))

(defn- fetch-some
  [dbi rtx pred [range-type k1 k2] k-type v-type ignore-key?]
  (assert (not (and (= v-type :ignore) ignore-key?))
          "Cannot ignore both key and value")
  (let [info (l/range-info rtx range-type k1 k2)]
    (when k1 (l/put-start-key rtx k1 k-type))
    (when k2 (l/put-stop-key rtx k2 k-type))
    (with-open [^AutoCloseable iterable (l/iterate-kv dbi rtx info)]
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
              (recur iter))))))))

(defn- fetch-range-filtered
  [dbi rtx pred [range-type k1 k2] k-type v-type ignore-key?]
  (assert (not (and (= v-type :ignore) ignore-key?))
          "Cannot ignore both key and value")
  (let [info (l/range-info rtx range-type k1 k2)]
    (when k1 (l/put-start-key rtx k1 k-type))
    (when k2 (l/put-stop-key rtx k2 k-type))
    (with-open [^AutoCloseable iterable (l/iterate-kv dbi rtx info)]
      (loop [^Iterator iter (.iterator ^Iterable iterable)
             holder         (transient [])]
        (if (.hasNext iter)
          (let [kv (.next iter)]
            (if (pred kv)
              (let [v       (when (not= v-type :ignore)
                              (b/read-buffer
                                (.rewind ^ByteBuffer (l/v kv)) v-type))
                    holder' (conj! holder
                                   (if ignore-key?
                                     v
                                     [(read-key kv k-type v true) v]))]
                (recur iter holder'))
              (recur iter holder)))
          (persistent! holder))))))

(defn- fetch-range-filtered-count
  [dbi rtx pred [range-type k1 k2] k-type]
  (let [info (l/range-info rtx range-type k1 k2)]
    (when k1 (l/put-start-key rtx k1 k-type))
    (when k2 (l/put-stop-key rtx k2 k-type))
    (with-open [^AutoCloseable iterable (l/iterate-kv dbi rtx info)]
      (loop [^Iterator iter (.iterator ^Iterable iterable)
             c              0]
        (if (.hasNext iter)
          (let [kv (.next iter)]
            (if (pred kv)
              (recur iter (inc c))
              (recur iter c)))
          c)))))

(defmacro scan
  [call error]
  `(do
     (assert (not (l/closed-kv? ~'lmdb)) "LMDB env is closed.")
     (let [~'dbi (l/get-dbi ~'lmdb ~'dbi-name false)
           ~'rtx (if (l/writing? ~'lmdb)
                   @(l/write-txn ~'lmdb)
                   (l/get-rtx ~'lmdb))]
       (try
         ~call
         (catch Exception ~'e
           (st/print-stack-trace ~'e)
           ~error)
         (finally
           (when-not (l/writing? ~'lmdb)
             (l/return-rtx ~'lmdb ~'rtx)))))))

(defn get-value
  [lmdb dbi-name k k-type v-type ignore-key?]
  (scan
    (fetch-value dbi rtx k k-type v-type ignore-key?)
    (raise "Fail to get-value: " (ex-message e)
           {:dbi dbi-name :k k :k-type k-type :v-type v-type})))

(defn get-first
  [lmdb dbi-name k-range k-type v-type ignore-key?]
  (scan
    (fetch-first dbi rtx k-range k-type v-type ignore-key?)
    (raise "Fail to get-first: " (ex-message e)
           {:dbi    dbi-name :k-range k-range
            :k-type k-type   :v-type  v-type})))

(defn get-range
  [lmdb dbi-name k-range k-type v-type ignore-key?]
  (scan
    (fetch-range dbi rtx k-range k-type v-type ignore-key?)
    (raise "Fail to get-range: " (ex-message e)
           {:dbi    dbi-name :k-range k-range
            :k-type k-type   :v-type  v-type})))

(defn range-count
  [lmdb dbi-name k-range k-type]
  (scan
    (fetch-range-count dbi rtx k-range k-type)
    (raise "Fail to range-count: " (ex-message e)
           {:dbi dbi-name :k-range k-range :k-type k-type})))

(defn get-some
  [lmdb dbi-name pred k-range k-type v-type ignore-key?]
  (scan
    (fetch-some dbi rtx pred k-range k-type v-type ignore-key?)
    (raise "Fail to get-some: " (ex-message e)
           {:dbi    dbi-name :k-range k-range
            :k-type k-type   :v-type  v-type})))

(defn range-filter
  [lmdb dbi-name pred k-range k-type v-type ignore-key?]
  (scan
    (fetch-range-filtered dbi rtx pred k-range k-type v-type ignore-key?)
    (raise "Fail to range-filter: " (ex-message e)
           {:dbi    dbi-name :k-range k-range
            :k-type k-type   :v-type  v-type})))

(defn range-filter-count
  [lmdb dbi-name pred k-range k-type]
  (scan
    (fetch-range-filtered-count dbi rtx pred k-range k-type)
    (raise "Fail to range-filter-count: " (ex-message e)
           {:dbi dbi-name :k-range k-range :k-type k-type})))

(defn visit
  [lmdb dbi-name visitor k-range k-type]
  (scan
    (let [[range-type k1 k2] k-range
          info               (l/range-info rtx range-type k1 k2)]
      (when k1 (l/put-start-key rtx k1 k-type))
      (when k2 (l/put-stop-key rtx k2 k-type))
      (with-open [^AutoCloseable iterable (l/iterate-kv dbi rtx info)]
        (loop [^Iterator iter (.iterator ^Iterable iterable)]
          (when (.hasNext iter)
            (when-not (= (visitor (.next iter)) :datalevin/terminate-visit)
              (recur iter))))))
    (raise "Fail to visit: " (ex-message e)
           {:dbi dbi-name :k-range k-range :k-type k-type})))
