(ns ^:no-doc datalevin.binding.scan
  "Index scan routines"
  (:require [datalevin.bits :as b]
            [datalevin.constants :as c]
            [datalevin.binding.scan :as scan]
            [datalevin.lmdb :as lmdb :refer [IKV]])
  (:import [java.nio ByteBuffer]
           [java.util Iterator]
           [java.lang AutoCloseable]
           [org.lmdbjava CursorIterable$KeyVal]))

(extend-protocol IKV
  CursorIterable$KeyVal
  (k [this] (.key ^CursorIterable$KeyVal this))
  (v [this] (.val ^CursorIterable$KeyVal this)))

(defn fetch-value
  [dbi rtx k k-type v-type ignore-key?]
  (lmdb/put-key rtx k k-type)
  (when-let [^ByteBuffer bb (lmdb/get-kv dbi rtx)]
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
                      (.rewind ^ByteBuffer (lmdb/k kv))
                      (lmdb/k kv))
                    k-type))))

(defn fetch-first
  [dbi rtx [range-type k1 k2] k-type v-type ignore-key?]
  (lmdb/put-start-key rtx k1 k-type)
  (lmdb/put-stop-key rtx k2 k-type)
  (with-open [^AutoCloseable iterable (lmdb/iterate-kv dbi rtx range-type)]
    (let [^Iterator iter (.iterator ^Iterable iterable)]
      (when (.hasNext iter)
        (let [kv (.next iter)
              v  (when (not= v-type :ignore) (b/read-buffer (lmdb/v kv) v-type))]
          (if ignore-key?
            (if v v true)
            [(read-key kv k-type v) v]))))))

(defn fetch-range
  [dbi rtx [range-type k1 k2] k-type v-type ignore-key?]
  (assert (not (and (= v-type :ignore) ignore-key?))
          "Cannot ignore both key and value")
  (lmdb/put-start-key rtx k1 k-type)
  (lmdb/put-stop-key rtx k2 k-type)
  (with-open [^AutoCloseable iterable (lmdb/iterate-kv dbi rtx range-type)]
    (loop [^Iterator iter (.iterator ^Iterable iterable)
           holder         (transient [])]
      (if (.hasNext iter)
        (let [kv      (.next iter)
              v       (when (not= v-type :ignore)
                        (b/read-buffer (lmdb/v kv) v-type))
              holder' (conj! holder
                             (if ignore-key?
                               v
                               [(read-key kv k-type v) v]))]
          (recur iter holder'))
        (persistent! holder)))))

(defn fetch-range-count
  [dbi rtx [range-type k1 k2] k-type]
  (lmdb/put-start-key rtx k1 k-type)
  (lmdb/put-stop-key rtx k2 k-type)
  (with-open [^AutoCloseable iterable (lmdb/iterate-kv dbi rtx range-type)]
    (loop [^Iterator iter (.iterator ^Iterable iterable)
           c              0]
      (if (.hasNext iter)
        (do (.next iter) (recur iter (inc c)))
        c))))

(defn fetch-some
  [dbi rtx pred [range-type k1 k2] k-type v-type ignore-key?]
  (assert (not (and (= v-type :ignore) ignore-key?))
          "Cannot ignore both key and value")
  (lmdb/put-start-key rtx k1 k-type)
  (lmdb/put-stop-key rtx k2 k-type)
  (with-open [^AutoCloseable iterable (lmdb/iterate-kv dbi rtx range-type)]
    (loop [^Iterator iter (.iterator ^Iterable iterable)]
      (when (.hasNext iter)
        (let [kv (.next iter)]
          (if (pred kv)
            (let [v (when (not= v-type :ignore)
                      (b/read-buffer (.rewind ^ByteBuffer (lmdb/v kv)) v-type))]
              (if ignore-key?
                v
                [(read-key kv k-type v true) v]))
            (recur iter)))))))

(defn fetch-range-filtered
  [dbi rtx pred [range-type k1 k2] k-type v-type ignore-key?]
  (assert (not (and (= v-type :ignore) ignore-key?))
          "Cannot ignore both key and value")
  (lmdb/put-start-key rtx k1 k-type)
  (lmdb/put-stop-key rtx k2 k-type)
  (with-open [^AutoCloseable iterable (lmdb/iterate-kv dbi rtx range-type)]
    (loop [^Iterator iter (.iterator ^Iterable iterable)
           holder         (transient [])]
      (if (.hasNext iter)
        (let [kv (.next iter)]
          (if (pred kv)
            (let [v       (when (not= v-type :ignore)
                            (b/read-buffer
                              (.rewind ^ByteBuffer (lmdb/v kv)) v-type))
                  holder' (conj! holder
                                 (if ignore-key?
                                   v
                                   [(read-key kv k-type v true) v]))]
              (recur iter holder'))
            (recur iter holder)))
        (persistent! holder)))))

(defn fetch-range-filtered-count
  [dbi rtx pred [range-type k1 k2] k-type]
  (lmdb/put-start-key rtx k1 k-type)
  (lmdb/put-stop-key rtx k2 k-type)
  (with-open [^AutoCloseable iterable (lmdb/iterate-kv dbi rtx range-type)]
    (loop [^Iterator iter (.iterator ^Iterable iterable)
           c              0]
      (if (.hasNext iter)
        (let [kv (.next iter)]
          (if (pred kv)
            (recur iter (inc c))
            (recur iter c)))
        c))))
