(ns ^:no-doc datalevin.scan
  "Index scan routines common to all bindings"
  (:require
   [datalevin.bits :as b]
   [datalevin.spill :as sp]
   [datalevin.util :as u :refer [raise]]
   [datalevin.lmdb :as l])
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
   `(do
      (l/check-ready ~'lmdb)
      (let [~'dbi (l/get-dbi ~'lmdb ~'dbi-name false)
            ~'rtx (if (l/writing? ~'lmdb)
                    @(l/write-txn ~'lmdb)
                    (l/get-rtx ~'lmdb))
            ~'cur (l/get-cursor ~'dbi ~'rtx)]
        (try
          ~call
          (catch Exception ~'e
            ~error)
          (finally
            (if (l/read-only? ~'rtx)
              (l/return-cursor ~'dbi ~'cur)
              (l/close-cursor ~'dbi ~'cur))
            (when-not (or (l/writing? ~'lmdb) ~keep-rtx?)
              (l/return-rtx ~'lmdb ~'rtx))))))))

(defn get-value
  [lmdb dbi-name k k-type v-type ignore-key?]
  (scan
    (do (l/put-key rtx k k-type)
        (when-let [^ByteBuffer bb (l/get-kv dbi rtx)]
          (if ignore-key?
            (b/read-buffer bb v-type)
            [(b/expected-return k k-type) (b/read-buffer bb v-type)])))
    (raise "Fail to get-value: " e
           {:dbi dbi-name :k k :k-type k-type :v-type v-type})))

(defn get-first
  [lmdb dbi-name k-range k-type v-type ignore-key?]
  (scan
    (let [iterable       (l/iterate-kv dbi rtx cur k-range k-type v-type)
          ^Iterator iter (.iterator ^Iterable iterable)]
      (when (.hasNext iter)
        (let [kv (.next iter)
              v  (when (not= v-type :ignore)
                   (b/read-buffer (l/v kv) v-type))]
          (if ignore-key?
            (if v v true)
            [(b/read-buffer (l/k kv) k-type) v]))))
    (raise "Fail to get-first: " e
           {:dbi    dbi-name :k-range k-range
            :k-type k-type   :v-type  v-type})))

(defn get-first-n
  [lmdb dbi-name n k-range k-type v-type ignore-key?]
  (scan
    (let [iterable       (l/iterate-kv dbi rtx cur k-range k-type v-type)
          ^Iterator iter (.iterator ^Iterable iterable)
          ^SpillableVector holder
          (sp/new-spillable-vector nil (:spill-opts (l/opts lmdb)))]
      (loop [i 0]
        (if (and (< i ^long n) (.hasNext iter))
          (let [kv (.next iter)
                v  (when (not= v-type :ignore)
                     (b/read-buffer (l/v kv) v-type))]
            (.cons holder (if ignore-key?
                            v
                            [(b/read-buffer (l/k kv) k-type) v]))
            (recur (inc i)))
          holder)))
    (raise "Fail to get-first-n: " e
           {:dbi    dbi-name :k-range k-range
            :k-type k-type   :v-type  v-type})))

(defn get-range
  [lmdb dbi-name k-range k-type v-type ignore-key?]
  (scan
    (do
      (assert (not (and (= v-type :ignore) ignore-key?))
              "Cannot ignore both key and value")
      (let [iterable       (l/iterate-kv dbi rtx cur k-range k-type v-type)
            ^Iterator iter (.iterator ^Iterable iterable)
            ^SpillableVector holder
            (sp/new-spillable-vector nil (:spill-opts (l/opts lmdb)))]
        (loop []
          (if (.hasNext iter)
            (let [kv (.next iter)
                  v  (when (not= v-type :ignore)
                       (b/read-buffer (l/v kv) v-type))]
              (.cons holder (if ignore-key?
                              v
                              [(b/read-buffer (l/k kv) k-type) v]))
              (recur))
            holder))))
    (raise "Fail to get-range: " e
           {:dbi    dbi-name :k-range k-range
            :k-type k-type   :v-type  v-type})))

(defn- range-seq*
  [lmdb dbi rtx cur k-range k-type v-type ignore-key?
   {:keys [batch-size] :or {batch-size 100}}]
  (assert (not (and (= v-type :ignore) ignore-key?))
          "Cannot ignore both key and value")
  (let [^Iterable itb  (l/iterate-kv dbi rtx cur k-range k-type v-type)
        ^Iterator iter (.iterator itb)
        item           (fn [kv]
                         (let [v (when (not= v-type :ignore)
                                   (b/read-buffer (l/v kv) v-type))]
                           (if ignore-key?
                             (if v v true)
                             [(b/read-buffer (l/k kv) k-type) v])))
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
      (close [_] (l/return-rtx lmdb rtx))

      Object
      (toString [this] (str (apply list this))))))

(defn range-seq
  ([lmdb dbi-name k-range k-type v-type ignore-key?]
   (range-seq lmdb dbi-name k-range k-type v-type ignore-key? nil))
  ([lmdb dbi-name k-range k-type v-type ignore-key? opts]
   (scan
     (range-seq* lmdb dbi rtx cur k-range k-type v-type ignore-key? opts)
     (raise "Fail in range-seq: " e
            {:dbi    dbi-name :k-range k-range
             :k-type k-type   :v-type  v-type})
     true)))

(defn range-count*
  ([iterable]
   (let [^Iterator iter (.iterator ^Iterable iterable)]
     (loop [c 0]
       (if (.hasNext iter)
         (do (.next iter) (recur (unchecked-inc c)))
         c))))
  ([iterable cap]
   (let [^Iterator iter (.iterator ^Iterable iterable)]
     (loop [c 0]
       (if (= c cap)
         c
         (if (.hasNext iter)
           (do (.next iter) (recur (unchecked-inc c)))
           c))))))

(defn range-count
  [lmdb dbi-name k-range k-type]
  (scan
    (let [iterable (l/iterate-kv dbi rtx cur k-range k-type nil)]
      (range-count* iterable))
    (raise "Fail to range-count: " e
           {:dbi dbi-name :k-range k-range :k-type k-type})))

(defn key-range
  [lmdb dbi-name k-range k-type]
  (scan
    (let [^Iterable iterable (l/iterate-key dbi rtx cur k-range k-type)
          ^Iterator iter     (.iterator iterable)
          ^SpillableVector holder
          (sp/new-spillable-vector nil (:spill-opts (l/opts lmdb)))]
      (loop []
        (if (.hasNext iter)
          (let [kv (.next iter)]
            (.cons holder (b/read-buffer (l/k kv) k-type))
            (recur))
          holder)))
    (raise "Fail to get key-range: " e
           {:dbi dbi-name :k-range k-range :k-type k-type })))

(defn visit-key-range
  [lmdb dbi-name visitor k-range k-type raw-pred?]
  (scan
    (let [^Iterable iterable (l/iterate-key dbi rtx cur k-range k-type)
          ^Iterator iter     (.iterator iterable)]
      (loop []
        (when (.hasNext iter)
          (let [k   (l/k (.next iter))
                res (if raw-pred?
                      (visitor k)
                      (visitor (b/read-buffer k k-type)))]
            (when-not (identical? res :datalevin/terminate-visit)
              (recur))))))
    (raise "Fail to visit key range: " e
           {:dbi dbi-name :k-range k-range :k-type k-type })))

(defn key-range-count
  [lmdb dbi-name k-range k-type cap]
  (scan
    (let [^Iterable iterable (l/iterate-key dbi rtx cur k-range k-type)]
      (if cap
        (range-count* iterable cap)
        (range-count* iterable)))
    (raise "Fail to get key-range-count: " e
           {:dbi dbi-name :k-range k-range :k-type k-type })))

(defn get-some
  [lmdb dbi-name pred k-range k-type v-type ignore-key? raw-pred?]
  (scan
    (do
      (assert (not (and (= v-type :ignore) ignore-key?))
              "Cannot ignore both key and value")
      (let [iterable       (l/iterate-kv dbi rtx cur k-range k-type v-type)
            ^Iterator iter (.iterator ^Iterable iterable)]
        (loop []
          (when (.hasNext iter)
            (let [kv             (.next iter)
                  ^ByteBuffer kb (l/k kv)
                  ^ByteBuffer vb (l/v kv)]
              (if raw-pred?
                (if (pred kv)
                  (let [v (when (not= v-type :ignore)
                            (b/read-buffer (.rewind vb) v-type))]
                    (if ignore-key?
                      v [(b/read-buffer (.rewind kb) k-type) v]))
                  (recur))
                (let [rk (b/read-buffer kb k-type)
                      rv (b/read-buffer vb v-type)]
                  (if (pred rk rv)
                    (let [v (when (not= v-type :ignore) rv)]
                      (if ignore-key? v [rk v]))
                    (recur)))))))))
    (raise "Fail to get-some: " e
           {:dbi    dbi-name :k-range k-range
            :k-type k-type   :v-type  v-type})))

(defn range-filter
  [lmdb dbi-name pred k-range k-type v-type ignore-key? raw-pred?]
  (scan
    (do
      (assert (not (and (= v-type :ignore) ignore-key?))
              "Cannot ignore both key and value")
      (let [^SpillableVector holder
            (sp/new-spillable-vector nil (:spill-opts (l/opts lmdb)))
            iterable       (l/iterate-kv dbi rtx cur k-range k-type v-type)
            ^Iterator iter (.iterator ^Iterable iterable)]
        (loop []
          (if (.hasNext iter)
            (let [kv             (.next iter)
                  ^ByteBuffer kb (l/k kv)
                  ^ByteBuffer vb (l/v kv)]
              (if raw-pred?
                (if (pred kv)
                  (let [v (when (not= v-type :ignore)
                            (b/read-buffer (.rewind vb) v-type))]
                    (.cons holder (if ignore-key?
                                    v
                                    [(b/read-buffer (.rewind kb) k-type) v]))
                    (recur))
                  (recur))
                (let [rk (b/read-buffer kb k-type)
                      rv (b/read-buffer vb v-type)]
                  (if (pred rk rv)
                    (let [v (when (not= v-type :ignore) rv)]
                      (.cons holder (if ignore-key? v [rk v]))
                      (recur))
                    (recur)))))
            holder))))
    (raise "Fail to range-filter: " e
           {:dbi    dbi-name :k-range k-range
            :k-type k-type   :v-type  v-type})))

(defn- range-keep*
  [^Iterable iterable ^SpillableVector holder pred k-type v-type raw-pred?]
  (let [^Iterator iter (.iterator iterable)]
    (loop []
      (if (.hasNext iter)
        (let [kv (.next iter)]
          (if raw-pred?
            (do (when-let [res (pred kv)] (.cons holder res))
                (recur))
            (let [^ByteBuffer kb (l/k kv)
                  ^ByteBuffer vb (l/v kv)
                  rk             (b/read-buffer kb k-type)
                  rv             (b/read-buffer vb v-type)]
              (when-let [res (pred rk rv)] (.cons holder res))
              (recur))))
        holder))))

(defn range-keep
  [lmdb dbi-name pred k-range k-type v-type raw-pred?]
  (scan
    (let [holder (sp/new-spillable-vector nil (:spill-opts (l/opts lmdb)))

          iterable (l/iterate-kv dbi rtx cur k-range k-type v-type)]
      (range-keep* iterable holder pred k-type v-type raw-pred?))
    (raise "Fail to range-keep: " e
           {:dbi    dbi-name :k-range k-range
            :k-type k-type   :v-type  v-type})))

(defn- range-some*
  [iterable pred k-type v-type raw-pred?]
  (let [^Iterator iter (.iterator ^Iterable iterable)]
    (loop []
      (when (.hasNext iter)
        (let [kv (.next iter)]
          (if raw-pred?
            (or (pred kv)
                (recur))
            (or (pred (b/read-buffer (l/k kv) k-type)
                      (b/read-buffer (l/v kv) v-type))
                (recur))))))))

(defn range-some
  [lmdb dbi-name pred k-range k-type v-type raw-pred?]
  (scan
    (let [iterable (l/iterate-kv dbi rtx cur k-range k-type v-type)]
      (range-some* iterable pred k-type v-type raw-pred?))
    (raise "Fail to find some in range: " e
           {:dbi dbi-name :key-range k-range})))

(defn- filter-count*
  ([iterable pred raw-pred? k-type v-type]
   (let [^Iterator iter (.iterator ^Iterable iterable)]
     (loop [c 0]
       (if (.hasNext iter)
         (let [kv (.next iter)]
           (if raw-pred?
             (if (pred kv)
               (recur (unchecked-inc c))
               (recur c))
             (if (pred (b/read-buffer (l/k kv) k-type)
                       (when v-type (b/read-buffer (l/v kv) v-type)))
               (recur (unchecked-inc c))
               (recur c))))
         c))))
  ([iterable pred raw-pred? k-type v-type cap]
   (let [^Iterator iter (.iterator ^Iterable iterable)]
     (loop [c 0]
       (if (= c cap)
         c
         (if (.hasNext iter)
           (let [kv (.next iter)]
             (if raw-pred?
               (if (pred kv)
                 (recur (unchecked-inc c))
                 (recur c))
               (if (pred (b/read-buffer (l/k kv) k-type)
                         (when v-type (b/read-buffer (l/v kv) v-type)))
                 (recur (unchecked-inc c))
                 (recur c))))
           c))))))

(defn range-filter-count
  [lmdb dbi-name pred k-range k-type v-type raw-pred?]
  (scan
    (let [iterable (l/iterate-kv dbi rtx cur k-range k-type v-type)]
      (filter-count* iterable pred raw-pred? k-type v-type))
    (raise "Fail to range-filter-count: " e
           {:dbi dbi-name :k-range k-range :k-type k-type :v-type v-type})))

(defn- visit*
  [iterable visitor raw-pred? k-type v-type]
  (let [^Iterator iter (.iterator ^Iterable iterable)]
    (loop []
      (when (.hasNext iter)
        (let [kv  (.next iter)
              res (if raw-pred?
                    (visitor kv)
                    (visitor (b/read-buffer (l/k kv) k-type)
                             (b/read-buffer (l/v kv) v-type)))]
          (when-not (identical? res :datalevin/terminate-visit)
            (recur)))))))

(defn visit
  [lmdb dbi-name visitor k-range k-type v-type raw-pred?]
  (scan
    (let [iterable (l/iterate-kv dbi rtx cur k-range k-type v-type)]
      (visit* iterable visitor raw-pred? k-type v-type))
    (raise "Fail to visit: " e
           {:dbi dbi-name :k-range k-range :k-type k-type :v-type v-type})))

(defn list-range
  [lmdb dbi-name k-range k-type v-range v-type]
  (scan
    (let [^SpillableVector holder
          (sp/new-spillable-vector nil (:spill-opts (l/opts lmdb)))
          iterable       (l/iterate-list dbi rtx cur k-range k-type
                                         v-range v-type)
          ^Iterator iter (.iterator ^Iterable iterable)]
      (loop []
        (if (.hasNext iter)
          (let [kv (.next iter)]
            (.cons holder [(b/read-buffer (l/k kv) k-type)
                           (b/read-buffer (l/v kv) v-type)])
            (recur))
          holder)))
    (raise "Fail to get list range: " e
           {:dbi dbi-name :key-range k-range :val-range v-range})) )

(defn list-range-first
  [lmdb dbi-name k-range k-type v-range v-type]
  (scan
    (let [iterable       (l/iterate-list dbi rtx cur k-range k-type
                                         v-range v-type)
          ^Iterator iter (.iterator ^Iterable iterable)]
      (when (.hasNext iter)
        (let [kv (.next iter)]
          [(b/read-buffer (l/k kv) k-type)
           (b/read-buffer (l/v kv) v-type)])))
    (raise "Fail to get first of list range: " e
           {:dbi dbi-name :key-range k-range :val-range v-range})) )

(defn list-range-first-n
  [lmdb dbi-name n k-range k-type v-range v-type]
  (scan
    (let [^SpillableVector holder
          (sp/new-spillable-vector nil (:spill-opts (l/opts lmdb)))
          iterable       (l/iterate-list dbi rtx cur k-range k-type
                                         v-range v-type)
          ^Iterator iter (.iterator ^Iterable iterable)]
      (loop [i 0]
        (if (and (< i ^long n) (.hasNext iter))
          (let [kv (.next iter)]
            (.cons holder [(b/read-buffer (l/k kv) k-type)
                           (b/read-buffer (l/v kv) v-type)])
            (recur (inc i)))
          holder)))
    (raise "Fail to get first n of list range: " e
           {:dbi dbi-name :key-range k-range :val-range v-range})) )

(defn list-range-count
  [lmdb dbi-name k-range k-type v-range v-type cap]
  (scan
    (let [iterable (l/iterate-list dbi rtx cur k-range k-type
                                   v-range v-type)]
      (if cap
        (range-count* iterable cap)
        (range-count* iterable)))
    (raise "Fail to get list range count: " e
           {:dbi dbi-name :key-range k-range :val-range v-range})) )

(defn list-range-filter
  [lmdb dbi-name pred k-range k-type v-range v-type raw-pred?]
  (scan
    (let [^SpillableVector holder
          (sp/new-spillable-vector nil (:spill-opts (l/opts lmdb)))
          iterable       (l/iterate-list dbi rtx cur k-range k-type
                                         v-range v-type)
          ^Iterator iter (.iterator ^Iterable iterable)]
      (loop []
        (if (.hasNext iter)
          (let [kv             (.next iter)
                ^ByteBuffer kb (l/k kv)
                ^ByteBuffer vb (l/v kv)]
            (if raw-pred?
              (if (pred kv)
                (do (.cons holder [(b/read-buffer (.rewind kb) k-type)
                                   (b/read-buffer (.rewind vb) v-type)])
                    (recur))
                (recur))
              (let [rk (b/read-buffer kb k-type)
                    rv (b/read-buffer vb v-type)]
                (if (pred rk rv)
                  (do (.cons holder [rk rv])
                      (recur))
                  (recur)))))
          holder)))
    (raise "Fail to filter list range: " e
           {:dbi dbi-name :key-range k-range :val-range v-range})) )

(defn list-range-keep
  [lmdb dbi-name pred k-range k-type v-range v-type raw-pred?]
  (scan
    (let [^SpillableVector holder
          (sp/new-spillable-vector nil (:spill-opts (l/opts lmdb)))
          iterable (l/iterate-list dbi rtx cur k-range k-type
                                   v-range v-type)]
      (range-keep* iterable holder pred k-type v-type raw-pred?))
    (raise "Fail to keep list range: " e
           {:dbi dbi-name :key-range k-range :val-range v-range})))

(defn- range-some*
  [iterable pred k-type v-type raw-pred?]
  (let [^Iterator iter (.iterator ^Iterable iterable)]
    (loop []
      (when (.hasNext iter)
        (let [kv (.next iter)]
          (if raw-pred?
            (or (pred kv) (recur))
            (or (pred (b/read-buffer (l/k kv) k-type)
                      (b/read-buffer (l/v kv) v-type))
                (recur))))))))

(defn list-range-some
  [lmdb dbi-name pred k-range k-type v-range v-type raw-pred?]
  (scan
    (let [iterable (l/iterate-list dbi rtx cur k-range k-type
                                   v-range v-type)]
      (range-some* iterable pred k-type v-type raw-pred?))
    (raise "Fail to find some in list range: " e
           {:dbi dbi-name :key-range k-range :val-range v-range})))

(defn list-range-filter-count
  [lmdb dbi-name pred k-range k-type v-range v-type raw-pred? cap]
  (scan
    (let [iterable (l/iterate-list dbi rtx cur k-range k-type
                                   v-range v-type)]
      (if cap
        (filter-count* iterable pred raw-pred? k-type v-type cap)
        (filter-count* iterable pred raw-pred? k-type v-type)))
    (raise "Fail to count filtered list range: " e
           {:dbi dbi-name :key-range k-range :val-range v-range})))

(defn visit-list-range
  [lmdb dbi-name visitor k-range k-type v-range v-type raw-pred?]
  (scan
    (let [iterable (l/iterate-list dbi rtx cur k-range k-type
                                   v-range v-type)]
      (visit* iterable visitor raw-pred? k-type v-type))
    (raise "Fail to visit list range: " e
           {:dbi dbi-name :key-range k-range :val-range v-range})))

(defn operate-list-val-range
  [lmdb dbi-name operator v-range v-type]
  (scan
    (let [iterable (l/iterate-list-val dbi rtx cur v-range v-type)]
      (operator iterable))
    (raise "Fail to operate list val range: " e
           {:dbi dbi-name :val-range v-range})))
