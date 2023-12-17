(ns ^:no-doc datalevin.built-ins
  (:require
   [clojure.string :as str]
   ;; [java-time.api :as t]
   ;; [java-time.core :as tc
   ;;  :refer [Plusable Minusable Convert As KnowsTimeBetween]]
   [datalevin.db :as db]
   [datalevin.storage :as st]
   [datalevin.datom :as dd]
   [datalevin.search :as s]
   [datalevin.entity :as de]
   [datalevin.util :as u #?(:cljs :refer-macros :clj :refer) [raise]])
  #?(:clj
     (:import
      [java.util Date]
      [java.time Instant]
      [datalevin.storage Store]
      [datalevin.search SearchEngine]
      [datalevin.db DB])))

;; time

;; (defn- -now [] (t/java-date (t/instant )))

;; (defn- utc-date-time [d] (t/local-date-time (t/instant d) "UTC"))

;; (extend-type Date
;;   Plusable
;;   (seq-plus [d xs]
;;     (Date/from ^Instant (tc/seq-plus (t/instant d) xs)))

;;   Minusable
;;   (seq-minus [d xs]
;;     (Date/from ^Instant (tc/seq-minus (t/instant d) xs)))

;;   Convert
;;   (-convert [x y]
;;     (if (instance? Date y)
;;       y
;;       (Date/from ^Instant (t/instant y))))

;;   As
;;   (as* [o k]
;;     (t/as (utc-date-time o) k))

;;   KnowsTimeBetween
;;   (time-between [o e u]
;;     (t/time-between (utc-date-time o) (utc-date-time e) u)))

(defn- -differ?
  [& xs]
  (let [l (count xs)]
    (not= (take (/ l 2) xs) (drop (/ l 2) xs))))

(defn- -get-else
  [db e a else-val]
  (when (nil? else-val)
    (raise "get-else: nil default value is not supported" {:error :query/where}))
  (if-some [datom (db/-first db [(db/entid db e) a])]
    (:v datom)
    else-val))

(defn- -get-some
  [db e & as]
  (reduce
    (fn [_ a]
      (when-some [datom (db/-first db [(db/entid db e) a])]
        (reduced [(:a datom) (:v datom)])))
    nil
    as))

(defn- -missing?
  [db e a]
  (nil? (get (de/entity db e) a)))

(defn- and-fn [& args]
  (reduce (fn [a b]
            (if b b (reduced b))) true args))

(defn- or-fn [& args]
  (reduce (fn [a b]
            (if b (reduced b) b)) nil args))

(defn- fulltext*
  [store lmdb engines query opts domain]
  (let [engine (engines domain)]
    (sequence
      (map (fn [d]
             (if (= :g (nth d 0))
               (st/gt->datom lmdb (peek d))
               (st/e-aid-v->datom store d))))
      (s/search engine query opts))))

(defn fulltext
  ([db query]
   (fulltext db query nil))
  ([^DB db arg1 arg2]
   (let [^Store store (.-store db)
         lmdb         (.-lmdb store)
         engines      (.-search-engines store)
         datomic?     (keyword? arg1)
         domains      (if datomic?
                        [(u/keyword->string arg1)]
                        (:domains arg2))
         query        (if datomic? arg2 arg1)
         opts         (if datomic? nil arg2)]
     (when datomic?
       (when-not (-> store st/schema arg1 :db.fulltext/autoDomain)
         (raise (str ":db.fulltext/autoDomain is not true for " arg1) {})))
     (sequence
       (mapcat #(fulltext* store lmdb engines query opts %))
       (if (seq domains) domains (keys engines))))))

#_(defn- typed-compare
    [x y]
    (try
      (dd/compare-with-type x y)
      (catch ClassCastException e
        (cond
          (t/= x y) 0
          (t/< x y) -1
          :else     1))
      (catch Exception e (throw e))))

(defn- less
  ([x] true)
  ([x y] (neg? ^long #_(typed-compare x y)
               (dd/compare-with-type x y)))
  ([x y & more]
   (if (less x y)
     (if (next more)
       (recur y (first more) (next more))
       (less y (first more)))
     false)))

(defn- greater
  ([x] true)
  ([x y] (pos? ^long #_(typed-compare x y)
               (dd/compare-with-type x y)))
  ([x y & more]
   (if (greater x y)
     (if (next more)
       (recur y (first more) (next more))
       (greater y (first more)))
     false)))

(defn- less-equal
  ([x] true)
  ([x y] (not (pos? ^long #_(typed-compare x y)
                    (dd/compare-with-type x y))))
  ([x y & more]
   (if (less-equal x y)
     (if (next more)
       (recur y (first more) (next more))
       (less-equal y (first more)))
     false)))

(defn- greater-equal
  ([x] true)
  ([x y] (not (neg? ^long #_(typed-compare x y)
                    (dd/compare-with-type x y))))
  ([x y & more]
   (if (greater-equal x y)
     (if (next more)
       (recur y (first more) (next more))
       (greater-equal y (first more)))
     false)))

#_(defn- not-equal
    ([x] false)
    ([x y] (not (t/= x y)))
    ([x y & more] (not (apply t/= x y more))))

(def query-fns {
                ;; '=                           t/=,
                '=                           =,
                '==                          ==,
                ;; 'not=                        not-equal
                ;; '!=                          not-equal,
                'not=                        not=
                '!=                          not=,
                '<                           less
                '>                           greater
                '<=                          less-equal
                '>=                          greater-equal
                ;; '+                           t/plus
                ;; '-                           t/minus
                '+                           +
                '-                           -
                '*                           *,
                '/                           /,
                'quot                        quot,
                'rem                         rem,
                'mod                         mod,
                'inc                         inc,
                'dec                         dec,
                'max                         max,
                'min                         min,
                'zero?                       zero?,
                'pos?                        pos?,
                'neg?                        neg?,
                'even?                       even?,
                'odd?                        odd?,
                'compare                     compare,
                'rand                        rand,
                'rand-int                    rand-int,
                'true?                       true?,
                'false?                      false?,
                'nil?                        nil?,
                'some?                       some?,
                'not                         not,
                'and                         and-fn,
                'or                          or-fn,
                'complement                  complement,
                'identical?                  identical?,
                'identity                    identity,
                'keyword                     keyword,
                'meta                        meta,
                'name                        name,
                'namespace                   namespace,
                'type                        type,
                'vector                      vector,
                'list                        list,
                'set                         set,
                'hash-map                    hash-map,
                'array-map                   array-map,
                'count                       count,
                'range                       range,
                'not-empty                   not-empty,
                'empty?                      empty?,
                'contains?                   contains?,
                'str                         str,
                'pr-str                      pr-str,
                'print-str                   print-str,
                'println-str                 println-str,
                'prn-str                     prn-str,
                'subs                        subs,
                'get                         get
                're-find                     re-find,
                're-matches                  re-matches,
                're-seq                      re-seq,
                're-pattern                  re-pattern,
                '-differ?                    -differ?,
                'get-else                    -get-else,
                'get-some                    -get-some,
                'missing?                    -missing?,
                'ground                      identity,
                'fulltext                    fulltext,
                'tuple                       vector,
                'untuple                     identity
                'clojure.string/blank?       str/blank?,
                'clojure.string/includes?    str/includes?,
                'clojure.string/starts-with? str/starts-with?,
                'clojure.string/ends-with?   str/ends-with?
                ;; 'now                         -now
                ;; 'local-date-time             t/local-date-time
                ;; 'as                          t/as
                ;; 'time-between                t/time-between
                ;; 'years                       t/years
                ;; 'months                      t/months
                ;; 'weeks                       t/weeks
                ;; 'days                        t/days
                ;; 'hours                       t/hours
                ;; 'minutes                     t/minutes
                ;; 'seconds                     t/seconds
                ;; 'millis                      t/millis
                ;; 'weekday?                    t/weekday?
                ;; 'weekend?                    t/weekend?
                ;; 'monday?                     t/monday?
                ;; 'tuesday?                    t/tuesday?
                ;; 'wednesday?                  t/wednesday?
                ;; 'thursday?                   t/thursday?
                ;; 'friday?                     t/friday?
                ;; 'saturday?                   t/saturday?
                ;; 'sunday?                     t/sunday?
                })


;; Aggregates

(defn- aggregate-sum [coll]
  (reduce + 0 coll))

(defn- aggregate-avg [coll]
  (/ ^double (aggregate-sum coll) (count coll)))

(defn- aggregate-median [coll]
  (let [terms (sort coll)
        size  (count coll)
        med   (bit-shift-right size 1)]
    (cond-> ^double (nth terms med)
      (even? size)
      (-> (+ ^double (nth terms ^long (dec med)))
          (/ 2)))))

(defn- aggregate-variance [coll]
  (let [mean (aggregate-avg coll)
        sum  (aggregate-sum
               (for [x    coll
                     :let [delta (- ^double x ^double mean)]]
                 (* delta delta)))]
    (/ ^double sum (count coll))))

(defn- aggregate-stddev [coll]
  (#?(:cljs js/Math.sqrt :clj Math/sqrt) (aggregate-variance coll)))

(defn- aggregate-min
  ([coll]
   (reduce
     (fn [acc x]
       (if (neg? (compare x acc))
         x acc))
     (first coll) (next coll)))
  ([n coll]
   (vec
     (reduce (fn [acc x]
               (cond
                 (< (count acc) ^long n)
                 (sort compare (conj acc x))
                 (neg? (compare x (last acc)))
                 (sort compare (conj (butlast acc) x))
                 :else acc))
             [] coll))))

(defn- aggregate-max
  ([coll]
   (reduce
     (fn [acc x]
       (if (pos? (compare x acc))
         x acc))
     (first coll) (next coll)))
  ([n coll]
   (vec
     (reduce (fn [acc x]
               (cond
                 (< (count acc) ^long n)
                 (sort compare (conj acc x))
                 (pos? (compare x (first acc)))
                 (sort compare (conj (next acc) x))
                 :else acc))
             [] coll))))

(defn- aggregate-rand
  ([coll] (rand-nth coll))
  ([n coll] (vec (repeatedly n #(rand-nth coll)))))

(defn- aggregate-sample [n coll]
  (vec (take n (shuffle coll))))

(defn- aggregate-count-distinct [coll]
  (count (distinct coll)))

(def aggregates
  {'sum            aggregate-sum
   'avg            aggregate-avg
   'median         aggregate-median
   'variance       aggregate-variance
   'stddev         aggregate-stddev
   'distinct       set
   'min            aggregate-min
   'max            aggregate-max
   'rand           aggregate-rand
   'sample         aggregate-sample
   'count          count
   'count-distinct aggregate-count-distinct})
