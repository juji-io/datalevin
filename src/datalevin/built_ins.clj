(ns ^:no-doc datalevin.built-ins
  "built-in query functions"
  (:require
   [clojure.string :as str]
   [datalevin.db :as db]
   [datalevin.datom :as dd]
   [datalevin.storage :as st]
   [datalevin.search :as s]
   [datalevin.entity :as de]
   [datalevin.util :as u :refer [raise]])
  (:import
   [datalevin.utl LikeFSM LRUCache]
   [datalevin.storage Store]
   [datalevin.db DB]))

(def like-cache (LRUCache. 512))

(defn- like
  ([input pattern]
   (like input pattern nil false))
  ([input pattern opts]
   (like input pattern opts false))
  ([input ^String pattern {:keys [escape]} not?]
   (let [matcher
         (let [k [pattern escape not?]]
           (or (.get ^LRUCache like-cache k)
               (let [mf (let [pb  (.getBytes pattern)
                              fsm (if escape
                                    (LikeFSM. pb escape)
                                    (LikeFSM. pb))
                              f   #(.match fsm (.getBytes ^String %))]
                          (if not? #(not (f %)) f))]
                 (.put ^LRUCache like-cache k mf)
                 mf)))]
     (matcher input))))

(defn- not-like
  ([input pattern]
   (not-like input pattern nil))
  ([input pattern opts]
   (like input pattern opts true)))

(defn- in
  ([input coll]
   (in input coll false))
  ([input coll not?]
   (assert (and (coll? coll) (not (map? coll)))
           "function `in` expects a collection")
   (let [checker (let [s (set coll)]
                   (if not? #(not (s %)) s))]
     (checker input))))

(defn- not-in [input coll] (in input coll true))

(defn- -differ?
  [& xs]
  (let [l  (count xs)
        hl (/ l 2)]
    (not= (take hl xs) (drop hl xs))))

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

(defn- and-fn
  [& args]
  (reduce (fn [_ b] (if b b (reduced b))) true args))

(defn- or-fn
  [& args]
  (reduce (fn [_ b] (if b (reduced b) b)) nil args))

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

(defn- less
  ([_] true)
  ([x y]
   (neg? ^long (dd/compare-with-type x y)))
  ([x y & more]
   (if (less x y)
     (if (next more)
       (recur y (first more) (next more))
       (less y (first more)))
     false)))

(defn- greater
  ([_] true)
  ([x y] (pos? ^long (dd/compare-with-type x y)))
  ([x y & more]
   (if (greater x y)
     (if (next more)
       (recur y (first more) (next more))
       (greater y (first more)))
     false)))

(defn- less-equal
  ([_] true)
  ([x y] (not (pos? ^long (dd/compare-with-type x y))))
  ([x y & more]
   (if (less-equal x y)
     (if (next more)
       (recur y (first more) (next more))
       (less-equal y (first more)))
     false)))

(defn- greater-equal
  ([_] true)
  ([x y] (not (neg? ^long (dd/compare-with-type x y))))
  ([x y & more]
   (if (greater-equal x y)
     (if (next more)
       (recur y (first more) (next more))
       (greater-equal y (first more)))
     false)))

(defn- smallest
  ([x] x)
  ([x y]
   (if (neg? ^long (dd/compare-with-type x y)) x y))
  ([x y & more]
   (reduce smallest (smallest x y) more)))

(defn- largest
  ([x] x)
  ([x y]
   (if (pos? ^long (dd/compare-with-type x y)) x y))
  ([x y & more]
   (reduce largest (largest x y) more)))

(def query-fns
  {'=                           =,
   '==                          ==,
   'not=                        not=,
   '!=                          not=,
   '<                           less
   '>                           greater
   '<=                          less-equal
   '>=                          greater-equal
   '+                           +,
   '-                           -,
   '*                           *,
   '/                           /,
   'quot                        quot,
   'rem                         rem,
   'mod                         mod,
   'inc                         inc,
   'dec                         dec,
   'max                         largest,
   'min                         smallest,
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
   'like                        like
   'not-like                    not-like
   'in                          in
   'not-in                      not-in
   'clojure.string/blank?       str/blank?,
   'clojure.string/includes?    str/includes?,
   'clojure.string/starts-with? str/starts-with?,
   'clojure.string/ends-with?   str/ends-with?})

;; Aggregates

(defn- aggregate-sum [coll] (reduce + 0 coll))

(defn aggregate-avg [coll] (/ ^double (aggregate-sum coll) (count coll)))

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

(defn- aggregate-stddev [coll] (Math/sqrt (aggregate-variance coll)))

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
