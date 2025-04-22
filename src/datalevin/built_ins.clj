;;
;; Copyright (c) Nikita Prokopov, Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns datalevin.built-ins
  "Built-in predicates or functions for Datalog query, i.e. used in `q`"
  (:refer-clojure :exclude [and or < > <= >= min max = == not= + - * / quot rem
                            mod inc dec zero? pos? neg? even? odd? compare rand
                            rand-int true? false? nil? some? not complement
                            identical? identity keyword meta name namespace
                            type vector list set hash-map array-map count
                            range not-empty empty? contains? str print-str
                            println-str prn-str subs get re-find re-matches
                            re-seq re-pattern distinct])
  (:require
   [datalevin.db :as db]
   [datalevin.datom :as dd]
   [datalevin.storage :as st]
   [datalevin.search :as s]
   [datalevin.vector :as v]
   [datalevin.entity :as de]
   [datalevin.util :as u :refer [raise]])
  (:import
   [java.nio.charset StandardCharsets]
   [datalevin.utl LikeFSM LRUCache]
   [datalevin.storage Store]
   [datalevin.db DB]))

(def ^:no-doc like-cache (LRUCache. 256))

(defn like
  "Predicate similar to `LIKE` in SQL, e.g. `[(like ?name \"%Smith\")]`"
  ([input pattern]
   (like input pattern nil false))
  ([input pattern opts]
   (like input pattern opts false))
  ([input ^String pattern {:keys [escape]} not?]
   (let [matcher
         (let [k [pattern escape not?]]
           (clojure.core/or
             (.get ^LRUCache like-cache k)
             (let [mf (let [pb  (.getBytes pattern StandardCharsets/UTF_8)
                            fsm (if escape
                                  (LikeFSM. pb escape)
                                  (LikeFSM. pb))
                            f   #(.match fsm
                                         (.getBytes ^String %
                                                    StandardCharsets/UTF_8))]
                        (if not? #(clojure.core/not (f %)) f))]
               (.put ^LRUCache like-cache k mf)
               mf)))]
     (matcher input))))

(defn not-like
  "Predicate similar to `NOT LIKE` in SQL, e.g. `[(no-like ?name \"%Smith\")]`"
  ([input pattern]
   (not-like input pattern nil))
  ([input pattern opts]
   (like input pattern opts true)))

(defn in
  "Predicate similar to `IN` in SQL, e.g.
  `[(in ?name [\"Smith\" \"Cohen\" \"Doe\"])]`"
  ([input coll]
   (in input coll false))
  ([input coll not?]
   (assert (clojure.core/and (coll? coll) (clojure.core/not (map? coll)))
           "function `in` expects a collection")
   (let [checker (let [s (clojure.core/set coll)]
                   (if not? #(clojure.core/not (s %)) s))]
     (checker input))))

(defn not-in
  "Predicate similar to `NOT IN` in SQL,
  e.g. `[(not-in ?name [\"Smith\" \"Cohen\" \"Doe\"])]`"
  [input coll] (in input coll true))

(defn- -differ?
  [& xs]
  (let [l  (clojure.core/count xs)
        hl (clojure.core// l 2)]
    (clojure.core/not= (take hl xs) (drop hl xs))))

(defn get-else
  "Function. Return the value of attribute `a` of entity `e`, or `else-val` if
  it doesn't exist. e.g. `[(get-else $ ?a :artist/name \"N/A\") ?name]`"
  [db e a else-val]
  (when (clojure.core/nil? else-val)
    (raise "get-else: nil default value is not supported" {:error :query/where}))
  (if-some [datom (db/-first db [(db/entid db e) a])]
    (:v datom)
    else-val))

(defn get-some
  "Function. Takes a DB, an entity, and one or more cardinality one attributes,
   return a tuple of the first found attribute and its value. e.g.
  `[(get-some $ ?e :country :artist :book) [?attr ?val]]`"
  [db e & as]
  (unreduced
    (reduce
      (fn [_ a]
        (when-some [datom (db/-first db [(db/entid db e) a])]
          (reduced [(:a datom) (:v datom)])))
      nil
      as)))

(def ground
  "Function. Same as Clojure `identity`. E.g.
  `[(ground [:a :e :i :o :u]) [?vowel ...]]`"
  clojure.core/identity)

(defn missing?
  "Predicate that returns true if the entity has no value for the attribute in DB
  e.g. [(missing? $ ?e :sales)]"
  [db e a]
  (clojure.core/nil? (clojure.core/get (de/entity db e) a)))

(defn- and-fn
  [& args]
  (unreduced (reduce (fn [_ b] (if b b (reduced b))) true args)))

(def and
  "Predicate that is similar to Clojure `and`
  e.g. [(and (= ?g \"f\"\") (like ?n \"A%\"\"))]"
  and-fn)

(defn- or-fn
  [& args]
  (unreduced (reduce (fn [_ b] (if b (reduced b) b)) nil args)))

(def or
  "Predicate that is similar to Clojure `or`
  e.g. [(or (= ?g \"f\"\") (like ?n \"A%\"\"))]"
  or-fn)

(defn- fulltext*
  [store lmdb engines query opts domain]
  (let [engine (engines domain)]
    (sequence
      (map (fn [d]
             (if (clojure.core/= :g (nth d 0))
               (st/gt->datom lmdb (peek d))
               (st/e-aid-v->datom store d))))
      (s/search engine query opts))))

(defn fulltext
  "Function that does fulltext search. Returns matching datoms in the form
  of [e a v] for convenient vector destructuring.

  The last argument of the 4 arity function is the search option map.
  See [[datalevin.core.search]].

  When neither an attribute nor a `:domans` is specified, a full DB search
  is performed.

  For example:

  * Full DB search: `[(fulltext $ \"red\") [[?e ?a ?v]]]`

  * Attribute specific search: `[(fulltext $ :color \"red\") [[?e ?a ?v]]]`

  * Domain specific search:

    `[(fulltext $ \"red\" {:domains [\"color\"]} [[?e ?a ?v]])]`"
  ([db query]
   (fulltext db query nil))
  ([db arg1 arg2]
   (fulltext db arg1 arg2 nil))
  ([^DB db arg1 arg2 arg3]
   (let [^Store store (.-store db)
         lmdb         (.-lmdb store)
         engines      (.-search-engines store)
         attr?        (keyword? arg1)
         domains      (if attr?
                        [(u/keyword->string arg1)]
                        (:domains arg2))
         query        (if attr? arg2 arg1)
         opts         (if attr? arg3 arg2)]
     (when attr?
       (when-not (-> store st/schema arg1 :db.fulltext/autoDomain)
         (raise ":db.fulltext/autoDomain is not true for " arg1
                {})))
     (sequence
       (comp (mapcat #(fulltext* store lmdb engines query opts %))
          dd/datom->vec-xf)
       (if (seq domains) domains (keys engines))))))

(defn- vec-neighbors*
  [store lmdb indices query opts domain]
  (when-let [index (indices domain)]
    (sequence
      (map (fn [d]
             (if (clojure.core/= :g (nth d 0))
               (st/gt->datom lmdb (peek d))
               (st/e-aid-v->datom store d))))
      (v/search-vec index query opts))))

(defn vec-neighbors
  "Function that does vector similarity search. Returns matching datoms in the
  form of [e a v] for convenient vector destructuring.

  The last argument of the 4 arity function is the search option map.
  See [[datalevin.core.search-vec]].

  When neither an attribute nor a `:domains` is specified, an exception will
  be thrown.

  For example:

  * Attribute specific search:
         `[(vec-neighbors $ :color ?query-vec) [[?e ?a ?v]]]`

  * Domain specific search:
        `[(vec-neighbors $ ?query-vec {:domains [\"color\"]} [[?e ?a ?v]])]`"
  ([db query]
   (vec-neighbors db query nil))
  ([db arg1 arg2]
   (vec-neighbors db arg1 arg2 nil))
  ([^DB db arg1 arg2 arg3]
   (let [^Store store (.-store db)
         lmdb         (.-lmdb store)
         indices      (.-vector-indices store)
         attr?        (keyword? arg1)
         domains      (if attr?
                        [(v/attr-domain arg1)]
                        (:domains arg2))
         query        (if attr? arg2 arg1)
         opts         (if attr? arg3 arg2)]
     (sequence
       (comp (mapcat #(vec-neighbors* store lmdb indices query opts %))
          dd/datom->vec-xf)
       (if (and (sequential? domains) (seq domains))
         domains
         (raise "Need a vector search domain." {}))))))

(defn- less
  ([_] true)
  ([x y]
   (clojure.core/neg? ^long (dd/compare-with-type x y)))
  ([x y & more]
   (if (less x y)
     (if (next more)
       (recur y (first more) (next more))
       (less y (first more)))
     false)))

(def <
  "Predicate similar to Clojure `<`"
  less)

(defn- greater
  ([_] true)
  ([x y] (clojure.core/pos? ^long (dd/compare-with-type x y)))
  ([x y & more]
   (if (greater x y)
     (if (next more)
       (recur y (first more) (next more))
       (greater y (first more)))
     false)))

(def >
  "Predicate similar to Clojure `>`"
  greater)

(defn- less-equal
  ([_] true)
  ([x y]
   (clojure.core/not (clojure.core/pos? ^long (dd/compare-with-type x y))))
  ([x y & more]
   (if (less-equal x y)
     (if (next more)
       (recur y (first more) (next more))
       (less-equal y (first more)))
     false)))

(def <=
  "Predicate similar to Clojure `<=`"
  less-equal)

(defn- greater-equal
  ([_] true)
  ([x y]
   (clojure.core/not (clojure.core/neg? ^long (dd/compare-with-type x y))))
  ([x y & more]
   (if (greater-equal x y)
     (if (next more)
       (recur y (first more) (next more))
       (greater-equal y (first more)))
     false)))

(def >=
  "Predicate similar to Clojure `>=`"
  greater-equal)

(defn- smallest
  ([x] x)
  ([x y]
   (if (clojure.core/neg? ^long (dd/compare-with-type x y)) x y))
  ([x y & more]
   (reduce smallest (smallest x y) more)))

(def min
  "Function similar to Clojure `min`"
  smallest)

(defn- largest
  ([x] x)
  ([x y]
   (if (clojure.core/pos? ^long (dd/compare-with-type x y)) x y))
  ([x y & more]
   (reduce largest (largest x y) more)))

(def max
  "function similar to Clojure `max`"
  largest)

(def =
  "Predicate similar to Clojure `=`"
  clojure.core/=)

(def ==
  "Predicate similar to Clojure `==`"
  clojure.core/==)

(def not=
  "Predicate similar to Clojure `not=`"
  clojure.core/not=)

(def !=
  "Predicate similar to Clojure `not=`"
  clojure.core/not=)

(def +
  "Function similar to Clojure `+`"
  clojure.core/+)

(def -
  "Function similar to Clojure `-`"
  clojure.core/-)

(def *
  "Function similar to Clojure `*`"
  clojure.core/*)

(def /
  "Function similar to Clojure `/`"
  clojure.core//)

(def quot
  "Function similar to Clojure `quot`"
  clojure.core/quot)

(def rem
  "Function similar to Clojure `rem`"
  clojure.core/rem)

(def mod
  "Function similar to Clojure `mod`"
  clojure.core/mod)

(def inc
  "Function similar to Clojure `inc`"
  clojure.core/inc)

(def dec
  "Function similar to Clojure `dec`"
  clojure.core/dec)

(def zero?
  "Predicate similar to Clojure `zero?`"
  clojure.core/zero?)

(def pos?
  "Predicate similar to Clojure `pos?`"
  clojure.core/pos?)

(def neg?
  "Predicate similar to Clojure `neg?`"
  clojure.core/neg?)

(def even?
  "Predicate similar to Clojure `even?`"
  clojure.core/even?)

(def odd?
  "Predicate similar to Clojure `odd?`"
  clojure.core/odd?)

(def compare
  "Function similar to Clojure `compare`"
  clojure.core/compare)

(def rand
  "Function similar to Clojure `rand`"
  clojure.core/rand)

(def rand-int
  "Function similar to Clojure `rand-int`"
  clojure.core/rand-int)

(def true?
  "Predicate similar to Clojure `true?`"
  clojure.core/true?)

(def false?
  "Predicate similar to Clojure `false?`"
  clojure.core/false?)

(def nil?
  "Predicate similar to Clojure `nil?`"
  clojure.core/nil?)

(def some?
  "Predicate similar to Clojure `some?`"
  clojure.core/some?)

(def not
  "Predicate similar to Clojure `not`"
  clojure.core/not)

(def complement
  "Function similar to Clojure `complement`"
  clojure.core/complement)

(def identical?
  "Predicate similar to Clojure `identical?`"
  clojure.core/identical?)

(def identity
  "Function similar to Clojure `identity`"
  clojure.core/identity)

(def keyword
  "Function similar to Clojure `keyword`"
  clojure.core/keyword)

(def meta
  "Function similar to Clojure `meta`"
  clojure.core/meta)

(def name
  "Function similar to Clojure `name`"
  clojure.core/name)

(def namespace
  "Function similar to Clojure `namespace`"
  clojure.core/namespace)

(def type
  "Function similar to Clojure `type`"
  clojure.core/type)

(def vector
  "Function similar to Clojure `vector`"
  clojure.core/vector)

(def list
  "Function similar to Clojure `list`"
  clojure.core/list)

(def set
  "Function similar to Clojure `set`"
  clojure.core/set)

(def hash-map
  "Function similar to Clojure `hash-map`"
  clojure.core/hash-map)

(def array-map
  "Function similar to Clojure `array-map`"
  clojure.core/array-map)

(def count
  "Function similar to Clojure `count`"
  clojure.core/count)

(def range
  "Function similar to Clojure `range`"
  clojure.core/range)

(def not-empty
  "Function similar to Clojure `not-empty`"
  clojure.core/not-empty)

(def empty?
  "Function similar to Clojure `empty?`"
  clojure.core/empty?)

(def contains?
  "Function similar to Clojure `contains?`"
  clojure.core/contains?)

(def str
  "Function similar to Clojure `str`"
  clojure.core/str)

(def print-str
  "Function similar to Clojure `print-str`"
  clojure.core/print-str)

(def println-str
  "Function similar to Clojure `println-str`"
  clojure.core/println-str)

(def prn-str
  "Function similar to Clojure `prn-str`"
  clojure.core/prn-str)

(def subs
  "Function similar to Clojure `subs`"
  clojure.core/subs)

(def get
  "Function similar to Clojure `get`"
  clojure.core/get)

(def re-find
  "Function similar to Clojure `re-find`"
  clojure.core/re-find)

(def re-matches
  "Function similar to Clojure `re-matches`"
  clojure.core/re-matches)

(def re-seq
  "Function similar to Clojure `re-seq`"
  clojure.core/re-seq)

(def re-pattern
  "Function similar to Clojure `re-pattern`"
  clojure.core/re-pattern)

(def tuple
  "Function similar to Clojure `vector`"
  clojure.core/vector)

(def untuple
  "Function similar to Clojure `identity`"
  clojure.core/identity)

(def query-fns
  {'=             =,
   '==            ==,
   'not=          not=,
   '!=            not=,
   '<             less
   '>             greater
   '<=            less-equal
   '>=            greater-equal
   '+             +,
   '-             -,
   '*             *,
   '/             /,
   'quot          quot,
   'rem           rem,
   'mod           mod,
   'inc           inc,
   'dec           dec,
   'max           largest,
   'min           smallest,
   'zero?         zero?,
   'pos?          pos?,
   'neg?          neg?,
   'even?         even?,
   'odd?          odd?,
   'compare       compare,
   'rand          rand,
   'rand-int      rand-int,
   'true?         true?,
   'false?        false?,
   'nil?          nil?,
   'some?         some?,
   'not           not,
   'and           and-fn,
   'or            or-fn,
   'complement    complement,
   'identical?    identical?,
   'identity      identity,
   'keyword       keyword,
   'meta          meta,
   'name          name,
   'namespace     namespace,
   'type          type,
   'vector        vector,
   'list          list,
   'set           set,
   'hash-map      hash-map,
   'array-map     array-map,
   'count         count,
   'range         range,
   'not-empty     not-empty,
   'empty?        empty?,
   'contains?     contains?,
   'str           str,
   'pr-str        pr-str,
   'print-str     print-str,
   'println-str   println-str,
   'prn-str       prn-str,
   'subs          subs,
   'get           get
   're-find       re-find,
   're-matches    re-matches,
   're-seq        re-seq,
   're-pattern    re-pattern,
   '-differ?      -differ?,
   'get-else      get-else,
   'get-some      get-some,
   'missing?      missing?,
   'ground        identity,
   'fulltext      fulltext,
   'vec-neighbors vec-neighbors,
   'tuple         vector,
   'untuple       identity
   'like          like
   'not-like      not-like
   'in            in
   'not-in        not-in})

;; Aggregates

(defn- aggregate-sum [coll] (reduce + 0 coll))

(def sum
  "Aggregation function that adds up collection"
  aggregate-sum)

(defn aggregate-avg ^double [coll]
  (/ ^double (aggregate-sum coll) (clojure.core/count coll)))

(def avg
  "Aggregation function that calculates the average."
  aggregate-avg)

(defn- aggregate-median [coll]
  (let [terms (sort coll)
        size  (clojure.core/count coll)
        med   (bit-shift-right size 1)]
    (cond-> ^double (nth terms med)
      (even? size)
      (-> (+ ^double (nth terms ^long (dec med)))
          (/ 2)))))

(def median
  "Aggregation function that calculates the median."
  aggregate-median)

(defn- aggregate-variance ^double [coll]
  (let [mean (aggregate-avg coll)
        sum  (aggregate-sum
               (for [x    coll
                     :let [delta (- ^double x ^double mean)]]
                 (* delta delta)))]
    (/ ^double sum (clojure.core/count coll))))

(def variance
  "Aggregation function that calculates the variance."
  aggregate-variance)

(defn- aggregate-stddev [coll] (Math/sqrt (aggregate-variance coll)))

(def stddev
  "Aggregation function that calculates the stddev."
  aggregate-stddev)

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
                 (< (clojure.core/count acc) ^long n)
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
                 (< (clojure.core/count acc) ^long n)
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

(defn sample
  "Aggregation function that randomly sample from a collection."
  [n coll]
  (aggregate-sample n coll))

(defn- aggregate-count-distinct [coll]
  (clojure.core/count (clojure.core/distinct coll)))

(defn distinct
  "Aggregation function that returns the distinctive values of a collection."
  [coll]
  (set coll))

(defn count-distinct
  "Aggregation function that count the distinctive values of a collection."
  [coll]
  (aggregate-count-distinct coll))

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
