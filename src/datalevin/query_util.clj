(ns ^:no-doc datalevin.query-util
  "Some utility functions for Datalog query processing"
  (:require
   [clojure.string :as str]
   [clojure.set :as set]
   [datalevin.util :as u :refer [raise cond+ conjv concatv]]
   ))

(defn single
  [coll]
  (assert (nil? (next coll)) "Expected single element")
  (first coll))

(defn intersect-keys
  [attrs1 attrs2]
  (set/intersection (set (keys attrs1)) (set (keys attrs2))))

(defn looks-like?
  [pattern form]
  (cond
    (= '_ pattern)    true
    (= '[*] pattern)  (sequential? form)
    (symbol? pattern) (= form pattern)

    (sequential? pattern)
    (if (= (last pattern) '*)
      (and (sequential? form)
           (every? (fn [[pattern-el form-el]] (looks-like? pattern-el form-el))
                   (map vector (butlast pattern) form)))
      (and (sequential? form)
           (= (count form) (count pattern))
           (every? (fn [[pattern-el form-el]] (looks-like? pattern-el form-el))
                   (map vector pattern form))))
    :else ;; (predicate? pattern)
    (pattern form)))

(defn source? [sym] (and (symbol? sym) (= \$ (first (name sym)))))

(defn free-var? [sym] (and (symbol? sym) (= \? (first (name sym)))))

(defn placeholder? [sym]
  (and (symbol? sym) (str/starts-with? (name sym) "?placeholder")))

(defn lookup-ref? [form] (looks-like? [keyword? '_] form))

(def rule-head #{'_ 'or 'or-join 'and 'not 'not-join})

(defn rule?
  [context clause]
  (cond+
    (not (sequential? clause))
    false

    :let [head (if (source? (first clause))
                 (second clause)
                 (first clause))]

    (not (symbol? head))
    false

    (free-var? head)
    false

    (contains? rule-head head)
    false

    (not (contains? (:rules context) head))
    (raise "Unknown rule '" head " in " clause
           {:error :query/where :form clause})

    :else true))

(defn collect-vars [clause] (set (u/walk-collect clause free-var?)))
