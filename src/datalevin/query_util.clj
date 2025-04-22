;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.query-util
  "Some utility functions for Datalog query processing"
  (:require
   [clojure.string :as str]
   [datalevin.util :as u :refer [cond+ raise]]))

(defn single
  [coll]
  (assert (nil? (next coll)) "Expected single element")
  (first coll))

(defn intersect-keys
  [attrs1 attrs2]
  (u/intersection (set (keys attrs1)) (set (keys attrs2))))

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
