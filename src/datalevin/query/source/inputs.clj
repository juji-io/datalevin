(ns ^:no-doc datalevin.query.source.inputs
  "Bounded item and tuple-prefix readers shared by input and collection capture."
  (:require [datalevin.query.source :as source]
            [datalevin.query.source.value :refer [fail!]]
            [datalevin.util :as u])
  (:import [clojure.lang Seqable]
           [java.util Iterator Map]))

(defn item-reader
  "Pull one borrowed item at a time. Own it before advancing a reusable Java
  producer; unlike seq on an Iterable, this path never prefetches a chunk."
  [input]
  (when-not (or (u/seqable? input) (u/array? input)) (fail! :query-source/invalid :inputs))
  ;; seq on a Java Iterable may prefetch a chunk. Read its iterator directly so
  ;; a producer can reuse mutable row buffers after each owned handoff.
  (if (and (not (instance? Seqable input))
           (or (instance? Iterable input) (instance? Map input)))
    (let [^Iterator iterator (if (instance? Map input)
                               (.iterator (.entrySet ^Map input))
                               (.iterator ^Iterable input))]
      #(when (.hasNext iterator) [(.next iterator)]))
    (let [tail (volatile! input) started? (volatile! false)]
      (fn []
        (let [items (if @started? (next @tail) (seq @tail))]
          (vreset! started? true)
          (vreset! tail items)
          (when items [(first items)]))))))

(defn tuple-row
  "Read only a tuple binding's declared prefix, skipping ignored values and
  leaving an unused or unbounded tail untouched. The caller owns the result."
  [control columns input]
  ;; Read only the declared prefix, matching native tuple binding. Do not count
  ;; or realize an unused tail, or inspect values bound to `_`.
  (let [read! (item-reader input)]
    (reduce (fn [row variable]
              (source/check-active! control)
              (if-let [item (read!)]
                (if variable (assoc row variable (first item)) row)
                (fail! :query-source/invalid :inputs))) {} columns)))

