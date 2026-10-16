(ns ^:no-doc datalevin.query.source.inputs
  "Bounded, owned scalar and flat relational query inputs."
  (:require [datalevin.db :as db]
            [datalevin.query.source :as source]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :as value :refer [fail!]]
            [datalevin.util :as u])
  (:import [clojure.lang Seqable]
           [datalevin.parser BindScalar BindTuple BindColl BindIgnore Variable SrcVar]
           [java.util Iterator LinkedHashSet Map]))

(defn- column [binding]
  (cond
    (instance? BindIgnore binding) nil
    (and (instance? BindScalar binding) (instance? Variable (:variable binding)))
    (:symbol (:variable binding))
    :else (fail! :query-source/unsupported :inputs)))

(defn- layout [binding]
  (cond
    (instance? BindIgnore binding) {:kind :ignore}
    (instance? BindScalar binding)
    (if (instance? SrcVar (:variable binding))
      {:kind :source :symbol (:symbol (:variable binding))}
      {:kind :scalar :columns [(column binding)]})
    (instance? BindTuple binding)
    {:kind :tuple :columns (mapv column (:bindings binding))}
    (instance? BindColl binding)
    (let [child (:binding binding)]
      (if (instance? BindTuple child)
        {:kind :relation :columns (mapv column (:bindings child))}
        {:kind :collection :columns [(column child)]}))
    :else (fail! :query-source/unsupported :inputs)))

(defn- item-reader [input]
  (when-not (u/seqable? input) (fail! :query-source/invalid :inputs))
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

(defn- tuple-row [control columns input]
  ;; Read only the declared prefix, matching native tuple binding. Do not count
  ;; or realize an unused tail, or inspect values bound to `_`.
  (let [read! (item-reader input)]
    (reduce (fn [row variable]
              (source/check-active! control)
              (if-let [item (read!)]
                (if variable (assoc row variable (first item)) row)
                (fail! :query-source/invalid :inputs))) {} columns)))

(defn- own-row! [runtime row]
  (run! value/execution-value! (vals row))
  (rt/charge! runtime row)
  (value/owned-copy row))

(defn prepare!
  "Capture inputs before provider acquisition. Relational inputs become local
  join tables; only scalar/tuple bindings enter provider planning requests."
  [runtime parsed inputs]
  (when-not (= (count (:qin parsed)) (count inputs)) (fail! :query-source/invalid :inputs))
  (let [layouts (mapv layout (:qin parsed))
        control (:control runtime)]
    (rt/charge! runtime layouts)
    (reduce
      (fn [result [{:keys [kind columns symbol]} input]]
        (source/check-active! control)
        (case kind
          :ignore result
          :source (if (or (satisfies? source/IQuerySource input) (db/db? input))
                    (assoc-in result [:sources symbol] input)
                    (fail! :query-source/unsupported :inputs))
          (let [scratch (source/reserve! control {:retained-bytes (+ 256 (* 256 (count columns)))})]
            (try
              (let [tuple? (#{:tuple :relation} kind)
                    row #(own-row! runtime
                           (if tuple? (tuple-row control columns %)
                               (if-let [variable (first columns)] {variable %} {})))]
                (if (#{:scalar :tuple} kind)
                  (update result :bindings merge (row input))
                  (let [read! (item-reader input)
                        variables (set (keep identity columns))
                        _ (rt/charge! runtime variables)
                        rows (loop [rows []]
                               (source/check-active! control)
                               (if-let [item (read!)]
                                 (let [owned (row (first item))]
                                   (recur (conj rows owned)))
                                 rows))]
                    (update result :relations conj {:variables variables :rows rows}))))
              (finally (source/release! scratch))))))
      {:sources {} :bindings {} :relations []} (map vector layouts inputs))))

(defn variables [{:keys [bindings relations]}]
  (into (set (keys bindings)) (mapcat :variables) relations))

(defn check-values!
  "Validate every supplied value of a variable, including nullable input rows."
  [control {:keys [bindings relations]} variable check!]
  (when (contains? bindings variable) (check! (bindings variable)))
  (doseq [{:keys [variables rows]} relations :when (contains? variables variable)
          row rows]
    (source/check-active! control)
    (check! (row variable))))

(defn with-entity-ids
  "Visit distinct native entity IDs from a retained relation. Rows remain
  intact for joins, including their duplicates and other columns. Borrow
  already charged Long values; other values cannot match native entity keys."
  [control rows variable f]
  (let [token (source/reserve! control
                {:retained-bytes (value/add-charge 512
                                   (value/multiply-charge 128 (count rows)))})
        ids (LinkedHashSet.)]
    (try
      (doseq [row rows]
        (source/check-active! control)
        (let [id (row variable)]
          (when (instance? Long id) (.add ids id))))
      (source/check-active! control)
      (f ids)
      (finally
        (.clear ids)
        (source/release! token)))))
