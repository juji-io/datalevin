(ns ^:no-doc datalevin.query.source.collection
  "Invocation-owned local collection sources and controlled shared matching."
  (:require [datalevin.db :as db]
            [datalevin.query.resolve.pattern :as pattern]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.inputs :as inputs]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :as value :refer [fail!]]
            [datalevin.util :as u])
  (:import [datalevin.parser Constant Placeholder Variable]
           [java.util IdentityHashMap List]
           [org.eclipse.collections.impl.list.mutable FastList]))

(defrecord ^:private OwnedCollection [control query-id closed? rows])

(defn owned? [x] (instance? OwnedCollection x))

(defn collection? [x]
  (or (owned? x) (u/seqable? x) (u/array? x)))

(defn- retain! [runtime token]
  (rt/own! runtime #(control/release! token)))

(defn- containers! [runtime rows width]
  (when (> rows Integer/MAX_VALUE) (fail! :query-source/limit :inputs))
  (retain! runtime
    (control/reserve! (:control runtime)
      {:retained-bytes (+' 1024 (*' rows (+' 192 (*' 32 width))))})))

(defn check!
  "An owned source is usable only within its capturing invocation."
  [runtime source]
  (control/check-active! (:control runtime))
  (when-not (and (owned? source) (identical? (:control runtime) (:control source))
                 (= (:query-id runtime) (:query-id source)) (not @(:closed? source)))
    (fail! :query-source/invalid :inputs)))

(defn capture!
  "Own every local fact before a caller iterator advances or a provider runs.
  A collection source has no declared row width, so its finite rows are owned
  completely. Unbounded producers stop at the invocation quota/deadline."
  [runtime input]
  (if (owned? input)
    (do (check! runtime input) input)
    (let [check! #(control/check-active! (:control runtime))
          read! (inputs/item-reader input) closed? (atom false)
          _ (containers! runtime 0 0)
          rows (FastList.)]
      (rt/own! runtime #(do (reset! closed? true) (.clear rows)))
      (loop []
        (check!)
        (when-let [[input-row] (read!)]
          (when (= (.size rows) Integer/MAX_VALUE) (fail! :query-source/limit :inputs))
          (containers! runtime 1 0)
          (let [read-value! (inputs/item-reader input-row)
                row (loop [row []]
                      (check!)
                      (if-let [[x] (read-value!)]
                        (do (containers! runtime 1 1)
                            (retain! runtime (rt/charge-copy! runtime x))
                            (value/execution-value! x check!)
                            (recur (conj row (value/owned-copy x check!)))) row))]
            (.add rows row))
          (recur)))
      (->OwnedCollection (:control runtime) (:query-id runtime) closed? rows))))

(defn capture-entries!
  "Detach local sources before the first external descriptor callback. Aliases
  of the same borrowed collection share a single complete owned capture."
  [runtime entries]
  (let [seen (IdentityHashMap.)]
    (mapv (fn [entry]
            (if (and (contains? entry :native) (not (db/db? (:native entry))))
              (let [input (:native entry)
                    captured (or (.get seen input)
                                 (let [source (capture! runtime input)]
                                   (.put seen input source) source))]
                (assoc entry :native captured)) entry)) entries)))

(defn columns
  "Infer conservative column types only from a complete owned local relation."
  [check! rel]
  (let [^List tuples (:tuples rel)]
    (mapv (fn [[v index]]
            (loop [i 0 type nil nullable? false]
              (check!)
              (if (= i (.size tuples))
                {:variable v :type (or type :data) :nullable? nullable?}
                (let [x (aget ^objects (.get tuples i) (int index))
                      t (when (some? x) (value/inferred-type x))]
                  (recur (inc i) (cond (nil? t) type (nil? type) t (= type t) type :else :data)
                         (or nullable? (nil? x)))))))
          (sort-by val (:attrs rel)))))

(defn spec!
  "Build a local pattern's actual typed output through the shared matcher.
  Matching reads owned input only and completes before provider planning."
  [runtime source node]
  (check! runtime source)
  (let [check! #(check! runtime source)
        terms (:pattern node)
        _ (containers! runtime 1 (count terms))
        _ (when-not (every? #(or (instance? Constant %) (instance? Variable %)
                                (instance? Placeholder %)) terms)
            (fail! :query-source/unsupported :planning))
        variables (vec (distinct (keep #(when (instance? Variable %) (:symbol %)) terms)))
        positions (into {} (keep-indexed #(when (instance? Variable %2) [(:symbol %2) %1])) terms)
        literals (into {} (keep-indexed #(when (and (instance? Constant %2)
                                                    (or (= '_ (:value %2))
                                                        (and (symbol? (:value %2))
                                                             (.startsWith (name (:value %2)) "?"))))
                                            [%1 (:value %2)])) terms)
        raw (mapv (fn [i x]
                    (cond (contains? literals i) '_
                          (instance? Constant x) (:value x)
                          (instance? Variable x) (:symbol x)
                          :else '_)) (range) terms)
        emitted (volatile! 0)
        rel (pattern/lookup-pattern-coll-controlled (:rows source) raw
              {:check! check! :columns variables :positions (mapv positions variables)
               :literal-values literals :invalid! #(fail! :query-source/invalid :inputs)
               :admit-row! #(do (vswap! emitted inc)
                                (when (> @emitted Integer/MAX_VALUE) (fail! :query-source/limit :inputs))
                                (containers! runtime 1 (count variables)))})]
    {:requires [] :columns (columns check! rel) :relation rel :collection? true}))
