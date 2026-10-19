(ns ^:no-doc datalevin.query.source.input-program
  "Own ordinary parsed :in bindings before v2 descriptor/session callbacks.
  Binding expansion uses the engine's Relation representation and shared join."
  (:require [datalevin.db :as db]
            [datalevin.parser :as parser]
            [datalevin.query.source :as source]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.inputs :as inputs]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :as value :refer [fail!]]
            [datalevin.relation :as relation])
  (:import [datalevin.parser BindColl BindIgnore BindScalar BindTuple Constant Function Predicate RulesVar SrcVar Variable]
           [java.util List]
           [org.eclipse.collections.impl.list.mutable FastList]))

(defn- ensure! [ok] (when-not ok (fail! :query-source/invalid :inputs)))

(defn parse!
  "Keep ordinary parser diagnostics inside the invocation boundary."
  [parse form]
  (try (parse form)
       (catch clojure.lang.ExceptionInfo e
         (if (= "parser" (some-> e ex-data :error namespace))
           (fail! :query-source/invalid :syntax)
           (throw e)))))

(defn- retain! [runtime token]
  (rt/own! runtime #(control/release! token)))

(defn- containers! [runtime rows width]
  (when (> rows Integer/MAX_VALUE) (fail! :query-source/limit :inputs))
  (retain! runtime
    (control/reserve! (:control runtime)
      {:retained-bytes (value/add-charge 512
                         (value/multiply-charge rows (+ 192 (* 32 width))))})))

(defn- layout! [runtime binding depth]
  (control/check-active! (:control runtime))
  (when (> depth 128) (fail! :query-source/limit :inputs))
  (containers! runtime 1 1)
  (cond
    (instance? BindIgnore binding) {:kind :ignore :columns [] :single? true}
    (instance? BindScalar binding)
    (let [v (:variable binding)]
      (cond
        (instance? SrcVar v) {:kind :source :symbol (:symbol v)}
        (instance? RulesVar v) {:kind :rules}
        (instance? Variable v) {:kind :scalar :columns [(:symbol v)] :single? true}
        :else (fail! :query-source/unsupported :inputs)))
    (instance? BindTuple binding)
    (let [children (mapv #(layout! runtime % (inc depth)) (:bindings binding))]
      {:kind :tuple :children children :columns (into [] (mapcat :columns) children)
       :single? (every? :single? children)
       :flat? (every? #(#{:scalar :ignore} (:kind %)) children)})
    (instance? BindColl binding)
    (let [child (layout! runtime (:binding binding) (inc depth))]
      {:kind :collection :child child :columns (:columns child)})
    :else (fail! :query-source/unsupported :inputs)))

(defn- owned-singleton! [runtime columns values]
  (containers! runtime 1 (count columns))
  (relation/relation! (zipmap columns (range))
    (doto (FastList.) (.add (object-array values)))))

(defn- singleton! [runtime check! columns values]
  (retain! runtime (rt/charge-copy! runtime values))
  (run! #(value/execution-value! % check!) values)
  (owned-singleton! runtime columns (value/owned-copy values check!)))

(defn- binding! [runtime check! join! {:keys [kind columns children child flat?]} input]
  (check!)
  (case kind
    :ignore (singleton! runtime check! [] [])
    :scalar (singleton! runtime check! columns [input])
    :tuple
    (if flat?
      (let [scratch (control/reserve! (:control runtime)
                      {:retained-bytes (+ 512 (* 256 (count children)))})]
        (try
          (let [read! (inputs/item-reader input)
                values (reduce (fn [values {:keys [kind]}]
                                 (check!)
                                 (if-let [[x] (read!)]
                                   (if (= :ignore kind) values
                                       (do (retain! runtime (rt/charge-copy! runtime x))
                                           (value/execution-value! x check!)
                                           (conj values (value/owned-copy x check!))))
                                   (fail! :query-source/invalid :inputs))) [] children)]
            (owned-singleton! runtime columns values))
          (finally (control/release! scratch))))
      (let [read! (inputs/item-reader input)]
        (reduce (fn [rel child]
                  (check!)
                  (if-let [[item] (read!)]
                    (join! rel (binding! runtime check! join! child item))
                    (fail! :query-source/invalid :inputs)))
                (singleton! runtime check! [] []) children)))
    :collection
    (let [read! (inputs/item-reader input)
          _ (containers! runtime 0 (count columns))
          rows (FastList.) attrs (zipmap columns (range))]
      (loop []
        (check!)
        (when-let [[item] (read!)]
          ;; Complete and own the child before pulling a producer's next item.
          ;; A nested binding can itself expand into several correlated rows.
          (let [rel (binding! runtime check! join! child item)
                ^List tuples (:tuples rel) n (.size tuples)]
            (when (> (+' (.size rows) n) Integer/MAX_VALUE)
              (fail! :query-source/limit :inputs))
            (containers! runtime n (count columns))
            (dotimes [i n]
              (check!)
              (.add rows (.get tuples i))))
          (recur)))
      (relation/relation! attrs rows))
    (fail! :query-source/unsupported :inputs)))

(defn- columns! [check! rel]
  ;; Only a complete owned input establishes a homogeneous type. Empty and
  ;; heterogeneous columns remain :data; a later fact may narrow them by join.
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

(defn- window! [parsed scalars]
  (reduce (fn [parsed key]
            (let [x (get parsed key)]
              (if (symbol? x)
                (let [v (get scalars x ::missing)]
                  (when-not (and (integer? v) (<= (if (= key :qlimit) 1 0) v Long/MAX_VALUE))
                    (fail! :query-source/invalid :window))
                  (assoc parsed key v))
                parsed))) parsed [:qlimit :qoffset]))

(defn prepare!
  "Consume the declared number of arguments and bind scalar, tuple, collection
  and nested relation inputs. join! is the compiler's bounded shared hash join.
  Return owned actual input columns/rows, scalar constraints, parsed rules and
  source aliases. Local collection source clauses remain a separate compiler
  gate. No provider callback occurs here; ignored arguments/tails stay unread."
  [runtime parsed arguments join!]
  (let [check! #(control/check-active! (:control runtime))
        layouts (mapv #(layout! runtime % 0) (:qin parsed))
        read! (inputs/item-reader arguments)
        initial {:entries [] :rules [] :bindings {} :scalars {}
                 :relation (singleton! runtime check! [] [])}
        prepared
        (reduce
          (fn [result {:keys [kind symbol columns single?] :as layout}]
            (check!)
            (let [item (read!)]
              (ensure! item)
              (let [input (first item)]
                (case kind
                  :source
                  (update result :entries conj
                    (cond (db/db? input) {:symbol symbol :native input}
                          (satisfies? source/IQuerySource input) {:symbol symbol :source input}
                          :else (fail! :query-source/unsupported :inputs)))
                  :rules
                  (do (retain! runtime (rt/charge-copy! runtime input))
                      (when-not (or (nil? input) (sequential? input))
                        (fail! :query-source/unsupported :inputs))
                      (assoc result :rules (parse! parser/parse-rules (value/owned-copy input check!))))
                  (let [rel (binding! runtime check! join! layout input)
                        ^List tuples (:tuples rel)
                        constants (when (and single? (= 1 (.size tuples)))
                                    (zipmap columns (vec ^objects (.get tuples 0))))]
                    (cond-> (update result :relation join! rel)
                      constants (update :bindings merge constants)
                      (= :scalar kind) (update :scalars merge constants))))))) initial layouts)]
    ;; Pull only one extra argument to detect arity; never count an unused tail.
    (ensure! (nil? (read!)))
    (let [parsed (window! parsed (:scalars prepared))
          plain (into #{} (remove #(.startsWith (name %) "?")) (keys (:scalars prepared)))
          ;; Plain scalar names currently serve dynamic windows. Function
          ;; arguments using them need the ordinary host-symbol resolver bridge.
          _ (when (and (seq plain)
                       (seq (parser/collect
                              #(and (or (instance? Predicate %) (instance? Function %))
                                    (some (fn [arg] (and (instance? Constant arg) (contains? plain (:value arg)))) (:args %)))
                              (:qwhere parsed))))
              (fail! :query-source/unsupported :inputs))
          rel (:relation prepared)
          columns (filterv #(not (contains? plain (:variable %))) (columns! check! rel))
          ;; Dropped window parameters do not change row multiplicity.
          attrs (:attrs rel) rows (FastList.)]
      (containers! runtime (.size ^List (:tuples rel)) (count columns))
      (doseq [^objects row (:tuples rel)]
        (check!)
        (.add rows (object-array (mapv #(aget row (int (attrs (:variable %)))) columns))))
      (assoc prepared :parsed parsed :columns columns
             :bindings (apply dissoc (:bindings prepared) plain)
             :relation (relation/relation! (zipmap (map :variable columns) (range)) rows)))))
