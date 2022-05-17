(ns datalevin.query
  (:require
   [clojure.edn :as edn]
   [clojure.set :as set]
   [clojure.string :as str]
   [clojure.walk :as walk]
   [taoensso.timbre :as log]
   [datalevin.db :as db]
   [datalevin.search :as s]
   [datalevin.util :as u :refer [raise]]
   [datalevin.lru :as lru]
   [datalevin.entity :as de]
   [datalevin.parser :as dp]
   [datalevin.pull-api :as dpa]
   [datalevin.pull-parser :as dpp])
  (:import [datalevin.parser BindColl BindIgnore BindScalar BindTuple
            Constant Pull FindColl FindRel FindScalar FindTuple PlainSymbol
            RulesVar SrcVar Variable]
           [datalevin.storage Store]
           [datalevin.search SearchEngine]
           [datalevin.db DB]
           [java.util ArrayList]))

(def ^:const lru-cache-size 100)

(def ^:private query-cache
  (volatile! (lru/lru lru-cache-size :constant)))

(defn memoized-parse-query [q]
  (if-some [cached (get @query-cache q nil)]
    cached
    (let [qp (dp/parse-query q)]
      (vswap! query-cache assoc q qp)
      qp)))

(defrecord Context [rels sources rules])

(defrecord Relation [attrs tuples])

(defn ^Relation prod-rel
  ([] (Relation. {} []))
  ([ra rb]
   ))

(defn sum-rel [ra rb]
  )

(defn parse-rules [rules]
  (let [rules (if (string? rules) (edn/read-string rules) rules)]
    (dp/parse-rules rules) ;; validation
    (group-by ffirst rules)))

(defn ^Relation empty-rel [binding]
  (let [vars (->> (dp/collect-vars-distinct binding)
                  (map :symbol))]
    (Relation. (zipmap vars (range)) [])))

(defprotocol IBinding
  ^Relation (in->rel [binding value]))

(extend-protocol IBinding
  BindIgnore
  (in->rel [_ _]
    (prod-rel))

  BindScalar
  (in->rel [binding value]
    (Relation. {(get-in binding [:variable :symbol]) 0}
               [#_(into-array Object [value])]))

  BindColl
  (in->rel [binding coll]
    (cond
      (not (u/seqable? coll))
      (raise "Cannot bind value " coll " to collection " (dp/source binding)
             {:error :query/binding, :value coll, :binding (dp/source binding)})
      (empty? coll)
      (empty-rel binding)
      :else
      (->> coll
           (map #(in->rel (:binding binding) %))
           (reduce sum-rel))))

  BindTuple
  (in->rel [binding coll]
    (cond
      (not (u/seqable? coll))
      (raise "Cannot bind value " coll " to tuple " (dp/source binding)
             {:error :query/binding, :value coll, :binding (dp/source binding)})
      (< (count coll) (count (:bindings binding)))
      (raise "Not enough elements in a collection " coll " to bind tuple "
             (dp/source binding)
             {:error :query/binding, :value coll, :binding (dp/source binding)})
      :else
      (reduce prod-rel
              (map #(in->rel %1 %2) (:bindings binding) coll)))))

(defn resolve-in [context [binding value]]
  (cond
    (and (instance? BindScalar binding)
         (instance? SrcVar (:variable binding)))
    (update context :sources assoc (get-in binding [:variable :symbol]) value)
    (and (instance? BindScalar binding)
         (instance? RulesVar (:variable binding)))
    (assoc context :rules (parse-rules value))
    :else
    (update context :rels conj (in->rel binding value))))

(defn resolve-ins [context bindings values]
  (let [cb (count bindings)
        cv (count values)]
    (cond
      (< cb cv)
      (raise "Extra inputs passed, expected: "
             (mapv #(:source (meta %)) bindings) ", got: " cv
             {:error :query/inputs :expected bindings :got values})
      (> cb cv)
      (raise "Too few inputs passed, expected: "
             (mapv #(:source (meta %)) bindings) ", got: " cv
             {:error :query/inputs :expected bindings :got values})
      :else
      (reduce resolve-in context (zipmap bindings values)))))

(def ^{:dynamic true
       :doc     "Default pattern source. Lookup refs, patterns, rules will
be resolved with it"}
  *implicit-source* nil)

(defn q* [context clauses]
  (binding [*implicit-source* (get (:sources context) '$)]
    ))

(defn collect [context symbols]
  )

(defn aggregate [find-elements context resultset]
  )

(defn- pull [find-elements context resultset]
  )

(defn map* [f xs]
  (reduce #(conj %1 (f %2)) (empty xs) xs))

(defn tuples->return-map [return-map tuples]
  (let [symbols (:symbols return-map)
        idxs    (range 0 (count symbols))]
    (map*
      (fn [tuple]
        (reduce
          (fn [m i] (assoc m (nth symbols i) (nth tuple i)))
          {} idxs))
      tuples)))

(defprotocol IPostProcess
  (post-process [find return-map tuples]))

(extend-protocol IPostProcess
  FindRel
  (post-process [_ return-map tuples]
    (if (nil? return-map)
      tuples
      (tuples->return-map return-map tuples)))

  FindColl
  (post-process [_ return-map tuples]
    (into [] (map first) tuples))

  FindScalar
  (post-process [_ return-map tuples]
    (ffirst tuples))

  FindTuple
  (post-process [_ return-map tuples]
    (if (some? return-map)
      (first (tuples->return-map return-map [(first tuples)]))
      (first tuples))))

(defn q [q & inputs]
  (let [parsed-q      (memoized-parse-query q)
        find          (:qfind parsed-q)
        find-elements (dp/find-elements find)
        find-vars     (dp/find-vars find)
        result-arity  (count find-elements)
        with          (:qwith parsed-q)
        all-vars      (concat find-vars (map :symbol with))
        q             (cond-> q
                        (sequential? q) dp/query->map)
        wheres        (:where q)
        context       (-> (Context. [] {} {})
                          (resolve-ins (:qin parsed-q) inputs))
        resultset     (-> context
                          (q* wheres)
                          (collect all-vars))]
    (cond->> resultset
      (:with q)
      (mapv #(vec (subvec % 0 result-arity)))
      (some dp/aggregate? find-elements)
      (aggregate find-elements context)
      (some dp/pull? find-elements)
      (pull find-elements context)
      true
      (post-process find (:qreturn-map parsed-q)))))
