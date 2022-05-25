(ns datalevin.query-new
  (:require
   [clojure.edn :as edn]
   [clojure.set :as set]
   [clojure.string :as str]
   [clojure.walk :as walk]
   [taoensso.timbre :as log]
   [datalevin.db :as db]
   [datalevin.search :as s]
   [datalevin.storage :as st]
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

;; Utilities

(defn single [coll]
  (assert (nil? (next coll)) "Expected single element")
  (first coll))

(defn intersect-keys [attrs1 attrs2]
  (set/intersection (set (keys attrs1))
                    (set (keys attrs2))))

(defn concatv [& xs]
  (into [] cat xs))

(defn zip
  ([a b] (mapv vector a b))
  ([a b & rest] (apply mapv vector a b rest)))

(defn same-keys? [a b]
  (and (= (count a) (count b))
       (every? #(contains? b %) (keys a))
       (every? #(contains? b %) (keys a))))

(defn- looks-like? [pattern form]
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

(defn source? [sym]
  (and (symbol? sym)
       (= \$ (first (name sym)))))

(defn free-var? [sym]
  (and (symbol? sym)
       (= \? (first (name sym)))))

(defn attr? [form]
  (or (keyword? form) (string? form)))

(defn lookup-ref? [form]
  (looks-like? [attr? '_] form))

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
               [(into-array Object [value])]))

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
  (log/debug "context" context)
  )

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
  (let [parsed-q      (log/spy (memoized-parse-query q))
        find          (:qfind parsed-q)
        find-elements (dp/find-elements find)
        find-vars     (dp/find-vars find)
        result-arity  (count find-elements)
        with          (:qwith parsed-q)
        all-vars      (concat find-vars (map :symbol with))
        q             (cond-> q
                        (sequential? q) dp/query->map)
        context       (-> (Context. [] {} {})
                          (resolve-ins (:qin parsed-q) inputs))
        resultset     (-> context
                          (q* (log/spy (:where q)))
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

(comment

  (require '[datalevin.core :as d])
  (def next-eid (volatile! -1))
  (defn random-man []
    {:db/id      (vswap! next-eid inc)
     :first-name (rand-nth ["James" "John" "Robert" "Michael" "William" "David"
                            "Richard" "Charles" "Joseph" "Thomas"])
     :last-name  (rand-nth ["Smith" "Johnson" "Williams" "Brown" "Jones" "Garcia"
                            "Miller" "Davis" "Rodriguez" "Martinez"])
     :age        (rand-int 100)
     :salary     (rand-int 100000)})
  (def people100 (shuffle (take 100 (repeatedly random-man))))
  (defn rand-str []
    (apply str
           (for [i (range (+ 3 (rand-int 10)))]
             (char (+ (rand 26) 65)))))
  (defn random-school []
    {:db/id      (vswap! next-eid inc)
     :name       (rand-str)
     :ownership  (rand-nth [:private :public])
     :enrollment (rand-int 10000)})
  (def school20 (shuffle (take 20 (repeatedly random-school))))
  (def schema
    {:follows    {:db/valueType   :db.type/ref
                  :db/cardinality :db.cardinality/many }
     :school     {:db/valueType   :db.type/ref
                  :db/cardinality :db.cardinality/many }
     :born       {:db/valueType   :db.type/ref
                  :db/cardinality :db.cardinality/one}
     :location   {:db/valueType   :db.type/ref
                  :db/cardinality :db.cardinality/one}
     :name       {:db/valueType :db.type/string}
     :first-name {:db/valueType :db.type/string}
     :last-name  {:db/valueType :db.type/string}
     :ownership  {:db/valueType :db.type/keyword}
     :enrollment {:db/valueType :db.type/long}
     :age        {:db/valueType :db.type/long}
     :salary     {:db/valueType :db.type/long}})
  (defn random-place []
    {:db/id (vswap! next-eid inc)
     :name  (rand-str)})
  (def place15 (shuffle (take 15 (repeatedly random-place))))
  (def conn (d/create-conn nil schema))
  (d/transact! conn people100)
  (d/transact! conn school20)
  (d/transact! conn place15)
  (dotimes [_ 60]
    (d/transact! conn [{:db/id (rand-int 100) :follows (rand-int 100)}]))
  (dotimes [_ 80]
    (d/transact! conn [{:db/id (rand-int 100) :school (+ 100 (rand-int 20))}]))
  (dotimes [i 70]
    (d/transact! conn [{:db/id i :born (+ 120 (rand-int 15))}]))
  (dotimes [i 15]
    (d/transact! conn [{:db/id (+ 100 i) :location (+ 120 (rand-int 15))}]))

  (def store (.-store (d/db conn)))
  (def attrs (st/attrs store))
  {0 :db/ident, 7 :name, 1 :db/created-at, 4 :last-name, 13 :salary, 6 :school, 3 :enrollment, 12 :location, 2 :db/updated-at, 11 :born, 9 :first-name, 5 :age, 10 :ownership, 8 :follows}
  (def classes (st/classes store))
  (def rclasses (st/rclasses store))
  (def entities (st/entities store))
  (def rentities (st/rentities store))
  (def links (st/links store))
  (def rlinks (st/rlinks store))

  (q '[:find ?e
       :in $ ?fn
       :where
       [?e :first-namme ?fn]
       [?e :follows]]
     (d/db conn) "David")

  )
