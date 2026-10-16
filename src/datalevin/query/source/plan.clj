(ns ^:no-doc datalevin.query.source.plan
  "Invocation-local validation and complete source-clause planning."
  (:require [clojure.set :as set]
            [datalevin.db :as db]
            [datalevin.parser :as dp]
            [datalevin.query.source :as source]
            [datalevin.query.source.contract :as contract]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.inputs :as input]
            [datalevin.query.source.native :as native]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :as value :refer [fail!]]
            [datalevin.util :as u])
  (:import [datalevin.parser Variable DefaultSrc Constant
            Placeholder Pattern Predicate FindRel BindScalar]
           [java.util IdentityHashMap]))

(defn source-inputs? [inputs] (boolean (some #(satisfies? source/IQuerySource %) inputs)))

(defn- term [x]
  (cond
    (instance? Variable x) [:var (:symbol x)]
    (instance? Constant x) [:const (:value x)]
    (instance? Placeholder x) [:ignore]
    :else (fail! :query-source/unsupported :clauses)))

(defn- clause [id x]
  (cond
    (instance? Pattern x)
    {:id id :kind :pattern :terms (mapv term (:pattern x))
     :source-symbol (if (instance? DefaultSrc (:source x)) '$ (:symbol (:source x)))}
    (instance? Predicate x)
    (if-let [operator ({'= := 'not= :not= '< :< '<= :<= '> :> '>= :>=} (:symbol (:fn x)))]
      (if (= 2 (count (:args x)))
        {:id id :kind :predicate :operator operator :semantics-version 1
         :args (mapv term (:args x))}
        (fail! :query-source/unsupported :clauses))
      (fail! :query-source/unsupported :clauses))
    :else (fail! :query-source/unsupported :clauses)))

(defn- own-clauses! [runtime clauses]
  (doseq [clause clauses
          term (or (:terms clause) (:args clause))
          :when (= :const (first term))]
    (value/execution-value! (second term)))
  ;; Capture caller-owned constants before any provider callback can observe
  ;; the query. Reserve first, including mutable leaves in value metadata.
  (rt/charge! runtime clauses)
  (value/owned-copy clauses))

(defn- window [x bindings scalar-inputs minimum default]
  (let [dynamic? (symbol? x)
        _ (when (and dynamic? (not (contains? scalar-inputs x)))
            (fail! :query-source/invalid :window))
        x (if dynamic? (get bindings x ::missing) x)]
    (cond (and (not dynamic?) (or (nil? x) (= -1 x))) default
          (and (integer? x) (<= minimum x Long/MAX_VALUE)) x
          :else (fail! :query-source/invalid :window))))

(defn- find! [parsed]
  (when (or (seq (:qorder parsed)) (seq (:qhaving parsed)))
    (fail! :query-source/unsupported :projection))
  (doseq [element (dp/find-elements (:qfind parsed))]
    (when-not (or (instance? Variable element)
                  (and (dp/aggregate? element)
                       (#{'count 'sum 'avg 'min 'max} (:symbol (:fn element)))
                       (= 1 (count (:args element)))
                       (instance? Variable (first (:args element)))))
      (fail! :query-source/unsupported :projection)))
  (when (and (not (instance? FindRel (:qfind parsed)))
             (or (:qlimit parsed) (:qoffset parsed)))
    (fail! :query-source/unsupported :window)))

(defn prepare!
  "Capture and validate all metadata/types before acquiring any provider view."
  [runtime parsed inputs options]
  (when (u/graal?) (fail! :query-source/unsupported :execution-mode))
  (find! parsed)
  (when (> (count (:qwhere parsed)) 256) (fail! :query-source/unsupported :clauses))
  (when-not (and (or (nil? options) (value/plain-map? options))
                (every? #{:limits :consistency :authorization} (keys options))
                (#{:snapshot :statement :best-effort} (get options :consistency :snapshot))
                (value/portable? (get options :authorization {:principal-id :application})))
    (fail! :query-source/invalid :context))
  (let [clauses (own-clauses! runtime (mapv clause (range) (:qwhere parsed)))
        {:keys [sources bindings] :as input-data} (input/prepare! runtime parsed inputs)
        scalar-inputs (into #{} (keep #(when (and (instance? BindScalar %)
                                                 (instance? Variable (:variable %)))
                                        (:symbol (:variable %)))) (:qin parsed))
        patterns (filterv #(= :pattern (:kind %)) clauses)
        predicates (filterv #(= :predicate (:kind %)) clauses)
        descriptors (IdentityHashMap.)
        entries (mapv (fn [[sym input]]
                        (rt/with-context runtime
                          {:source-symbol sym
                           :clause-ids (mapv :id (filter #(= sym (:source-symbol %)) patterns))}
                          (fn [runtime]
                            (if (satisfies? source/IQuerySource input)
                              (let [descriptor (or (.get descriptors input)
                                                   (let [d (rt/invoke runtime :descriptor #(source/-source-info input))]
                                                     (contract/negotiate d)
                                                     (.put descriptors input d)
                                                     (rt/charge! runtime d)
                                                     d))]
                                {:symbol sym :source input :descriptor descriptor})
                              (do (native/validate! input) {:symbol sym :native input}))))) sources)
        types (atom {})]
    (let [available (into (input/variables input-data) (mapcat contract/variables) patterns)
          projected (into (set (dp/find-vars (:qfind parsed))) (map :symbol) (:qwith parsed))]
      (when-not (and (set/subset? projected available)
                    (every? #(set/subset? (contract/variables %) available) predicates))
        (fail! :query-source/invalid :clauses)))
    (run! value/execution-value! (vals bindings))
    (doseq [{:keys [source-symbol terms]} patterns]
      (when-not (contains? sources source-symbol) (fail! :query-source/invalid :inputs))
      (when (db/db? (sources source-symbol))
        (when-not (and (= 3 (count terms)) (= :const (ffirst (next terms)))
                       (keyword? (second (second terms))))
          (fail! :query-source/unsupported :clauses))))
    (let [entries
          (mapv
            (fn [{:keys [symbol descriptor] :as entry}]
              (let [local (mapv #(dissoc % :source-symbol)
                               (filter #(= symbol (:source-symbol %)) patterns))
                    available (into (set (keys bindings)) (mapcat contract/variables) local)
                    eligible (filterv #(set/subset? (contract/variables %) available) predicates)
                    clauses (into local eligible)]
                (when descriptor
                  (doseq [clause clauses]
                    (rt/with-context runtime {:source-symbol symbol :clause-ids [(:id clause)]}
                      (fn [_] (contract/clauses! descriptor [clause]))))
                  (doseq [pattern local]
                    (rt/with-context runtime {:source-symbol symbol :clause-ids [(:id pattern)]}
                      (fn [_]
                        (doseq [[term column] (map vector (:terms pattern)
                                                  (contract/pattern-columns descriptor pattern))]
                          (when-not (= :ignore (first term)) (value/execution-type! (:type column)))
                          (when (= :var (first term))
                            (let [sym (second term) t (:type column)]
                              (when (and (contains? @types sym) (not= t (@types sym)))
                                (fail! :query-source/unsupported :types))
                              (swap! types assoc sym t)
                              (input/check-values! (:control runtime) input-data sym
                                (fn [x]
                                  (when-not (if (nil? x) (:nullable? column) (value/matches-type? t x))
                                    (fail! :query-source/invalid :types)))))))))))
                (assoc entry :clauses clauses :patterns local))) entries)]
      {:entries entries :bindings bindings :input-data input-data
       :predicates predicates :types @types
       :limit (window (:qlimit parsed) bindings scalar-inputs 1 nil)
       :offset (window (:qoffset parsed) bindings scalar-inputs 0 0)
       :parsed parsed :options options})))

(defn- select-cover [request response allow-once?]
  (contract/alternatives! request response)
  (let [patterns (into #{} (comp (filter #(= :pattern (:kind %))) (map :id)) (:clauses request))
        alternatives (:alternatives response)
        attempts (volatile! 0)]
    (letfn [(search [remaining covered selected]
              (when (> (long (vswap! attempts #(inc (long %)))) 4096)
                (fail! :query-source/unsupported :planning))
              (if (empty? remaining) selected
                  (let [id (first (sort remaining))]
                    (some (fn [a]
                            (when (and (contains? (:covers a) id)
                                       (or (= :reopen (get-in a [:properties :replay]))
                                           (and allow-once? (empty? selected)
                                                (set/subset? remaining (:covers a))))
                                       (empty? (set/intersection covered (:covers a))))
                              (search (set/difference remaining (:covers a))
                                      (set/union covered (:covers a)) (conj selected (:id a)))))
                          alternatives))))]
      (let [selected (search patterns #{} [])]
        (when (nil? selected) (fail! :query-source/unsupported :planning))
        (contract/selection! request response selected)))))

(defn acquire-and-plan! [runtime {:keys [entries bindings options] :as prepared}]
  (let [common {:spi-version 1 :features #{} :query-id (:query-id runtime)
                :authorization (get options :authorization {:principal-id :application})
                :consistency (get options :consistency :snapshot)
                :control (:control runtime)}
        common (assoc common :limits (control/limits (:control runtime)))
        groups (vals (group-by #(select-keys (:descriptor %) [:source-instance-id :compute-context-id])
                               (filter :descriptor entries)))
        acquired
        (mapcat
          (fn [group]
            (rt/with-context runtime
              {:source-symbol (:symbol (first group))
               :clause-ids (mapv :id (:clauses (first group)))}
              (fn [runtime]
                (let [request (assoc common :source-descriptors (vec (distinct (map :descriptor group))))
                      session (rt/invoke runtime :acquisition #(source/-open-session (:source (first group)) request))]
                  (when-not (satisfies? source/IQuerySourceSession session) (fail! :query-source/invalid :session))
                  (rt/own! runtime #(source/-close-session! session))
                  (let [registration (source/register-cancel! (:control runtime) #(source/-cancel-session! session %))]
                    (rt/own! runtime #(source/unregister-cancel! registration)))
                  (contract/session-info! request (rt/invoke runtime :session #(source/-session-info session)))
                  (mapv #(assoc % :session session :request request :allow-once? (= 1 (count group))) group))))) groups)
        planned
        (mapv
          (fn [{:keys [source symbol descriptor clauses session request] :as entry}]
            (rt/with-context runtime {:source-symbol symbol :clause-ids (mapv :id clauses)}
              (fn [runtime]
                (let [request (merge request {:source-symbol symbol :source-descriptor descriptor
                                             :clauses clauses :bindings bindings
                                             :required-vars (into #{} (mapcat contract/variables) clauses)
                                             :max-alternatives 32
                                             :demand {:quality :exact :ordering [] :offset 0 :limit nil}})
                      _ (contract/planning-request! request)
                      response (rt/invoke runtime :planning #(source/-plan-source source session request))
                      selected (select-cover request response (:allow-once? entry))]
                  (doseq [alternative selected]
                    (rt/with-context runtime {:alternative-id (:id alternative)}
                      (fn [runtime]
                        ;; Core retains the selected envelope; the provider
                        ;; accounts for opaque handle state through its session.
                        (rt/charge! runtime (dissoc alternative :plan)))))
                  (swap! (:explain runtime) update :sources conj
                         {:source symbol :consistency (:consistency request)
                          :pattern-count (count (:patterns entry))
                          :selected (mapv #(-> (select-keys % [:covers :prefilters])
                                                (assoc :properties
                                                       (select-keys (:properties %)
                                                                    [:replay :ordering :blocking-state]))) selected)})
                  (mapv #(assoc entry :alternative % :request request) selected))))) acquired)
        fragments (vec (mapcat identity planned))]
    (doseq [group (vals (group-by :session fragments))]
      (when (and (> (count group) 1)
                 (some #(= :once (get-in % [:alternative :properties :replay])) group))
        (rt/with-context runtime
          {:source-symbol (:symbol (first group))
           :clause-ids (mapv :id (:clauses (first group)))}
          (fn [_] (fail! :query-source/unsupported :planning)))))
    (assoc prepared :fragments fragments)))
