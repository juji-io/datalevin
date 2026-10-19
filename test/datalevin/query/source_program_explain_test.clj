(ns datalevin.query.source-program-explain-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.core :as d]
            [datalevin.query.source :as source]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.program :as program]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source-planning-test :as fixture]))

(def ^:private secret "private SQL, credentials, bindings and values")
(def ^:private pairs '[:find ?id ?name :in $s :where [$s ?id :row/value ?name]])
(defn- events-of [events kind] (filterv #(= kind (first %)) @events))
(defn- error [f] (try (f) nil (catch clojure.lang.ExceptionInfo e (ex-data e))))

(defn- provider [events config]
  (let [delegate (fixture/provider events config)]
    (reify source/IQuerySource
      (-source-info [_] (assoc (source/-source-info delegate) :test/private secret))
      (-open-session [_ request]
        (let [session (source/-open-session delegate request)]
          (reify source/IQuerySourceSession
            (-session-info [_]
              (assoc (source/-session-info session) :authorization-token secret :consistency :snapshot
                :data-version (with-meta 'public-data-version {:private secret})))
            (-open-cursor [_ open]
              (when (:deny-open? config) (throw (Exception. secret)))
              (let [cursor (source/-open-cursor session open)
                    metrics (atom (assoc (zipmap [:batches :rows :charged-bytes :elapsed-ns] (repeat 0))
                                    :read-rows nil :read-bytes nil :external-requests nil))]
                (reify source/IQuerySourceCursor
                  (-next-batch! [_ fetch]
                    (let [batch (source/-next-batch! cursor fetch)]
                      (swap! metrics #(-> % (update :batches inc) (update :rows + (:row-count batch))
                                          (update :charged-bytes + (:byte-count batch))))
                      batch))
                  (-cursor-metrics [_]
                    (swap! events conj [:metrics])
                    (let [metrics (assoc @metrics :test/private secret)]
                      (if-let [transform (:metrics config)] (transform metrics open) metrics)))
                  (-close-cursor! [_]
                    (source/-close-cursor! cursor)
                    (when (:cursor-close-error? config) (throw (Exception. secret)))))))
            (-cancel-session! [_ reason] (source/-cancel-session! session reason))
            (-close-session! [_] (source/-close-session! session)))))
      (-plan-source [_ session request]
        (update (source/-plan-source delegate session request) :alternatives
          #(mapv (fn [alternative]
                   (cond-> (-> alternative
                               (assoc :test/private secret :explain {:label secret})
                               (assoc-in [:plan :test/private] secret))
                     (:same-id? config) (assoc :id :same-alternative)
                     (:zero-estimate? config) (assoc-in [:estimate (if (seq (:requires alternative)) :rows-per-binding :rows)] 0)))
             (cond->> % (:same-id? config) (filter (fn [a] (= :attribute-enumeration (get-in a [:access :kind])))))))))))

(defn- explain! [events query inputs explain-options config & [runtime-options source-options]]
  (rt/with-runtime (or runtime-options {})
    (fn [runtime]
      (let [result (program/explain! runtime query (inputs (provider events config)) explain-options source-options)]
        (is (every? zero? (vals (control/usage (:control runtime)))))
        result))))

(deftest plan-only-explanation-selects-v2-accesses-without-opening-cursors
  (let [events (atom [])
        explanation (explain! events pairs vector {} {:deny-open? true})
        [region] (:regions explanation) [access] (:steps region)]
    (is (= {:query-source? true :spi-version 2 :executed? false}
           (select-keys explanation [:query-source? :spi-version :executed?])))
    (is (= 1 (count (events-of events :acquire))))
    (is (= 1 (count (events-of events :plan))))
    (is (= 1 (count (events-of events :session-close))))
    (is (empty? (events-of events :open)))
    (is (empty? (events-of events :fetch)))
    (is (empty? (events-of events :metrics)))
    (is (= [] (:cursors explanation)))
    (is (not (contains? explanation :result-rows)))
    (is (= :eav-structural-v1 (:policy region)))
    (is (= :structural (:selection region)))
    (is (nil? (:estimated-access-ns region)))
    (is (= [] (:fallback-reasons region)))
    (is (<= 0 (:planning-time-ms region)))
    (is (= {:kind :attribute-enumeration :anchor-clause 0} (:access access)))
    (is (= #{0} (:covers access)))
    (is (= #{} (:prefilters access)))
    (is (nil? (get-in access [:estimate :rows])))
    (is (= :unknown (get-in access [:estimate :confidence])))
    (is (= :unstable (:replay access)))
    (is (= :none (:blocking-state access)))
    (is (= :memory (get-in explanation [:operators :storage])))
    (is (= :reject (get-in explanation [:operators :spill-policy])))
    (is (= :none (get-in explanation [:operators :replay-spools])))
    (is (not (.contains (binding [*print-meta* true] (pr-str explanation)) secret)))))

(deftest requested-and-actual-consistency-and-definition-identity-stay-distinct
  (let [events (atom [])
        query '[:find ?id ?name :in $a $b :where [$a ?id :row/value ?name] [$b ?id :row/value ?name]]
        explanation (explain! events query (fn [p] [p p]) {} {}
                      nil {:consistency :best-effort :sources {'$b {:consistency :statement}}})
        sources (into {} (map (juxt :source identity)) (:sources explanation))]
    (is (= 1 (count (events-of events :acquire))))
    (is (= :best-effort (get-in sources ['$a :requested-consistency])))
    (is (= :statement (get-in sources ['$b :requested-consistency])))
    (is (every? #(= :statement (:effective-consistency %)) (vals sources)))
    (is (every? #(= :snapshot (:consistency %)) (vals sources)))
    (is (every? #(= 'public-data-version (:data-version %)) (vals sources)))
    (is (every? #(= "rows:v2" (:definition-fingerprint %)) (vals sources)))
    (is (every? #(= "scope:rows" (:scope-id %)) (vals sources)))
    (is (not (.contains (binding [*print-meta* true] (pr-str explanation)) secret)))))

(deftest executed-explanation-separates-estimates-cursor-metrics-and-outer-fan-out
  (doseq [intermediate? [true false]]
    (let [events (atom [])
          explanation (explain! events
                        '[:find ?tag ?name :in $s [[?id ?tag]] :where [$s ?id :row/value ?name]]
                        (fn [p] [p [[2 :second] [1 :first] [2 :another] [3 :missing]]])
                        {:run? true :intermediate-counts? intermediate?} {:lookup-only? true}
                        {:limits {:binding-rows 1 :batch-rows 1}})
          [region] (:regions explanation) [access] (:steps region)
          [operator] (:operators region) cursors (:cursors explanation)]
      (is (:executed? explanation))
      (is (= 3 (:result-rows explanation)))
      (is (= 3 (count cursors)))
      (is (= 3 (count (events-of events :metrics))))
      (is (= 3 (count (events-of events :cursor-close))))
      (is (= {:rows 1 :bytes 262144} (:binding-limits access)))
      (is (= :entity-lookup (get-in access [:access :kind])))
      (is (nil? (get-in access [:estimate :rows-per-binding])))
      (is (= 2 (reduce + (map #(get-in % [:metrics :rows]) cursors))))
      (is (every? #(nil? (get-in % [:metrics :read-rows])) cursors))
      (is (every? #(= {:batch-rows 1 :batch-bytes 1048576} (:batch-limits %)) cursors))
      (if intermediate?
        (do
          (is (= {:calls 1 :input-rows 4 :output-rows 3} (select-keys (:actual operator) [:calls :input-rows :output-rows])))
          (is (= [1 1 1] (mapv :input-rows cursors)))
          (is (= 2 (reduce + (map #(get-in % [:actual :rows]) cursors)))))
        (do
          (is (not (contains? operator :actual)))
          (is (every? #(not (contains? % :input-rows)) cursors))
          (is (every? #(not (contains? % :actual)) cursors))))
      (is (not (.contains (binding [*print-meta* true] (pr-str explanation)) secret))))))

(deftest repeated-alternative-ids-are-qualified-by-their-owning-regions
  (let [events (atom [])
        explanation (explain! events
                      '[:find ?id ?left ?right :in $s % :where ($s named ?id ?left) ($s named ?id ?right)]
                      (fn [p] [p '[[(named ?id ?name) [?id :row/value ?name]]]])
                      {:run? true} {:same-id? true})
        selected (mapcat :steps (:regions explanation))
        cursors (:cursors explanation)]
    (is (= 1 (count (events-of events :acquire))))
    (is (= 2 (count (events-of events :plan))))
    (is (= 2 (:result-rows explanation)))
    (is (= #{:same-alternative} (set (map :alternative-id selected))))
    (is (= 2 (count (set (map :region-id selected)))))
    (is (= (set (map (juxt :source :region-id :alternative-id) selected))
           (set (map (juxt :source-symbol :region-id :alternative-id) cursors))))))

(deftest residuals-and-aggregate-placement-contain-no-input-values
  (let [events (atom [])
        explanation (explain! events
                      '[:find (count ?id) . :in $s ?secret :where [$s ?id :row/value ?name] [(not= ?name ?secret)]]
                      (fn [p] [p secret]) {:run? true} {})
        operators (mapcat :operators (:regions explanation))]
    (is (= 1 (:result-rows explanation)))
    (is (get-in explanation [:operators :grouping]))
    (is (= [:access :predicate] (mapv :kind operators)))
    (is (= 'not= (:function (last operators))))
    (is (= 2 (get-in (last operators) [:actual :output-rows])))
    (is (not (.contains (binding [*print-meta* true] (pr-str explanation)) secret)))))

(deftest ordinary-execution-does-not-request-diagnostic-metrics
  (let [events (atom [])]
    (rt/with-runtime {}
      (fn [runtime]
        (is (= #{[1 "one"] [2 "two"]}
               (program/query! runtime pairs [(provider events {:metrics (fn [_ _] (throw (Exception. secret)))})])))))
    (is (empty? (events-of events :metrics)))))

(deftest empty-dependent-inputs-have-no-cursor-or-provider-metric-work
  (let [events (atom [])
        explanation (explain! events '[:find ?name :in $s [?id ...] :where [$s ?id :row/value ?name]]
                      (fn [p] [p []]) {:run? true} {:lookup-only? true})]
    (is (= 0 (:result-rows explanation)))
    (is (get-in explanation [:operators :empty-input?]))
    (is (= [] (:cursors explanation)))
    (is (empty? (events-of events :open)))
    (is (empty? (events-of events :metrics)))
    (is (= 1 (count (events-of events :session-close))))))

(deftest advisory-zero-estimates-stay-separate-from-actual-results
  (let [events (atom [])
        explanation (explain! events pairs vector {:run? true} {:zero-estimate? true})]
    (is (= 0 (get-in explanation [:regions 0 :steps 0 :estimate :rows])))
    (is (= 2 (:result-rows explanation)))
    (is (= 2 (get-in explanation [:cursors 0 :metrics :rows])))))

(deftest cancellation-during-metrics-closes-every-resource
  (let [events (atom []) saved (atom nil)
        failure (error (fn [] (rt/with-runtime {}
                               (fn [runtime]
                                 (reset! saved runtime)
                                 (program/explain! runtime pairs
                                   [(provider events {:metrics (fn [metrics request]
                                                                 (control/cancel! (:control request)
                                                                   {:error :query-source/cancelled :phase :metrics})
                                                                 metrics)})] {:run? true})))))]
    (is (= :query-source/cancelled (:error failure)))
    (is (= 1 (count (events-of events :cursor-close))))
    (is (= 1 (count (events-of events :session-close))))
    (is (every? zero? (vals (control/usage (:control @saved)))))))

(deftest invalid-explain-options-reject-before-descriptor-callbacks
  (doseq [[options expected] [[[] :query-source/invalid] [{:run? :yes} :query-source/invalid]
                             [{:intermediate-counts? nil} :query-source/invalid]
                             [{:unknown true} :query-source/unsupported]]]
    (let [events (atom [])
          failure (error (fn [] (explain! events pairs vector options {})))]
      (is (= expected (:error failure)))
      (is (= :explain (:phase failure)))
      (is (empty? @events)))))

(deftest metric-failures-retain-context-and-primary-errors-through-cleanup
  (doseq [metrics [(fn [m _] (assoc m :rows secret)) (fn [_ _] {}) (fn [_ _] (throw (Exception. secret)))]]
    (let [events (atom []) saved (atom nil)
          failure (try
                    (rt/with-runtime {}
                      (fn [runtime]
                        (reset! saved runtime)
                        (program/explain! runtime pairs
                          [(provider events {:metrics metrics :cursor-close-error? true :close-error? true})]
                          {:run? true}))) nil
                    (catch clojure.lang.ExceptionInfo e e))]
      (is (contains? #{:query-source/invalid :query-source/provider} (:error (ex-data failure))))
      (is (= :metrics (:phase (ex-data failure))))
      (is (= '$s (:source-symbol (ex-data failure))))
      (is (= 0 (:region-id (ex-data failure))))
      (is (seq (.getSuppressed ^Throwable failure)))
      (is (every? #(not (.contains (pr-str %) secret)) (cons failure (.getSuppressed ^Throwable failure))))
      (is (= 1 (count (events-of events :cursor-close))))
      (is (= 1 (count (events-of events :session-close))))
      (is (every? zero? (vals (control/usage (:control @saved))))))))

(deftest explanation-storage-and-results-share-the-invocation-limits
  (doseq [[limits expected] [[{:retained-bytes 1000} :retained-bytes] [{:result-rows 1} :result-rows]]]
    (let [events (atom []) saved (atom nil)
          failure (error (fn [] (rt/with-runtime {:limits limits}
                                 (fn [runtime]
                                   (reset! saved runtime)
                                   (program/explain! runtime pairs [(provider events {})] {:run? true})))))]
      (is (= :query-source/limit (:error failure)))
      (is (= expected (:category failure)))
      (is (every? zero? (vals (control/usage (:control @saved)))))
      (when (= expected :retained-bytes) (is (empty? @events))))))

(deftest native-recursive-operator-counts-survive-temporary-workspaces
  (let [events (atom []) n 100
        explanation (explain! events
                      '[:find ?n :in $rows $seed $s % :where ($rows walk ?n)]
                      (fn [p] [(mapv #(vector % (inc %)) (range n)) [[0]] p
                               '[[(walk ?n) [$seed ?n]] [(walk ?y) (walk ?x) [?x ?y]]]])
                      {:run? true} {} {:limits {:retained-bytes 6000000}})
        operators (mapcat :operators (:regions explanation))]
    (is (= (inc n) (:result-rows explanation)))
    (is (empty? (:cursors explanation)))
    (is (< 1 (count (set (map :program-id (:regions explanation))))))
    (is (some #(< 1 (get-in % [:actual :calls] 0)) operators))
    (is (every? #(or (not (:actual %)) (<= 0 (get-in % [:actual :elapsed-ns]))) operators))))

(deftest native-mixed-explanation-uses-the-existing-pinned-read-view
  (let [conn (d/create-conn nil {:edge {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}}
                           {:kv-opts {:inmemory? true} :background-sampling? false})
        events (atom [])]
    (try
      (d/transact! conn [{:db/id 1 :edge 2}])
      (let [explanation (explain! events
                          '[:find ?to ?name :in $n $s :where [$n 1 :edge ?to] [$s ?to :row/value ?name]]
                          (fn [p] [@conn p]) {:run? true}
                          {:lookup-only? true :planning (fn [_] (d/transact! conn [{:db/id 1 :edge 1}]))})
            operators (mapcat :operators (:regions explanation))]
        (is (= 1 (:result-rows explanation)))
        (is (= [:native :access] (mapv :kind operators)))
        (is (= 1 (get-in (first operators) [:actual :output-rows])))
        (is (= 1 (count (events-of events :open))))
        (is (= #{[1]} (d/q '[:find ?e :where [?e :edge 1]] @conn))))
      (finally (d/close conn)))))
