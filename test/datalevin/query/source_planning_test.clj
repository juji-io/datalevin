(ns datalevin.query.source-planning-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.parser :as parser]
            [datalevin.query.access :as access]
            [datalevin.query.plan :as plan]
            [datalevin.query.source :as source]
            [datalevin.query.source.access :as source-access]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.planning :as planning]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source-envelope-test :as envelope-fixture]
            [datalevin.query.source-session-test :as fixture])
  (:import [datalevin.utl QueryKey]))

(defn- caught [f] (try (f) nil (catch Throwable e e)))
(defn- error [f] (ex-data (caught f)))
(defn- events-of [events kind] (filterv #(= kind (first %)) @events))
(defn- interfaces [_ compiled]
  (into {} (map (fn [r] [(:region-id r) {:inputs [] :operators [] :core {}}]))
        (filter :live? (:regions compiled))))

(defn provider [events config]
  (let [descriptor (get config :descriptor fixture/descriptor)
        facts (get config :facts [[1 :row/value "one"] [2 :row/value "two"]])]
    (reify source/IQuerySource
      (-source-info [_] (swap! events conj [:descriptor]) descriptor)
      (-open-session [_ request]
        (swap! events conj [:acquire request])
        (reify source/IQuerySourceSession
          (-session-info [_]
            (swap! events conj [:info])
            {:spi-version 2 :features #{} :session-id :session
             :source-instance-id (:source-instance-id descriptor) :compute-context-id (:compute-context-id descriptor)
             :authorization-token :policy :consistency (:consistency request) :acquired-at-ms 0
             :data-version (when (= :snapshot (:consistency request)) :v1)
             :consumption {:opens (get config :opens :many) :max-open-cursors 1}
             :request-constraints {:max-concurrent-requests 1 :rate-limit nil}})
          (-open-cursor [_ open]
            (swap! events conj [:open open])
            (let [{:keys [clause request columns requires]} (:plan open)
                  bindings (if (seq requires) (:bindings open) [{:values []}])
                  matches
                  (into []
                        (mapcat
                          (fn [{:keys [values binding-id]}]
                            (let [inputs (merge (:bindings request) (zipmap (map :variable requires) values))]
                              (keep
                                (fn [fact]
                                  (when-let [row
                                             (reduce (fn [row [term value]]
                                                       (let [[tag variable] term]
                                                         (cond
                                                           (= :ignore tag) row
                                                           (= :const tag) (if (QueryKey/equiv variable value) row (reduced nil))
                                                           (contains? row variable) (if (QueryKey/equiv (row variable) value) row (reduced nil))
                                                           :else (assoc row variable value))))
                                                     inputs (map vector (:terms clause) fact))]
                                    {:row (mapv #(get row (:variable %)) columns) :binding-id binding-id})) facts)))) bindings)
                  tokens (atom [])
                  metrics (atom {:batches 0 :rows 0 :charged-bytes 0 :read-rows nil
                                 :read-bytes nil :external-requests 0 :elapsed-ns nil})]
              (reify source/IQuerySourceCursor
                (-next-batch! [_ fetch]
                  (swap! events conj [:fetch open])
                  (let [rows (mapv :row matches) ids (when (seq requires) (mapv :binding-id matches))
                        bytes (if ids (source/batch-charge-bytes columns rows ids) (source/batch-charge-bytes columns rows))
                        token (source/reserve! (:control fetch) {:retained-bytes bytes :in-flight-batches 1})]
                    (swap! tokens conj token)
                    (swap! metrics #(-> % (update :batches inc) (update :rows + (count rows))
                                        (update :charged-bytes + bytes)))
                    (cond-> {:columns columns :rows rows :row-count (count rows) :byte-count bytes
                             :exhausted? true :reservation token}
                      ids (assoc :binding-ids ids))))
                (-cursor-metrics [_] @metrics)
                (-close-cursor! [_]
                  (doseq [token @tokens] (source/release! token))
                  (swap! events conj [:cursor-close])))))
          (-cancel-session! [_ _] (swap! events conj [:cancel]))
          (-close-session! [_]
            (swap! events conj [:session-close])
            (when (:close-error? config) (throw (Exception. "private close detail"))))))
      (-plan-source [_ _ request]
        (swap! events conj [:plan request])
        (when-let [f (:planning config)] (f request))
        (let [alternatives
              (into []
                    (mapcat
                      (fn [clause]
                        (let [variables (vec (distinct (keep #(when (= :var (first %)) (second %)) (:terms clause))))
                              types (zipmap (keep #(when (= :var (first %)) (second %)) (:terms clause))
                                            (keep (fn [[term t]] (when (= :var (first term)) t))
                                                  (map vector (:terms clause) [:long :keyword :string])))
                              columns (mapv #(hash-map :variable % :type (types %) :nullable? false) variables)
                              [entity-tag entity] (first (:terms clause))
                              available (some #(when (= entity (:variable %)) %) (:available-inputs request))
                              lookup? (or (= :const entity-tag) (contains? (:bindings request) entity) available)
                              kinds (cond-> [] (not (:lookup-only? config)) (conj :attribute-enumeration)
                                            lookup? (conj :entity-lookup))]
                          (mapv
                            (fn [kind]
                              (let [requires (if (and (= :entity-lookup kind) available
                                                      (not (contains? (:bindings request) entity))) [available] [])]
                                (-> (envelope-fixture/alternative (boolean (seq requires)))
                                    (assoc :id (str (name kind) ":" (:id clause)) :scope-id (:scope-id request)
                                           :requires requires :columns columns :covers #{(:id clause)}
                                           :access {:kind kind :anchor-clause (:id clause)}
                                           :plan {:request request :clause clause :requires requires :columns columns})
                                    (cond-> (:bad-estimate? config) (assoc :estimate nil))))) kinds)))) (:clauses request))]
          {:alternatives (if (:reverse? config) (vec (reverse alternatives)) alternatives)})))))

(defn- capture! [runtime entries query & [bindings input-vars options]]
  (planning/capture! runtime entries (parser/parse-query query) [] (or bindings {}) (or input-vars #{}) options))

(defn- execute [schedule]
  (reduce (fn [[cols tuples] {:keys [kind value]}]
            (let [s (if (= :core kind) (:step value)
                        (plan/access-step (:expr value) (:path value) (access/complete-demand :exact nil)
                                          (:bounds value) (:work value) cols))]
              [(:cols s) (plan/-execute s nil tuples)])) [[] nil] (:operators schedule)))

(deftest owned-lifecycle-captures-shared-aliases-once-and-plans-each-region-pair-once
  (let [events (atom []) saved (atom nil)]
    (rt/with-runtime {}
      (fn [runtime]
        (reset! saved runtime)
        (let [source (provider events {})
              compilation (capture! runtime [{:symbol '$a :source source} {:symbol '$b :source source}]
                             '[:find ?e ?v :in $a $b :where [$a ?e :row/value ?v] [$b ?e :row/value ?v]])]
          (is (= 1 (count (events-of events :descriptor))))
          (is (empty? (events-of events :acquire)))
          (let [planned (planning/plan! runtime compilation interfaces)
                [schedule] (planning/schedules runtime planned)]
            (is (= 1 (count (events-of events :acquire))))
            (is (= 2 (count (events-of events :plan))))
            (is (= ['$a '$b] (mapv (comp :source-symbol second) (events-of events :plan))))
            (is (empty? (events-of events :open)))
            (let [[cols tuples] (execute schedule)]
              (is (= ['?e '?v] cols))
              (is (= #{[1 "one"] [2 "two"]} (set (map vec tuples)))))
            (is (= 2 (count (events-of events :open))))))))
    (is (= 1 (count (events-of events :session-close))))
    (is (every? zero? (vals (control/usage (:control @saved)))))))

(deftest actual-shared-input-operator-seeds-dependent-access-and-preserves-multiplicity
  (let [events (atom []) outers [[2 :a] [1 :b] [2 :c] [2 :a] [3 :missing]]]
    (rt/with-runtime {:limits {:binding-rows 1}}
      (fn [runtime]
        (let [compilation (capture! runtime [{:symbol '$s :source (provider events {:lookup-only? true})}
                                             {:symbol '$n :native outers}]
                             '[:find ?e ?tag ?v :in $s $n :where [$n ?e ?tag] [$s ?e :row/value ?v]])
              planned
              (planning/plan! runtime compilation
                (fn [runtime compiled]
                  (let [[r] (:regions compiled)
                        operator (source-access/core-operator! runtime (:region-id r) 0 []
                                   [{:variable '?e :type :long :nullable? false}
                                    {:variable '?tag :type :keyword :nullable? false}] #{}
                                   (plan/map->InitStep {:in #{} :cols ['?e '?tag] :result (mapv object-array outers)}))]
                    {(:region-id r) {:inputs [] :operators [operator]
                                     :core (zipmap (map :path (:core-clauses r)) (repeat 0))}})))
              [schedule] (planning/schedules runtime planned)
              [cols tuples] (execute schedule)]
          (is (= [:core :access] (mapv :kind (:operators schedule))))
          (is (= ['?e '?tag '?v] cols))
          (is (= (frequencies [[2 :a "two"] [1 :b "one"] [2 :c "two"] [2 :a "two"]])
                 (frequencies (map vec tuples))))
          (is (= 3 (count (events-of events :open))))
          (is (= 3 (count (set (mapcat #(map :binding-id (:bindings (second %))) (events-of events :open)))))))))))

(deftest invalid-unused-response-closes-the-entire-compilation-before-returning-error
  (let [events (atom []) saved (atom nil)
        result
        (error #(rt/with-runtime {}
                  (fn [runtime]
                    (reset! saved runtime)
                    (let [c (capture! runtime [{:symbol '$s :source (provider events {:bad-estimate? true})}]
                                         '[:find ?e :in $s :where [$s ?e :row/value _]])
                          failure (error (fn [] (planning/plan! runtime c interfaces)))]
                      (is (= 1 (count (events-of events :session-close))))
                      (is (every? zero? (vals (control/usage (:control runtime)))))
                      (is (= :query-source/invalid (:error failure)))))))]
    (is (contains? #{:query-source/cancelled :query-source/invalid} (:error result)))
    (is (empty? (events-of events :open)))
    (is (every? zero? (vals (control/usage (:control @saved)))))))

(deftest whole-query-one-shot-audit-precedes-the-first-execution-cursor
  (let [events (atom [])
        result (error #(rt/with-runtime {}
                         (fn [runtime]
                           (let [p (provider events {:opens :once})
                                 c (capture! runtime [{:symbol '$a :source p} {:symbol '$b :source p}]
                                      '[:find ?e :in $a $b :where [$a ?e :row/value _] [$b ?e :row/value _]])]
                             (planning/plan! runtime c interfaces)))))]
    (is (= :query-source/unsupported (:error result)))
    (is (= 2 (count (events-of events :plan))))
    (is (= 1 (count (events-of events :session-close))))
    (is (empty? (events-of events :open)))))

(deftest private-native-residual-or-branch-work-cannot-disappear-from-a-plan
  (doseq [query ['[:find ?e :in $s :where [$s ?e :row/value ?v] [(not= ?v "hidden")]]
                 '[:find ?e :in $s :where (or [$s ?e :row/value "one"] [$s ?e :row/value "two"])]]]
    (let [events (atom [])
          result (error #(rt/with-runtime {}
                           (fn [runtime]
                             (let [c (capture! runtime [{:symbol '$s :source (provider events {})}] query)]
                               (planning/plan! runtime c interfaces)))))]
      (is (= :query-source/unsupported (:error result)))
      (is (empty? (events-of events :acquire)))
      (is (empty? (events-of events :plan)))
      (is (empty? (events-of events :open))))))

(deftest malformed-inputs-or-unsupported-consistency-fail-before-acquisition
  (doseq [options [{:consistency :bogus} {:sources {'$unknown {:consistency :snapshot}}}]
          :let [events (atom [])]]
    (is (= :query-source/invalid
           (:error (error #(rt/with-runtime {}
                             (fn [runtime]
                               (capture! runtime [{:symbol '$s :source (provider events {})}]
                                 '[:find ?e :in $s :where [$s ?e :row/value _]] {} #{} options)))))))
    (is (empty? (events-of events :acquire)))))

(deftest compilation-has-one-owner-and-can-only-be-planned-once
  (let [events (atom [])]
    (rt/with-runtime {}
      (fn [runtime]
        (let [c (capture! runtime [{:symbol '$s :source (provider events {})}]
                   '[:find ?e :in $s :where [$s ?e :row/value _]])
              planned (planning/plan! runtime c interfaces)]
          (is (= :query-source/invalid (:error (error #(planning/plan! runtime c interfaces)))))
          (rt/with-runtime {} (fn [foreign]
                               (is (= :query-source/invalid (:error (error #(planning/schedules foreign planned)))))))
          (is (= 1 (count (events-of events :acquire))))
          (is (= 1 (count (events-of events :plan)))))))))

(deftest primary-planning-failure-is-redacted-and-preserved-when-close-fails
  (let [events (atom [])
        failure (caught #(rt/with-runtime {}
                           (fn [runtime]
                             (let [c (capture! runtime
                                       [{:symbol '$s :source (provider events
                                                              {:close-error? true
                                                               :planning (fn [_] (throw (Exception. "private SQL secret")))})}]
                                       '[:find ?e :in $s :where [$s ?e :row/value _]])]
                               (planning/plan! runtime c interfaces)))))]
    (is (= :query-source/provider (:error (ex-data failure))))
    (is (= '$s (:source-symbol (ex-data failure))))
    (is (= 0 (:region-id (ex-data failure))))
    (is (nil? (.getCause ^Throwable failure)))
    (is (= 1 (count (.getSuppressed ^Throwable failure))))
    (is (= 1 (count (events-of events :session-close))))
    (is (empty? (events-of events :open)))))
