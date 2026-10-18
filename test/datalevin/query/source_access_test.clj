(ns datalevin.query.source-access-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.query.source.access :as source-access]
            [datalevin.query.access :as access]
            [datalevin.query.plan :as plan]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.permissions :as permissions]
            [datalevin.query.source.runtime :as rt])
  (:import [java.io Closeable]
           [java.util ArrayList Collections Random]))

(defn- column
  ([variable] (column variable :long false))
  ([variable type nullable?] {:variable variable :type type :nullable? nullable?}))

(defn- pattern [id e a v]
  {:id id :kind :pattern :terms [[:var e] [:const a] [:var v]]})

(defn- request [source region clauses available]
  {:source-symbol source :region-id region :scope-id "scope:all" :clauses clauses
   :bindings {} :available-inputs available :max-alternatives 32
   :required-vars (into #{} (comp (mapcat #(or (:terms %) (:args %)))
                                 (keep #(when (= :var (first %)) (second %)))) clauses)
   :demand {:quality :exact :ordering [] :offset 0 :limit nil}})

(defn- estimate [requires]
  (assoc (zipmap (if (seq requires)
                  [:startup-ns :per-binding-ns :rows-per-binding :row-bytes :requests-per-open
                   :requests-per-binding :transfer-bytes-per-binding :peak-retained-bytes]
                  [:startup-ns :run-ns :rows :row-bytes :requests-per-open :transfer-bytes
                   :peak-retained-bytes]) (repeat nil))
         :confidence :unknown :provenance {:kind :unknown :id nil :observed-at-ms nil :valid-for-ms nil}))

(defn- alternative [id covers kind requires columns]
  {:id id :plan (Object.) :covers covers :prefilters #{} :requires requires :columns columns
   :scope-id "scope:all" :completion :all-bindings
   :access {:kind kind :anchor-clause (first (sort covers))}
   :properties {:replay :unstable :ordering [] :blocking-state :none}
   :constraints {:max-binding-rows 10 :max-binding-bytes 4096 :max-request-binding-rows 2}
   :estimate (estimate requires) :explain {:label "private provider label"}})

(defn- error [f]
  (try (f) nil (catch clojure.lang.ExceptionInfo e (ex-data e))))

(defn- consumption
  ([runtime] (consumption runtime {:consumption {:opens :many :max-open-cursors 1}}))
  ([runtime info]
   (let [resource (permissions/consumption (:control runtime) info)]
     (rt/own! runtime #(.close ^Closeable resource))
     resource)))

(defn- method [events]
  (reify access/IAccessMethod
    (-access-plans [_ _] (swap! events conj :discover) [])
    (-open-access [_ _ _ _ _ _]
      (swap! events conj :open)
      (reify access/IAccessCursor
        (-next-batch [_]
          (swap! events conj :fetch)
          (access/->AccessBatch [[1 2]] nil true))
        (-close-cursor [_] (swap! events conj :close))))
    (-frontier-satisfies? [_ _ _ _ _] false)))

(defn- candidates [runtime request consumption alternatives events]
  (let [implementation (method events)]
    (mapv #(source-access/candidate! runtime request % consumption implementation) alternatives)))

(defn- ids [schedule]
  (mapv #(get-in % [:path :options :alternative-id]) (:plans schedule)))

(defn- shuffled [^Random random values]
  (let [items (ArrayList. ^java.util.Collection values)]
    (Collections/shuffle items random)
    (vec items)))

(defn- local-operator! [runtime region id requires columns after]
  (source-access/core-operator! runtime region id requires columns after
    (plan/map->IdentityStep {:in (set (map :variable requires)) :cols (mapv :variable columns)})))

(defrecord PlanningTrap [in cols private-value]
  plan/IStep
  (-type [_] (throw (Exception. "unexpected step callback")))
  (-execute [_ _ _] (throw (Exception. "unexpected execution")))
  (-execute-pipe [_ _ _ _] (throw (Exception. "unexpected pipe")))
  (-sample [_ _ _] (throw (Exception. "unexpected sample")))
  (-explain [_ _] (throw (Exception. "unexpected private explanation"))))

(deftest local-step-planning-and-explanation-use-only-its-explicit-interface
  (rt/with-runtime {}
    (fn [runtime]
      (let [private "do-not-emit-this-native-or-rule-body"
            opaque (reify Object (toString [_] (throw (Exception. "do not inspect body"))))
            column (assoc (column '?e) :private/annotation private)
            operator (source-access/core-operator! runtime 0 0 [] [column] #{}
                       (->PlanningTrap #{} ['?e] opaque))
            schedule (source-access/schedule! runtime 0 [] [] [] [operator])
            explain (source-access/explain! runtime schedule)]
        (is (= [(dissoc column :private/annotation)] (:columns operator)))
        (is (= [:core] (mapv :kind (:operators schedule))))
        (is (= [] (:steps explain)))
        (is (not (.contains (pr-str explain) private)))
        (is (not (contains? explain :operators)))))))

(deftest shared-local-exports-are-scheduled-before-dependent-access
  (rt/with-runtime {}
    (fn [runtime]
      (let [events (atom []) r (request '$s 3 [(pattern 0 '?e :a '?v)] [(column '?e)])
            plans (candidates runtime r (consumption runtime)
                              [(alternative :lookup #{0} :entity-lookup [(column '?e)] [(column '?v)])] events)
            operator (local-operator! runtime 3 0 [] [(column '?e)] #{})
            schedule (source-access/schedule! runtime 3 [] [r] plans [operator])]
        (is (= :query-source/unsupported
               (:error (error #(source-access/schedule! runtime 3 [] [r] plans)))))
        (is (= [:core :access] (mapv :kind (:operators schedule))))
        (is (identical? operator (get-in schedule [:operators 0 :value])))
        (is (= [:lookup] (ids schedule)))
        (is (empty? @events))))))

(deftest local-operators-cannot-hide-a-cycle-or-supply-a-nullable-or-incompatible-key
  (doseq [[requires columns] [[[(column '?v)] [(column '?e) (column '?v)]]
                              [[] [(column '?e :string false)]]
                              [[] [(column '?e :long true)]]]]
    (rt/with-runtime {}
      (fn [runtime]
        (let [events (atom []) r (request '$s 0 [(pattern 0 '?e :a '?v)] [(column '?e)])
              plans (candidates runtime r (consumption runtime)
                                [(alternative :lookup #{0} :entity-lookup [(column '?e)] [(column '?v)])] events)
              operator (local-operator! runtime 0 0 requires columns #{})]
          (is (= :query-source/unsupported
                 (:error (error #(source-access/schedule! runtime 0 [] [r] plans [operator])))))
          (is (empty? @events)))))))

(deftest core-ordering-barriers-are-enforced-even-for-already-bound-variables
  (rt/with-runtime {}
    (fn [runtime]
      (let [events (atom []) inputs [(column '?e)]
            r (request '$s 0 [(pattern 0 '?e :a '?v)] inputs)
            plans (candidates runtime r (consumption runtime)
                              [(alternative :lookup #{0} :entity-lookup inputs [(column '?v)])] events)
            first (local-operator! runtime 0 8 inputs inputs #{['$s 0 0]})
            second (local-operator! runtime 0 2 inputs inputs #{[:core 0 8]})
            schedule (source-access/schedule! runtime 0 inputs [r] plans [second first])]
        (is (= [:access :core :core] (mapv :kind (:operators schedule))))
        (is (= [8 2] (mapv (comp :id :value) (rest (:operators schedule)))))
        (is (empty? @events))))))

(deftest only-the-captured-shared-step-schema-can-enter-a-local-interface
  (rt/with-runtime {}
    (fn [runtime]
      (let [step (plan/map->IdentityStep {:in #{'?e} :cols ['?e '?v]})
            before (control/usage (:control runtime))]
        (doseq [[requires columns] [[[] [(column '?e) (column '?v)]]
                                   [[(column '?e)] [(column '?e)]]
                                   [[(column '?e)] [(column '?e) (column '?private)]]]]
          (is (= :query-source/invalid
                 (:error (error #(source-access/core-operator! runtime 0 0 requires columns #{} step))))))
        (is (= before (control/usage (:control runtime))))))))

(deftest local-operator-identities-cannot-cross-regions-or-invocations
  (rt/with-runtime {}
    (fn [runtime]
      (let [operator (local-operator! runtime 4 0 [] [(column '?e)] #{})]
        (is (= :query-source/invalid
               (:error (error #(source-access/schedule! runtime 0 [] [] [] [operator])))))
        (is (= :query-source/invalid
               (:error (error #(source-access/schedule! runtime 4 [] [] [] [operator operator])))))
        (rt/with-runtime {}
          (fn [other]
            (is (= :query-source/invalid
                   (:error (error #(source-access/schedule! other 4 [] [] [] [operator])))))))))))

(deftest local-ordering-edges-must-name-work-in-the-same-region
  (rt/with-runtime {}
    (fn [runtime]
      (doseq [after [#{[:core 8 0]} #{['$missing 4 0]}]]
        (let [operator (local-operator! runtime 4 0 [] [(column '?e)] after)
              before (control/usage (:control runtime))]
          (is (= :query-source/invalid
                 (:error (error #(source-access/schedule! runtime 4 [] [] [] [operator])))))
          (is (= before (control/usage (:control runtime)))))))))

(deftest bridge-uses-shared-access-records-without-native-costs-or-planning-reads
  (rt/with-runtime {}
    (fn [runtime]
      (let [r (request '$s 4 [(pattern 0 '?e :a '?v)] [])
            a (alternative :scan #{0} :attribute-enumeration [] [(column '?e) (column '?v)])
            events (atom [])
            [p] (candidates runtime r (consumption runtime) [a] events)
            schedule (source-access/schedule! runtime 4 [] [r] [p])]
        (is (instance? datalevin.query.access.AccessPlan p))
        (is (= #{['$s 4 0]} (get-in p [:expr :covers])))
        (is (= ['?e '?v] (get-in p [:expr :cols])))
        (is (nil? (:estimate p)))
        (is (= (:estimate a) (:advisory-estimate p)))
        (is (false? (access/planning-sample? (:path p))))
        (is (false? (access/reusable-sample? (:path p))))
        (is (= #{:complete} (get-in p [:path :capabilities])))
        (is (= [:scan] (ids schedule)))
        (is (= :eav-structural-v1 (:policy schedule)))
        (is (= :structural (:selection schedule)))
        (is (nil? (:estimated-access-ns schedule)))
        (is (= [] (:fallback-reasons schedule)))
        (source-access/explain! runtime schedule)
        (is (empty? @events))
        ;; Execute a bridged input-free path using the real shared plan step.
        ;; Full source lifecycle and result quotas still belong to its adapter.
        (let [step (plan/access-step (:expr p) (:path p) (access/complete-demand :exact #{'?e '?v}))]
          (is (= :query-source/unsupported (:error (error #(plan/-sample step nil nil)))))
          (is (= :query-source/unsupported (:error (error #(plan/access-sample-batch step nil)))))
          (is (empty? @events))
          (is (= [[1 2]] (vec (plan/-execute step nil nil)))))
        (is (= [:open :fetch :close] @events))))))

(deftest structural-choice-is-independent-of-response-order-and-estimates
  (let [random (Random. 273921)]
    (dotimes [trial 60]
      (rt/with-runtime {}
        (fn [runtime]
          (let [inputs [(column '?e) (column '?v)]
                r (request '$s 0 [(pattern 0 '?e :a '?v)] inputs)
                alts [(alternative "entity-z" #{0} :entity-lookup [(column '?e)] [(column '?v)])
                      (alternative "entity-a" #{0} :entity-lookup [(column '?e)] [(column '?v)])
                      (alternative :value #{0} :value-lookup [(column '?v)] [(column '?e)])
                      (alternative :scan #{0} :attribute-enumeration [] inputs)]
                alts (mapv (fn [i a]
                             (assoc a :estimate (if (even? (+ trial i))
                                                  (estimate (:requires a))
                                                  (assoc (into {} (map (fn [[k v]] [k (if (nil? v) 0 v)])
                                                                      (estimate (:requires a))))
                                                         :confidence :high))
                                      :explain {:label (str "private relabel " trial i)})) (range) alts)
                events (atom [])
                plans (candidates runtime r (consumption runtime) (shuffled random alts) events)]
            (is (= ["entity-a"] (ids (source-access/schedule! runtime 0 inputs [r] plans))))
            (is (empty? @events))))))))

(deftest dependency-ready-inputs-determine-lookup-versus-enumeration
  (doseq [[inputs expected] [[[] :scan] [[(column '?v)] :value] [[(column '?e)] :entity]]]
    (rt/with-runtime {}
      (fn [runtime]
        (let [available [(column '?e) (column '?v)]
              r (request '$s 0 [(pattern 0 '?e :a '?v)] available)
              events (atom [])
              alts [(alternative :entity #{0} :entity-lookup [(column '?e)] [(column '?v)])
                    (alternative :value #{0} :value-lookup [(column '?v)] [(column '?e)])
                    (alternative :scan #{0} :attribute-enumeration [] available)]
              plans (candidates runtime r (consumption runtime) alts events)]
          (is (= [expected] (ids (source-access/schedule! runtime 0 inputs [r] plans)))))))))

(deftest dependency-dag-uses-only-selected-typed-outputs
  (rt/with-runtime {}
    (fn [runtime]
      (let [r (request '$s 0 [(pattern 0 '?e :a '?a) (pattern 1 '?a :b '?b)]
                       [(column '?e) (column '?a)])
            events (atom [])
            plans (candidates runtime r (consumption runtime)
                              [(alternative :second #{1} :entity-lookup [(column '?a)] [(column '?b)])
                               (alternative :seed #{0} :entity-lookup [(column '?e)] [(column '?a)])] events)]
        (is (= [:seed :second]
               (ids (source-access/schedule! runtime 0 [(column '?e)] [r] plans))))
        (is (empty? @events))))))

(deftest missing-self-supplied-and-unseeded-cyclic-inputs-reject-before-open
  (doseq [case [:missing :self :cycle]]
    (rt/with-runtime {}
      (fn [runtime]
        (let [clauses (if (= case :cycle)
                        [(pattern 0 '?a :a '?b) (pattern 1 '?b :b '?a)]
                        [(pattern 0 '?a :a '?b)])
              r (request '$s 0 clauses [(column '?a) (column '?b)])
              events (atom [])
              alts (cond-> [(alternative :first #{0} :entity-lookup [(column '?a)]
                                         (if (= case :self) [(column '?a) (column '?b)] [(column '?b)]))]
                     (= case :cycle) (conj (alternative :second #{1} :entity-lookup [(column '?b)] [(column '?a)])))
              plans (candidates runtime r (consumption runtime) alts events)]
          (is (= :query-source/unsupported
                 (:error (error #(source-access/schedule! runtime 0 [] [r] plans)))))
          (is (empty? @events)))))))

(deftest initial-policy-skips-fusion-and-requires-complete-single-clause-coverage
  (doseq [separate? [false true]]
    (rt/with-runtime {}
      (fn [runtime]
        (let [r (request '$s 0 [(pattern 0 '?e :a '?a) (pattern 1 '?e :b '?b)] [])
              events (atom [])
              alts (cond-> [(alternative :fused #{0 1} :attribute-enumeration []
                                        [(column '?e) (column '?a) (column '?b)])]
                     separate? (into [(alternative :a #{0} :attribute-enumeration [] [(column '?e) (column '?a)])
                                      (alternative :b #{1} :attribute-enumeration [] [(column '?e) (column '?b)])]))
              plans (candidates runtime r (consumption runtime) alts events)]
          (if separate?
            (is (= [:a :b] (ids (source-access/schedule! runtime 0 [] [r] plans))))
            (is (= :query-source/unsupported
                   (:error (error #(source-access/schedule! runtime 0 [] [r] plans))))))
          (is (empty? @events)))))))

(deftest region-and-alias-identities-qualify-every-clause-and-alternative
  (rt/with-runtime {}
    (fn [runtime]
      (let [events (atom []) resource (consumption runtime)
            r1 (request '$a 1 [(pattern 0 '?e :a '?v)] [])
            r2 (request '$b 1 [(pattern 0 '?e :a '?v)] [])
            r3 (request '$a 2 [(pattern 0 '?e :a '?v)] [])
            a (alternative :same-id #{0} :attribute-enumeration [] [(column '?e) (column '?v)])
            [p1] (candidates runtime r1 resource [a] events)
            [p2] (candidates runtime r2 resource [a] events)
            [p3] (candidates runtime r3 resource [a] events)]
        (is (= [:same-id :same-id] (ids (source-access/schedule! runtime 1 [] [r2 r1] [p2 p1]))))
        (is (= ['$a '$b] (mapv #(get-in % [:expr :source])
                                  (:plans (source-access/schedule! runtime 1 [] [r1 r2] [p2 p1])))))
        (is (= :query-source/invalid
               (:error (error #(source-access/schedule! runtime 1 [] [r1] [p3])))))
        (is (= :query-source/unsupported
               (:error (error #(source-access/schedule! runtime 1 [] [r1 r2] [p1])))))
        (is (empty? @events))))))

(deftest session-default-one-shot-is-shared-across-aliases-and-replay-is-independent
  (doseq [replay [:identical :unstable]]
    (rt/with-runtime {}
      (fn [runtime]
        (let [events (atom []) resource (consumption runtime {:consumption {:opens :once :max-open-cursors 1}})
              r1 (request '$a 0 [(pattern 0 '?e :a '?v)] [])
              r2 (request '$b 0 [(pattern 0 '?e :a '?v)] [])
              a (assoc-in (alternative :read #{0} :attribute-enumeration [] [(column '?e) (column '?v)])
                          [:properties :replay] replay)
              plans (into (candidates runtime r1 resource [a] events)
                          (candidates runtime r2 resource [a] events))]
          (is (= :query-source/unsupported
                 (:error (error #(source-access/schedule! runtime 0 [] [r1 r2] plans)))))
          (is (empty? @events)))))))

(deftest independent-one-shot-groups-and-complete-feasibility-fallback
  (rt/with-runtime {}
    (fn [runtime]
      (let [events (atom [])
            resource (consumption runtime {:consumption-groups [{:id :a :opens :once :max-open-cursors 1}
                                                                {:id :b :opens :once :max-open-cursors 1}]})
            r (request '$s 0 [(pattern 0 '?e :a '?a) (pattern 1 '?e :b '?b)] [(column '?e)])
            first (assoc-in (alternative :preferred-but-conflicting #{0} :entity-lookup [(column '?e)] [(column '?a)])
                            [:properties :consumption-group] :a)
            fallback (assoc-in (alternative :fallback #{0} :attribute-enumeration [] [(column '?e) (column '?a)])
                               [:properties :consumption-group] :b)
            second (assoc-in (alternative :second #{1} :entity-lookup [(column '?e)] [(column '?b)])
                             [:properties :consumption-group] :a)
            plans (candidates runtime r resource [first second fallback] events)]
        (is (= [:fallback :second]
               (ids (source-access/schedule! runtime 0 [(column '?e)] [r] plans))))
        ;; Planning consumes no open attempts, even while testing feasibility.
        (with-open [_ (permissions/reserve-open! resource first {:consumption-group :a})]
          (is true))
        (is (empty? @events))))))

(deftest stale-permission-snapshots-do-not-restore-spent-opens
  (rt/with-runtime {}
    (fn [runtime]
      (let [events (atom []) resource (consumption runtime {:consumption {:opens :once :max-open-cursors 1}})
            r (request '$s 0 [(pattern 0 '?e :a '?v)] [])
            a (alternative :read #{0} :attribute-enumeration [] [(column '?e) (column '?v)])
            plans (candidates runtime r resource [a] events)]
        (is (= [:read] (ids (source-access/schedule! runtime 0 [] [r] plans))))
        (with-open [_ (permissions/reserve-open! resource a {})] (is true))
        (is (= :query-source/unsupported
               (:error (error #(source-access/schedule! runtime 0 [] [r] plans)))))
        (is (empty? @events))))))

(deftest known-lookups-can-remain-input-free
  (rt/with-runtime {}
    (fn [runtime]
      (let [r (assoc (request '$s 0 [(pattern 0 '?e :a '?v)] []) :bindings {'?e 42})
            a (alternative :constant #{0} :entity-lookup [] [(column '?v)])
            events (atom [])
            plans (candidates runtime r (consumption runtime) [a] events)]
        (is (= [:constant] (ids (source-access/schedule! runtime 0 [(column '?e)] [r] plans))))
        (is (= #{} (get-in (first plans) [:expr :requires])))
        (is (empty? @events))))))

(deftest typed-domains-and-nullability-must-admit-all-upstream-inputs
  (doseq [[upstream expected] [[(column '?e :long false) :success]
                               [(column '?e :long true) :query-source/unsupported]
                               [(column '?e :string false) :query-source/unsupported]]]
    (rt/with-runtime {}
      (fn [runtime]
        (let [r (request '$s 0 [(pattern 0 '?e :a '?v)] [(column '?e)])
              events (atom [])
              plans (candidates runtime r (consumption runtime)
                                [(alternative :lookup #{0} :entity-lookup [(column '?e)] [(column '?v)])] events)
              run #(source-access/schedule! runtime 0 [upstream] [r] plans)]
          (if (= :success expected)
            (is (= [:lookup] (ids (run))))
            (is (= expected (:error (error run))))))))))

(deftest unicode-keyword-string-and-numeric-order-is-canonical
  (doseq [[alternatives expected] [[[:z "a"] :z]
                                   [["\uD800\uDC00" "\uE000"] "\uE000"]
                                   [[:z/z :z/a] :z/a]]]
    (rt/with-runtime {}
      (fn [runtime]
        (let [r (request '$s 0 [(pattern 10 '?e :a '?v) (pattern 2 '?e :b '?v)] [])
              events (atom [])
              alts (into [(alternative :first #{2} :attribute-enumeration [] [(column '?e) (column '?v)])]
                         (map #(alternative % #{10} :attribute-enumeration [] [(column '?e) (column '?v)])) alternatives)
              plans (candidates runtime r (consumption runtime) alts events)]
          (is (= [:first expected] (ids (source-access/schedule! runtime 0 [] [r] plans)))))))))

(deftest invalid-region-scope-anchor-correlation-and-live-columns-reject
  (doseq [mutate [(fn [a] (assoc a :scope-id "scope:other"))
                  (fn [a] (assoc a :covers #{99}))
                  (fn [a] (assoc a :columns []))
                  (fn [a] (assoc-in a [:access :anchor-clause] 1))
                  (fn [a] (assoc a :requires [(column '?private)]))
                  (fn [a] (assoc a :requires [(column '?e :string false)]))
                  (fn [a] (assoc-in a [:access :kind] :value-lookup))]]
    (rt/with-runtime {}
      (fn [runtime]
        (let [r (request '$s 0 [(pattern 0 '?e :a '?v)] [(column '?e :long false)])
              resource (consumption runtime)
              a (alternative :lookup #{0} :entity-lookup [(column '?e :long false)] [(column '?v)])
              events (atom [])]
          (is (= :query-source/invalid
                 (:error (error #(candidates runtime r resource [(mutate a)] events)))))
          (is (empty? @events)))))))

(deftest selected-outputs-cannot-use-incompatible-complete-types
  (rt/with-runtime {}
    (fn [runtime]
      (let [r (request '$s 0 [(pattern 0 '?e :a '?v) (pattern 1 '?e :b '?v)] [])
            events (atom []) resource (consumption runtime)
            plans (candidates runtime r resource
                              [(alternative :long #{0} :attribute-enumeration [] [(column '?e) (column '?v)])
                               (alternative :string #{1} :attribute-enumeration []
                                            [(column '?e) (column '?v :string false)])] events)]
        (is (= :query-source/unsupported
               (:error (error #(source-access/schedule! runtime 0 [] [r] plans)))))
        (is (empty? @events))))))

(deftest predicate-claims-stay-in-their-region-and-retain-all-operands
  (rt/with-runtime {}
    (fn [runtime]
      (let [p {:id 2 :kind :predicate :operator :> :semantics-version 1 :args [[:var '?v] [:var '?limit]]}
            r (request '$s 0 [(pattern 0 '?e :a '?v) p] [(column '?limit)])
            events (atom []) resource (consumption runtime)
            a (assoc (alternative :prefilter #{0} :attribute-enumeration [(column '?limit)]
                                   [(column '?e) (column '?v)]) :prefilters #{2})
            plans (candidates runtime r resource [a] events)]
        (is (= [:prefilter] (ids (source-access/schedule! runtime 0 [(column '?limit)] [r] plans))))
        (is (= :query-source/invalid
               (:error (error #(candidates runtime r resource [(assoc a :requires [])] events)))))
        (is (= :query-source/invalid
               (:error (error #(candidates runtime r resource [(assoc a :prefilters #{9})] events)))))
        (is (empty? @events))))))

(deftest explanation-and-core-error-location-omit-private-provider-fields
  (let [private "not-for-diagnostics"]
    (rt/with-runtime {}
      (fn [runtime]
        (let [r (request (with-meta '$s {:secret private}) 7 [(pattern 0 '?e :a '?v)] [])
              events (atom []) resource (consumption runtime)
              a (-> (alternative :scan #{0} :attribute-enumeration []
                                 [(assoc (column '?e) :private/column private)
                                  (column (with-meta '?v {:secret private}))])
                    (assoc :private/annotation private
                           :plan (reify Object (toString [_] (throw (Exception. "do not print handle")))))
                    (assoc-in [:estimate :private/annotation] private)
                    (assoc-in [:estimate :provenance :private/annotation] private)
                    (assoc-in [:access :private/annotation] private)
                    (assoc-in [:constraints :private/annotation] private)
                    (with-meta {:secret private}))
              plans (candidates runtime r resource [a] events)
              schedule (source-access/schedule! runtime 7 [] [r] plans)
              explanation (source-access/explain! runtime schedule)
              printed (binding [*print-meta* true] (pr-str explanation))]
          (is (= :eav-structural-v1 (:policy explanation)))
          (is (= 7 (get-in explanation [:steps 0 :region-id])))
          (is (= (:estimate (alternative :scan #{0} :attribute-enumeration [] []))
                 (get-in explanation [:steps 0 :estimate])))
          (is (not (.contains printed private)))
          (is (not (.contains printed "private provider label")))
          (is (not (.contains printed ":plan ")))
          (is (nil? (get-in explanation [:steps 0 :consumption-group])))
          (is (empty? @events)))))
    (let [data (error #(rt/with-runtime {}
                         (fn [runtime]
                           (rt/with-context runtime {:source-symbol '$s :region-id 7 :clause-ids [0]
                                                     :alternative-id :scan}
                             (fn [_] (throw (ex-info private {:error :query-source/provider
                                                             :region-id private :secret private})))))))]
      (is (= 7 (:region-id data)))
      (is (= :scan (:alternative-id data)))
      (is (not (.contains (pr-str data) private))))))

(deftest metadata-admission-precedes-discarded-alternative-traversal
  (let [saved (atom nil) visited (atom false)]
    (is (= :query-source/limit
           (:error (error #(rt/with-runtime {:limits {:retained-bytes 16000}}
                             (fn [runtime]
                               (reset! saved runtime)
                               (let [r (request '$s 0 [(pattern 0 '?e :a '?v)] [])
                                     a (assoc (alternative :huge #{0} :attribute-enumeration []
                                                           [(column '?e) (column '?v)])
                                              :private/annotation [(apply str (repeat 20000 "x"))
                                                                   (reify Object
                                                                     (toString [_] (reset! visited true) "opaque"))])]
                                 (candidates runtime r (consumption runtime) [a] (atom [])))))))))
    (is (false? @visited))
    (is (every? zero? (vals (control/usage (:control @saved)))))))

(deftest planner-workspace-is-released-and-quota-failures-open-no-cursors
  (rt/with-runtime {}
    (fn [runtime]
      (let [events (atom []) resource (consumption runtime)
            r (request '$s 0 [(pattern 0 '?e :a '?v)] [])
            plans (candidates runtime r resource
                              [(alternative :scan #{0} :attribute-enumeration [] [(column '?e) (column '?v)])] events)
            ctl (:control runtime)
            before (:retained-bytes (control/usage ctl))
            schedule (source-access/schedule! runtime 0 [] [r] plans)]
        ;; The only new live state is the admitted selected-plan container.
        (is (= (+ before 1536) (:retained-bytes (control/usage ctl))))
        (is (= [:scan] (ids schedule)))
        (let [remaining (- (:retained-bytes (control/limits ctl)) (:retained-bytes (control/usage ctl)))
              held (control/reserve! ctl {:retained-bytes (- remaining 100)})]
          (try
            (is (= :query-source/limit
                   (:error (error #(source-access/schedule! runtime 0 [] [r] plans)))))
            (finally (control/release! held))))
        (is (= (+ before 1536) (:retained-bytes (control/usage ctl))))
        (is (empty? @events))))))

(deftest planner-bound-and-cancellation-never-produce-partial-coverage
  (testing "infeasible independent work exhausts the existing bound"
    (rt/with-runtime {}
      (fn [runtime]
        (let [events (atom []) resource (consumption runtime)
              r (request '$s 0 (mapv #(pattern % '?e :a '?v) (range 14)) [])
              plans (candidates runtime r resource
                                (mapv #(alternative (str %) #{%} :attribute-enumeration []
                                                    [(column '?e) (column '?v)]) (range 13)) events)
              before (control/usage (:control runtime))]
          (is (= :query-source/unsupported
                 (:error (error #(source-access/schedule! runtime 0 [] [r] plans)))))
          (is (= before (control/usage (:control runtime))))
          (is (empty? @events))))))
  (testing "cancellation before scheduling"
    (let [saved (atom nil) events (atom [])]
      (is (= :query-source/cancelled
             (:error (error #(rt/with-runtime {}
                               (fn [runtime]
                                 (reset! saved runtime)
                                 (let [r (request '$s 0 [(pattern 0 '?e :a '?v)] [])
                                       plans (candidates runtime r (consumption runtime)
                                                         [(alternative :scan #{0} :attribute-enumeration []
                                                                       [(column '?e) (column '?v)])] events)]
                                   (control/cancel! (:control runtime)
                                     {:error :query-source/cancelled :phase :planning})
                                   (source-access/schedule! runtime 0 [] [r] plans))))))))
      (is (empty? @events))
      (is (every? zero? (vals (control/usage (:control @saved))))))))

(deftest all-regions-share-consumption-permissions-and-have-distinct-identities
  (doseq [opens [:once :many]]
    (rt/with-runtime {}
      (fn [runtime]
        (let [events (atom []) resource (consumption runtime {:consumption {:opens opens :max-open-cursors 1}})
              a (alternative :same #{0} :attribute-enumeration [] [(column '?e) (column '?v)])
              schedules (mapv (fn [region]
                                (let [r (request '$s region [(pattern 0 '?e :a '?v)] [])
                                      plans (candidates runtime r resource [a] events)]
                                  (source-access/schedule! runtime region [] [r] plans))) [1 2])
              before (control/usage (:control runtime))]
          (if (= :many opens)
            (is (nil? (source-access/validate-query! runtime schedules)))
            (is (= :query-source/unsupported
                   (:error (error #(source-access/validate-query! runtime schedules))))))
          (is (= :query-source/invalid
                 (:error (error #(source-access/validate-query! runtime [(first schedules) (first schedules)])))))
          (is (= before (control/usage (:control runtime))))
          (is (empty? @events)))))))

(deftest explain-shows-dependent-caps-and-only-explicit-consumption-groups
  (rt/with-runtime {:limits {:binding-rows 4 :binding-bytes 2048}}
    (fn [runtime]
      (let [events (atom [])
            resource (consumption runtime {:consumption-groups [{:id :a :opens :once :max-open-cursors 1}
                                                                {:id :b :opens :many :max-open-cursors 1}]})
            r (request '$s 0 [(pattern 0 '?e :a '?v)] [(column '?e)])
            a (assoc-in (alternative :lookup #{0} :entity-lookup [(column '?e)] [(column '?v)])
                        [:properties :consumption-group] :a)
            plans (candidates runtime r resource [a] events)
            schedule (source-access/schedule! runtime 0 [(column '?e)] [r] plans)
            explanation (source-access/explain! runtime schedule)]
        (is (nil? (source-access/validate-query! runtime [schedule])))
        (is (= {:rows 4 :bytes 2048} (get-in explanation [:steps 0 :binding-limits])))
        (is (= :a (get-in explanation [:steps 0 :consumption-group])))
        (is (= :unstable (get-in explanation [:steps 0 :replay])))
        (is (<= 0 (:planning-time-ms explanation)))
        (is (empty? @events))))))
