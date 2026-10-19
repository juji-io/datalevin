(ns datalevin.query.source-envelope-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.envelope :as envelope]
            [datalevin.query.source.permissions :as permissions]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :as value]
            [datalevin.query.source-session-test :as fixture]))

(def columns [{:variable '?e :type :long :nullable? false}
              {:variable '?v :type :string :nullable? false}])
(defn unknown-estimate [dependent?]
  (merge (zipmap (if dependent?
                  [:startup-ns :per-binding-ns :rows-per-binding :row-bytes :requests-per-open
                   :requests-per-binding :transfer-bytes-per-binding :peak-retained-bytes]
                  [:startup-ns :run-ns :rows :row-bytes :requests-per-open :transfer-bytes :peak-retained-bytes])
                (repeat nil))
         {:confidence :unknown :provenance {:kind :unknown :id nil :observed-at-ms nil :valid-for-ms nil}}))

(defn request [runtime dependent?]
  {:spi-version 2 :features #{} :query-id (:query-id runtime) :authorization {:principal-id :application}
   :consistency :snapshot :control (:control runtime) :limits (control/limits (:control runtime))
   :source-symbol '$s :source-descriptor fixture/descriptor :region-id 4 :scope-id "scope:rows"
   :clauses [{:id 0 :kind :pattern :terms [[:var '?e] [:const :row/value] [:var '?v]]}]
   :bindings {} :available-inputs (if dependent? [(first columns)] []) :required-vars #{'?e '?v}
   :demand {:quality :exact :ordering [] :offset 0 :limit nil} :max-alternatives 32})

(defn alternative [dependent?]
  {:id :lookup :plan (Object.) :covers #{0} :prefilters #{} :requires (if dependent? [(first columns)] [])
   :columns columns :scope-id "scope:rows" :completion :all-bindings
   :access {:kind (if dependent? :entity-lookup :attribute-enumeration) :anchor-clause 0}
   :properties {:replay :unstable :ordering [] :blocking-state :none}
   :constraints {:max-binding-rows 256 :max-binding-bytes 262144 :max-request-binding-rows 32}
   :estimate (unknown-estimate dependent?) :explain {:label "fixture"}})

(defn- error [f] (try (f) nil (catch clojure.lang.ExceptionInfo e (ex-data e))))
(defn- with-fixture [f]
  (rt/with-runtime {}
    (fn [runtime]
      (with-open [consumption (permissions/consumption (:control runtime)
                               {:consumption {:opens :once :max-open-cursors 1}})]
        (f runtime consumption)))))

(deftest both-revised-shapes-capture-without-spending-one-shot-permission
  (with-fixture
    (fn [runtime consumption]
      (doseq [dependent? [false true]]
        (let [a (alternative dependent?)
              captured (envelope/capture-response! runtime (request runtime dependent?) {:alternatives [a]} consumption)
              [owned] (envelope/alternatives runtime captured)]
          (is (= a owned))
          (is (not (identical? a owned)))
          (is (identical? (:plan a) (:plan owned)))
          (is (false? (:spent? (permissions/planning-permission consumption owned)))))))))

(deftest every-required-alternative-field-is-validated-before-selection
  (with-fixture
    (fn [runtime consumption]
      (doseq [dependent? [false true] key (keys (alternative dependent?))]
        (let [before (control/usage (:control runtime))]
          (is (= :query-source/invalid
                 (:error (error #(envelope/capture-response! runtime (request runtime dependent?)
                                   {:alternatives [(dissoc (alternative dependent?) key)]} consumption)))))
          (is (= before (control/usage (:control runtime)))))))))

(deftest estimates-retain-zero-partial-unknown-and-stale-statistics
  (doseq [dependent? [false true]
          n [nil 0 1 1.5 1/3 99999999999999999999999999999N
             (bigint (.pow (biginteger 10) 400)) (bigdec "1E+400")]]
    (let [unknown (unknown-estimate dependent?)
          row-key (if dependent? :rows-per-binding :rows)
          estimate (assoc unknown row-key n
                          :confidence (if (nil? n) :unknown :low)
                          :provenance {:kind :statistics :id "old-model" :observed-at-ms nil :valid-for-ms 0})]
      (is (identical? estimate (envelope/estimate! estimate dependent? (fn []))))))
  (doseq [dependent? [false true]
          [key val] [[:confidence :medium] [:confidence :bogus] [:provenance nil]
                     [:startup-ns -1] [:startup-ns Double/NaN] [:startup-ns Double/POSITIVE_INFINITY]
                     [:startup-ns "1"] [:startup-ms 0]]]
    (is (= :query-source/invalid
           (:error (error #(envelope/estimate! (assoc (unknown-estimate dependent?) key val) dependent? (fn [])))))))
  (doseq [dependent? [false true] key (keys (unknown-estimate dependent?))]
    (is (= :query-source/invalid
           (:error (error #(envelope/estimate! (dissoc (unknown-estimate dependent?) key) dependent? (fn [])))))))
  (doseq [[key val] [[:kind :sample] [:id :private] [:observed-at-ms -1] [:valid-for-ms 0.5] [:extra true]]]
    (is (= :query-source/invalid
           (:error (error #(envelope/estimate! (assoc-in (unknown-estimate false) [:provenance key] val)
                                                false (fn []))))))))

(deftest dependent-estimates-cannot-use-per-invocation-fields-or-the-old-envelope
  (doseq [dependent? [false true]]
    (is (= :query-source/invalid
           (:error (error #(envelope/estimate! (unknown-estimate (not dependent?)) dependent? (fn [])))))))
  (is (= :query-source/invalid
         (:error (error #(envelope/estimate! {:rows 1 :row-bytes 1 :startup-ms 0 :per-row-ns 1
                                              :requests 1 :confidence :high} false (fn [])))))))

(deftest malformed-unused-alternatives-are-rejected-and-all-charges-roll-back
  (with-fixture
    (fn [runtime consumption]
      (doseq [[path val] [[[:covers] #{}] [[:covers] #{9}] [[:prefilters] #{0}]
                          [[:id] (keyword "")]
                          [[:scope-id] "scope:other"] [[:completion] :some-bindings]
                          [[:access :anchor-clause] 9] [[:access :kind] :scan]
                          [[:access :other] 0] [[:properties :replay] :reopen]
                          [[:properties :ordering] ['?e]] [[:properties :blocking-state] :unknown]
                          [[:properties :consumption-group] :invented]
                          [[:constraints :max-binding-rows] 0] [[:constraints :max-binding-bytes] -1]
                          [[:constraints :max-request-binding-rows] 0.5]
                          [[:columns 0 :type] :bigint] [[:columns 0 :nullable?] true]
                          [[:columns 0 :variable] '?hidden] [[:requires] columns]
                          [[:explain :label] ""] [[:estimate] nil]]]
        (let [before (control/usage (:control runtime))
              bad (-> (alternative false) (assoc :id :unused) (assoc-in path val))]
          (is (= :query-source/invalid
                 (:error (error #(envelope/capture-response! runtime (request runtime false)
                                   {:alternatives [(alternative false) bad]} consumption)))))
          (is (= before (control/usage (:control runtime)))))))))

(deftest request-identity-scope-typed-inputs-and-region-local-demand-are-checked
  (with-fixture
    (fn [runtime consumption]
      (doseq [[path val] [[[:spi-version] 1] [[:query-id] :foreign] [[:control] (Object.)]
                          [[:region-id] -1] [[:scope-id] "scope:other"] [[:source-symbol] '?s]
                          [[:demand :limit] 1] [[:demand :offset] 1] [[:max-alternatives] 0]
                          [[:available-inputs] [(assoc (first columns) :variable '?bad)]]
                          [[:clauses 0 :id] "opaque"] [[:clauses 0 :terms 1] [:var '?a]]
                          [[:clauses 0 :terms 0] [:const "bad entity"]]
                          [[:bindings] {'?hidden 1}] [[:required-vars] #{'?hidden}]]]
        (is (some? (:error (error #(envelope/capture-response! runtime
                                    (assoc-in (request runtime true) path val)
                                    {:alternatives [(alternative true)]} consumption)))))))))

(deftest fact-constants-preserve-ordinary-equality-across-numeric-representations
  (rt/with-runtime {}
    (fn [runtime]
      (doseq [entity [1 1N (java.math.BigInteger/valueOf 1)]]
        (let [r (-> (request runtime false)
                    (assoc-in [:clauses 0 :terms 0] [:const entity])
                    (assoc :required-vars #{'?v}))]
          (is (identical? r (envelope/request! runtime r)))))
      (doseq [entity [nil "one"]]
        (is (= :query-source/invalid
               (:error (error #(envelope/request! runtime
                                 (assoc-in (request runtime false) [:clauses 0 :terms 0] [:const entity]))))))))))

(deftest exact-predicates-and-prefilters-keep-all-required-local-columns
  (with-fixture
    (fn [runtime consumption]
      (let [r (update (request runtime false) :clauses conj
                      {:id 1 :kind :predicate :operator :not= :semantics-version 1
                       :args [[:var '?v] [:const "hidden"]]})]
        (doseq [claim [:covers :prefilters]]
          (let [a (update (alternative false) claim conj 1)
                captured (envelope/capture-response! runtime r {:alternatives [a]} consumption)]
            (is (= [a] (envelope/alternatives runtime captured)))))
        (is (= :query-source/invalid
               (:error (error #(envelope/capture-response! runtime r
                                 {:alternatives [(assoc (alternative false) :columns [(first columns)])]}
                                 consumption)))))))))

(deftest qualified-annotations-are-validated-and-detached-with-only-explicit-handles-opaque
  (with-fixture
    (fn [runtime consumption]
      (let [backing [:unused :owned :unused]
            a (assoc (alternative false) :example/annotation (with-meta (subvec backing 1 2) {:note :owned}))
            captured (envelope/capture-response! runtime (request runtime false) {:alternatives [a]} consumption)
            [owned] (envelope/alternatives runtime captured)]
        (is (nil? (value/vector-parent (:example/annotation owned))))
        (is (= {:note :owned} (meta (:example/annotation owned))))
        (doseq [response [{:alternatives [(assoc a :example/handle (Object.))]}
                          {:alternatives [a] :example/handle (Object.)}
                          {:alternatives (with-meta [a] {:example/handle (Object.)})}
                          {:alternatives [(with-meta a {:example/handle (Object.)})]}]]
          (is (= :query-source/invalid
                 (:error (error #(envelope/capture-response! runtime (request runtime false) response consumption))))))))))

(deftest hidden-vector-backing-is-admitted-even-when-the-visible-candidate-is-valid
  (rt/with-runtime {:limits {:retained-bytes 20000}}
    (fn [runtime]
      (with-open [consumption (permissions/consumption (:control runtime)
                               {:consumption {:opens :many :max-open-cursors 1}})]
        (let [before (control/usage (:control runtime))
              parent [(alternative false)
                      (assoc (alternative false) :example/large (apply str (repeat 30000 "x")))]]
          (is (= :query-source/limit
                 (:error (error #(envelope/capture-response! runtime (request runtime false)
                                   {:alternatives (subvec parent 0 1)} consumption)))))
          (is (= before (control/usage (:control runtime)))))))))

(deftest explicit-consumption-groups-are-validated-during-planning-without-spending-them
  (rt/with-runtime {}
    (fn [runtime]
      (with-open [consumption (permissions/consumption (:control runtime)
                               {:consumption-groups [{:id :left :opens :once :max-open-cursors 1}
                                                     {:id :right :opens :many :max-open-cursors 2}]})]
        (doseq [group [nil :missing :left :right]]
          (let [a (cond-> (alternative false) group (assoc-in [:properties :consumption-group] group))]
            (if (#{:left :right} group)
              (let [captured (envelope/capture-response! runtime (request runtime false) {:alternatives [a]} consumption)]
                (is (= [a] (envelope/alternatives runtime captured)))
                (is (false? (:spent? (permissions/planning-permission consumption a)))))
              (let [failure (error #(envelope/capture-response! runtime (request runtime false)
                                     {:alternatives [a]} consumption))]
                (is (= :query-source/invalid (:error failure)))
                (is (= :planning (:phase failure)))))))))))

(deftest selection-drops-unused-envelopes-and-cannot-change-or-cross-query-ownership
  (let [saved (atom nil)]
    (with-fixture
      (fn [runtime consumption]
        (let [a (alternative false) b (assoc a :id :unused :example/large (apply str (repeat 20000 "x")))
              captured (envelope/capture-response! runtime (request runtime false) {:alternatives [a b]} consumption)
              before (:retained-bytes (control/usage (:control runtime)))]
          (reset! saved captured)
          (is (= [a] (envelope/select! runtime captured [:lookup])))
          (is (> (- before (:retained-bytes (control/usage (:control runtime)))) 20000))
          (is (= [a] (envelope/alternatives runtime captured)))
          (is (= :query-source/invalid (:error (error #(envelope/select! runtime captured [:lookup])))))
          (rt/with-runtime {} (fn [foreign]
                               (is (= :query-source/invalid
                                      (:error (error #(envelope/alternatives foreign captured))))))))))
    (rt/with-runtime {} (fn [runtime]
                         (is (= :query-source/invalid
                                (:error (error #(envelope/alternatives runtime @saved)))))))))
