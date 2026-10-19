(ns datalevin.query.source-collection-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.test.check :as check]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [datalevin.core :as d]
            [datalevin.parser :as parser]
            [datalevin.query.source :as source]
            [datalevin.query.source.collection :as collection]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.planning :as planning]
            [datalevin.query.source.program :as program]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source-planning-test :as fixture])
  (:import [datalevin.utl QueryKey]
           [java.util Date Iterator]))

(defn- error [f] (try (f) nil (catch clojure.lang.ExceptionInfo e (ex-data e))))
(defn- events-of [events kind] (filterv #(= kind (first %)) @events))
(defn- query! [query arguments & [options]]
  (rt/with-runtime (or options {})
    (fn [runtime]
      (let [result (program/query! runtime query arguments)]
        (is (every? zero? (vals (control/usage (:control runtime)))))
        result))))

(deftest ordinary-local-tuple-and-eav-patterns-use-shared-matching
  (doseq [[query facts arguments]
          [['[:find ?id :in $rows :where [$rows ?id "two"]] [[1 "one"] [2 "two"]] []]
           ['[:find ?id ?a ?v :in $rows :where [$rows ?id ?a ?v]]
            [[1 :name "one"] [2 :age 2] [3 :absent nil]] []]
           ['[:find ?id :in $rows ?v :where [$rows ?id ?v]] [[1 "one"] [2 "two"]] ["two"]]
           ['[:find ?v :in $rows :where [$rows ?v ?v]] [[1 1] [1 2] [2N 2] [3 3N]] []]
           ['[:find ?id ?v :in $rows :where [$rows ?id _ ?v]] [[1 :a "one" :extra] [2 :b "two" :extra]] []]
           ['[:find ?id :in $rows :where [$rows ?id :a _]] [[1 :a nil] [2 :b nil]] []]
           ['[:find ?a ?b :in $rows :where [$rows ?a 1] [$rows ?b 2]] [[1 1] [2 2] [3 1]] []]
           ['[:find ?v :in $rows :where [$rows ?v]] [] []]]]
    (let [arguments (into [facts] arguments)]
      (is (= (apply d/q query arguments) (query! query arguments)) (pr-str query))))
  (is (= #{[1] [2]} (query! '[:find ?v :where [?v]] [[[1] [2]]]))))

(deftest generated-local-patterns-match-the-existing-collection-engine
  (is (:pass?
        (check/quick-check 100
          (prop/for-all [facts (gen/vector (gen/tuple (gen/choose 0 4) (gen/elements [:a :b])
                                                       (gen/elements [nil 1 2 "one" "two"])) 0 15)]
            (every? (fn [query] (= (d/q query facts) (query! query [facts])))
              ['[:find ?e ?a ?v :in $rows :where [$rows ?e ?a ?v]]
               '[:find ?e ?v :in $rows :where [$rows ?e :a ?v]]
               '[:find ?e :in $rows :where [$rows ?e _ 1N]]
               '[:find ?e :in $rows :where [$rows ?e _ ?e]]]))))))

(deftest complete-local-output-types-seed-dependent-source-access
  (let [events (atom []) query '[:find ?tag ?v :in $rows $s
                                :where [$rows ?e ?tag] [$s ?e :row/value ?v]]
        facts [[1 :first] [2 :second] [1 :third] [1 :first] [3 :missing]]]
    (is (= (d/q query facts [[1 :row/value "one"] [2 :row/value "two"]])
           (query! query [facts (fixture/provider events {:lookup-only? true})])))
    (is (= 1 (count (events-of events :open))))
    (is (= #{:long} (into #{} (keep (fn [column] (when (= '?e (:variable column)) (:type column))))
                          (:available-inputs (second (first (events-of events :plan)))))))
    (is (= 1 (count (events-of events :session-close))))))

(deftest exact-empty-local-matches-prune-dependent-work-before-provider-planning
  (doseq [[query facts rules]
          [['[:find ?v :in $rows $s % :where [$rows ?e] [$s ?e :row/value ?v]] [] []]
           ['[:find ?v :in $rows $s % :where [$rows ?e :missing] [$s ?e :row/value ?v]] [[1 :present]] []]
           ['[:find ?v :in $rows $s % :where ($rows ids ?e) [$s ?e :row/value ?v]] []
            '[[(ids ?e) [?e _]]]]]]
    (let [events (atom [])]
      (is (= #{} (query! query [facts (fixture/provider events {:lookup-only? true}) rules])))
      (is (= 1 (count (events-of events :acquire))))
      (is (empty? (events-of events :plan)))
      (is (empty? (events-of events :open)))
      (is (= 1 (count (events-of events :session-close)))))))

(deftest exact-empty-local-branches-do-not-prune-live-union-work
  (let [events (atom [])
        query '[:find ?e ?v :in $rows $s
                :where (or (and [$rows ?e :missing] [$s ?e :row/value ?v])
                           [$s ?e :row/value ?v])]]
    (is (= #{[1 "one"] [2 "two"]} (query! query [[[1 :present]] (fixture/provider events {})])))
    (is (= 1 (count (events-of events :plan))))
    (is (= 1 (count (events-of events :open))))))

(deftest local-collection-joins-retain-correlations-and-every-outer-witness
  (let [events (atom [])
        query '[:find ?e ?tag ?v :in $rows $s [[?e ?tag]]
                :where [$rows ?e ?tag] [$s ?e :row/value ?v]]
        outer [[1 :a] [1 :b] [1 :a] [2 :c] [2 :wrong]]]
    (rt/with-runtime {}
      (fn [runtime]
        (let [c (planning/capture! runtime
                  [{:symbol '$rows :native [[1 :a] [1 :a] [1 :b] [2 :c]]}
                   {:symbol '$s :source (fixture/provider events {:lookup-only? true})}]
                  (parser/parse-query query) [] {} #{'?e '?tag} nil)
              p (program/compile! runtime c
                  [{:variable '?e :type :long :nullable? false}
                   {:variable '?tag :type :keyword :nullable? false}] outer)
              result (program/execute! runtime p)
              rows (mapv (fn [^objects row]
                           (mapv #(aget row (int (get (:attrs result) %))) ['?e '?tag '?v])) (:tuples result))]
          (is (= (frequencies [[1 :a "one"] [1 :a "one"] [1 :a "one"] [1 :a "one"]
                               [1 :b "one"] [2 :c "two"]])
                 (frequencies rows))))))))

(deftest local-patterns-inside-external-rule-and-union-regions-use-shared-scopes
  (let [facts [[1 :a] [1 :a] [2 :b] [2 :c]]
        rules '[[(named [?e] ?tag ?name) [$rows ?e ?tag] [$s ?e :row/value ?name]]]
        query '[:find ?e ?tag ?name :in $rows $s % [?e ...]
                :where (or ($s named ?e ?tag ?name)
                           (and [$rows ?e ?tag] [$s ?e :row/value ?name]))]
        events (atom [])]
    (is (= #{[1 :a "one"] [2 :b "two"] [2 :c "two"]}
           (query! query [facts (fixture/provider events {:lookup-only? true}) rules [1 2 2]])))
    (is (= 1 (count (events-of events :session-close))))))

(deftest positive-local-only-rules-preserve-required-input-correlations
  (let [facts [[1 :row/value "one"] [2 :row/value "two"]]
        rules '[[(named [?id] ?name) [?id :row/value ?name]]]
        query '[:find ?e ?tag ?name :in $rows % [[?e ?tag]]
                :where ($rows named ?e ?name)]
        outer [[1 :a] [1 :b] [2 :c] [1 :a]]]
    (is (= (d/q '[:find ?e ?tag ?name :in $rows [[?e ?tag]]
                  :where [$rows ?e :row/value ?name]] facts outer)
           (query! query [facts rules outer])))))

(deftest local-only-nested-rule-heads-complete-before-wildcard-unification
  (let [facts [[1 :a] [1 :b] [2 :c] [2 :c]]
        rules '[[(pair ?e ?tag) [?e ?tag]]
                [(nested ?e ?tag) (pair ?e ?tag)]
                [(same ?e ?other) [?e _] [(identity ?e) ?other]]]]
    (is (= [[2]] (query! '[:find (count ?e) :in $rows % :where ($rows nested ?e _)] [facts rules])))
    (rt/with-runtime {}
      (fn [runtime]
        (let [c (planning/capture! runtime [{:symbol '$rows :native facts}]
                  (parser/parse-query '[:find ?e :in $rows % :where ($rows nested ?e _)])
                  (parser/parse-rules rules) {} #{} nil)
              rel (program/execute! runtime (program/compile! runtime c [] nil))]
          (is (= {1 1, 2 1}
                 (frequencies (map #(aget ^objects % (int (get (:attrs rel) '?e))) (:tuples rel))))))))
    (is (= #{[1] [2]} (query! '[:find ?e :in $rows % :where ($rows same ?e ?e)] [facts rules])))
    (is (= #{[:a] [:b]} (query! '[:find ?tag :in $rows % :where ($rows nested 1N ?tag)] [facts rules])))))

(deftest local-rule-literal-nulls-and-repeated-null-terms-remain-valid-facts
  (let [facts [[1 nil] [2 "two"] [nil nil]] rules '[[(pair ?e ?v) [?e ?v]]]]
    (is (= #{[1] [nil]}
           (query! '[:find ?e :in $rows % :where ($rows pair ?e nil)] [facts rules])))
    (is (= #{[nil]} (query! '[:find ?e :in $rows % :where ($rows pair ?e ?e)] [facts rules])))))

(deftest recursive-local-rule-programs-retain-the-controlled-emission-gate
  (let [events (atom [])
        rules '[[(reachable ?e) [?e _]] [(reachable ?e) [?e ?next] (reachable ?next)]]]
    (is (= :query-source/unsupported
           (:error (error (fn [] (query! '[:find ?v :in $rows $s % :where
                                           ($rows reachable ?e) [$s ?e :row/value ?v]]
                                  [[[1 2] [2 3]] (fixture/provider events {}) rules]))))))
    (is (empty? (events-of events :acquire)))))

(deftest local-nullable-and-heterogeneous-keys-do-not-falsify-readiness
  (let [facts [[nil :null] [1 :a] ["other" :other] [2 :b]] events (atom [])
        query '[:find ?tag ?v :in $rows $s :where [$rows ?e ?tag] [$s ?e :row/value ?v]]]
    (is (= #{[:a "one"] [:b "two"]} (query! query [facts (fixture/provider events {})])))
    (is (every? #(empty? (:bindings (second %))) (events-of events :open)))))

(deftest zero-column-local-reads-and-empty-sources-keep-their-normal-meaning
  (doseq [[facts query arguments]
          [[[[1] [1] [2]] '[:find ?v :in $rows ?v :where [$rows _]] [10]]
           [[] '[:find ?v :in $rows ?v :where [$rows _]] [10]]
           [[[1] [2]] '[:find (count ?v) :with ?tag :in $rows [[?v ?tag]]
                        :where [$rows _]] [[[1 :a] [2 :b]]]]
           [[[1]] '[:find ?v :in $rows ?v :where [$rows _ :ignored-beyond-row]] [10]]]]
    (is (= (apply d/q query facts arguments) (query! query (into [facts] arguments))))))

(deftest local-patterns-preserve-byte-composite-and-repeated-numeric-equality
  (let [a (byte-array [1 2]) b (byte-array [1 2])
        facts [[a [1 :a]] [b [2 :b]]]
        query {:find ['?key '?v] :in ['$rows] :where [['$rows '?key '?v]]}
        result (query! query [facts])]
    (is (= 2 (count result)))
    (is (every? #(QueryKey/equals a (first %)) result))
    (is (= #{[[1 :a]]} (query! {:find ['?v] :in ['$rows] :where [['$rows a '?v]]}
                            [[[b [1 :a]] [(byte-array [3]) [2 :b]]]]))))
  (let [query '[:find ?x :in $rows :where [$rows ?x ?x]]
        facts [[(byte-array [1]) (byte-array [1])]
               [[1 :a] '(1 :a)] [1 1N] [1N 1]]]
    ;; Repeated terms use ordinary equivalence; retained output keys still
    ;; distinguish the original Long and BigInt representations.
    (is (= 4 (count (query! query [facts]))))))

(deftest parsed-literal-pattern-syntax-is-not-reinterpreted-as-a-wildcard
  (doseq [constant ['_ '?literal]]
    (rt/with-runtime {}
      (fn [runtime]
        (let [source (collection/capture! runtime [[constant 1] ['other 2]])
              spec (collection/spec! runtime source
                     (parser/->Pattern (parser/->DefaultSrc)
                       [(parser/->Constant constant) (parser/->Variable '?value)]))]
          (is (= [[1]] (mapv vec (get-in spec [:relation :tuples])))))))))

(deftest local-source-aliases-capture-a-one-shot-producer-only-once
  (let [iterators (atom 0) reads (atom 0)
        rows (reify Iterable
               (iterator [_]
                 (swap! iterators inc)
                 (reify Iterator
                   (hasNext [_] (< @reads 2))
                   (next [_] [(long (swap! reads inc))]))))
        query '[:find ?a ?b :in $a $b :where [$a ?a] [$b ?b]]]
    (is (= #{[1 1] [1 2] [2 1] [2 2]} (query! query [rows rows])))
    (is (= 1 @iterators))
    (is (= 2 @reads))))

(deftest reusable-local-facts-and-leaves-are-owned-before-next-pulls
  (let [payload (byte-array 1) row (object-array 2)
        rows (reify Iterable
               (iterator [_]
                 (let [i (atom 0)]
                   (reify Iterator
                     (hasNext [_] (< @i 3))
                     (next [_]
                       (let [n (swap! i inc)]
                         (aset-byte payload 0 (byte n))
                         (aset row 0 (long n)) (aset row 1 payload) row))))))
        query '[:find ?e ?payload :in $rows :where [$rows ?e ?payload]]]
    (is (= #{[1 [1]] [2 [2]] [3 [3]]}
           (into #{} (map (fn [[e bytes]] [e (vec bytes)])) (query! query [rows])))))
  (let [payload (byte-array 1)
        row (reify Iterable
              (iterator [_]
                (let [i (atom 0)]
                  (reify Iterator
                    (hasNext [_] (< @i 2))
                    (next [_] (aset-byte payload 0 (byte (swap! i inc))) payload)))))
        query '[:find ?a ?b :in $rows :where [$rows ?a ?b]]]
    (is (= [[[1] [2]]]
           (mapv (fn [[a b]] [(vec a) (vec b)]) (query! query [[row]]))))))

(deftest all-local-sources-are-detached-before-external-descriptors
  (doseq [ordinary? [false true]]
    (let [payload (byte-array [1]) instant (Date. 100) events (atom [])
          facts [[payload instant]] delegate (fixture/provider events {})
          provider (reify source/IQuerySource
                     (-source-info [_]
                       (aset-byte payload 0 (byte 9)) (.setTime instant 900)
                       (source/-source-info delegate))
                     (-open-session [_ request] (source/-open-session delegate request))
                     (-plan-source [_ session request] (source/-plan-source delegate session request)))
          query '[:find ?payload ?instant ?name :in $s $rows
                  :where [$rows ?payload ?instant] [$s _ :row/value ?name]]
          result (rt/with-runtime {}
                   (fn [runtime]
                     (if ordinary? (program/query! runtime query [provider facts])
                         (program/run! runtime
                           (planning/capture! runtime [{:symbol '$s :source provider} {:symbol '$rows :native facts}]
                             (parser/parse-query query) [] {} #{} nil) [] nil))))]
      (is (= #{[[1] 100 "one"] [[1] 100 "two"]}
             (into #{} (map (fn [[p ^Date t name]] [(vec p) (.getTime t) name])) result)))
      (is (= 1 (count (events-of events :session-close)))))))

(deftest local-capture-stops-at-quota-and-releases-partial-work
  (doseq [facts [(repeat [1]) [(repeat 1)]]]
    (let [events (atom []) saved (atom nil)]
      (is (= :query-source/limit
             (:error (error #(rt/with-runtime {:limits {:retained-bytes 100000}}
                               (fn [runtime]
                                 (reset! saved runtime)
                                 (program/query! runtime
                                   '[:find ?v :in $s $rows :where [$rows ?e] [$s ?e :row/value ?v]]
                                   [(fixture/provider events {}) facts])))))))
      (is (empty? @events))
      (is (every? zero? (vals (control/usage (:control @saved))))))))

(deftest local-capture-observes-cancellation-before-descriptor-callbacks
  (let [events (atom []) saved (atom nil)]
    (is (= :query-source/cancelled
           (:error (error
                     (fn []
                       (rt/with-runtime {}
                         (fn [runtime]
                           (reset! saved runtime)
                           (let [rows (reify Iterable
                                        (iterator [_]
                                          (reify Iterator
                                            (hasNext [_] true)
                                            (next [_]
                                              (control/cancel! (:control runtime)
                                                {:error :query-source/cancelled :phase :inputs})
                                              [1]))))]
                             (program/query! runtime
                               '[:find ?v :in $s $rows :where [$rows ?e] [$s ?e :row/value ?v]]
                               [(fixture/provider events {}) rows])))))))))
    (is (empty? @events))
    (is (every? zero? (vals (control/usage (:control @saved)))))))

(deftest local-cartesian-output-is-admitted-before-materialization
  (let [saved (atom nil)]
    (is (= :query-source/limit
           (:error (error
                     (fn []
                       (rt/with-runtime {}
                         (fn [runtime]
                           (reset! saved runtime)
                           (let [c (planning/capture! runtime
                                     [{:symbol '$a :native (mapv vector (range 20))}
                                      {:symbol '$b :native (mapv vector (range 20))}]
                                     (parser/parse-query '[:find ?a ?b :in $a $b :where [$a ?a] [$b ?b]])
                                     [] {} #{} nil)
                                 p (program/compile! runtime c [] nil)
                                 ctl (:control runtime)
                                 remaining (- (:retained-bytes (control/limits ctl))
                                              (:retained-bytes (control/usage ctl)))
                                 held (control/reserve! ctl {:retained-bytes (- remaining 30000)})]
                             (try (program/execute! runtime p)
                                  (finally (control/release! held)))))))))))
    (is (every? zero? (vals (control/usage (:control @saved)))))))

(deftest malformed-missing-local-fields-fail-before-provider-acquisition
  (let [events (atom []) saved (atom nil)]
    (is (= :query-source/invalid
           (:error (error #(rt/with-runtime {}
                             (fn [runtime]
                               (reset! saved runtime)
                               (program/query! runtime
                                 '[:find ?v :in $s $rows :where [$rows ?e ?tag] [$s ?e :row/value ?v]]
                                 [(fixture/provider events {}) [[1]]])))))))
    (is (empty? (events-of events :acquire)))
    (is (every? zero? (vals (control/usage (:control @saved)))))))

(deftest foreign-and-closed-owned-collections-cannot-be-reused
  (let [saved (atom nil)]
    (rt/with-runtime {} #(reset! saved (collection/capture! % [[1]])))
    (is (= :query-source/invalid
           (:error (error #(query! '[:find ?v :in $rows :where [$rows ?v]] [@saved]))))))
  (let [saved (atom nil)]
    (is (= :query-source/invalid
           (:error (error #(rt/with-runtime {}
                             (fn [runtime]
                               (let [c (planning/capture! runtime [{:symbol '$rows :native [[1]]}]
                                         (parser/parse-query '[:find ?v :in $rows :where [$rows ?v]]) [] {} #{} nil)]
                                 (reset! saved (:native (first (:entries c))))
                                 ((:close c))
                                 (collection/check! runtime @saved))))))))))
