(ns datalevin.query.source-native-plan-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.core :as d]
            [datalevin.interpret :as inter]
            [datalevin.parser :as parser]
            [datalevin.query.access :as access]
            [datalevin.query.execute :as execute]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.native :as native]
            [datalevin.query.source.planning :as planning]
            [datalevin.query.source.program :as program]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source-planning-test :as fixture]
            [datalevin.util :as u])
  (:import [datalevin.utl QueryKey]
           [java.util Arrays Date UUID]))

(defn- events-of [events kind] (filterv #(= kind (first %)) @events))
(def symbol-predicate-bound "Fixture resolved through a qualified predicate argument." 'stored)
(def ?host-predicate-value "Fixture resolved before the same-named tuple column." 0)
(defn- error [f] (try (f) nil (catch clojure.lang.ExceptionInfo e (ex-data e))))
(defn- with-db [schema facts f]
  (let [dir (str u/tmp-dir "spi-v2-native-plan-" (UUID/randomUUID))
        conn (d/get-conn dir schema)]
    (try (d/transact! conn facts) (f @conn conn)
         (finally (d/close conn) (u/delete-files dir)))))

(defn- capture! [runtime database events query rules config & [input-vars]]
  (planning/capture! runtime
    [{:symbol '$n :native database} {:symbol '$s :source (fixture/provider events config)}]
    (parser/parse-query query) (parser/parse-rules rules) {} (or input-vars #{}) nil))

(defn- rows [rel variables]
  (mapv (fn [^objects row] (mapv #(aget row (int (get (:attrs rel) %))) variables)) (:tuples rel)))

(defn- execute-input! [runtime database events query columns input config]
  (let [c (capture! runtime database events query [] config (set (map :variable columns)))]
    (native/with-views [database]
      #(program/execute! runtime (program/compile! runtime c columns input %)))))

(deftest native-value-constants-and-equality-predicates-skip-unrelated-payloads
  (let [giant (apply str (repeat 1000000 "z"))]
    (with-db {:native/text {:db/valueType :db.type/string :db/cardinality :db.cardinality/many}
              :native/private {:db/valueType :db.type/string :db/cardinality :db.cardinality/many :db/noindex true}}
      [{:db/id 1 :native/text ["wanted" giant] :native/private ["wanted" giant]}
       {:db/id 2 :native/text ["wanted" giant]}]
      (fn [database _]
        (doseq [[clauses expected]
                [[ '[[$n ?e :native/text "wanted"] [$s ?e :row/value ?v]] #{["one"] ["two"]}]
                 [ '[[$n ?e :native/text ?text] [(= ?text "wanted")] [$s ?e :row/value ?v]] #{["one"] ["two"]}]
                 [ '[[$n 1 :native/text "wanted"] [$s 1 :row/value ?v]] #{["one"]}]
                 [ '[[$n 1 :native/private "wanted"] [$s 1 :row/value ?v]] #{["one"]}]
                 [ '[[$n 1 :native/private ?text] [(= "wanted" ?text)] [$s 1 :row/value ?v]] #{["one"]}]
                 [ '[[$n ?e :native/text 100] [$s ?e :row/value ?v]] #{}]
                 [ '[[$n 1 :native/private 100] [$s 1 :row/value ?v]] #{}]]]
          (let [events (atom []) saved (atom nil)]
            (rt/with-runtime {:limits {:retained-bytes 300000}}
              (fn [runtime]
                (reset! saved runtime)
                (let [query {:find ['?v] :in ['$n '$s] :where clauses}]
                  (is (= expected (program/run! runtime (capture! runtime database events query [] {:lookup-only? true}) [] nil))
                      (pr-str query)))))
            (is (= 1 (count (events-of events :session-close))))
            (is (every? zero? (vals (control/usage (:control @saved)))))))))))

(deftest native-value-links-deduplicate-input-keys-and-restore-every-outer-witness
  (with-db {:native/text {:db/valueType :db.type/string :db/cardinality :db.cardinality/many}}
    [{:db/id 1 :native/text ["wanted" (apply str (repeat 1000000 "z"))]}
     {:db/id 2 :native/text "wanted"}]
    (fn [database _]
      (let [events (atom []) saved (atom nil)]
        (rt/with-runtime {:limits {:retained-bytes 400000}}
          (fn [runtime]
            (reset! saved runtime)
            (let [result (execute-input! runtime database events
                           '[:find ?e ?text ?tag ?v :in $n $s [[?text ?tag]] :where
                             [$n ?e :native/text ?text] [$s ?e :row/value ?v]]
                           [{:variable '?text :type :string :nullable? true}
                            {:variable '?tag :type :keyword :nullable? false}]
                           [["wanted" :a] ["wanted" :b] ["wanted" :a] ["missing" :missing] [nil :nil]]
                           {:lookup-only? true})]
              (is (= (frequencies [[1 "wanted" :a "one"] [2 "wanted" :a "two"]
                                   [1 "wanted" :b "one"] [2 "wanted" :b "two"]
                                   [1 "wanted" :a "one"] [2 "wanted" :a "two"]])
                     (frequencies (rows result ['?e '?text '?tag '?v]))))
              (is (= 1 (count (events-of events :open)))))))
        (is (every? zero? (vals (control/usage (:control @saved)))))))))

(deftest native-entity-value-lookups-preserve-correlated-keys-on-unindexed-attributes
  (with-db {:native/private {:db/valueType :db.type/string :db/cardinality :db.cardinality/many :db/noindex true}}
    [{:db/id 1 :native/private ["wanted" (apply str (repeat 1000000 "z"))]}
     {:db/id 2 :native/private "other"}]
    (fn [database _]
      (rt/with-runtime {:limits {:retained-bytes 300000}}
        (fn [runtime]
          (let [result (execute-input! runtime database (atom [])
                         '[:find ?e ?text ?tag :in $n $s [[?e ?text ?tag]] :where [$n ?e :native/private ?text]]
                         [{:variable '?e :type :long :nullable? false}
                          {:variable '?text :type :string :nullable? true}
                          {:variable '?tag :type :keyword :nullable? false}]
                         [[1 "wanted" :a] [1 "other" :missing] [2 "other" :b]
                          [1 "wanted" :a] [2 "wanted" :missing] [1 nil :nil]] {})]
            (is (= (frequencies [[1 "wanted" :a] [1 "wanted" :a] [2 "other" :b]])
                   (frequencies (rows result ['?e '?text '?tag]))))))))))

(deftest native-range-candidates-retain-shared-predicate-semantics
  (doseq [[type values endpoints]
          [[:long [Long/MIN_VALUE -1 0 1 2 Long/MAX_VALUE] [-1 0 1]]
           [:double [Double/NEGATIVE_INFINITY -1.0 0.0 1.0 Double/POSITIVE_INFINITY] [-1.0 -0.0 0.0 1.0]]
           [:float [Float/NEGATIVE_INFINITY (float -1.0) (float 0.0) (float 1.0) Float/POSITIVE_INFINITY]
            [(float -1.0) (float -0.0) (float 0.0) (float 1.0)]]
           [:instant [(Date. -1) (Date. 0) (Date. 1)] [(Date. -1) (Date. 0) (Date. 1)]]]]
    (with-db {:native/value {:db/valueType (keyword "db.type" (name type)) :db/cardinality :db.cardinality/many}}
      (mapv #(hash-map :db/id 1 :native/value %) values)
      (fn [database _]
        (doseq [operator ['< '<= '> '>= '=] bound endpoints first? [true false]
                entity ['?e 1]]
          (let [predicate [(apply list operator (if first? ['?x bound] [bound '?x]))]
                query {:find ['?x] :in ['$n '$s]
                       :where [['$n entity :native/value '?x] predicate ['$s 1 :row/value '_]]}
                facts (mapv #(vector 1 :native/value %) values)]
            (rt/with-runtime {}
              (fn [runtime]
                (is (= (d/q query facts [[1 :row/value "one"]])
                       (program/run! runtime (capture! runtime database (atom []) query [] {}) [] nil))
                    (pr-str type predicate entity))))))))))

(deftest native-range-conjunctions-intersect-without-removing-logical-endpoints
  (with-db {:native/value {:db/valueType :db.type/long :db/cardinality :db.cardinality/many}}
    (mapv #(hash-map :db/id 1 :native/value %) (range 1000))
    (fn [database _]
      (doseq [predicates ['[[(>= ?x 495)] [(< ?x 500)]]
                           '[[(< 495 ?x)] [(<= ?x 500)]]
                           '[[(>= ?x 500)] [(< ?x 495)]]
                           '[[(> ?x 500)] [(< ?x 500)]]
                           '[[(= ?x 500)] [(< ?x 500)]]]
              entity ['?e 1]]
        (let [query {:find ['?x] :in ['$n '$s]
                     :where (into [['$n entity :native/value '?x]]
                                  (concat predicates [['$s 1 :row/value '_]]))}]
          (rt/with-runtime {:limits {:retained-bytes 300000}}
            (fn [runtime]
              (is (= (d/q query (mapv #(vector 1 :native/value %) (range 1000)) [[1 :row/value "one"]])
                     (program/run! runtime (capture! runtime database (atom []) query [] {}) [] nil))
                  (pr-str predicates entity)))))))))

(deftest native-equality-predicates-admit-false-and-byte-content-bounds
  (with-db {:native/flag {:db/valueType :db.type/boolean}
            :native/bytes {:db/valueType :db.type/bytes}}
    [{:db/id 1 :native/flag false :native/bytes (byte-array [1 2])}
     {:db/id 2 :native/flag true :native/bytes (byte-array [3 4])}]
    (fn [database _]
      (doseq [[attribute bound] [[:native/flag false] [:native/bytes (byte-array [1 2])]]
              first? [true false]]
        (rt/with-runtime {}
          (fn [runtime]
            (let [query {:find ['?e] :in ['$n '$s]
                         :where [['$n '?e attribute '?value]
                                 [(apply list '= (if first? ['?value bound] [bound '?value]))]
                                 ['$s 1 :row/value '_]]}]
              (is (= #{[1]} (program/run! runtime (capture! runtime database (atom []) query [] {}) [] nil))))))))))

(deftest native-predicate-ranges-preserve-the-shared-resolvers-host-value-lookup
  (with-db {:native/symbol {:db/valueType :db.type/symbol}
            :native/number {:db/valueType :db.type/long}}
    [{:db/id 1 :native/symbol 'stored :native/number 0}
     {:db/id 2 :native/symbol 'other :native/number 1}]
    (fn [database _]
      (binding [*ns* (the-ns 'datalevin.query.source-native-plan-test)]
        (doseq [[query expected]
                [['[:find ?e :in $n $s :where
                    [$n ?e :native/symbol ?value]
                    [(= ?value datalevin.query.source-native-plan-test/symbol-predicate-bound)]
                    [$s 1 :row/value _]] #{[1]}]
                 ['[:find ?e :in $n $s :where
                    [$n ?e :native/number ?host-predicate-value] [(= ?host-predicate-value 0)]
                    [$s 1 :row/value _]] #{[1] [2]}]]]
          (rt/with-runtime {}
            (fn [runtime]
              (is (= expected (program/run! runtime (capture! runtime database (atom []) query [] {}) [] nil))))))))))

(deftest native-value-ranges-keep-complete-giant-collision-buckets
  (let [prefix (apply str (repeat 500 "shared")) a (str prefix "a") b (str prefix "b")
        unrelated (apply str (repeat 1000000 "z"))]
    (with-db {:native/text {:db/valueType :db.type/string :db/cardinality :db.cardinality/many}
              :native/private {:db/valueType :db.type/string :db/cardinality :db.cardinality/many :db/noindex true}}
      [{:db/id 1 :native/text [a b unrelated] :native/private [a b unrelated]} {:db/id 2 :native/text a}]
      (fn [database _]
        (doseq [[entity attribute expected] [['?e :native/text #{[1] [2]}] [1 :native/private #{[1] [2]}]]
                predicate? [false true]]
          (let [query {:find ['?e] :in ['$n '$s]
                       :where (cond-> [['$n entity attribute (if predicate? '?text a)] ['$s '?e :row/value '_]]
                                predicate? (conj [(list '= '?text a)]))}]
            (rt/with-runtime {:limits {:retained-bytes 800000}}
              (fn [runtime]
                (is (= expected (program/run! runtime (capture! runtime database (atom []) query [] {}) [] nil)))))))
        (rt/with-runtime {:limits {:retained-bytes 800000}}
          (fn [runtime]
            (let [result (execute-input! runtime database (atom [])
                           '[:find ?e ?text :in $n $s [?text ...] :where
                             [$n ?e :native/text ?text] [$s ?e :row/value _]]
                           [{:variable '?text :type :string :nullable? false}] [[a] [b] [a]] {})]
              (is (= (frequencies [[1 a] [2 a] [1 b] [1 a] [2 a]])
                     (frequencies (rows result ['?e '?text])))))))))))

(deftest native-value-seeks-preserve-logical-constants-and-representation-sensitive-keys
  (doseq [[type values wanted]
          [[:boolean [false true] [false true]]
           [:long [-1 0 1] [-1 0 1]]
           [:string ["a" "é界"] ["a" "absent"]]
           [:keyword [:a :scope/a] [:a :absent]]
           [:symbol ['a 'scope/a] ['a 'absent]]
           [:uuid [#uuid "00000000-0000-0000-0000-000000000000"] [#uuid "00000000-0000-0000-0000-000000000000"]]
           [:double [-1.0 0.0 1.0] [-0.0 0.0 1.0 Double/NaN]]
           [:float [(float -1.0) (float 0.0) (float 1.0)] [(float -0.0) (float 0.0) (float 1.0) Float/NaN]]
           [:bytes [(byte-array [1 2]) (byte-array [3 4])] [(byte-array [1 2]) (byte-array [5 6])]]
           [:bigdec [1.0M 1.00M 2M] [1.0M 2M]]
           [:data [[1 (byte-array [1 2])] {:x (byte-array [3 4])} Double/NaN]
            [[1 (byte-array [1 2])] {:x (byte-array [3 4])} Double/NaN]]]]
    (with-db {:native/value (if (= type :data) {} {:db/valueType (keyword "db.type" (name type))})}
      (mapv (fn [i v] {:db/id (long (inc i)) :native/value v}) (range) values)
      (fn [database _]
        (doseq [bound wanted literal? [true false]]
          (let [events (atom [])
                query {:find ['?e] :in (cond-> ['$n '$s] (not literal?) (conj '?wanted))
                       :where [['$n '?e :native/value (if literal? bound '?wanted)] ['$s 1 :row/value '_]]}
                ;; Floating index encoding normalizes signed zero. Compare the
                ;; decoded facts, and pin constant and input-key equality separately.
                facts (d/datoms database :eav)
                expected (into #{} (keep (fn [datom]
                                          (when (and (= :native/value (:a datom))
                                                     (if literal? (QueryKey/equiv bound (:v datom))
                                                         (QueryKey/equals bound (:v datom)))) [(:e datom)]))) facts)]
            (rt/with-runtime {}
              (fn [runtime]
                (is (= expected
                       (if literal? (program/run! runtime (capture! runtime database events query [] {}) [] nil)
                           (set (rows (execute-input! runtime database events query
                                        [{:variable '?wanted :type type :nullable? false}] [[bound]] {}) ['?e]))))
                    (pr-str type literal? bound))))))))))

(deftest native-value-links-use-pinned-index-data-through-provider-writes
  (with-db {:native/score {:db/valueType :db.type/long}} [{:db/id 1 :native/score 100}]
    (fn [database conn]
      (let [events (atom [])]
        (rt/with-runtime {}
          (fn [runtime]
            (let [result (execute-input! runtime database events
                           '[:find ?e ?score ?v :in $n $s ?score :where
                             [$n ?e :native/score ?score] [(>= ?score 100)] [$s ?e :row/value ?v]]
                           [{:variable '?score :type :long :nullable? false}] [[100]]
                           {:lookup-only? true
                            :planning (fn [_]
                                        (d/update-schema conn nil nil {:native/score :native/renamed})
                                        (d/transact! conn [[:db/add 1 :native/renamed 999]]))})]
              (is (= [[1 100 "one"]] (rows result ['?e '?score '?v]))))))
        (is (= 999 (d/q '[:find ?v . :where [1 :native/renamed ?v]] @conn)))
        (is (= 1 (count (events-of events :session-close))))))))

(deftest native-bound-encoding-failure-releases-scratch-and-compilation-resources
  (with-db {:native/text {:db/valueType :db.type/string}} [{:db/id 1 :native/text "wanted"}]
    (fn [database _]
      (let [events (atom []) saved (atom nil)
            failure (error
                      (fn []
                        (rt/with-runtime {:limits {:retained-bytes 1000000}}
                          (fn [runtime]
                            (reset! saved runtime)
                            (native/with-views [database]
                              (fn [snapshots]
                                (let [c (capture! runtime database events
                                          '[:find ?v :in $n $s :where
                                            [$n ?e :native/text "wanted"] [$s ?e :row/value ?v]] [] {:lookup-only? true})
                                      p (program/compile! runtime c [] nil snapshots)
                                      ;; The controlled driver fits; bound encoding must
                                      ;; receive a separate reservation before cursor open.
                                      remaining (- 1000000 (:retained-bytes (control/usage (:control runtime))))
                                      held (control/reserve! (:control runtime) {:retained-bytes (- remaining 10000)})]
                                  (rt/own! runtime #(control/release! held))
                                  (program/execute! runtime p))))))))]
        (is (= :query-source/limit (:error failure)))
        (is (= :retained-bytes (:category failure)))
        (is (= 1 (count (events-of events :plan))))
        (is (empty? (events-of events :open)))
        (is (= 1 (count (events-of events :session-close))))
        (is (every? zero? (vals (control/usage (:control @saved)))))))))

(deftest native-initializations-seed-dependent-access-in-the-shared-region
  (with-db {:native/score {:db/valueType :db.type/long}}
    [{:db/id 1 :native/score 100} {:db/id 2 :native/score 200} {:db/id 3 :native/score 300}]
    (fn [database _]
      (doseq [query ['[:find ?e ?score ?v :in $n $s :where
                       [$n ?e :native/score ?score] [$s ?e :row/value ?v]]
                     '[:find (sum ?score) . :in $n $s :where
                       [$s ?e :row/value ?v] [$n ?e :native/score ?score]]
                     '[:find ?e ?v :in $n $s :where
                       [$n ?e :native/score _] [$s ?e :row/value ?v]]]]
        (let [events (atom []) saved (atom nil)]
          (rt/with-runtime {:limits {:binding-rows 1}}
            (fn [runtime]
              (reset! saved runtime)
              (let [c (capture! runtime database events query [] {:lookup-only? true})]
                (is (= (d/q query database [[1 :row/value "one"] [2 :row/value "two"]])
                       (program/run! runtime c [] nil)))
                (is (= 1 (count (events-of events :plan))))
                (is (= 3 (count (events-of events :open)))))))
          (is (= 1 (count (events-of events :session-close))))
          (is (every? zero? (vals (control/usage (:control @saved))))))))))

(deftest native-steps-retain-the-same-view-through-provider-work-and-nested-rules
  (with-db {:native/score {:db/valueType :db.type/long}}
    [{:db/id 1 :native/score 100} {:db/id 2 :native/score 200}]
    (fn [database conn]
      (let [events (atom [])
            query '[:find ?e ?score ?v :in $n $s % :where ($s selected ?e ?score ?v)]
            rules '[[(selected ?e ?score ?v)
                     [$n ?e :native/score ?score] [?e :row/value ?v]]]]
        (rt/with-runtime {}
          (fn [runtime]
            (let [c (capture! runtime database events query rules
                      {:lookup-only? true
                       :planning (fn [_] (d/transact! conn [[:db/add 1 :native/score 999]]))})]
              (is (= #{[1 100 "one"] [2 200 "two"]} (program/run! runtime c [] nil)))
              (is (= 999 (d/q '[:find ?v . :where [1 :native/score ?v]] @conn))))))))))

(deftest native-read-plans-keep-the-captured-schema-through-renames
  (with-db {:native/score {:db/valueType :db.type/long}} [{:db/id 1 :native/score 100}]
    (fn [database conn]
      (let [events (atom [])]
        (rt/with-runtime {}
          (fn [runtime]
            (let [c (capture! runtime database events
                      '[:find ?score ?v :in $n $s :where
                        [$n ?e :native/score ?score] [$s ?e :row/value ?v]] []
                      {:lookup-only? true
                       :planning (fn [_]
                                   (d/update-schema conn nil nil {:native/score :native/renamed})
                                   (d/transact! conn [[:db/add 1 :native/renamed 777]]))})]
              (is (= #{[100 "one"]} (program/run! runtime c [] nil)))
              (is (= 777 (d/q '[:find ?score . :where [1 :native/renamed ?score]] @conn))))))))))

(deftest empty-native-attributes-complete-without-opening-dependent-accesses
  (with-db {:native/score {:db/valueType :db.type/long}} [{:db/id 1 :native/score 100}]
    (fn [database _]
      (let [events (atom [])]
        (rt/with-runtime {}
          (fn [runtime]
            (let [c (capture! runtime database events
                      '[:find ?v :in $n $s :where
                        [$n ?e :native/missing ?score] [$s ?e :row/value ?v]] [] {:lookup-only? true})]
              (is (empty? (program/run! runtime c [] nil)))
              (is (= 1 (count (events-of events :plan))))
              (is (empty? (events-of events :open))))))))))

(deftest native-type-proofs-eliminate-external-work-in-an-empty-rule
  (with-db {:native/score {:db/valueType :db.type/long}} [{:db/id 1 :native/score 100}]
    (fn [database _]
      (let [events (atom [])]
        (rt/with-runtime {}
          (fn [runtime]
            (let [c (capture! runtime database events
                      '[:find ?v :in $n $s % :where ($s selected "not-an-entity" ?v)]
                      '[[(selected [?key] ?v) [$n ?key :native/score ?score] [?e :row/value ?v]]] {})]
              (is (empty? (program/run! runtime c [] nil)))
              (is (= 1 (count (events-of events :acquire))))
              (is (empty? (events-of events :plan)))
              (is (empty? (events-of events :open))))))))))

(deftest native-fragment-planning-does-not-invoke-installed-access-methods
  (with-db {:native/score {:db/valueType :db.type/long}} [{:db/id 1 :native/score 100}]
    (fn [database _]
      (let [events (atom []) invoked (atom 0)
            method (reify access/IAccessMethod
                     (-access-plans [_ _] (swap! invoked inc) (throw (Exception. "unexpected preparation")))
                     (-open-access [_ _ _ _ _ _] (throw (Exception. "unexpected open")))
                     (-frontier-satisfies? [_ _ _ _ _] false))]
        (rt/with-runtime {}
          (fn [runtime]
            (let [c (capture! runtime database events
                      '[:find ?v :in $n $s :where [$n ?e :native/score ?score] [$s ?e :row/value ?v]] [] {})]
              (binding [execute/*access-methods* [method]]
                (is (= #{["one"]} (program/run! runtime c [] nil))))
              (is (zero? @invoked)))))))))

(deftest unsupported-native-read-shapes-reject-before-provider-acquisition
  (with-db {:native/score {:db/valueType :db.type/long}
            :native/private {:db/valueType :db.type/long :db/noindex true}}
    [{:db/id 1 :native/score 100 :native/private 200}]
    (fn [database _]
      (doseq [[database pattern] [[database '[[:native/identity 1] :native/score ?score]]
                                  [database '[?e ?attribute ?score]]
                                  [database '[_ :native/private ?score]]
                                  [database '[?e :native/score ?e]]
                                  [[[1 :native/score 100]] '[?e :native/score ?score]]]]
        (let [events (atom [])
              query {:find ['?v] :in ['$n '$s]
                     :where [(into ['$n] pattern) ['$s '?e :row/value '?v]]}
              failure (error (fn [] (rt/with-runtime {}
                                     (fn [runtime]
                                       (program/run! runtime (capture! runtime database events query [] {}) [] nil)))))]
          (is (= :query-source/unsupported (:error failure)))
          (is (empty? (events-of events :acquire)))
          (is (empty? (events-of events :plan))))))))

(deftest bound-native-entities-skip-unrelated-giants-and-restore-outer-witnesses
  (with-db {:native/text {:db/valueType :db.type/string :db/cardinality :db.cardinality/many}}
    [{:db/id 1 :native/text ["a" "b"]} {:db/id 2 :native/text (apply str (repeat 1000000 "x"))}]
    (fn [database _]
      (rt/with-runtime {:limits {:retained-bytes 300000}}
        (fn [runtime]
          (let [events (atom []) query '[:find ?e ?tag ?text ?v :in $n $s [[?e ?tag]] :where
                                         [$n ?e :native/text ?text] [$s ?e :row/value ?v]]
                columns [{:variable '?e :type :long :nullable? false}
                         {:variable '?tag :type :keyword :nullable? false}]
                c (capture! runtime database events query [] {:lookup-only? true} #{'?e '?tag})
                result (native/with-views [database]
                         (fn [snapshots]
                           (program/execute! runtime
                             (program/compile! runtime c columns [[1 :left] [1 :right] [1 :left]] snapshots))))]
            (is (= (frequencies [[1 :left "a" "one"] [1 :left "b" "one"]
                                 [1 :right "a" "one"] [1 :right "b" "one"]
                                 [1 :left "a" "one"] [1 :left "b" "one"]])
                   (frequencies (rows result ['?e '?tag '?text '?v]))))
            (is (= 1 (count (events-of events :open))))))))))

(deftest fixed-native-entities-value-constants-and-wildcards-use-ordinary-equality
  (with-db {:native/score {:db/valueType :db.type/long}
            :native/decimal {:db/valueType :db.type/bigdec}
            :native/private {:db/valueType :db.type/long :db/noindex true}}
    [{:db/id 1 :native/score 100 :native/decimal 1.0M :native/private 7}
     {:db/id 2 :native/score 200 :native/decimal 2.0M}]
    (fn [database _]
      (doseq [query ['[:find ?score ?v :in $n $s :where [$n 1N :native/score ?score] [$s 1 :row/value ?v]]
                     '[:find ?e ?v :in $n $s :where [$n ?e :native/score 100N] [$s ?e :row/value ?v]]
                     '[:find ?e ?v :in $n $s :where [$n ?e :native/decimal 1.00M] [$s ?e :row/value ?v]]
                     '[:find ?v :in $n $s :where [$n 1 :native/private _] [$s 1 :row/value ?v]]
                     '[:find ?v :in $n $s :where [$n 1 :native/private 7N] [$s 1 :row/value ?v]]
                     '[:find ?score ?v :in $n $s :where [$n _ :native/score ?score] [$s 1 :row/value ?v]]]]
        (let [events (atom []) actual (rt/with-runtime {}
                                     (fn [runtime]
                                       (program/run! runtime (capture! runtime database events query [] {}) [] nil)))]
          ;; The eager relation is the constant-equality oracle. Native index
          ;; bounds currently differ for BigInt IDs and decimal scales.
          (is (= (d/q query [[1 :native/score 100] [1 :native/decimal 1.0M] [1 :native/private 7]
                            [2 :native/score 200] [2 :native/decimal 2.0M]]
                       [[1 :row/value "one"] [2 :row/value "two"]]) actual)
              (pr-str query)))))))

(deftest unindexed-native-attributes-can-depend-on-an-external-entity-supplier
  (with-db {:native/private {:db/valueType :db.type/string :db/noindex true}}
    [{:db/id 1 :native/private "small"} {:db/id 2 :native/private (apply str (repeat 1000000 "x"))}]
    (fn [database _]
      (let [events (atom []) query '[:find ?e ?private ?v :in $n $s :where
                                     [$n ?e :native/private ?private] [$s ?e :row/value ?v]]]
        (rt/with-runtime {:limits {:retained-bytes 300000}}
          (fn [runtime]
            (is (= #{[1 "small" "one"]}
                   (program/run! runtime
                     (capture! runtime database events query [] {:facts [[1 :row/value "one"]]}) [] nil)))
            (is (= 1 (count (events-of events :open))))))))))

(deftest literal-rule-arguments-filter-native-facts-and-retain-their-original-values
  (with-db {:native/score {:db/valueType :db.type/long}}
    [{:db/id 1 :native/score 100} {:db/id 2 :native/score 200}]
    (fn [database _]
      (doseq [[query rules expected]
              [['[:find ?copy ?score ?v :in $n $s % :where ($s selected 1N ?copy ?score ?v)]
                '[[(selected [?key] ?copy ?score ?v)
                   [$n ?key :native/score ?score] [?key :row/value ?v] [(identity ?key) ?copy]]]
                #{[1N 100 "one"]}]
               ['[:find ?copy ?v :in $n $s % :where ($s selected 100N ?copy ?v)]
                '[[(selected ?score ?copy ?v)
                   [$n ?e :native/score ?score] [?e :row/value ?v] [(identity ?score) ?copy]]]
                #{[100N "one"]}]]]
        (let [events (atom [])]
          (rt/with-runtime {}
            (fn [runtime]
              (let [actual (program/run! runtime (capture! runtime database events query rules {:lookup-only? true}) [] nil)]
                (is (= expected actual))
                (is (every? #(instance? clojure.lang.BigInt (first %)) actual))))))))))

(deftest entity-key-spooling-precedes-native-lookup-and-preserves-duplicate-keys
  (with-db {:native/private {:db/valueType :db.type/string :db/noindex true}}
    [{:db/id 1 :native/private (apply str (repeat 100000 "x"))}]
    (fn [database _]
      (rt/with-runtime {:limits {:retained-bytes (* 4 1024 1024)}}
        (fn [runtime]
          (let [query '[:find ?e ?text :in $n $s [?e ...] :where [$n ?e :native/private ?text]]
                c (capture! runtime database (atom []) query [] {} #{'?e})
                result (native/with-views [database]
                         #(program/execute! runtime
                            (program/compile! runtime c [{:variable '?e :type :data :nullable? true}]
                              (concat (repeat 100 [1]) [[nil] [1N]]) %)))]
            (is (= 100 (count (:tuples result))))
            (is (every? #(= 100000 (count (second %))) (rows result ['?e '?text])))))))))

(deftest portable-nippy-custom-native-values-use-the-controlled-raw-decoder
  (with-db {} []
    (fn [_ conn]
      (d/register-type conn :app/ranked {:index {:type :long :order-fn (inter/inter-fn [v] (:rank v))}})
      (d/update-schema conn {:native/custom {:db/valueType :app/ranked :db/noindex true}
                            :native/indexed-custom {:db/valueType :app/ranked}})
      (let [x {:rank 1 :nested [nil :value]}]
        (d/transact! conn [{:db/id 1 :native/custom x :native/indexed-custom x}])
        (rt/with-runtime {}
          (fn [runtime]
            (is (= #{[x "one"]}
                   (program/run! runtime
                     (capture! runtime @conn (atom [])
                       '[:find ?value ?v :in $n $s :where [$n 1 :native/custom ?value] [$s 1 :row/value ?v]] [] {})
                     [] nil)))
            (is (= [[x 1 "one"]]
                   (rows (execute-input! runtime @conn (atom [])
                           '[:find ?value ?e ?v :in $n $s ?value :where
                             [$n ?e :native/indexed-custom ?value] [$s ?e :row/value ?v]]
                           [{:variable '?value :type :data :nullable? false}] [[x]] {:lookup-only? true})
                         ['?value '?e '?v])))))))))

(deftest controlled-steps-read-multiple-pinned-native-stores
  (with-db {:native/score {:db/valueType :db.type/long}} [{:db/id 1 :native/score 100}]
    (fn [left left-conn]
      (with-db {:native/other {:db/valueType :db.type/long}} [{:db/id 1 :native/other 200}]
        (fn [right right-conn]
          (let [events (atom []) query '[:find ?a ?b ?v :in $n $m $s :where
                                         [$n ?e :native/score ?a] [$m ?e :native/other ?b] [$s ?e :row/value ?v]]]
            (rt/with-runtime {}
              (fn [runtime]
                (let [provider (fixture/provider events
                                 {:lookup-only? true
                                  :planning (fn [_]
                                              (d/transact! left-conn [[:db/add 1 :native/score 111]])
                                              (d/transact! right-conn [[:db/add 1 :native/other 222]]))})
                      c (planning/capture! runtime
                          [{:symbol '$n :native left} {:symbol '$m :native right} {:symbol '$s :source provider}]
                          (parser/parse-query query) [] {} #{} nil)]
                  (is (= #{[100 200 "one"]} (program/run! runtime c [] nil))))))))))))

(deftest controlled-native-decoding-owns-giants-tuples-and-data
  (let [blob (byte-array (map unchecked-byte (range 2000)))
        text (apply str (repeat 2000 "value"))
        data (with-meta [nil {:payload blob}] {:origin :stored})]
    (with-db {:native/blob {:db/valueType :db.type/bytes}
              :native/text {:db/valueType :db.type/string}
              :native/data {}
              :native/tuple {:db/valueType :db.type/tuple :db/tupleTypes [:db.type/long :db.type/string]}}
      [{:db/id 1 :native/blob blob :native/text text :native/data data :native/tuple [1 nil]}]
      (fn [database _]
        (doseq [[attribute expected] [[:native/blob blob] [:native/text text]
                                     [:native/data data] [:native/tuple [1 nil]]]]
          (let [events (atom []) query {:find ['?v] :in ['$n '$s]
                                        :where [['$n '?e attribute '?v] ['$s '?e :row/value '_]]}
                actual (rt/with-runtime {}
                         (fn [runtime]
                           (program/run! runtime (capture! runtime database events query [] {:lookup-only? true}) [] nil)))
                v (ffirst actual)]
            (if (bytes? expected)
              (is (Arrays/equals ^bytes expected ^bytes v))
              (if (= attribute :native/data)
                (do (is (= (meta expected) (meta v)))
                    (is (Arrays/equals ^bytes blob ^bytes (get-in v [1 :payload]))))
                (is (= expected v))))
            (is (= 1 (count (events-of events :open))))))))))

(deftest native-giant-admission-fails-before-dependent-provider-opens-and-unwinds
  (with-db {:native/text {:db/valueType :db.type/string}}
    [{:db/id 1 :native/text (apply str (repeat 1000000 "x"))}]
    (fn [database _]
      (let [events (atom []) saved (atom nil)
            failure
            (error (fn []
                     (rt/with-runtime {:limits {:retained-bytes 300000}}
                       (fn [runtime]
                         (reset! saved runtime)
                         (let [c (capture! runtime database events
                                   '[:find ?e ?v :in $n $s :where
                                     [$n ?e :native/text ?text] [$s ?e :row/value ?v]] [] {:lookup-only? true})
                               failure (error #(program/run! runtime c [] nil))]
                           (is (= :query-source/limit (:error failure)))
                           (is (= 1 (count (events-of events :plan))))
                           (is (empty? (events-of events :open)))
                           (is (= 1 (count (events-of events :session-close))))
                           (is (every? zero? (vals (control/usage (:control @saved))))))))))]
        (is (= :query-source/limit (:error failure)))))))

(deftest native-wildcards-do-not-decode-giants-that-exceed-the-invocation-quota
  (with-db {:native/text {:db/valueType :db.type/string}}
    [{:db/id 1 :native/text (apply str (repeat 1000000 "x"))}]
    (fn [database _]
      (let [events (atom [])]
        (rt/with-runtime {:limits {:retained-bytes 300000}}
          (fn [runtime]
            (let [c (capture! runtime database events
                      '[:find ?v :in $n $s :where
                        [$n ?e :native/text _] [$s ?e :row/value ?v]] [] {:lookup-only? true})]
              (is (= #{["one"]} (program/run! runtime c [] nil)))
              (is (= 1 (count (events-of events :open)))))))))))

(deftest controlled-native-programs-cannot-use-an-escaped-read-view
  (with-db {:native/score {:db/valueType :db.type/long}} [{:db/id 1 :native/score 100}]
    (fn [database _]
      (let [events (atom [])
            failure
            (error (fn []
                     (rt/with-runtime {}
                       (fn [runtime]
                         (let [c (capture! runtime database events
                                   '[:find ?v :in $n $s :where
                                     [$n ?e :native/score ?score] [$s ?e :row/value ?v]] [] {})
                               p (native/with-views [database]
                                   #(program/compile! runtime c [] nil %))]
                           (is (= :query-source/invalid (:error (error #(program/result! runtime p)))))
                           (is (empty? (events-of events :open)))
                           (is (= 1 (count (events-of events :session-close)))))))))]
        (is (= :query-source/invalid (:error failure)))))))
