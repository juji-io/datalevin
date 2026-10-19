(ns datalevin.query.source-program-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.core :as d]
            [datalevin.parser :as parser]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.planning :as planning]
            [datalevin.query.source.program :as program]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source-planning-test :as fixture]
            [datalevin.query.source-session-test :as session-fixture])
  (:import [java.math BigDecimal BigInteger MathContext]
           [java.util Iterator]))

(defn- rows [rel variables]
  (mapv (fn [^objects tuple] (mapv #(aget tuple (int (get (:attrs rel) %))) variables)) (:tuples rel)))
(defn- error [f] (try (f) nil (catch clojure.lang.ExceptionInfo e (ex-data e))))
(defn- events-of [events kind] (filterv #(= kind (first %)) @events))
(defn- column [v t] {:variable v :type t :nullable? false})

(defn- compile! [runtime events query rules columns input config]
  (let [c (planning/capture! runtime
            [{:symbol '$s :source (fixture/provider events config)}]
            (parser/parse-query query) (parser/parse-rules rules) {} (set (map :variable columns)) nil)]
    (program/compile! runtime c columns input)))

(deftest automatic-compiler-emits-rule-boundaries-and-dependent-child-accesses
  (let [events (atom []) saved (atom nil)
        input [[2 :a] [1 :b] [2 :c] [2 :a] [3 :missing]]]
    (rt/with-runtime {:limits {:binding-rows 1}}
      (fn [runtime]
        (reset! saved runtime)
        (let [p (compile! runtime events
                  '[:find ?e ?tag ?v :in $s % [[?e ?tag]] :where ($s named ?e ?v)]
                  '[[(named [?id] ?name) [?id :row/value ?name]]]
                  [(column '?e :long) (column '?tag :keyword)] input {:lookup-only? true})]
          (is (empty? (events-of events :open)))
          (let [result (program/execute! runtime p)]
            (is (= (frequencies [[2 :a "two"] [1 :b "one"] [2 :c "two"] [2 :a "two"]])
                   (frequencies (rows result ['?e '?tag '?v])))))
          (is (= 1 (count (events-of events :plan))))
          (is (= 3 (count (events-of events :open))))
          (is (= 3 (count (set (mapcat #(map :binding-id (:bindings (second %))) (events-of events :open))))))
          (is (= :query-source/invalid (:error (error #(program/execute! runtime p))))))))
    (is (= 1 (count (events-of events :session-close))))
    (is (every? zero? (vals (control/usage (:control @saved)))))))

(deftest nested-calls-and-residuals-use-shared-rule-set-and-predicate-semantics
  (let [events (atom [])
        query '[:find ?e ?v :in $s % :where ($s selected ?e ?v)]
        rules '[[(named ?id ?name) [?id :row/value ?name]]
                [(selected ?e ?v) (named ?e ?v) [(not= ?v "two")]]]]
    (rt/with-runtime {}
      (fn [runtime]
        (let [p (compile! runtime events query rules [] nil {})
              result (program/execute! runtime p)]
          (is (= [[1 "one"]] (rows result ['?e '?v])))
          (is (= 1 (count (events-of events :plan))))
          (is (= 1 (count (events-of events :open))))
          (is (= (d/q query [[1 :row/value "one"] [2 :row/value "two"]] rules)
                 (set (rows result ['?e '?v])))))))))

(deftest full-head-set-keys-survive-wildcard-and-repeated-caller-arguments
  (doseq [arg ['_ '?x]]
    (let [events (atom [])
          query {:find ['?x] :in ['$s '%] :where [(list '$s 'same '?x arg)]}
          rules '[[(same ?a ?b) [?a :row/value _] [(identity ?a) ?b]]]]
      (rt/with-runtime {}
        (fn [runtime]
          (let [p (compile! runtime events query rules [] nil {})
                result (program/execute! runtime p)]
            (is (= #{[1] [2]} (set (rows result ['?x]))))
            (is (= (d/q query [[1 :row/value "one"] [2 :row/value "two"]] rules)
                   (set (rows result ['?x]))))))))))

(deftest literal-call-arguments-are-scoped-without-importing-sibling-filters
  (let [events (atom [])
        query '[:find ?a ?b :in $s % :where ($s named ?a "one") ($s named ?b "two")]
        rules '[[(named ?e ?v) [?e :row/value ?v]]]]
    (rt/with-runtime {}
      (fn [runtime]
        (let [p (compile! runtime events query rules [] nil {})
              result (program/execute! runtime p)]
          (is (= [[1 2]] (rows result ['?a '?b])))
          (is (= 2 (count (events-of events :plan))))
          (is (= #{[:const "one"] [:const "two"]}
                 (set (map #(get-in (second %) [:clauses 0 :terms 2]) (events-of events :plan)))))
          (is (= 2 (count (events-of events :open)))))))))

(deftest typed-empty-rules-prune-the-whole-conjunction-before-provider-planning
  (doseq [query ['[:find ?v :in $s % :where ($s named "not-an-entity" ?v)]
                 '[:find ?v :in $s % :where [$s ?other :row/value _]
                   ($s nested "not-an-entity" ?v)]
                 '[:find ?v :in $s % :where
                   (or ($s named "not-an-entity" ?v)
                       ($s nested "also-not-an-entity" ?v))]]]
    (let [events (atom []) saved (atom nil)
          rules '[[(named [?e] ?v) [?e :row/value ?v]]
                  [(nested ?e ?v) (named ?e ?v)]]]
      (rt/with-runtime {}
        (fn [runtime]
          (reset! saved runtime)
          (let [p (compile! runtime events query rules [] nil {})]
            (is (empty? (program/result! runtime p)))
            (is (= 1 (count (events-of events :descriptor))))
            (is (= 1 (count (events-of events :acquire))))
            (is (empty? (events-of events :plan)))
            (is (empty? (events-of events :open))))))
      (is (= 1 (count (events-of events :session-close))))
      (is (every? zero? (vals (control/usage (:control @saved))))))))

(deftest typed-empty-branches-do-not-remove-live-union-or-rule-alternatives
  (doseq [[query rules]
          [['[:find ?v :in $s % :where
              (or ($s named "not-an-entity" ?v) ($s named 1 ?v))]
            '[[(named [?e] ?v) [?e :row/value ?v]]]]
           ['[:find ?v :in $s % :where ($s selected ?v)]
            '[[(named [?e] ?v) [?e :row/value ?v]]
              [(selected ?v) (named "not-an-entity" ?v)]
              [(selected ?v) (named 1 ?v)]]]]]
    (let [events (atom [])]
      (rt/with-runtime {}
        (fn [runtime]
          (let [p (compile! runtime events query rules [] nil {})]
            (is (= #{["one"]} (program/result! runtime p)))
            (is (= 1 (count (events-of events :plan))))
            (is (= 1 (count (events-of events :open))))))))))

(deftest numeric-literal-rule-arguments-match-facts-without-changing-their-representation
  (doseq [[query rules expected]
          [['[:find ?v :in $s % :where ($s named 1N ?v)]
            '[[(named [?e] ?v) [?e :row/value ?v]]] #{["one"]}]
           ['[:find ?copy ?v :in $s % :where ($s named 1N ?copy ?v)]
            '[[(named ?e ?copy ?v) [?e :row/value ?v] [(identity ?e) ?copy]]]
            #{[1N "one"]}]
           ['[:find ?copy ?v :in $s % :where ($s outer 1N ?copy ?v)]
            '[[(named ?e ?copy ?v) [?e :row/value ?v] [(identity ?e) ?copy]]
              [(outer ?e ?copy ?v) (named ?e ?copy ?v)]] #{[1N "one"]}]
           ['[:find ?v :in $s % :where ($s keyed 1N ?v)]
            '[[(keyed ?key ?v) [?e :row/value ?v] [(identity ?e) ?key]]] #{["one"]}]
           ['[:find ?v :in $s % :where ($s keyed 1N ?v)]
            '[[(keyed ?key ?v) [?e :row/value ?v] [(ground 1) ?key]]] #{["one"] ["two"]}]
           ['[:find ?copy ?v :in $s % :where ($s keyed 1N ?copy ?v)]
            '[[(keyed ?key ?copy ?v) [?e :row/value ?v] [(identity ?key) ?key]
               [(identity ?key) ?copy]]] #{[1N "one"] [1N "two"]}]
           ['[:find ?copy ?v :in $s % :where ($s keyed 1N ?copy ?v)]
            '[[(keyed ?key ?copy ?v)
               (or (and [?key :row/value ?v] [(identity ?key) ?copy])
                   (and [?key :row/value ?v] [(identity ?key) ?copy]))]] #{[1N "one"]}]
           ['[:find ?copy ?v :in $s % :where ($s keyed 1N ?copy ?v)]
            '[[(keyed ?key ?copy ?v) [?key ?attribute ?v] [(identity ?key) ?copy]]]
            #{[1N "one"]}]
           ['[:find ?v :in $s % :where ($s keyed 1N ?v)]
            '[[(keyed ?key ?v) [?e :row/value ?v] [(ground nil) ?key]]] #{}]]]
    (let [events (atom []) saved (atom nil)]
      (rt/with-runtime {}
        (fn [runtime]
          (reset! saved runtime)
          (let [actual (program/result! runtime (compile! runtime events query rules [] nil {}))]
            (is (= expected actual) (pr-str query rules))
            ;; Legacy rule expansion substitutes literals into function output
            ;; bindings, which its parser rejects. Check these scoped semantics
            ;; against explicit results, including the retained BigInt class.
            (when (some #(= '?copy (:symbol %)) (:elements (:qfind (parser/parse-query query))))
              (is (every? #(instance? clojure.lang.BigInt (first %)) actual))))))
      (is (= 1 (count (events-of events :session-close))))
      (is (every? zero? (vals (control/usage (:control @saved))))))))

(deftest late-function-type-proofs-propagate-up-and-down-nested-regions
  (doseq [[query rules expected-plans expected]
          [['[:find ?v :in $s % :where ($s keyed "not-an-id" ?v)]
            '[[(keyed ?key ?v) [?e :row/value ?v] [(identity ?e) ?key]]]
            0 #{}]
           ['[:find ?v :in $s % :where [$s ?sibling :row/value _] ($s outer "not-an-id" ?v)]
            '[[(keyed ?key ?v) [?e :row/value ?v] [(identity ?e) ?key]]
              [(outer ?key ?v) (keyed ?key ?v)]]
            0 #{}]
           ['[:find ?v :in $s % :where ($s keyed "not-an-id" ?v)]
            '[[(keyed ?key ?v) [?e :row/value ?v] [(identity ?e) ?key]]
              [(keyed ?key ?v) [?e :row/value ?v] [(ground "not-an-id") ?key]]]
            1 #{["one"] ["two"]}]]]
    (let [events (atom [])]
      (rt/with-runtime {}
        (fn [runtime]
          (let [p (compile! runtime events query rules [] nil {})]
            (is (= expected (program/result! runtime p)))
            (is (= expected-plans (count (events-of events :plan))))
            (is (= expected-plans (count (events-of events :open))))))))))

(deftest multiple-rule-branches-deduplicate-the-complete-head-before-outer-fanout
  (let [events (atom [])
        rules '[[(named ?e ?v) [?e :row/value ?v]]
                [(named ?other ?name) [?other :row/value ?name]]]]
    (rt/with-runtime {}
      (fn [runtime]
        (let [p (compile! runtime events
                  '[:find ?e ?tag ?v :in $s % [[?e ?tag]] :where ($s named ?e ?v)]
                  rules [(column '?e :long) (column '?tag :keyword)] [[1 :a] [1 :a] [2 :b]] {})
              result (program/execute! runtime p)]
          (is (= (frequencies [[1 :a "one"] [1 :a "one"] [2 :b "two"]])
                 (frequencies (rows result ['?e '?tag '?v]))))
          (is (= 2 (count (events-of events :plan))))
          (is (= 2 (count (events-of events :open)))))))))

(deftest union-projection-hides-private-variables-and-preserves-outer-witnesses
  (let [events (atom [])
        query '[:find ?e ?tag :in $s [[?e ?tag]]
                :where (or-join [?e]
                         (and [$s ?e :row/value ?left] [(= ?left "one")])
                         (and [$s ?e :row/value ?right] [(not= ?right "missing")]))]]
    (rt/with-runtime {}
      (fn [runtime]
        (let [p (compile! runtime events query []
                  [(column '?e :long) (column '?tag :keyword)] [[1 :a] [1 :a] [2 :b]] {})
              result (program/execute! runtime p)]
          (is (= #{'?e '?tag} (set (keys (:attrs result)))))
          (is (= (frequencies [[1 :a] [1 :a] [2 :b]]) (frequencies (rows result ['?e '?tag]))))
          (is (= 2 (count (events-of events :open)))))))))

(def ^:private two-attributes
  (-> session-fixture/descriptor
      (assoc-in [:pattern-contract :attributes :row/other] {:entity-type :long :value-type :string})
      (assoc-in [:logical-schema :attributes :row/other]
                {:entity-domain :row :reference-domain nil :cardinality :many :absence :no-fact})))

(deftest attribute-dispatch-routes-complete-keys-once-and-restores-the-attribute
  (let [events (atom [])
        facts [[1 :row/value "one"] [1 :row/other "other"] [2 :row/value "two"]]
        input [[1 :row/value :a] [1 :row/other :b] [1 :row/value :a] [2 :row/other :missing]]]
    (rt/with-runtime {}
      (fn [runtime]
        (let [p (compile! runtime events
                  '[:find ?e ?a ?v ?tag :in $s [[?e ?a ?tag]] :where [$s ?e ?a ?v]] []
                  [(column '?e :long) (column '?a :keyword) (column '?tag :keyword)] input
                  {:descriptor two-attributes :facts facts :lookup-only? true})
              result (program/execute! runtime p)]
          (is (= (frequencies [[1 :row/value "one" :a] [1 :row/other "other" :b] [1 :row/value "one" :a]])
                 (frequencies (rows result ['?e '?a '?v '?tag]))))
          (is (= 2 (count (events-of events :plan))))
          (is (= 2 (count (events-of events :open)))))))))

(deftest wildcard-attribute-union-keeps-fact-multiplicity-until-terminal-projection
  (let [events (atom []) facts [[1 :row/value "same"] [1 :row/other "same"]]]
    (rt/with-runtime {}
      (fn [runtime]
        (let [p (compile! runtime events '[:find ?e ?v :in $s :where [$s ?e _ ?v]] [] [] nil
                  {:descriptor two-attributes :facts facts})
              result (program/execute! runtime p)]
          (is (= [[1 "same"] [1 "same"]] (rows result ['?e '?v])))
          (is (= 2 (count (events-of events :open)))))))))

(deftest core-steps-are-instantiated-after-the-selected-source-layout-is-known
  (rt/with-runtime {}
    (fn [runtime]
      (let [events (atom [])
            p (compile! runtime events
                '[:find ?e ?tag ?copy :in $s ?tag
                  :where [$s ?e :row/value ?v] [(identity ?v) ?copy] [(not= ?copy "two")]] []
                [(column '?tag :keyword)] [[:carried]] {})
            result (program/execute! runtime p)]
        (is (= [[1 :carried "one"]] (rows result ['?e '?tag '?copy])))))))

(deftest explicit-null-filter-proves-required-rule-keys-without-dropping-outer-witnesses
  (let [events (atom [])]
    (rt/with-runtime {}
      (fn [runtime]
        (let [p (compile! runtime events
                  '[:find ?e ?v :in $s % [?e ...] :where [(some? ?e)] ($s named ?e ?v)]
                  '[[(named [?id] ?name) [?id :row/value ?name]]]
                  [(assoc (column '?e :long) :nullable? true)] [[nil] [1] [1] [2]] {:lookup-only? true})
              result (program/execute! runtime p)]
          (is (= (frequencies [[1 "one"] [1 "one"] [2 "two"]])
                 (frequencies (rows result ['?e '?v]))))
          (is (= 1 (count (events-of events :open)))))))))

(deftest null-filters-cannot-invent-a-narrow-key-type-from-future-fact-outputs
  (let [events (atom [])
        failure (error (fn [] (rt/with-runtime {}
                          (fn [runtime]
                            (compile! runtime events
                              '[:find ?e ?v :in $s % [?e ...]
                                :where [(some? ?e)] ($s named ?e ?v)]
                              '[[(named [?id] ?name) [?id :row/value ?name]]]
                              [(assoc (column '?e :data) :nullable? true)] [["wrong-domain"] [1]]
                              {:lookup-only? true})))))]
    (is (= :query-source/unsupported (:error failure)))
    (is (empty? (events-of events :open)))))

(deftest exact-join-cardinality-admission-allows-a-large-sparse-outer-join
  (let [events (atom []) n 300 input (mapv #(vector (long %)) (range n))
        facts (mapv #(vector (long %) :row/value (str %)) (range n))]
    (rt/with-runtime {:limits {:retained-bytes (* 16 1024 1024)}}
      (fn [runtime]
        (let [p (compile! runtime events
                  '[:find ?e ?v :in $s % [?e ...] :where ($s named ?e ?v)]
                  '[[(named [?id] ?name) [?id :row/value ?name]]]
                  [(column '?e :long)] input {:facts facts :lookup-only? true})
              result (program/execute! runtime p)]
          (is (= n (count (:tuples result))))
          (is (= (set (map (fn [[e _ v]] [e v]) facts)) (set (rows result ['?e '?v])))))))))

(deftest unsupported-native-or-arbitrary-core-work-rejects-before-acquisition
  (let [events (atom [])
        result (error #(rt/with-runtime {}
                         (fn [runtime]
                           (compile! runtime events
                             '[:find ?e :in $s :where [$s ?e :row/value ?v] [(str ?v) ?out]] [] [] nil {}))))]
    (is (= :query-source/unsupported (:error result)))
    (is (empty? (events-of events :acquire)))
    (is (empty? (events-of events :open)))))

(deftest terminal-aggregates-with-and-return-shapes-match-the-native-engine
  (let [facts [[1 :row/value "one"] [1 :row/value "other"] [2 :row/value "two"]]
        rules '[[(named ?e ?v) [?e :row/value ?v]]]
        queries
        ['[:find ?e ?v :in $s % :where ($s named ?e ?v)]
         '[:find ?e :with ?v :in $s % :where ($s named ?e ?v)]
         '[:find [?e ...] :in $s % :where ($s named ?e ?v)]
         '[:find [?e ?v] :in $s % :where ($s named ?e ?v)]
         '[:find (count ?e) . :in $s % :where ($s named ?e ?v)]
         '[:find (count ?e) . :with ?v :in $s % :where ($s named ?e ?v)]
         '[:find (sum ?e) . :with ?v :in $s % :where ($s named ?e ?v)]
         '[:find (avg ?e) . :with ?v :in $s % :where ($s named ?e ?v)]
         '[:find (min ?e) . :in $s % :where ($s named ?e ?v)]
         '[:find (max ?e) . :in $s % :where ($s named ?e ?v)]
         '[:find ?v (sum ?e) :in $s % :where ($s named ?e ?v)]
         '[:find ?e ?v :keys entity value :in $s % :where ($s named ?e ?v)]
         '[:find [?e ?v] :keys entity value :in $s % :where ($s named ?e ?v)]
         '{:find [?e ?v] :in [$s %] :where [($s named ?e ?v)] :limit 1 :offset 1}]]
    (doseq [query queries]
      (let [events (atom []) result
            (rt/with-runtime {}
              (fn [runtime]
                (program/result! runtime (compile! runtime events query rules [] nil {:facts facts}))))
            expected (d/q query facts rules)
            unordered-first? (or (instance? datalevin.parser.FindTuple (:qfind (parser/parse-query query)))
                                 (map? query))
            valid? (if unordered-first?
                     (if (map? query) (and (= 1 (count result)) (contains? (set (map #(vector (first %) (last %)) facts)) (first result)))
                         (let [row (if (map? result) [(:entity result) (:value result)] result)]
                           (contains? (set (map #(vector (first %) (last %)) facts)) row)))
                     (= expected result))]
        (is valid? (str query))
        ;; Force any returned collection again after all sessions have closed.
        (is (not (instance? clojure.lang.LazySeq result)))
        (is (= 1 (count (events-of events :session-close))))))))

(deftest result-ownership-is-captured-before-provider-planning-and-survives-close
  (let [events (atom []) data (byte-array [7 8])
        query {:find ['?e '?payload] :in ['$s] :where [['$s '?e :row/value '_]
                                                      [(list 'ground data) '?payload]]}
        result
        (rt/with-runtime {}
          (fn [runtime]
            (program/result! runtime
              (compile! runtime events query [] [] nil
                {:planning (fn [_] (aset-byte data 0 (byte 99)))}))))]
    (is (= #{[1 [7 8]] [2 [7 8]]} (set (map (fn [[e bytes]] [e (vec bytes)]) result))))
    (is (= [99 8] (vec data)))
    (is (= 1 (count (events-of events :session-close))))))

(deftest result-quotas-fail-instead-of-returning-a-prefix-and-close-now
  (let [events (atom []) saved (atom nil)
        failure
        (error #(rt/with-runtime {:limits {:result-rows 1}}
                  (fn [runtime]
                    (reset! saved runtime)
                    (let [p (compile! runtime events '[:find ?e ?v :in $s :where [$s ?e :row/value ?v]] [] [] nil {})
                          e (error (fn [] (program/result! runtime p)))]
                      (is (= :query-source/limit (:error e)))
                      (is (= 1 (count (events-of events :session-close))))
                      (is (every? zero? (vals (control/usage (:control runtime)))))))))]
    (is (= :query-source/limit (:error failure)))
    (is (every? zero? (vals (control/usage (:control @saved)))))))

(deftest shared-aggregate-hooks-reject-giant-arithmetic-before-expansion
  (doseq [[aggregate values math-context]
          [['sum [(BigDecimal. BigInteger/ONE 1000000000) 1M] nil]
           ['avg [1M 2M] (MathContext. Integer/MAX_VALUE)]]]
    (let [events (atom [])
          query {:find [(list aggregate '?amount) '.] :in ['$s '[?amount ...]]
                 :where [['$s '_ :row/value '_]]}
          failure
          (error #(binding [*math-context* math-context]
                    (rt/with-runtime {:limits {:retained-bytes (* 1024 1024)}}
                      (fn [runtime]
                        (program/result! runtime
                          (compile! runtime events query [] [(column '?amount :bigdec)]
                                    (mapv vector values) {}))))))]
      (is (= :query-source/limit (:error failure)))
      (is (= :retained-bytes (:category failure)))
      (is (= 1 (count (events-of events :session-close)))))))

(deftest streaming-typed-inputs-are-owned-before-reused-buffers-advance
  (let [events (atom []) buffer (object-array 2) bytes (byte-array 1)
        input (reify Iterable
                (iterator [_]
                  (let [i (atom 0)]
                    (reify Iterator
                      (hasNext [_] (< @i 2))
                      (next [_]
                        (let [n (swap! i inc)]
                          (aset-byte bytes 0 (byte n))
                          (aset buffer 0 (long n)) (aset buffer 1 bytes) buffer))))))
        result (rt/with-runtime {}
                 (fn [runtime]
                   (program/result! runtime
                     (compile! runtime events
                       '[:find ?e ?payload :in $s [[?e ?payload]] :where [$s ?e :row/value _]] []
                       [(column '?e :long) (column '?payload :bytes)] input {}))))]
    (is (= #{[1 [1]] [2 [2]]} (set (map (fn [[e x]] [e (vec x)]) result))))
    (is (= 1 (count (events-of events :session-close))))))

(deftest typed-input-tuples-leave-unused-unbounded-tails-unread
  (let [events (atom [])
        tuple (cons 1 (lazy-seq (throw (Exception. "unused tuple tail"))))
        result (rt/with-runtime {}
                 (fn [runtime]
                   (program/result! runtime
                     (compile! runtime events '[:find ?e :in $s [?e ...] :where [$s ?e :row/value _]] []
                               [(column '?e :long)] [tuple] {}))))]
    (is (= #{[1]} result))))
