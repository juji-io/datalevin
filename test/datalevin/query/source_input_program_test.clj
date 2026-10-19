(ns datalevin.query.source-input-program-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.test.check :as check]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [datalevin.core :as d]
            [datalevin.query.source :as source]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.program :as program]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source-planning-test :as fixture])
  (:import [java.util Iterator]))

(defn- error [f] (try (f) nil (catch clojure.lang.ExceptionInfo e (ex-data e))))
(defn- events-of [events kind] (filterv #(= kind (first %)) @events))
(defn- query! [query arguments & [options]]
  (rt/with-runtime (or options {})
    (fn [runtime]
      (let [result (program/query! runtime query arguments)]
        (is (every? zero? (vals (control/usage (:control runtime)))))
        result))))

(deftest ordinary-scalar-tuple-collection-and-relation-inputs-match-native-binding
  (doseq [[query arguments]
          [['[:find ?a ?b :in ?a [?b _] :where [(some? ?a)]] [1 ["one" (Object.) :unused]]]
           ['[:find ?a ?b :in [?a ...] [[?b _]] :where [(some? ?a)]]
            [[1 2 2] [["one" (Object.)] ["two" (Object.)]]]]
           ['[:find ?a ?b ?c :in [[?a [?b ...]] ?c] :where [(some? ?a)]]
            [[[1 ["x" "y"]] :tag]]]
           ['[:find ?a ?b :in [[?a [[?b]]]] :where [(some? ?a)]]
            [[[1 [["x"] ["y"]]] [2 [["z"]]]]]]
           ['[:find ?a ?b :in [[?a ?b]] :where [(some? ?a)]]
            [[[1 "x" :unused] [2 "y" :unused]]]]
           ['[:find ?a :in [?a ...] :where [(nil? ?a)]] [[nil 1 "x" :key]]]]]
    (is (= (apply d/q query arguments) (query! query arguments)) (pr-str query))))

(deftest relational-bags-survive-products-until-with-and-aggregate-projection
  (let [query '[:find (count ?e) :with ?tag ?suffix
                :in $s [[?e ?tag]] [?suffix ...]
                :where [$s ?e :row/value _]]
        input [[1 :a] [1 :a] [1 :b] [2 :c]]
        suffix [:x :y :y] events (atom [])]
    (is (= [[6]] (query! query [(fixture/provider events {:lookup-only? true}) input suffix])))
    (is (= (d/q query [[1 :row/value "one"] [2 :row/value "two"]] input suffix)
           [[6]]))
    (is (= 1 (count (events-of events :open))))
    (is (= 1 (count (events-of events :session-close))))))

(deftest normal-source-aliases-and-rules-are-captured-once
  (let [events (atom []) provider (fixture/provider events {:lookup-only? true})
        query '[:find ?v ?other :in $ $alias % ?e
                :where (named ?e ?v) ($alias named ?e ?other)]
        rules '[[(named [?id] ?name) [?id :row/value ?name]]]]
    (is (= #{["two" "two"]} (query! query [provider provider rules 2])))
    (is (= 1 (count (events-of events :descriptor))))
    (is (= 1 (count (events-of events :acquire))))
    (is (= 1 (count (events-of events :session-close))))))

(deftest generated-nested-input-correlations-match-the-existing-engine
  (is (:pass?
        (check/quick-check 75
          (prop/for-all [groups (gen/vector (gen/tuple (gen/choose 0 4)
                                                       (gen/vector (gen/elements [:a :b :c]) 0 4)) 0 5)
                        suffixes (gen/vector (gen/elements [nil :x :y]) 0 4)]
            (let [query '[:find ?id ?tag ?suffix :in [[?id [?tag ...]]] [?suffix ...]
                          :where [(some? ?suffix)]]
                  arguments [groups suffixes]]
              (= (apply d/q query arguments) (query! query arguments))))))))

(deftest nullable-and-heterogeneous-owned-input-columns-do-not-invent-key-types
  (let [events (atom []) query '[:find ?e ?v :in $s [?e ...] :where [$s ?e :row/value ?v]]]
    (is (= #{[1 "one"] [2 "two"]}
           (query! query [(fixture/provider events {}) [nil 1 "other" 2]])))
    ;; A possible fact output cannot make heterogeneous nullable seeds ready
    ;; for a typed lookup. The shared scheduler must choose enumeration first.
    (is (every? #(empty? (:bindings (second %))) (events-of events :open)))))

(deftest empty-and-zero-column-bindings-preserve-normal-results
  (doseq [[query arguments]
          [['[:find ?a :in [?a ...] :where [(some? ?a)]] [[]]]
           ['[:find ?a :in [?a ...] :where [(some? ?a)]] [[1 2]]]
           ['[:find ?a :in ?a [_ ...] :where [(some? ?a)]] [1 [(Object.) (Object.)]]]
           ['[:find ?a :in ?a [[_]] :where [(some? ?a)]] [1 [[(Object.)] [(Object.)]]]]
           ['[:find ?a :in [?a [?b ...]] :where [(some? ?a)]] [[1 []]]]]]
    (is (= (apply d/q query arguments) (query! query arguments)))))

(deftest normal-inputs-reach-every-shared-terminal-result-shape
  (doseq [query ['[:find ?v . :in ?v :where [(some? ?v)]]
                 '[:find [?v] :in ?v :where [(some? ?v)]]
                 '[:find [?v ...] :in ?v :where [(some? ?v)]]
                 '[:find ?v :keys value :in ?v :where [(some? ?v)]]
                 '[:find [?v] :keys value :in ?v :where [(some? ?v)]]]]
    (is (= (d/q query 1) (query! query [1]))))
  (let [events (atom [])]
    (is (= #{["one"] ["two"]}
           (query! '[:find ?v :where [?e :row/value ?v]] [(fixture/provider events {})])))))

(deftest scalar-window-parameters-use-only-direct-scalar-bindings
  (doseq [[limit offset] [[1 0] [2 1] [Long/MAX_VALUE 0]]]
    (let [query '[:find ?v :in [?v ...] limit ?offset :limit limit :offset ?offset
                  :where [(some? ?v)]]
          arguments [[1 2 3] limit offset]]
      (is (= (apply d/q query arguments) (query! query arguments)))))
  (doseq [[binding input] [['?limit nil] ['?limit -1] ['?limit 0] ['?limit 1.0]
                          ['?limit (inc (bigint Long/MAX_VALUE))]
                          [['?limit] [1]] [['?limit '...] [1]]]]
    (let [events (atom []) query {:find ['?v] :in ['$s binding] :limit '?limit
                                 :where '[[$s ?e :row/value ?v]]}]
      (is (= :query-source/invalid
             (:error (error #(query! query [(fixture/provider events {}) input])))))
      (is (empty? @events)))))

(deftest arity-and-binding-failures-precede-descriptor-callbacks
  (doseq [arguments [[:missing] [:extra [1] :extra] [:short []] [:not-seq 1]]]
    (let [events (atom []) query '[:find ?v :in $s [?id] :where [$s ?id :row/value ?v]]
          [_ input extra] arguments
          inputs (if (= :missing (first arguments)) [(fixture/provider events {})]
                     (cond-> [(fixture/provider events {}) input] extra (conj extra)))]
      (is (= :query-source/invalid (:error (error #(query! query inputs)))))
      (is (empty? @events)))))

(deftest invalid-query-and-rule-syntax-is-redacted-before-provider-callbacks
  (doseq [[query rules]
          [['[:find ?v :in $s % :where ($s named ?v)] '[[(named)]]]
           ['[:find ?v :in $s % ?e ?e :where [$s ?e :row/value ?v]] []]]]
    (let [events (atom []) result (error #(query! query [(fixture/provider events {}) rules 1 2]))]
      (is (= :query-source/invalid (:error result)))
      (is (= :syntax (:phase result)))
      (is (not (contains? result :form)))
      (is (empty? @events)))))

(deftest tuple-prefix-and-ignored-inputs-never-realize-unused-values
  (let [tail (lazy-seq (throw (Exception. "unused secret tail")))
        query '[:find ?a :in [?a _] _ :where [(some? ?a)]]]
    (is (= #{[1]} (query! query [(concat [1 (Object.)] tail) tail])))))

(deftest argument-arity-check-does-not-count-an-unbounded-tail
  (let [reads (atom 0) arguments (reify Iterable
                                 (iterator [_]
                                   (reify Iterator
                                     (hasNext [_] true)
                                     (next [_] (swap! reads inc)))))
        query '[:find ?a :in ?a :where [(some? ?a)]]]
    (is (= :query-source/invalid (:error (error #(query! query arguments)))))
    (is (= 2 @reads))))

(deftest reusable-outer-and-inner-tuple-buffers-are-owned-before-advancing
  (let [payload (byte-array 1) row (object-array 2)
        input (reify Iterable
                (iterator [_]
                  (let [i (atom 0)]
                    (reify Iterator
                      (hasNext [_] (< @i 3))
                      (next [_]
                        (let [n (swap! i inc)]
                          (aset-byte payload 0 (byte n))
                          (aset row 0 (long n)) (aset row 1 payload) row))))))
        query '[:find ?e ?payload :in [[?e ?payload]] :where [(some? ?e)]]]
    (is (= #{[1 [1]] [2 [2]] [3 [3]]}
           (into #{} (map (fn [[e bytes]] [e (vec bytes)])) (query! query [input])))))
  (let [payload (byte-array 1)
        tuple (reify Iterable
                (iterator [_]
                  (let [i (atom 0)]
                    (reify Iterator
                      (hasNext [_] (< @i 2))
                      (next [_] (aset-byte payload 0 (byte (swap! i inc))) payload)))))
        query '[:find ?a ?b :in [?a ?b] :where [(some? ?a)]]]
    (is (= [[[1] [2]]]
           (mapv (fn [[a b]] [(vec a) (vec b)]) (query! query [tuple]))))))

(deftest input-products-and-streams-hit-quota-before-provider-callbacks
  (doseq [arguments [(fn [provider] [provider (range 50) (range 50)])
                     (fn [provider] [provider (repeat 1) [1]])]]
    (let [events (atom []) saved (atom nil)
          query '[:find ?v :in $s [?a ...] [?b ...] :where [$s ?e :row/value ?v]]]
      (is (= :query-source/limit
             (:error (error #(rt/with-runtime {:limits {:retained-bytes 300000}}
                               (fn [runtime]
                                 (reset! saved runtime)
                                 (program/query! runtime query (arguments (fixture/provider events {})))))))))
      (is (empty? @events))
      (is (every? zero? (vals (control/usage (:control @saved))))))))

(deftest input-product-quota-check-precedes-cartesian-row-allocation
  (let [read-counts (atom [0 0]) events (atom [])
        inputs (mapv (fn [column]
                       (reify Iterable
                         (iterator [_]
                           (reify Iterator
                             (hasNext [_] (< (get @read-counts column) 50))
                             (next [_]
                               (long (get (swap! read-counts update column inc) column))))))) [0 1])
        query '[:find ?v :in $s [?a ...] [?b ...] :where [$s ?e :row/value ?v]]]
    (is (= :query-source/limit
           (:error (error #(query! query (into [(fixture/provider events {})] inputs)
                            {:limits {:retained-bytes 300000}})))))
    (is (= [50 50] @read-counts))
    (is (empty? @events))))

(deftest unsupported-plain-function-inputs-reject-before-provider-callbacks
  (let [events (atom [])]
    (is (= :query-source/unsupported
           (:error (error #(query! '[:find ?v :in $s bound :where
                                     [$s ?e :row/value _] [(identity bound) ?v]]
                            [(fixture/provider events {}) 1])))))
    (is (empty? @events))))

(deftest input-consumption-observes-query-cancellation
  (let [events (atom []) saved (atom nil)]
    (is (= :query-source/cancelled
           (:error (error #(rt/with-runtime {}
                             (fn [runtime]
                               (reset! saved runtime)
                               (let [input (reify Iterable
                                             (iterator [_]
                                               (reify Iterator
                                                 (hasNext [_] true)
                                                 (next [_]
                                                   (control/cancel! (:control runtime)
                                                     {:error :query-source/cancelled :phase :inputs})
                                                   1))))]
                                 (program/query! runtime
                                   '[:find ?v :in $s [?e ...] :where [$s ?e :row/value ?v]]
                                   [(fixture/provider events {}) input]))))))))
    (is (empty? @events))
    (is (every? zero? (vals (control/usage (:control @saved)))))))

(deftest query-timeout-covers-input-consumption
  (let [events (atom []) saved (atom nil)
        input (reify Iterable
                (iterator [_]
                  (reify Iterator
                    (hasNext [_] true)
                    (next [_] (Thread/sleep 50) 1))))]
    (is (= :query/timeout
           (:error (error #(rt/with-runtime {}
                             (fn [runtime]
                               (reset! saved runtime)
                               (program/query! runtime
                                 '[:find ?v :in $s [?e ...] :timeout 5 :where [$s ?e :row/value ?v]]
                                 [(fixture/provider events {}) input])))))))
    (is (empty? @events))
    (is (every? zero? (vals (control/usage (:control @saved)))))))

(deftest input-and-query-values-survive-provider-mutation
  (let [bytes (byte-array [1]) tuple (object-array [bytes]) events (atom [])
        delegate (fixture/provider events {})
        provider (reify source/IQuerySource
                   (-source-info [_] (aset-byte bytes 0 (byte 9)) (source/-source-info delegate))
                   (-open-session [_ request] (source/-open-session delegate request))
                   (-plan-source [_ session request] (source/-plan-source delegate session request)))
        query {:find ['?payload '?constant '?v] :in ['$s ['?payload]]
               :where [['$s '?e :row/value '?v] [(list 'ground bytes) '?constant]]}
        result (query! query [provider tuple])]
    (is (= #{[[1] [1] "one"] [[1] [1] "two"]}
           (into #{} (map (fn [[p c v]] [(vec p) (vec c) v])) result)))
    (is (= [9] (vec bytes)))))

(deftest failed-session-planning-closes-inputs-and-sessions-immediately
  (let [events (atom []) saved (atom nil)]
    (is (= :query-source/provider
           (:error (error #(rt/with-runtime {}
                             (fn [runtime]
                               (reset! saved runtime)
                               (program/query! runtime
                                 '[:find ?v :in $s [?e ...] :where [$s ?e :row/value ?v]]
                                 [(fixture/provider events {:planning (fn [_] (throw (Exception. "secret")))}) [1 2]])))))))
    (is (= 1 (count (events-of events :session-close))))
    (is (every? zero? (vals (control/usage (:control @saved)))))))
