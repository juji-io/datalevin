(ns datalevin.query.source-native-rules-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.core :as d]
            [datalevin.parser :as parser]
            [datalevin.query.source :as source]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.planning :as planning]
            [datalevin.query.source.program :as program]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :as value]
            [datalevin.query.source-planning-test :as fixture]
            [datalevin.util :as u])
  (:import [java.util Random UUID]))

(defn- events-of [events kind] (filterv #(= kind (first %)) @events))
(defn- error [f] (try (f) nil (catch clojure.lang.ExceptionInfo e (ex-data e))))
(defn- query! [query arguments & [options]]
  (rt/with-runtime (or options {})
    (fn [runtime]
      (let [result (program/query! runtime query arguments)]
        (is (every? zero? (vals (control/usage (:control runtime)))))
        result))))

(defn- closure [edges]
  (loop [paths (set edges)]
    (let [next (into paths (for [[a b] paths [c d] edges :when (= b c)] [a d]))]
      (if (= paths next) paths (recur next)))))

(def linear-rules
  '[[(reach ?x ?y) [?x ?y]]
    [(reach ?from ?to) [?from ?mid] (reach ?mid ?to)]])

(deftest generated-recursive-query-emission-matches-an-independent-oracle
  (let [random (Random. 1618033)]
    (dotimes [i 30]
      (let [edges (vec (repeatedly (.nextInt random 18)
                                  #(vector (.nextInt random 6) (.nextInt random 6))))
            rules (if (even? i) linear-rules
                      '[[(reach ?x ?y) [?x ?y]]
                        [(reach ?a ?b) (reach ?a ?mid) (reach ?mid ?b)]])
            expected (closure edges)]
        (is (= expected (query! '[:find ?a ?b :in $rows % :where ($rows reach ?a ?b)] [edges rules])))
        (is (= (into #{} (keep (fn [[a b]] (when (= a 1) [b]))) expected)
               (query! '[:find ?b :in $rows % :where ($rows reach 1 ?b)] [edges rules])))
        (is (= (into #{} (keep (fn [[a b]] (when (= a b) [a]))) expected)
               (query! '[:find ?a :in $rows % :where ($rows reach ?a ?a)] [edges rules])))))))

(deftest mutually-recursive-heads-and-acyclic-dependents-reach-the-same-fixpoint
  (let [edges (mapv #(vector % (inc %)) (range 6))
        rules '[[(even ?n) [$seed ?n]]
                [(even ?next) (odd ?previous) [?previous ?next]]
                [(odd ?n) (even ?previous) [?previous ?n]]
                [(selected ?id) (even ?id) [(not= ?id 0)]]]]
    (is (= #{[0] [2] [4] [6]}
           (query! '[:find ?n :in $rows $seed % :where ($rows even ?n)] [edges [[0]] rules])))
    (is (= #{[2] [4] [6]}
           (query! '[:find ?n :in $rows $seed % :where ($rows selected ?n)] [edges [[0]] rules])))))

(deftest union-base-cases-seed-recursion-inside-one-compiled-body
  (let [rules '[[(reach ?x ?y)
                 (or-join [?x ?y] [?x ?y]
                   (and (reach ?x ?mid) [?mid ?y]))]]
        edges [[1 2] [2 3] [3 1] [3 4]]]
    (is (= (closure edges)
           (query! '[:find ?a ?b :in $rows % :where ($rows reach ?a ?b)] [edges rules])))))

(deftest constant-function-base-cases-seed-complete-native-heads
  (let [rules '[[(reach ?x ?y) [(ground 1) ?x] [(ground 2) ?y]]
                [(reach ?x ?y) (reach ?x ?mid) [?mid ?y]]]]
    (is (= #{[1 2] [1 3] [1 4]}
           (query! '[:find ?a ?b :in $rows % :where ($rows reach ?a ?b)] [[[2 3] [3 4]] rules])))))

(deftest source-qualified-recursive-references-keep-independent-relations
  (let [rules (conj linear-rules
                    '[(both ?x ?y) (or-join [?x ?y] ($a reach ?x ?y) ($b reach ?x ?y))])
        a [[1 2] [2 3]] b [[3 4] [4 5]]]
    (is (= (into (closure a) (closure b))
           (query! '[:find ?x ?y :in $a $b % :where ($a both ?x ?y)] [a b rules])))))

(deftest full-head-sets-and-outer-witness-bags-survive-compiled-recursion
  (let [edges [[1 2] [1 2] [1 3] [2 4]]
        input [[1 :a] [1 :b] [1 :a] [2 :c]]]
    (rt/with-runtime {}
      (fn [runtime]
        (let [compilation (planning/capture! runtime [{:symbol '$rows :native edges}]
                            (parser/parse-query '[:find ?start ?tag ?to :in $rows % [[?start ?tag]]
                                                  :where ($rows reach ?start ?to)])
                            (parser/parse-rules linear-rules) {} #{'?start '?tag} nil)
              p (program/compile! runtime compilation
                  [{:variable '?start :type :long :nullable? false}
                   {:variable '?tag :type :keyword :nullable? false}] input)
              result (program/execute! runtime p)
              actual (map (fn [^objects row]
                            (mapv #(aget row (int (get (:attrs result) %))) ['?start '?tag '?to])) (:tuples result))]
          (is (= (frequencies (for [[start tag] input [a b] (closure edges) :when (= start a)] [start tag b]))
                 (frequencies actual))))))))

(deftest wildcard-call-projection-keeps-rule-set-semantics
  (let [edges [[1 2] [1 2] [1 3] [2 1]]]
    (is (= #{[1] [2]} (query! '[:find ?a :in $rows % :where ($rows reach ?a _)] [edges linear-rules])))
    (is (= 1 (query! '[:find ?v . :in $rows % ?v :where ($rows reach _ _)] [edges linear-rules 1])))
    (is (= #{} (query! '[:find ?a :in $rows % :where ($rows reach ?a ?b)] [[] linear-rules])))))

(deftest unseeded-and-empty-positive-cycles-are-complete-empty-results
  (doseq [rules ['[[(loop ?x) (loop ?x)]]
                 '[[(left ?x) (right ?x)] [(right ?x) (left ?x)]]]]
    (let [name (first (ffirst rules))
          query {:find ['?x] :in ['$rows '%] :where [(list '$rows name '?x)]}]
      (is (= #{} (query! query [[] rules]))))))

(deftest recursive-byte-and-null-values-remain-owned-and-logically-equal
  (let [one (byte-array [1]) two (byte-array [2]) three (byte-array [3])
        edges [[one two] [(byte-array [2]) three] [three (byte-array [1])] [nil one]]
        result (query! '[:find ?a ?b :in $rows % :where ($rows reach ?a ?b)] [edges linear-rules])
        normalize #(when % (vec %))
        expected (closure (mapv #(mapv normalize %) edges))]
    (aset-byte one 0 (byte 99))
    (aset-byte two 0 (byte 99))
    (aset-byte three 0 (byte 99))
    (is (= expected (into #{} (map #(mapv normalize %)) result)))))

(deftest compiled-iteration-storage-does-not-accumulate-across-long-local-chains
  (let [n 250 rules '[[(walk ?n) [$seed ?n]] [(walk ?y) (walk ?x) [?x ?y]]]]
    (is (= (set (map vector (range (inc n))))
           (query! '[:find ?n :in $rows $seed % :where ($rows walk ?n)]
             [(mapv #(vector % (inc %)) (range n)) [[0]] rules]
             {:limits {:retained-bytes 6000000}})))))

(defn- with-db [f]
  (let [dir (str u/tmp-dir "spi-v2-native-rules-" (UUID/randomUUID))
        conn (d/get-conn dir {:edge {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
                             :root {:db/valueType :db.type/boolean}})]
    (try (d/transact! conn [{:db/id 1 :edge 2} {:db/id 2 :edge 3}]) (f conn)
         (finally (d/close conn) (u/delete-files dir)))))

(deftest repeated-native-body-scans-release-their-decoded-values-and-row-leases
  (with-db
    (fn [conn]
      (let [n 80 rules '[[(walk ?n) [?n :root true]] [(walk ?y) (walk ?x) [?x :edge ?y]]]]
        (d/transact! conn (into [{:db/id 1 :root true}]
                               (map (fn [i] {:db/id i :edge (inc i)})) (range 3 (inc n))))
        (is (= (set (map vector (range 1 (+ n 2))))
               (query! '[:find ?n :in $n % :where ($n walk ?n)] [@conn rules]
                 {:limits {:retained-bytes 4000000}})))))))

(deftest indexed-native-recursion-emits-typed-dependent-source-keys
  (with-db
    (fn [conn]
      (let [events (atom [])
            rules '[[(reach ?x ?y) [?x :edge ?y]]
                    [(reach ?x ?y) [?x :edge ?mid] (reach ?mid ?y)]]
            query '[:find ?from ?to ?name :in $n $s %
                    :where ($n reach ?from ?to) [$s ?from :row/value ?name]]]
        (is (= #{[1 2 "one"] [1 3 "one"] [2 3 "two"]}
               (query! query [@conn (fixture/provider events {:lookup-only? true}) rules])))
        (is (= 1 (count (events-of events :acquire))))
        (is (= 1 (count (events-of events :open))))
        (let [request (second (first (events-of events :plan)))]
          (is (= 1 (count (:clauses request))))
          (is (= [{:variable '?from :type :long :nullable? false}]
                 (filterv #(= '?from (:variable %)) (:available-inputs request)))))))))

(deftest provider-planning-writes-do-not-change-recursive-native-read-views
  (with-db
    (fn [conn]
      (let [events (atom []) delegate (fixture/provider events {:lookup-only? true})
            provider (reify source/IQuerySource
                       (-source-info [_] (source/-source-info delegate))
                       (-open-session [_ request] (source/-open-session delegate request))
                       (-plan-source [_ session request]
                         (d/transact! conn [{:db/id 3 :edge 4}])
                         (source/-plan-source delegate session request)))
            rules '[[(reach ?x ?y) [?x :edge ?y]]
                    [(reach ?x ?y) [?x :edge ?mid] (reach ?mid ?y)]]]
        (is (= #{[1 2 "one"] [1 3 "one"] [2 3 "two"]}
               (query! '[:find ?a ?b ?v :in $n $s %
                         :where ($n reach ?a ?b) [$s ?a :row/value ?v]] [@conn provider rules])))
        (is (= #{[3 4]} (d/q '[:find ?a ?b :where [?a :edge ?b] [(= ?b 4)]] @conn)))))))

(deftest unsupported-negative-cycles-missing-required-and-mapped-recursion-reject-before-acquisition
  (doseq [[rules call extra]
          [['[[(reach ?x ?y) [?x ?y]]
              [(reach ?x ?y) [?x ?mid] (reach ?mid ?y) (not-join [?x ?y] (reach ?x ?y))]]
            '(reach ?a ?b) []]
           ['[[(reach [?x] ?y) [?x ?y]]
              [(reach [?x] ?y) [?x ?mid] (reach ?mid ?y)]]
            '(reach ?a ?b) []]
           ['[[(reach ?x ?y) [$s ?x :row/value ?y]]
              [(reach ?x ?y) [$rows ?x ?mid] (reach ?mid ?y)]]
            '(reach ?a ?b) []]
           ['[[(reach ?x ?y) [?x ?y]] [(reach ?x ?y) [?x ?mid] (reach ?mid ?y)]
              [(copied ?x ?y) (reach ?x _) [(identity ?x) ?y]]]
            '(copied ?a ?b) []]]]
    (let [events (atom []) saved (atom nil)
          query {:find ['?a '?b] :in ['$rows '$s '%]
                 :where [(apply list '$rows call)]}
          failure (error #(rt/with-runtime {}
                            (fn [runtime]
                              (reset! saved runtime)
                              (program/query! runtime query
                                (into [[[1 2]] (fixture/provider events {}) rules] extra)))))]
      (is (= :query-source/unsupported (:error failure)))
      (is (empty? (events-of events :acquire)))
      (is (empty? (events-of events :open)))
      (is (every? zero? (vals (control/usage (:control @saved))))))))

(deftest recursive-runtime-quota-failure-closes-compiled-body-storage
  (let [saved (atom nil)
        failure (error (fn [] (rt/with-runtime {}
                          (fn [runtime]
                            (reset! saved runtime)
                            (let [c (planning/capture! runtime
                                      [{:symbol '$rows :native (mapv (fn [n] [n (inc n)]) (range 30))}]
                                      (parser/parse-query '[:find ?x ?y :in $rows % :where ($rows reach ?x ?y)])
                                      (parser/parse-rules linear-rules) {} #{} nil)
                                  p (program/compile! runtime c [] nil)
                                  ctl (:control runtime)
                                  remaining (- (:retained-bytes (control/limits ctl))
                                               (:retained-bytes (control/usage ctl)))
                                  token (source/reserve! ctl {:retained-bytes (- remaining 50000)})]
                              (try (program/execute! runtime p) (finally (source/release! token))))))))]
    (is (= :query-source/limit (:error failure)))
    (is (every? zero? (vals (control/usage (:control @saved)))))))

(deftest cancellation-during-initial-head-ownership-unwinds-the-emitted-program
  (let [saved (atom nil) armed? (atom false) visits (atom 0)
        item (reify value/IQuerySourceValue
               (-logical-type [_] {:kind :custom :name :test/native-rule-key :version 1 :fingerprint "v1"})
               (-retained-bytes [_]
                 (when @armed?
                   (swap! visits inc)
                   (control/cancel! (:control @saved) {:error :query-source/cancelled}))
                 64))
        failure (error (fn [] (rt/with-runtime {}
                          (fn [runtime]
                            (reset! saved runtime)
                            (let [c (planning/capture! runtime [{:symbol '$rows :native [[item item]]}]
                                      (parser/parse-query '[:find ?x ?y :in $rows % :where ($rows reach ?x ?y)])
                                      (parser/parse-rules linear-rules) {} #{} nil)
                                  p (program/compile! runtime c [] nil)]
                              (reset! armed? true)
                              (program/execute! runtime p))))))]
    (is (= :query-source/cancelled (:error failure)))
    (is (pos? @visits))
    (is (every? zero? (vals (control/usage (:control @saved)))))))
