(ns datalevin.query.source-negation-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.core :as d]
            [datalevin.join :as join]
            [datalevin.parser :as parser]
            [datalevin.query.source :as source]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.planning :as planning]
            [datalevin.query.source.program :as program]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :as value]
            [datalevin.query.source-planning-test :as fixture]
            [datalevin.relation :as relation]
            [datalevin.util :as u])
  (:import [java.util Random UUID]))

(defn- error [f] (try (f) nil (catch clojure.lang.ExceptionInfo e (ex-data e))))
(defn- events-of [events kind] (filterv #(= kind (first %)) @events))
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

(deftest flat-native-negation-and-private-not-join-locals
  (let [facts [[1 :name :a] [2 :name :b] [2 :blocked true] [3 :name :c]]]
    (doseq [q ['[:find ?e :in $rows :where [$rows ?e :name _] (not [$rows ?e :blocked true])]
               '[:find ?e :in $rows :where (not [$rows ?e :blocked true]) [$rows ?e :name _]]]]
      (is (= #{[1] [3]} (query! q [facts])))))
  (is (= #{[2 :outer]}
         (query! '[:find ?e ?private :in $rows [[?e ?private]]
                   :where (not-join [?e] [$rows ?e ?private])]
           [[[1 :inner]] [[1 :outer] [2 :outer] [2 :outer]]])))
  ;; Locals in separate implicit not clauses do not supply one another.
  (is (= #{[3]}
         (query! '[:find ?e :in $a $b [?e ...]
                   :where (not [$a ?e ?private]) (not [$b ?e ?private])]
           [[[1 :one]] [[2 :two]] [1 2 3]]))))

(deftest negative-children-can-be-empty-without-proving-the-parent-empty
  (is (= #{[1] [2]}
         (query! '[:find ?e :in $rows [?e ...] :where (not [$rows ?e _])] [[] [1 2 2]])))
  (is (= #{[1] [2]}
         (query! '[:find ?e :in $a $b [?e ...]
                   :where (not-join [?e] [$a ?e ?private] [$b ?private _])]
           [[[1 :x] [2 :y]] [] [1 2]])))
  (is (= #{} (query! '[:find ?e :in $rows [?e ...] :where (not [$rows ?e _])] [[[1 :x]] []]))))

(deftest literal-negation-filters-preserve-constant-equivalence
  (is (= #{} (query! '[:find ?x :in $rows ?x :where (not-join [?x] [$rows 1N :blocked true])]
                [[[1 :blocked true]] :outer])))
  (rt/with-runtime {}
    (fn [runtime]
      (let [capture (planning/capture! runtime [{:symbol '$rows :native [[1 :blocked true]]}]
                      (parser/parse-query '[:find ?x :in $rows ?x :where (not [$rows ?x :blocked true])])
                      [] {'?x 1N} #{'?x} nil
                      {:default-source '$rows :rule-relations {} :literal-vars #{'?x}})
            p (program/compile! runtime capture [{:variable '?x :type :bigint :nullable? false}] [[1N]])]
        (is (empty? (:tuples (program/execute! runtime p))))))))

(deftest source-free-negative-predicates-and-null-keys
  (is (= #{[1]}
         (query! '[:find ?x :in [?x ...] :where (not [(nil? ?x)])] [[nil 1]])))
  (is (= #{[nil]}
         (query! '[:find ?x :in [?x ...] :where (not-join [?x] [(identity ?x) ?copy])] [[nil 1]])))
  (is (= #{[1]}
         (query! '[:find ?x :in $rows [?x ...] :where (not [$rows ?x _])]
           [[[nil :present]] [nil 1]]))))

(deftest generated-stratified-complements-match-an-independent-reachability-oracle
  (let [random (Random. 223606)
        rules '[[(reach ?a ?b) [?a ?b]] [(reach ?a ?b) [?a ?mid] (reach ?mid ?b)]
                [(blocked ?b) [$blocked ?b]]
                [(allowed ?a ?b) (reach ?a ?b) (not (blocked ?b))]]]
    (dotimes [_ 20]
      (let [edges (vec (repeatedly (.nextInt random 16)
                                  #(vector (.nextInt random 6) (.nextInt random 6))))
            blocked (set (repeatedly (.nextInt random 5) #(.nextInt random 6)))
            expected (into #{} (remove #(contains? blocked (second %))) (closure edges))]
        (is (= expected
               (query! '[:find ?a ?b :in $rows $blocked % :where ($rows allowed ?a ?b)]
                 [edges (mapv vector blocked) rules])))))))

(deftest negative-facts-constrain-every-positive-recursive-round
  (let [rules '[[(reach ?a ?b) [?a ?b] (not [$blocked ?b])]
                [(reach ?a ?b) (reach ?a ?mid) [?mid ?b] (not [$blocked ?b])]]
        edges [[1 2] [2 3] [3 4] [1 5] [5 6] [6 1]] blocked #{3 6}
        filtered (remove #(contains? blocked (second %)) edges)]
    (is (= (closure filtered)
           (query! '[:find ?a ?b :in $rows $blocked % :where ($rows reach ?a ?b)]
             [edges (mapv vector blocked) rules])))))

(deftest required-negative-calls-complete-new-demands-before-deciding-absence
  (let [rules '[[(reach [?a] ?b) [?a ?b]] [(reach [?a] ?b) [?a ?mid] (reach ?mid ?b)]
                [(absent [?a ?b]) (not-join [?a ?b] (reach ?a ?b))]]
        edges [[1 2] [2 3] [3 1] [4 5]]
        seeds [[1 3] [1 5] [4 5] [4 1] [5 5] [1 5]]
        paths (closure edges)]
    (is (= (set (remove paths seeds))
           (query! '[:find ?a ?b :in $rows % [[?a ?b]] :where ($rows absent ?a ?b)]
             [edges rules seeds])))))

(deftest multiple-strata-and-source-qualified-negative-relations
  (let [rules '[[(present ?e) [?e]]
                [(missing [?e]) (not (present ?e))]
                [(kept [?e]) (not (missing ?e))]
                [(both [?e]) ($a kept ?e) ($b kept ?e)]]]
    (is (= #{[2]}
           (query! '[:find ?e :in $a $b % [?e ...] :where ($a both ?e)]
             [[[1] [2]] [[2] [3]] rules [1 2 3 4]])))))

(deftest stratum-descent-is-bounded-before-body-compilation-and-acquisition
  (doseq [n [12 33]]
    (let [rules (into '[[(p0 [?x]) [?x]]]
                  (for [i (range 1 (inc n))]
                    [(list (symbol (str "p" i)) '[?x]) (list 'not (list (symbol (str "p" (dec i))) '?x))]))
          query {:find ['?x] :in ['$rows '$s '% '?x] :where [(list '$rows (symbol (str "p" n)) '?x)]}
          events (atom []) saved (atom nil)
          result (atom nil)
          failure (error (fn [] (rt/with-runtime {}
                                 (fn [runtime]
                                   (reset! saved runtime)
                                   (reset! result (program/query! runtime query
                                                    [[[1]] (fixture/provider events {}) rules 1]))))))]
      (if (= n 12)
        (do (is (nil? failure)) (is (= #{[1]} @result)))
        (do (is (= :query-source/limit (:error failure))) (is (empty? (events-of events :acquire)))))
      (is (every? zero? (vals (control/usage (:control @saved))))))))

(deftest even-negation-in-a-positive-cycle-adds-no-premature-answers
  (doseq [[seed expected] [[[[1]] #{[1]}] [[] #{}]]]
    (let [rules '[[(known ?e) [$seed ?e]]
                  [(known ?e) [$domain ?e] (not (not (known ?e)))]]]
      (is (= expected
             (query! '[:find ?e :in $rows $seed $domain % :where ($rows known ?e)]
               [[] seed [[1] [2]] rules]))))))

(deftest negative-functions-require-their-observable-caller-inputs
  (let [rules '[[(copied ?x ?y) [?x ?y] (not-join [?x] [(identity ?x) ?out])]]
        events (atom []) saved (atom nil)
        failure (error (fn [] (rt/with-runtime {}
                               (fn [runtime]
                                 (reset! saved runtime)
                                 (program/query! runtime '[:find ?x ?y :in $rows $s % :where ($rows copied ?x ?y)]
                                   [[[1 2]] (fixture/provider events {}) rules])))))]
    (is (= :query-source/unsupported (:error failure)))
    (is (empty? (events-of events :acquire)))
    (is (every? zero? (vals (control/usage (:control @saved)))))))

(deftest negative-cycles-and-missing-join-inputs-reject-before-acquisition
  (doseq [[rules call]
          [['[[(bad [?x]) (not (bad ?x))]] '(bad ?x)]
           ['[[(left [?x]) (not (right ?x))] [(right [?x]) (left ?x)]] '(left ?x)]
           ['[[(left [?x]) (not (right ?x))] [(right [?x]) (not (left ?x))]] '(left ?x)]]]
    (let [events (atom []) saved (atom nil)
          failure (error (fn [] (rt/with-runtime {}
                                 (fn [runtime]
                                   (reset! saved runtime)
                                   (program/query! runtime
                                     {:find ['?x] :in ['$rows '$s '% '?x] :where [(apply list '$rows call)]}
                                     [[] (fixture/provider events {}) rules 1])))))]
      (is (= :query-source/unsupported (:error failure)))
      (is (empty? (events-of events :acquire)))
      (is (every? zero? (vals (control/usage (:control @saved)))))))
  (doseq [clause ['(not-join [?missing] [$rows ?missing _]) '(not [$rows ?missing _])
                  '(not-join [?x] [$s ?x :row/value _])]]
    (let [events (atom [])
          failure (error (fn [] (rt/with-runtime {}
                                 (fn [runtime]
                                   (program/query! runtime {:find ['?x] :in ['$rows '$s '?x] :where [clause]}
                                     [[] (fixture/provider events {}) 1])))))]
      (is (= :query-source/unsupported (:error failure)))
      (is (empty? (events-of events :acquire))))))

(deftest negative-comparisons-do-not-produce-head-types-or-admit-ambiguous-literals
  (doseq [rules ['[[(allowed [?x]) (not [?x :blocked true])]]
                 '[[(allowed [?x]) [(identity ?x) ?copy] [?copy :present true]]]]]
    (let [events (atom [])
          failure (error (fn [] (rt/with-runtime {}
                                 (fn [runtime]
                                   (program/query! runtime '[:find ?x :in $rows $s %
                                                             :where ($rows allowed 1N) [(ground :kept) ?x]]
                                     [[[1 :blocked true] [1 :present true]] (fixture/provider events {}) rules])))))]
      (is (= :query-source/unsupported (:error failure)))
      (is (empty? (events-of events :acquire)))))
  (let [rules '[[(allowed [?x]) (not [?x :blocked true])]]]
    (is (= #{} (query! '[:find ?x :in $rows % ?x :where ($rows allowed 1)]
                 [[[1 :blocked true]] rules :kept])))
    (rt/with-runtime {}
      (fn [runtime]
        (let [capture (planning/capture! runtime [{:symbol '$rows :native [[1 :blocked true]]}]
                        (parser/parse-query '[:find ?x :in $rows % ?x :where ($rows allowed ?x)])
                        (parser/parse-rules rules) {} #{'?x} nil)
              p (program/compile! runtime capture [{:variable '?x :type :data :nullable? true}] [[nil]])]
          (is (= [{:variable '?x :type :data :nullable? true}] (get-in p [:core-specs [0] :columns]))))))))

(deftest subtraction-preserves-surviving-bags-and-typed-composite-keys
  (rt/with-runtime {}
    (fn [runtime]
      (let [ctl (:control runtime) one (byte-array [1])
            a (relation/relation! {'?key 0 '?tag 1}
                (java.util.ArrayList. [(object-array [1 :long]) (object-array [1N :bigint])
                               (object-array [[one] :bytes]) (object-array [2 :keep]) (object-array [2 :keep])]))
            b (relation/relation! {'?key 0}
                (java.util.ArrayList. [(object-array [1N]) (object-array [[(byte-array [1])]])]))
            before (control/usage ctl)
            result (join/subtract-rel-bounded a b
                     {:check! #(source/check-active! ctl)
                      :reserve! (fn [_ bytes] (source/reserve! ctl {:retained-bytes bytes}))
                      :retain! (fn [token] (rt/own! runtime #(source/release! token)))
                      :release! source/release!})]
        (is (= {[:long] 1 [:keep] 2}
               (frequencies (map (fn [^objects row] [(aget row 1)]) (:tuples result)))))
        (is (> (:retained-bytes (control/usage ctl)) (:retained-bytes before)))))))

(deftest subtraction-rolls-back-index-and-output-reservations-on-failure
  (doseq [budget [2000 34200]]
    (rt/with-runtime {:limits {:retained-bytes budget}}
      (fn [runtime]
        (let [ctl (:control runtime)
              a (relation/relation! {'?key 0} (java.util.ArrayList. (mapv #(object-array [%]) (range 20))))
              b (relation/relation! {'?key 0} (java.util.ArrayList. (mapv #(object-array [%]) (range 50))))
              failure (error #(join/subtract-rel-bounded a b
                                {:check! (fn [] (source/check-active! ctl))
                                 :reserve! (fn [_ bytes] (source/reserve! ctl {:retained-bytes bytes}))
                                 :retain! (fn [token] (rt/own! runtime (fn [] (source/release! token))))
                                 :release! source/release!}))]
          (is (= :query-source/limit (:error failure)))
          (is (every? zero? (vals (control/usage ctl))))))))
  (let [saved (atom nil) visits (atom 0)
        failure
        (error #(rt/with-runtime {}
                  (fn [runtime]
                    (reset! saved runtime)
                    (let [ctl (:control runtime)
                          rel (relation/relation! {'?key 0} (java.util.ArrayList. (mapv (fn [x] (object-array [x])) (range 50))))]
                      (join/subtract-rel-bounded rel rel
                        {:check! (fn []
                                   (when (= 20 (swap! visits inc))
                                     (control/cancel! ctl {:error :query-source/cancelled}))
                                   (source/check-active! ctl))
                         :reserve! (fn [_ bytes] (source/reserve! ctl {:retained-bytes bytes}))
                         :retain! (fn [token] (rt/own! runtime (fn [] (source/release! token))))
                         :release! source/release!})))))]
    (is (= :query-source/cancelled (:error failure)))
    (is (every? zero? (vals (control/usage (:control @saved)))))))

(defn- with-db [f]
  (let [dir (str u/tmp-dir "spi-v2-negative-rules-" (UUID/randomUUID))
        conn (d/get-conn dir {:edge {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
                             :blocked {:db/valueType :db.type/boolean}})]
    (try (d/transact! conn [{:db/id 1 :edge 2} {:db/id 2 :edge 3} {:db/id 3 :blocked true}]) (f conn)
         (finally (d/close conn) (u/delete-files dir)))))

(deftest negative-native-strata-stay-pinned-and-export-positive-types
  (with-db
    (fn [conn]
      (let [events (atom []) delegate (fixture/provider events {:lookup-only? true})
            provider (reify source/IQuerySource
                       (-source-info [_] (source/-source-info delegate))
                       (-open-session [_ request] (source/-open-session delegate request))
                       (-plan-source [_ session request]
                         (d/transact! conn [{:db/id 2 :blocked true}])
                         (source/-plan-source delegate session request)))
            rules '[[(reach [?a] ?b) [?a :edge ?b]]
                    [(reach [?a] ?b) [?a :edge ?mid] (reach ?mid ?b)]
                    [(blocked [?e]) [?e :blocked true]]
                    [(allowed [?a] ?b) (reach ?a ?b) (not (blocked ?b))]]]
        (is (= #{[2 "two"]}
               (query! '[:find ?b ?name :in $n $s % ?a
                         :where ($n allowed ?a ?b) [$s ?b :row/value ?name]] [@conn provider rules 1])))
        (is (= [{:variable '?b :type :long :nullable? false}]
               (filterv #(= '?b (:variable %)) (:available-inputs (second (first (events-of events :plan)))))))
        (is (= 1 (count (events-of events :acquire))))
        (is (= #{[2] [3]} (d/q '[:find ?e :where [?e :blocked true]] @conn)))))))

(deftest negative-body-workspaces-do-not-accumulate-through-recursive-rounds
  (let [n 90 rules '[[(reach [?a] ?b) [?a ?b] (not [$blocked ?b])]
                     [(reach [?a] ?b) (reach ?a ?mid) [?mid ?b] (not [$blocked ?b])]]]
    (is (= (set (map vector (range 1 (inc n))))
           (query! '[:find ?to :in $rows $blocked % ?start :where ($rows reach ?start ?to)]
             [(mapv #(vector % (inc %)) (range n)) [] rules 0]
             {:limits {:retained-bytes 6000000}})))))

(deftest negative-child-cancellation-releases-subtraction-and-lower-strata-storage
  (let [saved (atom nil) armed? (atom false) visits (atom 0)
        item (reify value/IQuerySourceValue
               (-logical-type [_] {:kind :custom :name :test/negative-key :version 1 :fingerprint "v1"})
               (-retained-bytes [_]
                 (when @armed?
                   (swap! visits inc)
                   (control/cancel! (:control @saved) {:error :query-source/cancelled}))
                 64))
        rules '[[(present [?x] ?value) [?x ?value]] [(absent [?x]) (not-join [?x] (present ?x _))]]
        failure (error (fn [] (rt/with-runtime {}
                               (fn [runtime]
                                 (reset! saved runtime)
                                 (let [capture (planning/capture! runtime [{:symbol '$rows :native [[1 item]]}]
                                                 (parser/parse-query '[:find ?x :in $rows % ?x :where ($rows absent ?x)])
                                                 (parser/parse-rules rules) {} #{'?x} nil)
                                       p (program/compile! runtime capture
                                           [{:variable '?x :type :long :nullable? false}] [[1]])]
                                   (reset! armed? true)
                                   (program/execute! runtime p))))))]
    (is (= :query-source/cancelled (:error failure)))
    (is (pos? @visits))
    (is (every? zero? (vals (control/usage (:control @saved)))))))
