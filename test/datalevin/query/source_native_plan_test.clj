(ns datalevin.query.source-native-plan-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.core :as d]
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
  (:import [java.util Arrays UUID]))

(defn- events-of [events kind] (filterv #(= kind (first %)) @events))
(defn- error [f] (try (f) nil (catch clojure.lang.ExceptionInfo e (ex-data e))))
(defn- with-db [schema facts f]
  (let [dir (str u/tmp-dir "spi-v2-native-plan-" (UUID/randomUUID))
        conn (d/get-conn dir schema)]
    (try (d/transact! conn facts) (f @conn conn)
         (finally (d/close conn) (u/delete-files dir)))))

(defn- capture! [runtime database events query rules config]
  (planning/capture! runtime
    [{:symbol '$n :native database} {:symbol '$s :source (fixture/provider events config)}]
    (parser/parse-query query) (parser/parse-rules rules) {} #{} nil))

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
      (doseq [[database pattern] [[database '[1 :native/score ?score]]
                                  [database '[?e ?attribute ?score]]
                                  [database '[?e :native/private ?score]]
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
