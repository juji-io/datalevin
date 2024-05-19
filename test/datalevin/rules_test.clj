(ns datalevin.rules-test
  (:require
   [clojure.test :refer [deftest are is use-fixtures]]
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [datalevin.rules :as sut]
   [datalevin.core :as d]
   [datalevin.constants :as c]
   [datalevin.util :as u])
  (:import
   [java.util UUID]))

(use-fixtures :each db-fixture)

(deftest strongly-connected-components-test
  (are [graph sccs] (= (set (sut/tarjans-scc graph)) (set sccs))
    {:node        #{:unreachable}
     :reachable   #{:unreachable :reachable}
     :unreachable #{}}
    [#{:reachable} #{:node} #{:unreachable}]

    {1 [2]
     2 [3 4]
     3 [4 6]
     4 [1 5]
     5 [6]
     6 [7]
     7 [5]}
    [#{1 2 3 4} #{5 6 7}]

    {:a [:c :h]
     :b [:a :g]
     :c [:d]
     :d [:f]
     :e [:a :i]
     :f [:j]
     :g [:i]
     :h [:f :g]
     :i [:h]
     :j [:c]}
    [#{:e} #{:b} #{:a} #{:h :i :g} #{:c :j :f :d}]))

(deftest reachability-data-test
  (let [dir (u/tmp-dir (str "reachability-test-" (UUID/randomUUID)))
        db  (-> (d/empty-db
                  dir
                  {:node {:db/unique    :db.unique/identity
                          :db/valueType :db.type/symbol}
                   :link {:db/cardinality :db.cardinality/many
                          :db/valueType   :db.type/ref}}
                  {:kv-opts {:flags (conj c/default-env-flags :nosync)}})
                (d/db-with [{:db/id -1 :node 'a :link -2}
                            {:db/id -2 :node 'b :link -3}
                            {:db/id -3 :node 'c :link [-3 -4]}
                            {:db/id -4 :node 'd}]))]
    (is (= #{['a 'b]
             ['b 'c]
             ['c 'c]
             ['c 'd]
             ['a 'c]
             ['b 'd]
             ['a 'd]}
           (d/q '[:find ?n ?m
                  :in $ %
                  :where
                  (reachable ?X ?Y)
                  [?X :node ?n]
                  [?Y :node ?m]]
                db '[[(reachable ?X ?Y)
                      [?X :link ?Y]]
                     [(reachable ?X ?Y)
                      [?X :link ?Z]
                      (reachable ?Z ?Y)]])
           (d/q '[:find ?n ?m
                  :in $ %
                  :where
                  (reachable ?n ?m)]
                db '[[(reachable ?n ?m)
                      [?X :link ?Y]
                      [?X :node ?n]
                      [?Y :node ?m]
                      ]
                     [(reachable ?n ?m)
                      [?X :node ?n]
                      [?Y :node ?m]
                      [?Z :node ?o]
                      [?X :link ?Z]
                      (reachable ?o ?m)]])))
    (d/close-db db)
    (u/delete-files dir)))

(deftest math-genealogy-test
  (let [dir (u/tmp-dir (str "math-genealogy-test-" (UUID/randomUUID)))
        db  (-> (d/empty-db
                  dir
                  {:dissertation/cid  {:db/valueType :db.type/ref}
                   :dissertation/univ {:db/valueType :db.type/string}
                   :dissertation/area {:db/valueType :db.type/string}
                   :person/name       {:db/valueType :db.type/string}
                   :person/advised    {:db/valueType   :db.type/ref
                                       :db/cardinality :db.cardinality/many}}
                  {:kv-opts {:flags (conj c/default-env-flags :nosync)}})
                (d/db-with
                  [{:db/id       -1
                    :person/name "David Scott Warren"}
                   {:db/id             -2
                    :dissertation/cid  -1
                    :dissertation/univ "University of Michigan"
                    :dissertation/area "68—Computer science"}
                   {:db/id          -3
                    :person/name    "Joyce Barbara Friedman"
                    :person/advised [-2]}
                   {:db/id             -4
                    :dissertation/cid  -3
                    :dissertation/univ "Harvard University"
                    :dissertation/area "68—Computer science"}
                   {:db/id          -5
                    :person/name    "Stål Olav Aanderaa"
                    :person/advised [-4]}
                   {:db/id             -6
                    :dissertation/cid  -5
                    :dissertation/univ "Harvard University"
                    :dissertation/area "03—Mathematical logic and foundations"}
                   {:db/id          -7
                    :person/name    "Hao Wang"
                    :person/advised [-6]}
                   {:db/id             -8
                    :dissertation/cid  -7
                    :dissertation/univ "Harvard University",
                    :dissertation/area "03—Mathematical logic and foundations"}
                   {:db/id          -9
                    :person/name    "Willard Van Orman Quine"
                    :person/advised [-8]}
                   {:db/id             -10
                    :dissertation/cid  -9
                    :dissertation/univ "Harvard University"}
                   {:db/id          -11
                    :person/name    "Alfred North Whitehead"
                    :person/advised [-10]}
                   {:db/id            -12
                    :dissertation/cid -11}]))

        rule-author '[[(author ?d ?c)
                       [?d :dissertation/cid ?c]]]
        rule-adv    '[[(adv ?x ?y)
                       [?x :person/advised ?d]
                       (author ?d ?y)]]
        rule-area   '[[(area ?c ?a)
                       [?d :dissertation/cid ?c]
                       [?d :dissertation/area ?a]]]
        rule-univ   '[[(univ ?c ?u)
                       [?d :dissertation/cid ?c]
                       [?d :dissertation/univ ?u]]]
        rule-anc    '[[(anc ?x ?y)
                       (adv ?x ?y)]
                      [(anc ?x ?y)
                       (adv ?x ?z)
                       (anc ?z ?y)]]
        rule-q1     (into rule-author rule-adv)
        rule-q2     (into rule-q1 rule-univ)
        rule-q3     (into rule-q1 rule-area)
        rule-q4     (into rule-q1 rule-anc)]
    (is (= ["Stål Olav Aanderaa"]
           (d/q '[:find [?n ...]
                  :in $ %
                  :where
                  [?d :person/name "David Scott Warren"]
                  (adv ?x ?d)
                  (adv ?y ?x)
                  [?y :person/name ?n]]
                db rule-q1)))
    (is (= #{"Hao Wang" "Stål Olav Aanderaa" "Joyce Barbara Friedman"}
           (set (d/q '[:find [?n ...]
                       :in $ %
                       :where
                       (adv ?x ?y)
                       (univ ?x ?u)
                       (univ ?y ?u)
                       [?y :person/name ?n]]
                     db rule-q2))))
    (is (= ["Joyce Barbara Friedman"]
           (d/q '[:find [?n ...]
                  :in $ %
                  :where
                  (adv ?x ?y)
                  (area ?x ?a1)
                  (area ?y ?a2)
                  [(!= ?a1 ?a2)]
                  [?y :person/name ?n]]
                db rule-q3)))
    (is (= #{"Hao Wang" "Stål Olav Aanderaa" "Joyce Barbara Friedman"
             "Alfred North Whitehead" "Willard Van Orman Quine"}
           (set (d/q '[:find [?n ...]
                       :in $ %
                       :where
                       [?x :person/name "David Scott Warren"]
                       (anc ?y ?x)
                       [?y :person/name ?n]]
                     db rule-q4))))
    (d/close-db db)
    (u/delete-files dir)))

;; Need to extend the Datalog syntax to allow aggregation function in
;; the rule head
#_(deftest single-linear-regression-test
    (let [dir (u/tmp-dir (str "single-regression-test-" (UUID/randomUUID)))
          db  (-> (d/empty-db
                    dir
                    {:time {:db/valueType :db.type/long}
                     :mass {:db/valueType :db.type/long}}
                    {:kv-opts {:flags (conj c/default-env-flags :nosync)}})
                  (d/db-with [{:time 5 :mass 40}
                              {:time 7 :mass 120}
                              {:time 12 :mass 180}
                              {:time 16 :mass 210}
                              {:time 20 :mass 240}]))]
      (is (< 10
             (d/q '[:find ?p .
                    :in $ %
                    :where
                    (linear-regression ?p)]
                  db '[[(xtrain ?id ?v)
                        [?id :time ?v]]
                       [(ytrain ?id ?y)
                        [?id :mass ?y]]
                       [(predict ?t ?id (sum ?y))
                        (model ?t ?p)
                        (xtrain ?id ?v)
                        [(* ?v ?p) ?y]]
                       [(gradient ?t (sum ?g))
                        (ytrain ?id ?y)
                        (predict ?t ?id ?y')
                        (xtrain ?id ?v)
                        [(* 2 (- ?y' ?y) v?) ?g]]
                       [(model ?t ?p)
                        [(ground 0) ?t]
                        [(ground 0.01 ?p)]]
                       [(model ?t+1 ?p')
                        [(inc ?t) ?t+1]
                        (model ?t ?p)
                        (gradient ?t ?g)
                        [(- ?p (/ (* 0.1 ?g) 5)) ?p']
                        [(< ?t 100)]]
                       ])
             13))
      (d/close-db db)
      (u/delete-files dir)))
