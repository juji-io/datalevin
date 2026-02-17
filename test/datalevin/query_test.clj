(ns datalevin.query-test
  (:require
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.test :refer [deftest is are use-fixtures testing]]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.clojure-test :as test]
   [clojure.test.check.properties :as prop]
   [clojure.set :as set]
   [clojure.edn :as edn]
   [clojure.walk :as walk]
   [datalevin.core :as d]
   [datalevin.query :as sut]
   [datalevin.planner :as pl]
   [datalevin.constants :as c]
   [datalevin.interpret :as i]
   [datalevin.parser :as dp]
   [datalevin.util :as u])
  (:import
   [java.util UUID]))

(use-fixtures :each db-fixture)

(deftest single-encla-test
  (let [dir (u/tmp-dir (str "single-encla-test-" (UUID/randomUUID)))
        db  (-> (d/empty-db dir
                            {:name   {:db/unique :db.unique/identity}
                             :friend {:db/valueType :db.type/ref}
                             :aka    {:db/cardinality :db.cardinality/many}
                             :school {:db/valueType :db.type/keyword}}
                            {:kv-opts
                             {:flags (conj c/default-env-flags :nosync)}})
                (d/db-with [{:db/id  1,        :name   "Ivan", :age 15
                             :aka    ["robot" "ai"]
                             :school :ny/union :friend 2}
                            {:db/id 2, :name "Petr", :age 37 :friend 3
                             :aka   ["ai" "pi"]}
                            { :db/id 3, :name "Oleg", :age 37
                             :aka    ["bigmac"]}
                            { :db/id 4, :name "John" :age 15 }]))]
    (is (= #{[1] [2]}
           (d/q '[:find ?e
                  :where
                  [?e :aka ?a1]
                  [?e :aka ?a2]
                  [(not= ?a1 ?a2)]]
                db)))
    (is (= #{} (d/q '[:find ?e
                      :in $
                      :where
                      [?e :name "Ivan"]
                      [?e :age 17]]
                    db)))
    (is (= (:actual-result-size (d/explain {:run? true}
                                           '[:find ?e
                                             :in $
                                             :where
                                             [?e :name "Non-existent"]
                                             [?e :age 15]]
                                           db ))
           0))
    (is (empty? (d/q '[:find ?e
                       :in $
                       :where
                       [?e :name "Non-existent"]
                       [?e :age 2]]
                     db)))
    (is (= (set (d/q '[:find ?a
                       :in $
                       :where
                       [?e :age ?a]
                       [?e :aka "bigmac"]
                       [?e :name "Oleg"]]
                     db ))
           #{[37]}))
    (is (= (:actual-result-size (d/explain {:run? true}
                                           '[:find ?a
                                             :in $
                                             :where
                                             [?e :age ?a]
                                             [?e :aka "bigmac"]
                                             [?e :name "Oleg"]]
                                           db ))
           1))
    (is (= (d/q '[:find ?a
                  :in $ ?n
                  :where
                  [?e :friend]
                  [?e :name ?n]
                  [?e :age ?a]]
                db "Ivan")
           #{[15]}))
    (is (= [1] (d/q '[:find [?e ...]
                      :in $ ?ns-in
                      :where
                      [(namespace ?v) ?ns]
                      [(= ?ns ?ns-in)]
                      [?e :school ?v]]
                    db "ny")))
    (is (= (d/q '[:find ?e ?v
                  :in $ ?e
                  :where [?e :age ?v]]
                db [:name "Ivan"])
           #{[[:name "Ivan"] 15]}))
    (is (= (d/q '[:find ?v
                  :in $ ?e
                  :where [?e :age ?v]]
                db [:name "Ivan"])
           #{[15]}))
    (is (= (d/q '[:find ?v
                  :in $
                  :where [1 :age ?v]]
                db)
           #{[15]}))
    (is (= (d/q '[:find ?v ?s
                  :in $ ?e
                  :where
                  [?e :age ?v]
                  [?e :school ?s]]
                db [:name "Ivan"])
           #{[15 :ny/union]}))
    (is (= #{"robot" "ai" "bigmac" "pi"}
           (set (d/q '[:find [?aname ...]
                       :where
                       [_ :aka ?aname]]
                     db))))
    (is (= (d/q '[:find  ?a ?v
                  :in    $db ?e ?k
                  :where
                  [$db ?e ?a ?v]
                  [$db ?e :aka ?k]]
                db 1 "ai")
           #{[:name "Ivan"]
             [:age 15]
             [:friend 2]
             [:school :ny/union]
             [:aka "robot"]
             [:aka "ai"]}))
    (is (= (:actual-result-size (d/explain {:run? true}
                                           '[:find  ?a ?v
                                             :in    $db ?e ?k
                                             :where
                                             [$db ?e ?a ?v]
                                             [$db ?e :aka ?k]]
                                           db 1 "ai"))
           6))
    (is (= (d/q '[:find ?e
                  :where [?e :aka "ai"]] db)
           #{[1] [2]}))
    (is (= (d/q '[:find ?e
                  :where [?e :name]] db)
           #{[1] [2] [3] [4]}))
    (is (= (d/q '[:find  ?e ?v
                  :where
                  [?e :name "Ivan"]
                  [?e :age ?v]] db)
           #{[1 15]}))
    (is (= (d/q '[:find  ?a1
                  :where
                  [_ :age ?a1]
                  [(>= ?a1 22)]
                  [(odd? ?a1)]] db)
           #{[37]}))
    (is (= (:actual-result-size
            (d/explain {:run? true}
                       '[:find  ?a1
                         :where
                         [_ :age ?a1]
                         [(>= ?a1 22)]
                         [(odd? ?a1)]] db))
           1))
    (is (= (d/q '[:find  ?n ?a
                  :in ?k $
                  :where
                  [?e :aka ?k]
                  [?e :name ?n]
                  [?e :age  ?a]]
                "dragon_saver_94"
                [[1 :name "Ivan"]
                 [1 :age  19]
                 [1 :aka  "dragon_saver_94"]
                 [1 :aka  "-=autobot=-"]])
           #{["Ivan" 19]}))
    (is (nil? (:plan (d/explain {:run? true}
                                '[:find  ?n ?a
                                  :in ?k $
                                  :where
                                  [?e :aka ?k]
                                  [?e :name ?n]
                                  [?e :age  ?a]]
                                "dragon_saver_94"
                                [[1 :name "Ivan"]
                                 [1 :age  19]
                                 [1 :aka  "dragon_saver_94"]
                                 [1 :aka  "-=autobot=-"]]))))
    (is (= #{[3 :age 37] [2 :age 37] [4 :age 15] [1 :school :ny/union]}
           (d/q '[:find ?e ?a ?v
                  :where
                  [?e :name _]
                  [(get-some $ ?e :school :age) [?a ?v]]] db)))
    (is (= (:actual-result-size
            (d/explain {:run? true}
                       '[:find ?e ?a ?v
                         :where
                         [?e :name _]
                         [(get-some $ ?e :school :age) [?a ?v]]] db))
           4))
    (is (= (d/q '[:find  ?e ?a
                  :where
                  [?e :age ?a]
                  [?e :age 15]]
                db)
           #{[1 15] [4 15]}))
    (is (= (d/q '[:find  ?e
                  :in    $ ?adult
                  :where [?e :age ?a]
                  [(?adult ?a)]]
                db #(> ^long % 18))
           #{[2] [3]}))
    (is (= (:actual-result-size (d/explain {:run? true}
                                           '[:find  ?e
                                             :in    $ ?adult
                                             :where [?e :age ?a]
                                             [(?adult ?a)]]
                                           db #(> ^long % 18)))
           2))
    (is (= #{}
           (d/q '[:find ?name
                  :in $ ?my-fn
                  :where
                  [?e :name ?name]
                  [(?my-fn) ?result]
                  [(< ?result 3)]]
                db (fn [] 5))))
    (is (= 0
           (:actual-result-size (d/explain {:run? true} '[:find ?name
                                                          :in $ ?my-fn
                                                          :where
                                                          [?e :name ?name]
                                                          [(?my-fn) ?result]
                                                          [(< ?result 3)]]
                                           db (fn [] 5)))))
    (is (= (set (d/q '[:find ?a
                       :in $ ?n
                       :where
                       [?e :friend ?e1]
                       [?e :name ?n]
                       [?e1 :age ?a]]
                     db "Ivan"))
           #{[37]}))
    (is (= (d/q '[:find  ?e1 ?e2
                  :where
                  [?e1 :name ?n]
                  [?e2 :name ?n]] db)
           #{[1 1] [2 2] [3 3] [4 4] }))
    (is (= (d/q '[:find  ?e ?e2 ?n
                  :in $ ?i
                  :where
                  [?e :name ?i]
                  [?e :age ?a]
                  [?e2 :age ?a]
                  [?e2 :name ?n]] db "Ivan")
           #{[1 1 "Ivan"]
             [1 4 "John"]}))
    (is (= (d/q '[:find ?n
                  :in $ ?i
                  :where
                  [?e :name ?i]
                  [?e :age ?a]
                  [?e2 :age ?a2]
                  [(< ?a ?a2)]
                  [?e2 :name ?n]] db "Ivan")
           #{["Oleg"] ["Petr"]}))
    (is (= (d/q '[:find  ?n1 ?n2
                  :where
                  [?e1 :aka ?x]
                  [?e2 :aka ?x]
                  [?e1 :name ?n1]
                  [?e2 :name ?n2]] db)
           #{["Ivan" "Ivan"]
             ["Petr" "Petr"]
             ["Ivan" "Petr"]
             ["Petr" "Ivan"]
             ["Oleg" "Oleg"]}))
    (d/close-db db)
    (u/delete-files dir)))

(deftest rev-ref-test
  (let [dir (u/tmp-dir (str "rev-ref-test-" (UUID/randomUUID)))
        db  (-> (d/empty-db
                  dir
                  {:user/name      {:db/unique    :db.unique/identity
                                    :db/valueType :db.type/string}
                   :database/name  {:db/unique    :db.unique/identity
                                    :db/valueType :db.type/string}
                   :database/type  {:db/valueType :db.type/keyword}
                   :role/key       {:db/valueType :db.type/keyword
                                    :db/unique    :db.unique/identity}
                   :permission/act {:db/valueType :db.type/keyword}
                   :permission/obj {:db/valueType :db.type/keyword}
                   :permission/tgt {:db/valueType :db.type/ref}
                   :user-role/user {:db/valueType :db.type/ref}
                   :user-role/role {:db/valueType :db.type/ref}
                   :role-perm/role {:db/valueType :db.type/ref}
                   :role-perm/perm {:db/valueType :db.type/ref}}
                  {:kv-opts {:flags (conj c/default-env-flags :nosync)}})
                (d/db-with [{:db/id     -1
                             :user/name "datalevin"}
                            {:db/id    -2
                             :role/key :datalevin.role/datalevin}
                            {:db/id          -3
                             :user-role/user -1
                             :user-role/role -2}
                            {:db/id          -4
                             :permission/act ::control
                             :permission/obj ::server}
                            {:db/id          -5
                             :role-perm/perm -4
                             :role-perm/role -2}]))]
    (is (= (d/q '[:find (pull ?p [:permission/act :permission/obj])
                  :in $ ?uname
                  :where
                  [?u :user/name ?uname]
                  [?ur :user-role/user ?u]
                  [?ur :user-role/role ?r]
                  [?rp :role-perm/role ?r]
                  [?rp :role-perm/perm ?p]]
                db "datalevin")
           [[{:permission/obj :datalevin.query-test/server,
              :permission/act :datalevin.query-test/control}]]))
    (is (= (:actual-result-size
            (d/explain {:run? true}
                       '[:find (pull ?p [:permission/act :permission/obj])
                         :in $ ?uname
                         :where
                         [?u :user/name ?uname]
                         [?ur :user-role/user ?u]
                         [?ur :user-role/role ?r]
                         [?rp :role-perm/role ?r]
                         [?rp :role-perm/perm ?p]]
                       db "datalevin"))
           1))
    (is (= (d/q '[:find (pull ?p [:permission/act :permission/obj])
                  :in $ ?rk
                  :where
                  [?r :role/key ?rk]
                  [?ur :user-role/role ?r]
                  [?rp :role-perm/role ?r]
                  [?rp :role-perm/perm ?p]]
                db :datalevin.role/datalevin)
           [[{:permission/obj :datalevin.query-test/server,
              :permission/act :datalevin.query-test/control}]]))
    (is (= (:actual-result-size
            (d/explain {:run? true}
                       '[:find (pull ?p [:permission/act :permission/obj])
                         :in $ ?rk
                         :where
                         [?r :role/key ?rk]
                         [?ur :user-role/role ?r]
                         [?rp :role-perm/role ?r]
                         [?rp :role-perm/perm ?p]]
                       db :datalevin.role/datalevin))
           1))
    (d/close-db db)
    (u/delete-files dir)))

(deftest multiple-encla-test
  (let [dir (u/tmp-dir (str "multi-encla-test-" (UUID/randomUUID)))
        db  (-> (d/empty-db
                  dir
                  {:person/name   {:db/unique    :db.unique/identity
                                   :db/valueType :db.type/string}
                   :person/friend {:db/valueType   :db.type/ref
                                   :db/cardinality :db.cardinality/many}
                   :person/aka    {:db/cardinality :db.cardinality/many
                                   :db/valueType   :db.type/string}
                   :person/age    {:db/valueType :db.type/long}
                   :person/city   {:db/valueType :db.type/string}
                   :person/hobby  {:db/valueType   :db.type/string
                                   :db/cardinality :db.cardinality/many}
                   :person/school {:db/valueType   :db.type/ref
                                   :db/cardinality :db.cardinality/many}
                   :school/name   {:db/valueType :db.type/string
                                   :db/unique    :db.unique/identity}
                   :school/city   {:db/valueType :db.type/string}}
                  {:kv-opts {:flags (conj c/default-env-flags :nosync)}})
                (d/db-with [{:db/id         1,
                             :person/name   "Ivan",
                             :person/age    15
                             :person/aka    ["robot" "ai"]
                             :person/school 6
                             :person/city   "San Jose"
                             :person/hobby  ["video games" "chess"]
                             :person/friend 2}
                            {:db/id         2,
                             :person/name   "Petr",
                             :person/age    16
                             :person/aka    "fixer"
                             :person/school 7
                             :person/city   "Fremont"
                             :person/hobby  ["video games"]
                             :person/friend [1 3]}
                            {:db/id         3,
                             :person/name   "Oleg",
                             :person/city   "San Jose"
                             :person/age    22
                             :person/hobby  ["video games"]
                             :person/aka    ["bigmac"]
                             :person/friend [1 4 5]}
                            {:db/id         4,
                             :person/name   "John"
                             :person/school 7
                             :person/city   "San Jose"
                             :person/hobby  ["video games" "baseball"]
                             :person/age    15
                             :person/friend 5}
                            {:db/id         5,
                             :person/name   "James"
                             :person/school 7
                             :person/city   "Fremont"
                             :person/hobby  ["baseball"]
                             :person/age    16
                             :person/friend 4}
                            {:db/id       6
                             :school/city "San Jose"
                             :school/name "Leland"}
                            {:db/id       7
                             :school/city "Fremont"
                             :school/name "Mission"}]))]
    (is (= (set (d/q '[:find ?a2
                       :in $
                       :where
                       [?e :person/friend ?e1]
                       [?e :person/name ?n]
                       [(clojure.string/starts-with? ?n "O")]
                       [?e1 :person/friend ?e2]
                       [?e1 :person/name ?n1]
                       [(clojure.string/starts-with? ?n1 "I")]
                       [?e2 :person/age ?a2]
                       ]
                     db))
           #{[16]}))
    (is (= (set (d/q '[:find ?n1 ?n2
                       :in $ ?c
                       :where
                       [?e1 :person/city ?c]
                       [?e :school/city ?c]
                       [?e1 :person/name ?n1]
                       [?e1 :person/age ?a]
                       [?e2 :person/age ?a]
                       [?e2 :person/school ?e]
                       [?e2 :person/name ?n2]
                       [(not= ?n1 ?n2)]]
                     db "Fremont"))
           #{["James" "Petr"] ["Petr" "James"]}))
    (is (= (:actual-result-size (d/explain {:run? true}
                                           '[:find ?n1 ?n2
                                             :in $ ?c
                                             :where
                                             [?e1 :person/city ?c]
                                             [?e :school/city ?c]
                                             [?e1 :person/name ?n1]
                                             [?e1 :person/age ?a]
                                             [?e2 :person/age ?a]
                                             [?e2 :person/school ?e]
                                             [?e2 :person/name ?n2]
                                             [(not= ?n1 ?n2)]]
                                           db "Fremont"))
           2))
    (is (= (set (d/q '[:find ?n1
                       :in $ ?n
                       :where
                       [?e :person/school ?s]
                       [?e :person/name ?n]
                       [?e :person/city ?c1]
                       [?e1 :person/name ?n1]
                       [?e1 :person/school ?s]
                       [?e1 :person/city ?c2]
                       [(not= ?c1 ?c2)]
                       [?e :person/hobby ?h]
                       [?e1 :person/hobby ?h]]
                     db "John"))
           #{["James"] ["Petr"]}))
    (is (= (set (d/q '[:find ?n1
                       :in $ ?n
                       :where
                       [?e :person/school ?s]
                       [?e :person/name ?n]
                       [?e :person/city ?c]
                       [?e :person/hobby ?h]
                       [?e1 :person/name ?n1]
                       [?e1 :person/school ?s]
                       [?e1 :person/city ?c]
                       [?e1 :person/hobby ?h1]
                       [(not= ?h ?h1)]]
                     db "James"))
           #{["Petr"]}))
    (is (= (set (d/q '[:find ?n1
                       :in $ ?n
                       :where
                       [?e :person/school ?s]
                       [?e :person/name ?n]
                       [?e1 :person/school ?s]
                       [?e1 :person/name ?n1]]
                     db "John"))
           #{["James"] ["John"] ["Petr"]}))
    (is (= (set (d/q '[:find ?a
                       :in $ ?n
                       :where
                       [?e :person/friend ?e1]
                       [?e :person/name ?n]
                       [?e1 :person/age ?a]]
                     db "Ivan"))
           #{[16]}))
    (is (= (:actual-result-size (d/explain {:run? true}
                                           '[:find ?a
                                             :in $ ?n
                                             :where
                                             [?e :person/friend ?e1]
                                             [?e :person/name ?n]
                                             [?e1 :person/age ?a]]
                                           db "Ivan"))
           1))
    (is (= (d/q '[:find  ?e1 ?e2
                  :where
                  [?e1 :person/name ?n]
                  [?e2 :person/name ?n]] db)
           #{[1 1] [2 2] [3 3] [4 4] [5 5]}))
    (is (nil? (:actual-result-size (d/explain {}
                                              '[:find  ?e1 ?e2
                                                :where
                                                [?e1 :person/name ?n]
                                                [?e2 :person/name ?n]] db))))
    (is (= (d/q '[:find  ?e ?e2 ?n
                  :in $ ?i
                  :where
                  [?e :person/name ?i]
                  [?e :person/age ?a]
                  [?e2 :person/age ?a]
                  [?e2 :person/name ?n]] db "Ivan")
           #{[1 1 "Ivan"]
             [1 4 "John"]}))
    (is (= (:late-clauses (d/explain {:run? true}
                                     '[:find  ?e ?e2 ?n
                                       :in $ ?i
                                       :where
                                       [?e :person/name ?i]
                                       [?e :person/age ?a]
                                       [?e2 :person/age ?a]
                                       [?e2 :person/name ?n]] db "Ivan"))
           []))
    (is (= (d/q '[:find ?n
                  :in $ ?i
                  :where
                  [?e :person/name ?i]
                  [?e :person/age ?a]
                  [?e2 :person/age ?a2]
                  [(< ?a ?a2)]
                  [?e2 :person/name ?n]] db "Ivan")
           #{["Oleg"] ["Petr"] ["James"]}))
    (is (= (:actual-result-size (d/explain {:run? true}
                                           '[:find ?n
                                             :in $ ?i
                                             :where
                                             [?e :person/name ?i]
                                             [?e :person/age ?a]
                                             [?e2 :person/age ?a2]
                                             [(< ?a ?a2)]
                                             [?e2 :person/name ?n]] db "Ivan"))
           3))
    (d/close-db db)
    (u/delete-files dir)))

(deftest ranges-test
  (let [vs       (vec (range 20))
        vset     (set vs)
        select   #(into #{}
                        (comp (map (fn [[[_ l] [_ r]]] (subvec vs l r)))
                           cat)
                        %)
        to-range #(mapv (fn [[l r]] [[:closed l] [:closed r]]) %)]
    (testing "range combinations"
      (are [intervals]
          (let [ranges (to-range intervals)
                combined (pl/combine-ranges ranges)
                flipped (walk/postwalk
                          #(case %
                             :db.value/sysMax 20
                             :db.value/sysMin 0
                             %)
                          (pl/flip-ranges combined))]
            (= (select ranges)
               (select combined)
               (set/difference vset (select flipped))))
        [[1 4] [2 3] [7 8]]
        [[1 2] [3 4] [5 7]]
        [[2 9] [1 4] [2 5]]
        [[3 7] [1 4] [4 5]]
        [[6 6] [1 4] [2 4]]
        [[1 3] [1 4] [2 7]]
        [[1 5] [3 3] [1 4]]
        [[6 6] [1 4] [4 5]]
        [[1 3] [4 7] [2 4] [3 5]]
        [[1 2] [3 4] [3 5]]
        [[3 5] [2 7] [4 7] [8 9]]
        [[4 5] [1 2] [1 4]]
        [[1 2] [1 2] [4 5] [1 5]]
        [[7 9] [3 4] [1 2] [2 3]]))
    (testing "range insersections"
      (are [vs1 vs2 vs3]
          (let [r1 (to-range vs1)
                r2 (to-range vs2)
                r3 (to-range vs3)]
            (= (set/intersection (select r1) (select r2) (select r3))
               (select (pl/intersect-ranges r1 r2 r3))))
        [[1 6] [7 12]]         [[2 5] [8 11]] [[3 4] [9 10]]
        [[1 6] [7 12] [13 17]] [[2 5] [8 11]] [[3 4] [9 10]]
        [[1 3]]                [[1 4]]        [[2 6]]
        [[1 3]]                [[2 5]]        [[4 6]]
        [[1 2]]                [[2 3]]        [[3 4]]
        ))))

(test/defspec random-ranges-test
  100
  (prop/for-all
    [bases (gen/vector (gen/large-integer* {:min 1 :max 100}) 5)
     offsets (gen/vector (gen/large-integer* {:min 1 :max 50}) 5)
     targets (gen/vector-distinct gen/nat {:num-elements 5})]
    (let [ranges    (mapv (fn [b o] [[:open b] [:open (+ ^long b ^long o)]])
                          bases offsets)
          combined  (pl/combine-ranges ranges)
          in-range? (fn [ranges target]
                      (some (fn [[[_ l] [_ h]]] (<= l target h)) ranges))]
      (is (every? true?
                  (mapv (fn [x y] (= x y))
                        (mapv #(in-range? ranges %) targets)
                        (mapv #(in-range? combined %) targets)))))))

(deftest issue-259-260-test
  (let [dir (u/tmp-dir (str "issue-259-" (UUID/randomUUID)))
        db  (-> (d/empty-db dir
                            {:user/name  {:db/valueType :db.type/string}
                             :user/items {:db/valueType   :db.type/ref
                                          :db/cardinality :db.cardinality/many
                                          :db/isComponent true}
                             :item/name  {:db/valueType :db.type/string}})
                (d/db-with [{:user/name  "joe"
                             :user/items [{:item/name "pen"}]}
                            {:user/name  "larry"
                             :user/items [{:item/name "pen"}]}]))]
    (is (= #{[2]} (:result (d/explain {:run? true} '[:find ?e1
                                                     :where
                                                     [?u1 :user/name "joe"]
                                                     [?u1 :user/items ?e1]
                                                     [?u2 :user/items ?e2]
                                                     [?e1 :item/name ?n]
                                                     [4 :item/name ?n]]
                                      db))))
    (is (= #{[2]} (:result (d/explain {:run? true} '[:find ?e
                                                     :where
                                                     [1 :user/items ?e]
                                                     [?e :item/name "pen"]]
                                      db))))
    (is (= #{[2]} (d/q '[:find ?e
                         :where
                         [?u :user/name "joe"]
                         [?u :user/items ?e]
                         [?e :item/name "pen"]]
                       db)))
    (d/close-db db)
    (u/delete-files dir)))

(deftest extra-bound-var-test
  (let [dir (u/tmp-dir (str "extra-bound-" (UUID/randomUUID)))
        db  (-> (d/empty-db dir {:user/id     {:db/valueType :db.type/long
                                               :db/unique    :db.unique/identity}
                                 :entity/uuid {:db/valueType :db.type/uuid
                                               :db/unique    :db.unique/identity}
                                 :entity/user {:db/valueType :db.type/ref}
                                 :entity/item {:db/valueType :db.type/long}})
                (d/db-with [{:db/id 1 :user/id 1}
                            {:entity/uuid (UUID/randomUUID)
                             :entity/user [:user/id 1] :entity/item 5}
                            {:entity/uuid (UUID/randomUUID)
                             :entity/user [:user/id 1] :entity/item 5}]))]
    (is (= #{[2] [3]} (d/q '[:find ?e
                             :where
                             [?e :entity/user ?u]
                             [?e :entity/item 5]
                             [?u :user/id 1]]
                           db)))
    (d/close-db db)
    (u/delete-files dir)))

(deftest logic-pred-test
  (let [dir (u/tmp-dir (str "and-or-" (UUID/randomUUID)))
        db  (-> (d/empty-db dir {:text {:db/valueType :db.type/string}
                                 :line {:db/valueType :db.type/long}})
                (d/db-with [{:db/id 1 :text "床前明月光" :line 1}
                            {:db/id 2 :text "疑是地上霜" :line 2}
                            {:db/id 3 :text "举头望明月" :line 3}
                            {:db/id 4 :text "低头思故乡" :line 4}]))]
    (is (= #{[3]} (d/q '[:find ?e
                         :where
                         [?e :line ?l]
                         [?e :text ?t]
                         [(or (and (< 1 ?l) (like ?t "%月%"))
                              (and (odd? ?l) (like ?t "%头%")))]]
                       db)))
    (is (= #{[3]} (d/q '[:find ?e
                         :where
                         [?e :line ?l]
                         [?e :text ?t]
                         [(and (< 1 ?l) (like ?t "%月%"))]]
                       db)))
    (is (= #{[3]} (d/q '[:find ?e
                         :where
                         [?e :line ?l]
                         [(and (< 1 ?l) (odd? ?l))]]
                       db)))
    (is (= #{[3]} (d/q '[:find ?e
                         :where
                         [?e :text ?t]
                         [(and (like ?t "%月%")
                               (not-like ?t "%月光%"))]]
                       db)))
    (is (= #{[1][3][4]} (d/q '[:find ?e
                               :where
                               [?e :text ?t]
                               [(or (like ?t "%月%")
                                    (like ?t "%乡%"))]]
                             db)))
    (d/close-db db)
    (u/delete-files dir)))

(deftest issue-263-test
  (let [dir    (u/tmp-dir (str "issue-263-" (UUID/randomUUID)))
        schema {:type
                #:db{:valueType   :db.type/keyword,
                     :cardinality :db.cardinality/one,
                     :aid         3},
                :d/e
                #:db{:valueType   :db.type/ref,
                     :cardinality :db.cardinality/one,
                     :aid         4},
                :c/b
                #:db{:valueType   :db.type/ref,
                     :cardinality :db.cardinality/one,
                     :aid         5},
                :f/e
                #:db{:valueType   :db.type/ref,
                     :cardinality :db.cardinality/one,
                     :aid         6},
                :d/c
                #:db{:valueType   :db.type/ref,
                     :cardinality :db.cardinality/one,
                     :aid         7},
                :a/b
                #:db{:valueType   :db.type/ref,
                     :cardinality :db.cardinality/one,
                     :aid         8},
                :f/x
                #:db{:valueType   :db.type/ref,
                     :cardinality :db.cardinality/one,
                     :aid         9},
                :a/x
                #:db{:valueType   :db.type/ref,
                     :cardinality :db.cardinality/one,
                     :aid         10}}
        conn   (d/get-conn dir schema)]
    (d/transact! conn (edn/read-string (slurp "test/data/dump-maps.txt")))
    (is (= 89282
           (-> (d/entity @conn 89569)
               :a/x
               :db/id)))
    (is (= 17
           (d/q
             '[:find (count ?f) .
               :in $
               :where
               [89569 :a/x ?x]
               [89569 :a/b ?b]
               [?c :c/b ?b]
               [?d :d/c ?c]
               [?d :d/e ?e]
               [?f :f/e ?e]
               [?f :f/x ?x]] @conn)))
    (d/close conn)
    (u/delete-files dir)))

(deftest test-readme
  (let [dir  (u/tmp-dir (str "datalevin-test-readme-" (UUID/randomUUID)))
        conn (d/get-conn dir {:aka  {:db/cardinality :db.cardinality/many}
                              :name {:db/valueType :db.type/string
                                     :db/unique    :db.unique/identity}})]
    (d/transact! conn [{:name "Frege", :db/id -1, :nation "France", :aka ["foo" "fred"]}
                       {:name "Peirce", :db/id -2, :nation "france"}
                       {:name "De Morgan", :db/id -3, :nation "English"}])
    (is (= #{["France"]}
           (d/q '[:find ?nation
                  :in $ ?alias
                  :where
                  [?e :aka ?alias]
                  [?e :nation ?nation]]
                @conn
                "fred")))
    (d/transact! conn [[:db/retract 1 :name "Frege"]])
    (is (= [[{:db/id 1, :aka ["foo" "fred"], :nation "France"}]]
           (d/q '[:find (pull ?e [*])
                  :in $ ?alias
                  :where
                  [?e :aka ?alias]]
                @conn
                "fred")))
    (d/close conn)
    (u/delete-files dir)))

(deftest test-issue-269-273-277
  (let [dir  (u/tmp-dir (str "datalevin-test-269-" (UUID/randomUUID)))
        conn (d/get-conn dir
                         {:transaction/signature
                          {:db/unique      :db.unique/identity
                           :db/valueType   :db.type/string
                           :db/cardinality :db.cardinality/one}
                          :transaction/block-time
                          {:db/valueType   :db.type/long
                           :db/cardinality :db.cardinality/one}}
                         {:validate-data?    true
                          :closed-schema?    true
                          :auto-entity-time? true})]
    ;; TODO need to be more robust on Windows,
    ;; occasionally fail to key-range-list-count for these none existent key
    (when-not (u/windows?)
      (is (nil? (d/q '[:find ?c .
                       :in $ ?e
                       :where [?e :transaction/signature ?c]] @conn 1)))
      (is (nil? (d/q '[:find [?block-time ?signature]
                       :where
                       [?t :transaction/signature ?signature]
                       [?t :transaction/block-time ?block-time]]
                     @conn))))
    (d/transact! conn [{:transaction/signature  "foo"
                        :transaction/block-time 234324324}])
    (is (= [234324324]
           (d/q '[:find [(max ?bt)]
                  :where
                  [?t :transaction/block-time ?bt]]
                @conn)))
    (is (= 1 (d/q '[:find ?t .
                    :where
                    [?t :transaction/block-time _]]
                  @conn)))
    (is (= 1 (d/q '[:find ?t .
                    :where
                    [?t :transaction/signature "foo"]]
                  @conn)))
    (is (= 1 (d/q '[:find ?t .
                    :where
                    [?t :transaction/signature _]]
                  @conn)))
    (is (= #{{:btime 234324324}}
           (d/q '[:find ?bt
                  :keys btime
                  :where
                  [?t :transaction/block-time ?bt]]
                @conn)))
    (is (= #{{"btime" 234324324}}
           (d/q '[:find ?bt
                  :strs btime
                  :where
                  [?t :transaction/block-time ?bt]]
                @conn)))
    (is (= #{{'btime 234324324}}
           (d/q '[:find ?bt
                  :syms btime
                  :where
                  [?t :transaction/block-time ?bt]]
                @conn)))
    (d/close conn)
    (u/delete-files dir)))

(deftest order-by-test
  (let [dir    (u/tmp-dir (str "order-by-" (UUID/randomUUID)))
        schema (i/load-edn "test/data/movie-schema.edn")
        data   (i/load-edn "test/data/movie-data.edn")
        conn   (d/get-conn dir schema)
        q1     '[:find ?name
                 :where
                 [?e :person/name ?name]
                 :order-by [?name :desc]
                 :timeout 1000]
        q2     '[:find ?name
                 :where
                 [?e :person/name ?name]
                 :order-by ?name]
        q3     '[:find ?name ?born
                 :order-by [?born ?name]
                 :where
                 [?e :person/name ?name]
                 [?e :person/born ?born]]
        q4     '[:find ?name ?born
                 :order-by [?name ?born :desc]
                 :where
                 [?e :person/name ?name]
                 [?e :person/born ?born]]]
    (d/transact! conn data)
    (is (= (first (d/q q1 (d/db conn))) ["Veronica Cartwright"]))
    (is (= (first (d/q q2 (d/db conn))) ["Alan Rickman"]))
    (is (= (first (d/q q3 (d/db conn)))
           ["Richard Crenna" #inst "1926-11-30T00:00:00.000-00:00"]))
    (is (= (first (d/q q4 (d/db conn)))
           ["Alan Rickman" #inst "1946-02-21T00:00:00.000-00:00"]))
    (d/close conn)
    (u/delete-files dir)))

(deftest order-by-column-index-test
  (let [dir    (u/tmp-dir (str "order-by-idx-" (UUID/randomUUID)))
        schema (i/load-edn "test/data/movie-schema.edn")
        data   (i/load-edn "test/data/movie-data.edn")
        conn   (d/get-conn dir schema)
        ;; order by column index 0 (name) descending
        q1     '[:find ?name ?born
                 :where
                 [?e :person/name ?name]
                 [?e :person/born ?born]
                 :order-by [0 :desc]]
        ;; order by single column index ascending (default)
        q2     '[:find ?name ?born
                 :where
                 [?e :person/name ?name]
                 [?e :person/born ?born]
                 :order-by 0]
        ;; order by column index 1 (born) then column 0 (name)
        q3     '[:find ?name ?born
                 :order-by [1 0]
                 :where
                 [?e :person/name ?name]
                 [?e :person/born ?born]]
        ;; mixed: order by column 0 asc, then column 1 desc
        q4     '[:find ?name ?born
                 :order-by [0 :asc 1 :desc]
                 :where
                 [?e :person/name ?name]
                 [?e :person/born ?born]]]
    (d/transact! conn data)
    (is (= (first (d/q q1 (d/db conn)))
           ["Veronica Cartwright" #inst "1949-04-20T00:00:00.000-00:00"]))
    (is (= (first (d/q q2 (d/db conn)))
           ["Alan Rickman" #inst "1946-02-21T00:00:00.000-00:00"]))
    (is (= (first (d/q q3 (d/db conn)))
           ["Richard Crenna" #inst "1926-11-30T00:00:00.000-00:00"]))
    (is (= (first (d/q q4 (d/db conn)))
           ["Alan Rickman" #inst "1946-02-21T00:00:00.000-00:00"]))
    (d/close conn)
    (u/delete-files dir)))

(deftest limit-offset-test
  (let [dir    (u/tmp-dir (str "limit-offset-" (UUID/randomUUID)))
        schema (i/load-edn "test/data/movie-schema.edn")
        data   (i/load-edn "test/data/movie-data.edn")
        conn   (d/get-conn dir schema)
        q1     '[:find ?name
                 :where
                 [?e :person/name ?name]
                 :order-by ?name
                 :limit 2]
        q2     '[:find ?name
                 :where
                 [?e :person/name ?name]
                 :order-by ?name
                 :offset 1
                 :limit 1]
        q3     '[:find ?name
                 :where
                 [?e :person/name ?name]
                 :order-by ?name
                 :offset 0
                 :limit 1]]
    (d/transact! conn data)
    (is (= (d/q q1 (d/db conn)) [["Alan Rickman"] ["Alexander Godunov"]]))
    (is (= (d/q q2 (d/db conn)) [["Alexander Godunov"]]))
    (is (= (d/q q3 (d/db conn)) [["Alan Rickman"]]))
    (d/close conn)
    (u/delete-files dir)))

(deftest test-issue-288-cache-after-resolving-function-references
  ;; function needs to be qualified
  #_{:clj-kondo/ignore [:inline-def]}
  (defn big-name? [x] (> (count x) 16))
  #_{:clj-kondo/ignore [:inline-def]}
  (defn count-letters [[x]] (count x))
  (let [dir    (u/tmp-dir (str "issue-288-" (UUID/randomUUID)))
        schema (i/load-edn "test/data/movie-schema.edn")
        data   (i/load-edn "test/data/movie-data.edn")
        conn   (d/get-conn dir schema)
        q1     '[:find ?name (datalevin.query-test/count-letters ?name)
                 :where
                 [?e :person/name ?name]
                 [(datalevin.query-test/big-name? ?name)]
                 :order-by ?name
                 :limit 2]]
    (d/transact! conn data)
    (is (= (d/q q1 (d/db conn))
           [["Alexander Godunov" 17] ["Arnold Schwarzenegger" 21]]))
    (with-redefs [big-name?     (fn [x] (> (count x) 20))
                  count-letters (fn [[x]] (count (remove #{\space} x)))]
      (is (= (d/q q1 (d/db conn))
             [["Arnold Schwarzenegger" 20]])))
    (d/close conn)
    (u/delete-files dir)))

(deftest issue-284-test
  (let [dir    (u/tmp-dir (str "issue-284-" (UUID/randomUUID)))
        schema {:follows {:db/valueType   :db.type/ref
                          :db/cardinality :db.cardinality/many}
                :name    {:db/valueType :db.type/string
                          :db/unique    :db.unique/identity}}
        conn   (d/get-conn dir schema)
        data   [{:db/id   1 :name "P1",
                 :follows [{:db/id 2} {:db/id 3} {:db/id 4}]}
                {:db/id 2 :name "P2" :follows [{:db/id 10}]}
                {:db/id 3 :name "P3"}
                {:db/id 4 :name "P4"}
                {:db/id 10 :name "P10"}]]
    (d/transact! conn data)
    (is (= (d/q '[:find ?n
                  :where
                  [?p1 :follows ?p2]
                  [?p2 :follows ?p3]
                  [?p3 :name ?n]]
                (d/db conn))
           #{["P10"]}))
    (is (= (d/q '[:find ?p3
                  :where
                  [?p1 :follows ?p2]
                  [?p2 :follows ?p3]]
                (d/db conn))
           #{[10]}))
    (is (= (d/q '[:find ?p1 ?p2 ?p3
                  :where
                  [?p1 :name "P1"]
                  [?p1 :follows ?p2]
                  [?p2 :name "P2"]
                  [?p1 :follows ?p3]
                  [?p3 :name "P3"]]
                (d/db conn))
           #{[1 2 3]}))
    (is (= (d/q '[:find ?p1 ?p2 ?p3 ?p10
                  :where
                  [?p1 :name "P1"]
                  [?p1 :follows ?p2]
                  [?p1 :follows ?p3]
                  [?p2 :name "P2"]
                  [?p2 :follows ?p10]
                  [?p3 :name "P3"]]
                (d/db conn))
           #{[1 2 3 10]}))
    (is (= (d/q '[:find ?p1 ?p2 ?p3 ?p10
                  :where
                  [?p1 :name ?n1]
                  [?p1 :follows ?p2]
                  [?p1 :follows ?p3]
                  [?p2 :name "P2"]
                  [?p2 :follows ?p10]
                  [?p3 :name "P3"]]
                (d/db conn))
           #{[1 2 3 10]}))
    (is (= (d/q '[:find ?p1 ?p2 ?p3 ?p10
                  :where
                  [?p1 :name ?n1]
                  [?p1 :follows ?p2]
                  [?p1 :follows ?p3]
                  [?p2 :name ?n2]
                  [?p2 :follows ?p10]
                  [?p3 :name "P3"]]
                (d/db conn))
           #{[1 2 3 10]}))
    (d/close conn)
    (u/delete-files dir)))

(deftest issue-295-test
  (let [dir    (u/tmp-dir (str "issue-295-" (UUID/randomUUID)))
        schema {:release/artists {:db/valueType   :db.type/ref
                                  :db/cardinality :db.cardinality/many}}
        conn   (d/get-conn dir schema)
        data   [{:db/id           -1
                 :release/name    "foo"
                 :release/artists [{:artist/name    "bar"
                                    :artist/country :country/CA}]}]]
    (d/transact! conn data)
    (is (= (d/q '[:find ?release
                  :where
                  [?release :release/name]
                  (or-join [?release]
                           (and [?release :release/artists ?artist]
                                [?artist :artist/name ?n]
                                [(clojure.string/includes? ?n "bar")])
                           [?release :release/year 1970])]
                (d/db conn))
           #{[1]}))
    (d/close conn)
    (u/delete-files dir)))

(deftest issue-296-test
  (let [dir    (u/tmp-dir (str "issue-296-" (UUID/randomUUID)))
        schema {:question/answers {:db/cardinality :db.cardinality/many
                                   :db/isComponent true
                                   :db/valueType   :db.type/ref}
                :question/title   {:db/cardinality :db.cardinality/one
                                   :db/valueType   :db.type/string}
                :answer/value     {:db/valueType   :db.type/string
                                   :db/cardinality :db.cardinality/one}
                :answer/category  {:db/valueType   :db.type/string
                                   :db/cardinality :db.cardinality/one}}
        conn   (d/get-conn dir schema)
        data   [{:question/answers
                 [{:answer/value    "1"
                   :answer/category "strongly agree"}
                  {:answer/value    "2"
                   :answer/category "agree"}
                  {:answer/value    "3"
                   :answer/category "disagree"}
                  {:answer/value    "4"
                   :answer/category "strongly disagree"}]
                 :question/title "Datalog is awesome"}]]
    (d/transact! conn data)
    (is (= (d/q '[:find ?t ?av ?ac
                  :where
                  [?e :question/title ?t]
                  [?e :question/answers ?a]
                  [?a :answer/value ?av]
                  [?a :answer/category ?ac]]
                (d/db conn))
           #{["Datalog is awesome" "1" "strongly agree"]
             ["Datalog is awesome" "3" "disagree"]
             ["Datalog is awesome" "2" "agree"]
             ["Datalog is awesome" "4" "strongly disagree"]}))
    (d/close conn)
    (u/delete-files dir)))

(deftest or-join-bounded-values-test
  (testing "Or-join uses bounded values to limit target-side processing (SIP-like)"
    ;; Or-join extracts unique bounded values from input tuples and passes them
    ;; to constrain pattern lookups inside branches. This is a form of SIP.
    ;; When the bounded entity count is small, lookup-pattern-multi-entity
    ;; does point lookups instead of full table scans.
    (let [dir       (u/tmp-dir (str "or-join-sip-" (UUID/randomUUID)))
          schema    {:item/special {:db/valueType :db.type/boolean}
                     :item/value   {:db/valueType :db.type/long}}
          conn      (d/get-conn dir schema)
          ;; Create dataset:
          ;; - 100 "special" items (IDs 1-100) with :item/special true
          ;; - 10,000 "other" items (IDs 101-10100) with :item/special false
          ;; - All items have :item/value
          n-special (long 100)
          n-others  (long 10000)
          tx-data   (concat
                      ;; Special items
                      (for [^long i (range n-special)]
                        {:db/id        (- (inc i))
                         :item/special true
                         :item/value   (inc i)})
                      ;; Other items
                      (for [^long i (range n-others)]
                        {:db/id        (- (+ n-special 1 i))
                         :item/special false
                         :item/value   (+ n-special 1 i)}))]
      (d/transact! conn (vec tx-data))

      (let [db         (d/db conn)
            ;; Counter to track how many tuples are processed inside or-join
            counter    (atom 0)
            count-pred (fn [_] (swap! counter inc) true)
            ;; Query: Find special items and their values via or-join
            ;; The or-join branch should only process bounded entities (100),
            ;; not all entities with :item/value (10,100)
            query      '[:find ?e ?v
                         :in $ ?count-pred
                         :where
                         [?e :item/special true]
                         (or-join [?e ?v]
                                  (and [?e :item/value ?v]
                                       [(?count-pred ?v)]))]]

        ;; Execute query and measure tuple processing
        (reset! counter 0)
        (let [result        (d/q query db count-pred)
              process-count @counter]
          ;; Verify correctness
          (is (= (count result) n-special)
              "Should return all special items")

          ;; Verify effectiveness: counter should be close to n-special (100),
          ;; not n-special + n-others (10,100)
          ;; With SIP working: ~100 tuples processed (only special items)
          ;; Without SIP: ~10,100 tuples processed (all items scanned)
          (is (<= process-count (* 2 n-special))
              (str "Or-join SIP should limit processing to bounded entities. "
                   "Expected ~" n-special ", got " process-count
                   " (without SIP would be ~" (+ n-special n-others) ")"))))

      (d/close conn)
      (u/delete-files dir))))

(deftest rewrite-unused-vars-test
  (let [dir (u/tmp-dir (str "rewrite-unused-vars-" (UUID/randomUUID)))
        db  (-> (d/empty-db dir)
                (d/db-with [{:db/id 1 :name "Ivan" :age 15 :aka "iv"}
                            {:db/id 2 :name "Petr" :age 37 :aka "pi"}
                            {:db/id 3 :name "Oleg" :age 37}]))]
    (testing "unused pattern vars are rewritten to placeholders"
      (let [query        '[:find ?e
                           :where
                           [?e :name "Ivan"]
                           [?e :aka ?unused]]
            parsed       (dp/parse-query query)
            replacements (pl/unused-var-replacements parsed)
            placeholder  (get replacements '?unused)]
        (is (contains? replacements '?unused))
        (is (clojure.string/starts-with? (name placeholder)
                                         "?placeholder__unused"))
        (is (not (.contains (str (:opt-clauses (d/explain {:run? false} query db)))
                            "?unused")))
        (is (= (d/q query db)
               (d/q '[:find ?e
                      :where
                      [?e :name "Ivan"]
                      [?e :aka _]] db)))))

    (testing "unused function binding vars are rewritten to _"
      (let [query        '[:find ?e
                           :where
                           [?e :age ?a]
                           [(+ ?a 1) ?unused]]
            parsed       (dp/parse-query query)
            replacements (pl/unused-var-replacements parsed)]
        (is (= '_ (get replacements '?unused)))
        (is (not (.contains (str (:late-clauses (d/explain {:run? false} query db)))
                            "?unused")))
        (is (= (d/q query db)
               (d/q '[:find ?e
                      :where
                      [?e :age ?a]
                      [(+ ?a 1) _]] db)))))

    (d/close-db db)
    (u/delete-files dir)))

(deftest issue-304-305-test
  (let [dir  (u/tmp-dir (str "issue-304-" (UUID/randomUUID)))
        conn (d/get-conn dir {})]
    (d/transact! conn [{:db/id      -1
                        :node/title "test"
                        :block/uid  "test"}])
    (is (= (d/q '[:find ?node
                  :where
                  (or-join [?node]
                           (and
                             [?Page :node/title "OpposedBy"]
                             [?node :block/uid ?target-uid]))]
                (d/db conn))
           #{}))
    (is (= (d/q '[:find ?node
                  :where
                  (or-join [?node]
                           (and
                             [?node :node/title ?ParentPage-Title]
                             [(re-pattern "^(.*?)$") ?regex]
                             [(re-find ?regex ?ParentPage-Title)])
                           [?node :block/uid ?node-uid])]
                (d/db conn))
           #{[1]}))
    (d/close conn)
    (u/delete-files dir)))

(deftest issue-319-test
  (let [dir  (u/tmp-dir (str "issue-320-" (UUID/randomUUID)))
        conn (d/get-conn dir {:block/uid  {:db/unique :db.unique/identity}
                              :node/title {:db/unique :db.unique/identity}
                              :block/refs {:db/valueType   :db.type/ref
                                           :db/cardinality :db.cardinality/many}})]
    (d/transact! conn [{:db/id -1 :block/uid "test"}
                       {:db/id -2 :block/refs -1}])
    (is (= (d/q '[:find ?SBlock
                  :where
                  [?ParentPage :block/uid "test"]
                  [?SPage :node/title "Opposed By"]
                  [?ParentPage :block/uid ?some-uid]
                  [?SBlock :block/refs ?SPage]]
                @conn)
           #{}))
    (is (= (d/q '[:find ?SPage
                  :where
                  [?ParentPage :block/uid "test"]
                  [?SPage :node/title "Opposed By"]
                  [?ParentPage :block/uid ?some-uid]
                  ]
                @conn)
           #{}))
    (d/close conn)
    (u/delete-files dir)))

(deftest issue-320-test
  (let [dir  (u/tmp-dir (str "issue-320-" (UUID/randomUUID)))
        conn (d/get-conn dir)]
    (d/transact! conn [{:tt 0 :pa "X"}
                       {:td 0 :pa "X"}])
    (is (= (d/q '[:find ?t ?d
                  :where
                  [?t :tt]
                  [?t :pa ?p]
                  [?d :td]
                  [?d :pa ?p]]
                (d/db conn))
           #{[1 2]}))
    (d/close conn)
    (u/delete-files dir)))

(deftest issue-341-test
  (let [dir  (u/tmp-dir (str "issue-341-" (UUID/randomUUID)))
        conn (d/get-conn dir)]
    (d/transact! conn [{:foo 1}
                       {:bar 2}
                       {:foo 3 :bar 4}])
    (is (= (d/q '[:find ?x ?y
                  :where
                  [_ :foo ?x]
                  [_ :bar ?y]]
                (d/db conn))
           #{[3 4] [1 4] [1 2] [3 2]}))
    (d/close conn)
    (u/delete-files dir)))

(deftest test-fn-clause-unification
  ;; When a variable is bound by multiple function clauses, the values must unify
  (testing "conflicting function bindings should return empty result"
    (is (= (d/q '[:find ?f
                  :where
                  [(ground "different") ?e]
                  [(str "know " ?e) ?f]
                  [(str "something completely " ?e) ?f]])
           #{})))
  (testing "matching function bindings should return the value"
    (is (= (d/q '[:find ?f
                  :where
                  [(ground "world") ?e]
                  [(str "hello " ?e) ?f]
                  [(str "hello " ?e) ?f]])
           #{["hello world"]})))
  (testing "single function binding works normally"
    (is (= (d/q '[:find ?f
                  :where
                  [(ground "test") ?e]
                  [(str "hello " ?e) ?f]])
           #{["hello test"]}))))

(deftest sip-hash-join-test
  (testing "SIP (Sideways Information Passing) optimization for hash join with :_ref"
    (let [dir    (u/tmp-dir (str "sip-test-" (UUID/randomUUID)))
          schema {:person/friend  {:db/valueType   :db.type/ref
                                   :db/cardinality :db.cardinality/many}
                  :person/name    {:db/valueType :db.type/string}
                  :person/special {:db/valueType :db.type/boolean}}
          conn   (d/get-conn dir schema)
          ;; Create a dataset for meaningful SIP testing:
          ;; - Small set of "special" people (input side: 100 entities)
          ;; - Large set of friend references, most pointing to NON-special
          ;; - SIP should filter out non-special references
          n-special     (long 100)
          n-others      (long 10000)
          n-referencers (long 10000)
          ;; Generate data
          tx-data       (concat
                          ;; Special people (entity IDs will be 1-100)
                          (for [^long i (range n-special)]
                            {:db/id          (- (inc i))
                             :person/name    (str "Special-" i)
                             :person/special true})
                          ;; Other people (entity IDs 101-10100)
                          (for [^long i (range n-others)]
                            {:db/id       (- (+ n-special 1 i))
                             :person/name (str "Other-" i)})
                          ;; Referencers: each references 3 special + 10 others
                          ;; This creates many friend refs to non-special that SIP can filter
                          (for [^long i (range n-referencers)]
                            {:db/id         (- (+ n-special n-others 1 i))
                             :person/name   (str "Referrer-" i)
                             :person/friend (vec (concat
                                                   ;; 3 refs to special people
                                                   (for [^long j (range 3)]
                                                     (- (inc (long (mod (+ i j) n-special)))))
                                                   ;; 10 refs to other (non-special) people
                                                   (for [^long j (range 10)]
                                                     (- (+ n-special 1 (long (mod (+ i j) n-others)))))))}))]
      ;; Insert data
      (d/transact! conn (vec tx-data))

      (let [db    (d/db conn)
            ;; Query with MergeScanStep SIP path
            query '[:find ?referrer-name
                    :where
                    [?f :person/special true]
                    [?p :person/friend ?f]
                    [?p :person/name ?referrer-name]]]

        ;; Test with SIP enabled - verify hash join with SIP is selected
        (binding [c/hash-join-min-input-size 10
                  c/magic-cost-hash-join     0.1
                  c/sip-ratio-threshold      0
                  sut/*cache?*               false]
          (let [explain (d/explain {:run? true} query db)
                plan-str (str (:plan explain))]
            (is (.contains plan-str "with SIP")
                (str "Expected hash join with SIP in plan: " plan-str)))
          ;; Verify correctness
          (let [result (d/q query db)]
            (is (= (count result) n-referencers)
                "SIP should produce correct result count")))

        ;; Test without SIP - verify same results
        (binding [c/hash-join-min-input-size 10
                  c/magic-cost-hash-join     0.1
                  c/sip-ratio-threshold      1000000
                  sut/*cache?*               false]
          (let [explain (d/explain {:run? true} query db)
                plan-str (str (:plan explain))]
            (is (not (.contains plan-str "with SIP"))
                "SIP should be disabled"))
          (let [result (d/q query db)]
            (is (= (count result) n-referencers)
                "Without SIP should produce same result count"))))

      (d/close conn)
      (u/delete-files dir)))

  (testing "SIP optimization for hash join - InitStep path"
    ;; This tests the case where SIP modifies the InitStep directly
    ;; (when join-attr == init-attr). This happens when the target entity's
    ;; base plan has InitStep scanning the same attribute as the join.
    ;; We achieve this by adding a range constraint on the join attribute.
    (let [dir    (u/tmp-dir (str "sip-init-test-" (UUID/randomUUID)))
          schema {:person/friend  {:db/valueType   :db.type/ref
                                   :db/cardinality :db.cardinality/many}
                  :person/special {:db/valueType :db.type/boolean}}
          conn   (d/get-conn dir schema)
          ;; Create dataset:
          ;; - Small set of "special" people (100 entities, IDs 1-100)
          ;; - Large set of non-special people (10000 entities, IDs 101-10100)
          ;; - Referencers with friend references
          n-special     (long 100)
          n-others      (long 10000)
          n-referencers (long 10000)
          tx-data       (concat
                          ;; Special people (entity IDs 1-100)
                          (for [^long i (range n-special)]
                            {:db/id          (- (inc i))
                             :person/special true})
                          ;; Other people (entity IDs 101-10100) - non-special
                          (for [^long i (range n-others)]
                            {:db/id          (- (+ n-special 1 i))
                             :person/special false})
                          ;; Referencers: each references 3 special + 10 others
                          (for [^long i (range n-referencers)]
                            {:db/id         (- (+ n-special n-others 1 i))
                             :person/friend (vec (concat
                                                   ;; 3 refs to special people
                                                   (for [^long j (range 3)]
                                                     (- (inc (long (mod (+ i j) n-special)))))
                                                   ;; 10 refs to other people
                                                   (for [^long j (range 10)]
                                                     (- (+ n-special 1 (long (mod (+ i j) n-others)))))))}))]
      (d/transact! conn (vec tx-data))

      (let [db         (d/db conn)
            ;; Counter to track target side tuple processing
            counter    (atom 0)
            count-pred (fn [_] (swap! counter inc) true)
            ;; Query where join-attr equals init-attr (InitStep SIP path)
            ;; The predicate on ?g counts how many tuples are processed on target side
            query      '[:find ?p
                         :in $ ?count-pred
                         :where
                         [?f :person/special true]
                         [?p :person/friend ?g]
                         [(?count-pred ?g)]
                         [?p :person/friend ?f]]]

        ;; Test with SIP enabled - should process fewer tuples on target side
        (reset! counter 0)
        (binding [c/hash-join-min-input-size 10
                  c/magic-cost-hash-join     0.1
                  c/sip-ratio-threshold      0
                  sut/*cache?*               false]
          (let [explain     (d/explain {:run? true} query db count-pred)
                plan-str    (str (:plan explain))
                sip-count   @counter
                result      (d/q query db count-pred)]
            (is (.contains plan-str "with SIP")
                (str "Expected hash join with SIP in plan: " plan-str))
            (is (= (count result) n-referencers)
                "SIP InitStep path should produce correct result count")

            ;; Test without SIP - should process more tuples on target side
            (reset! counter 0)
            (binding [c/sip-ratio-threshold 1000000]
              (let [no-sip-explain (d/explain {:run? true} query db count-pred)
                    no-sip-plan    (str (:plan no-sip-explain))
                    no-sip-count   @counter
                    no-sip-result  (d/q query db count-pred)]
                (is (not (.contains no-sip-plan "with SIP"))
                    "SIP should be disabled")
                (is (= (count no-sip-result) n-referencers)
                    "Without SIP should produce same result count")
                ;; Key effectiveness test: SIP should reduce target side tuple count
                ;; Without SIP: processes all 130,000 friend refs (10K × 13)
                ;; With SIP: processes only refs to special people (~30,000)
                (is (< sip-count no-sip-count)
                    (str "SIP should reduce target tuples: with SIP=" sip-count
                         ", without SIP=" no-sip-count)))))))

      (d/close conn)
      (u/delete-files dir))))
