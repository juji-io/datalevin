(ns datalevin.query-test
  (:require
   [datalevin.query :as sut]
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.test :refer [deftest testing is use-fixtures]]
   [datalevin.core :as d]
   [datalevin.constants :as c]
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
    (is (= (set (d/q '[:find ?a
                       :in $ ?n
                       :where
                       [?e :friend]
                       [?e :name ?n]
                       [?e :age ?a]]
                     db "Ivan"))
           #{[15]}))
    (is (= [1] (d/q '[:find [?e ...]
                      :in $ ?ns-in
                      :where
                      [(namespace ?v) ?ns]
                      [(= ?ns ?ns-in)]
                      [?e :school ?v]]
                    db "ny")))
    (is (= (set (d/q '[:find ?e ?v
                       :in $ ?e
                       :where [?e :age ?v]]
                     db [:name "Ivan"]))
           #{[[:name "Ivan"] 15]}))
    (is (= (set (d/q '[:find ?v
                       :in $ ?e
                       :where [?e :age ?v]]
                     db [:name "Ivan"]))
           #{[15]}))
    (is (= (set (d/q '[:find ?v ?s
                       :in $ ?e
                       :where
                       [?e :age ?v]
                       [?e :school ?s]]
                     db [:name "Ivan"]))
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
    (is (= #{[3 :age 37] [2 :age 37] [4 :age 15] [1 :school :ny/union]}
           (d/q '[:find ?e ?a ?v
                  :where
                  [?e :name _]
                  [(get-some $ ?e :school :age) [?a ?v]]] db)))
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
    (is (= #{}
           (d/q '[:find ?name
                  :in $ ?my-fn
                  :where
                  [?e :name ?name]
                  [(?my-fn) ?result]
                  [(< ?result 3)]]
                db (fn [] 5))))

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
    (d/close-db db)
    (u/delete-files dir)))

#_(deftest multiple-encla-test
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
                               :person/school "Leland"
                               :person/city   "San Jose"
                               :person/hobby  ["video games" "chess"]
                               :person/friend 2}
                              {:db/id         2,
                               :person/name   "Petr",
                               :person/age    16
                               :person/aka    "fixer"
                               :person/school "Mission"
                               :person/city   "Fremont"
                               :person/hobby  ["video games"]
                               :person/friend [1 3]}
                              {:db/id        3,
                               :person/name  "Oleg",
                               :person/city  "San Jose"
                               :person/age   22
                               :person/hobby ["video games"]
                               :person/aka   ["bigmac"]}
                              {:db/id         4,
                               :person/name   "John"
                               :person/school "Mission"
                               :person/city   "Fremont"
                               :person/hobby  ["video games" "baseball"]
                               :person/age    15}]))]
      ;; (is (= (set (d/q '[:find ?a
      ;;                    :in $ ?n
      ;;                    :where
      ;;                    [?e :friend ?e1]
      ;;                    [?e :name ?n]
      ;;                    [?e1 :age ?a]]
      ;;                   db "Ivan"))
      ;;        #{[37]}))
      ;; (is (= (d/q '[:find  ?e1 ?e2
      ;;               :where
      ;;               [?e1 :name ?n]
      ;;               [?e2 :name ?n]] db)
      ;;        #{[1 1] [2 2] [3 3] [4 4] }))
      ;; (is (= (d/q '[:find  ?e ?e2 ?n
      ;;               :in $ ?i
      ;;               :where
      ;;               [?e :name ?i]
      ;;               [?e :age ?a]
      ;;               [?e2 :age ?a]
      ;;               [?e2 :name ?n]] db "Ivan")
      ;;        #{[1 1 "Ivan"]
      ;;          [1 4 "John"]}))
      ;; (is (= (d/q '[:find ?n
      ;;               :in $ ?i
      ;;               :where
      ;;               [?e :name ?i]
      ;;               [?e :age ?a]
      ;;               [?e2 :age ?a2]
      ;;               [(< ?a ?a2)]
      ;;               [?e2 :name ?n]] db "Ivan")
      ;;        #{["Oleg"] ["Petr"]}))
      (d/close-db db)
      (u/delete-files dir)))
