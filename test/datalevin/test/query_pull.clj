(ns datalevin.test.query-pull
  (:require
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.test :refer [deftest testing are is use-fixtures]]
   [datalevin.core :as d]
   [datalevin.util :as u])
  (:import [java.util UUID]))

(use-fixtures :each db-fixture)

(deftest test-basics
  (let [dir     (u/tmp-dir (str "pull-test-" (UUID/randomUUID)))
        test-db (d/db-with (d/empty-db dir)
                           [{:db/id 1 :name "Petr" :age 44}
                            {:db/id 2 :name "Ivan" :age 25}
                            {:db/id 3 :name "Oleg" :age 11}])]
    (are [find res] (= (set (d/q {:find find
                                  :where '[[?e :age ?a]
                                           [(>= ?a 18)]]}
                                 test-db))
                       res)
      '[(pull ?e [:name])]
      #{[{:name "Ivan"}] [{:name "Petr"}]}

      '[(pull ?e [*])]
      #{[{:db/id 2 :age 25 :name "Ivan"}] [{:db/id 1 :age 44 :name "Petr"}]}

      '[?e (pull ?e [:name])]
      #{[2 {:name "Ivan"}] [1 {:name "Petr"}]}

      '[?e ?a (pull ?e [:name])]
      #{[2 25 {:name "Ivan"}] [1 44 {:name "Petr"}]}

      '[?e (pull ?e [:name]) ?a]
      #{[2 {:name "Ivan"} 25] [1 {:name "Petr"} 44]})
    (d/close-db test-db)
    (u/delete-files dir)))

(deftest test-var-pattern
  (let [dir     (u/tmp-dir (str "pull-test-" (UUID/randomUUID)))
        test-db (d/db-with (d/empty-db dir)
                           [{:db/id 1 :name "Petr" :age 44}
                            {:db/id 2 :name "Ivan" :age 25}
                            {:db/id 3 :name "Oleg" :age 11}])]
    (are [find pattern res] (= (set (d/q {:find find
                                          :in   '[$ ?pattern]
                                          :where '[[?e :age ?a]
                                                   [(>= ?a 18)]]}
                                         test-db pattern))
                               res)
      '[(pull ?e ?pattern)] [:name]
      #{[{:name "Ivan"}] [{:name "Petr"}]}

      '[?e ?a ?pattern (pull ?e ?pattern)] [:name]
      #{[2 25 [:name] {:name "Ivan"}] [1 44 [:name] {:name "Petr"}]})
    (d/close-db test-db)
    (u/delete-files dir)))

(deftest test-multiple-sources
  (let [dir1 (u/tmp-dir (str "pull-test-" (UUID/randomUUID)))
        dir2 (u/tmp-dir (str "pull-test-" (UUID/randomUUID)))
        db1  (d/db-with (d/empty-db dir1) [{:db/id 1 :name "Ivan" :age 25}])
        db2  (d/db-with (d/empty-db dir2) [{:db/id 1 :name "Petr" :age 25}])]
    (is (= (set (d/q '[:find ?e (pull $1 ?e [:name])
                       :in $1 $2
                       :where [$1 ?e :age 25]]
                     db1 db2))
           #{[1 {:name "Ivan"}]}))

    (is (= (set (d/q '[:find ?e (pull $2 ?e [:name])
                       :in $1 $2
                       :where [$2 ?e :age 25]]
                     db1 db2))
           #{[1 {:name "Petr"}]}))

    (testing "$ is default source"
      (is (= (set (d/q '[:find ?e (pull ?e [:name])
                         :in $1 $
                         :where [$ ?e :age 25]]
                       db1 db2))
             #{[1 {:name "Petr"}]})))
    (d/close-db db1)
    (d/close-db db2)
    (u/delete-files dir1)
    (u/delete-files dir2)))

(deftest test-find-spec
  (let [dir     (u/tmp-dir (str "pull-test-" (UUID/randomUUID)))
        test-db (d/db-with (d/empty-db dir)
                           [{:db/id 1 :name "Petr" :age 44}
                            {:db/id 2 :name "Ivan" :age 25}
                            {:db/id 3 :name "Oleg" :age 11}])]
    (is (= (d/q '[:find (pull ?e [:name]) .
                  :where [?e :age 25]]
                test-db)
           {:name "Ivan"}))
    (is (= (set (d/q '[:find [(pull ?e [:name]) ...]
                       :where [?e :age ?a]]
                     test-db))
           #{{:name "Ivan"} {:name "Petr"} {:name "Oleg"}}))

    (is (= (set (d/q '[:find [(pull ?e [*]) ...]
                       :where [?e :age ?a]]
                     test-db))
           #{{:db/id 3, :name "Oleg", :age 11} {:db/id 2, :name "Ivan", :age 25}
             {:db/id 1, :name "Petr", :age 44}}))

    (is (= (d/q '[:find [?e (pull ?e [:name])]
                  :where [?e :age 25]]
                test-db)
           [2 {:name "Ivan"}]))
    (d/close-db test-db)
    (u/delete-files dir)))

(deftest test-find-spec-input
  (let [dir     (u/tmp-dir (str "pull-test-" (UUID/randomUUID)))
        test-db (d/db-with (d/empty-db dir)
                           [{:db/id 1 :name "Petr" :age 44}
                            {:db/id 2 :name "Ivan" :age 25}
                            {:db/id 3 :name "Oleg" :age 11}])]
    (is (= (d/q '[:find (pull ?e ?p) .
                  :in $ ?p
                  :where [(ground 2) ?e]]
                test-db [:name])
           {:name "Ivan"}))
    (is (= (d/q '[:find (pull ?e p) .
                  :in $ p
                  :where [(ground 2) ?e]]
                test-db [:name])
           {:name "Ivan"}))
    (d/close-db test-db)
    (u/delete-files dir)))

(deftest test-aggregates
  (let [dir (u/tmp-dir (str "pull-test-" (UUID/randomUUID)))
        db  (d/db-with (d/empty-db
                         dir
                         {:value {:db/cardinality :db.cardinality/many}})
                       [{:db/id 1 :name "Petr" :value [10 20 30 40]}
                        {:db/id 2 :name "Ivan" :value [14 16]}
                        {:db/id 3 :name "Oleg" :value 1}])]
    (is (= (set (d/q '[:find ?e (pull ?e [:name]) (min ?v) (max ?v)
                       :where [?e :value ?v]]
                     db))
           #{[1 {:name "Petr"} 10 40]
             [2 {:name "Ivan"} 14 16]
             [3 {:name "Oleg"} 1 1]}))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-lookup-refs
  (let [dir (u/tmp-dir (str "pull-test-" (UUID/randomUUID)))
        db  (d/db-with (d/empty-db dir {:name { :db/unique :db.unique/identity }})
                       [{:db/id 1 :name "Petr" :age 44}
                        {:db/id 2 :name "Ivan" :age 25}
                        {:db/id 3 :name "Oleg" :age 11}])]
    (is (= (set (d/q '[:find ?ref ?a (pull ?ref [:db/id :name])
                       :in   $ [?ref ...]
                       :where [?ref :age ?a]
                       [(>= ?a 18)]]
                     db [[:name "Ivan"] [:name "Oleg"] [:name "Petr"]]))
           #{[[:name "Petr"] 44 {:db/id 1 :name "Petr"}]
             [[:name "Ivan"] 25 {:db/id 2 :name "Ivan"}]}))
    (d/close-db db)
    (u/delete-files)))

(deftest test-cardinality-many
  (let [dir  (u/tmp-dir (str "datalevin-test-cardinality-" (UUID/randomUUID)))
        conn (d/get-conn dir {:alias {:db/cardinality :db.cardinality/many
                                      :db/valueType   :db.type/string}})]
    (d/transact! conn [{:db/id -1 :name "Peter" :alias ["Pete" "Pepe"]}])
    (is (= #{"Pete" "Pepe"}
           (set (:alias (first (d/q '[:find [(pull ?e [*]) ...]
                                      :where [?e :name "Peter"]]
                                    @conn))))))
    (d/close conn)
    (let [conn2 (d/get-conn dir)]
      (is (= #{"Pete" "Pepe"}
             (set (:alias (first (d/q '[:find [(pull ?e [*]) ...]
                                        :where [?e :name "Peter"]]
                                      @conn2))))))
      (d/close conn2))
    (u/delete-files dir)))
