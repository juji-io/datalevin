(ns pod.huahaiy.datalevin-test
  (:require [datalevin.util :as u]
            [datalevin.interpret :as i]
            [babashka.pods :as pods]
            [clojure.test :refer [deftest is testing]]
            [datalevin.core :as d])
  (:import [java.util UUID Date]))

;; TODO uberjar build hangs if this ns is included
;; not include this in dtlv-test for now
(pods/load-pod ["lein" "run" "-m" "pod.huahaiy.datalevin"])

(require '[pod.huahaiy.datalevin :as pd])

(pd/defpodfn custom-fn [n] (str "hello " n))

(pd/defpodfn age [birthday today]
  (quot (-  (.getTime today) (.getTime birthday))
        (* 1000 60 60 24 365)))

(deftest pod-test
  (testing "defpodfn"
    (let [dir    (u/tmp-dir (str "datalevin-podfn-test-" (UUID/randomUUID)))
          schema (i/load-edn "movie-schema.edn")
          data   (i/load-edn "movie-data.edn")
          conn   (pd/get-conn dir schema)
          q      '[:find ?age .
                   :in $ ?today
                   :where
                   [?e :person/name ?name]
                   [?e :person/born ?dob]
                   [(age ?dob ?today) ?age]] ]
      (pd/transact! conn data)
      (is (= (pd/q q (pd/db conn) #inst "2013-08-02T00:00:00.000-00:00") 72))
      (pd/close conn)
      (u/delete-files dir)))

  (testing "datalog readme"
    (let [dir  (u/tmp-dir (str "datalevin-pod-test-" (UUID/randomUUID)))
          conn (pd/get-conn dir {:aka  {:db/cardinality :db.cardinality/many}
                                 :name {:db/valueType :db.type/string
                                        :db/unique    :db.unique/identity}})]
      (pd/transact! conn [{:name "Frege", :db/id -1, :nation "France",
                           :aka  ["foo" "fred"]}
                          {:name "Peirce", :db/id -2, :nation "france"}
                          {:name "De Morgan", :db/id -3, :nation "English"}])
      (is (= #{["France"]}
             (pd/q '[:find ?nation
                     :in $ ?alias
                     :where
                     [?e :aka ?alias]
                     [?e :nation ?nation]]
                   (pd/db conn)
                   "fred")))
      (pd/transact! conn [[:db/retract 1 :name "Frege"]])
      (is (= [[{:db/id 1, :aka ["foo" "fred"], :nation "France"}]]
             (pd/q '[:find (pull ?e [*])
                     :in $ ?alias
                     :where
                     [?e :aka ?alias]]
                   (pd/db conn)
                   "fred")))
      (pd/close conn)
      (u/delete-files dir)))

  (testing "entity"
    (let [dir  (u/tmp-dir (str "datalevin-pod-test-" (UUID/randomUUID)))
          conn (pd/get-conn dir
                            {:aka {:db/cardinality :db.cardinality/many}})]
      (pd/transact! conn [{:db/id 1, :name "Ivan", :age 19, :aka ["X" "Y"]}
                          {:db/id 2, :name "Ivan", :sex "male", :aka ["Z"]}
                          [:db/add 3 :huh? false]])
      (let [e (pd/touch (pd/entity (pd/db conn) 1))]
        (is (= (:db/id e) 1))
        (is (= (:name e) "Ivan"))
        (is (= (e :name) "Ivan"))
        (is (= (:age e) 19))
        (is (= (:aka e) #{"X" "Y"}))
        (is (= true (contains? e :age)))
        (is (= false (contains? e :not-found))))
      (pd/close conn)
      (u/delete-files dir)))

  (testing "function"
    (let [dir  (u/tmp-dir (str "datalevin-pod-test-" (UUID/randomUUID)))
          conn (pd/get-conn dir)]
      (is (= #{["hello world"]}
             (pd/q '[:find ?greeting :where [(custom-fn "world") ?greeting]])))
      (pd/close conn)
      (u/delete-files dir)))

  (testing "pull"
    (let [datoms [[1 :name  "Petr"]
                  [1 :aka   "Devil"]
                  [1 :aka   "Tupen"]
                  [2 :name  "David"]
                  [3 :name  "Thomas"]
                  [4 :name  "Lucy"]
                  [5 :name  "Elizabeth"]
                  [6 :name  "Matthew"]
                  [7 :name  "Eunan"]
                  [8 :name  "Kerri"]
                  [9 :name  "Rebecca"]
                  [1 :child 2]
                  [1 :child 3]
                  [2 :father 1]
                  [3 :father 1]
                  [6 :father 3]
                  [10 :name  "Part A"]
                  [11 :name  "Part A.A"]
                  [10 :part 11]
                  [12 :name  "Part A.A.A"]
                  [11 :part 12]
                  [13 :name  "Part A.A.A.A"]
                  [12 :part 13]
                  [14 :name  "Part A.A.A.B"]
                  [12 :part 14]
                  [15 :name  "Part A.B"]
                  [10 :part 15]
                  [16 :name  "Part A.B.A"]
                  [15 :part 16]
                  [17 :name  "Part A.B.A.A"]
                  [16 :part 17]
                  [18 :name  "Part A.B.A.B"]
                  [16 :part 18]]
          schema {:aka    { :db/cardinality :db.cardinality/many }
                  :child  { :db/cardinality :db.cardinality/many
                           :db/valueType    :db.type/ref }
                  :friend { :db/cardinality :db.cardinality/many
                           :db/valueType    :db.type/ref }
                  :enemy  { :db/cardinality :db.cardinality/many
                           :db/valueType    :db.type/ref }
                  :father { :db/valueType :db.type/ref }

                  :part { :db/valueType  :db.type/ref
                         :db/isComponent true
                         :db/cardinality :db.cardinality/many }
                  :spec { :db/valueType  :db.type/ref
                         :db/isComponent true
                         :db/cardinality :db.cardinality/one }}
          test-db (pd/init-db datoms nil schema)]
      (is (= {:name "Petr" :aka ["Devil" "Tupen"]}
             (pd/pull test-db '[:name :aka] 1)))

      (is (= {:name "Matthew" :father {:db/id 3} :db/id 6}
             (pd/pull test-db '[:name :father :db/id] 6)))

      (is (= [{:name "Petr"} {:name "Elizabeth"}
              {:name "Eunan"} {:name "Rebecca"}]
             (pd/pull-many test-db '[:name] [1 5 7 9])))
      (pd/close-db test-db)))

  (testing "datoms"
    (let [datoms (set [[1 :name "Oleg"]
                       [1 :age 17]
                       [1 :aka "x"]])
          db     (pd/init-db datoms)
          res    (set (pd/datoms db :eavt))]
      (is (= datoms res))
      (pd/close-db db)))

  (testing "schema"
    (let [dir   (u/tmp-dir (str "pod-schema-test-" (UUID/randomUUID)))
          conn1 (pd/get-conn dir)
          s     {:a/b {:db/valueType :db.type/string}}
          s1    {:c/d {:db/valueType :db.type/string}}
          txs   [{:c/d "cd" :db/id -1}]
          conn2 (pd/create-conn nil s)]
      (is (= (pd/schema conn2) (pd/update-schema conn1 s)))
      (pd/update-schema conn1 s1)
      (pd/transact! conn1 txs)
      (is (not (nil? (second (first (pd/datoms (pd/db conn1) :eavt))))))
      (pd/close conn1)
      (pd/close conn2)
      (u/delete-files dir)))

  (testing "kv"
    (let [dir        (u/tmp-dir (str "pod-kv-test-" (UUID/randomUUID)))
          db         (pd/open-kv dir)
          misc-table "misc-test-table"
          date-table "date-test-table"]
      (is (not (pd/closed-kv? db)))
      (pd/open-dbi db misc-table)
      (pd/open-dbi db date-table)
      (pd/transact-kv
        db
        [[:put misc-table :datalevin "Hello, world!"]
         [:put misc-table 42 {:saying "So Long, and thanks for all the fish"
                              :source "The Hitchhiker's Guide to the Galaxy"}]
         [:put date-table #inst "1991-12-25" "USSR broke apart" :instant]
         [:put date-table #inst "1989-11-09" "The fall of the Berlin Wall"
          :instant]])
      (is (= "Hello, world!" (pd/get-value db misc-table :datalevin)))
      (pd/transact-kv db [[:del misc-table 42]])
      (is (nil? (pd/get-value db misc-table 42)))
      (is (= [[#inst "1989-11-09T00:00:00.000-00:00" "The fall of the Berlin Wall"]
              [#inst "1991-12-25T00:00:00.000-00:00" "USSR broke apart"]]
             (pd/get-range db date-table [:closed (Date. 0) (Date.)] :instant)))
      (pd/close-kv db)
      (u/delete-files dir))))
