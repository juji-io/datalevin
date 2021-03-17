(ns pod.huahaiy.datalevin-test
  (:require [datalevin.util :as u]
            [datalevin.test.pull-api :as tp]
            [babashka.pods :as pods]
            [clojure.test :refer [deftest is testing]])
  (:import [java.util UUID Date]))

;; TODO uberjar build hangs if this ns is included
;; not include this in dtlv-test for now
(pods/load-pod ["lein" "run" "-m" "pod.huahaiy.datalevin"])

(require '[pod.huahaiy.datalevin :as d])

(deftest pod-test
  (testing "datalog readme"
    (let [dir  (u/tmp-dir (str "datalevin-pod-test-" (UUID/randomUUID)))
          conn (d/get-conn dir {:aka  {:db/cardinality :db.cardinality/many}
                                :name {:db/valueType :db.type/string
                                       :db/unique    :db.unique/identity}})]
      (d/transact! conn [{:name "Frege", :db/id -1, :nation "France",
                          :aka  ["foo" "fred"]}
                         {:name "Peirce", :db/id -2, :nation "france"}
                         {:name "De Morgan", :db/id -3, :nation "English"}])
      (is (= #{["France"]}
             (d/q '[:find ?nation
                    :in $ ?alias
                    :where
                    [?e :aka ?alias]
                    [?e :nation ?nation]]
                  (d/db conn)
                  "fred")))
      (d/transact! conn [[:db/retract 1 :name "Frege"]])
      (is (= [[{:db/id 1, :aka ["foo" "fred"], :nation "France"}]]
             (d/q '[:find (pull ?e [*])
                    :in $ ?alias
                    :where
                    [?e :aka ?alias]]
                  (d/db conn)
                  "fred")))
      (d/close conn)))

  (testing "pull"
    (let [test-db (d/init-db tp/test-datoms nil tp/test-schema)]
      (is (= {:name "Petr" :aka ["Devil" "Tupen"]}
             (d/pull test-db '[:name :aka] 1)))

      (is (= {:name "Matthew" :father {:db/id 3} :db/id 6}
             (d/pull test-db '[:name :father :db/id] 6)))

      (is (= [{:name "Petr"} {:name "Elizabeth"}
              {:name "Eunan"} {:name "Rebecca"}]
             (d/pull-many test-db '[:name] [1 5 7 9])))
      (d/close-db test-db)))

  (testing "datoms"
    (let [db (d/init-db [[1 :name "Oleg"]
                         [1 :age 17]
                         [1 :aka "x"]])]
      (is (= #{[1 :age 17]
               [1 :aka "x"]
               [1 :name "Oleg"]}
             (set (d/datoms db :eavt))))
      (d/close-db db)))

  (testing "schema"
    (let [conn1 (d/get-conn (u/tmp-dir (str "pod-schema-test-" (UUID/randomUUID))))
          s     {:a/b {:db/valueType :db.type/string}}
          s1    {:c/d {:db/valueType :db.type/string}}
          txs   [{:c/d "cd" :db/id -1}]
          conn2 (d/create-conn nil s)]
      (is (= (d/schema conn2) (d/update-schema conn1 s)))
      (d/update-schema conn1 s1)
      (d/transact! conn1 txs)
      (is (not (nil? (second (first (d/datoms (d/db conn1) :eavt))))))
      (d/close conn1)
      (d/close conn2)))

  (testing "kv"
    (let [db         (d/open-kv (u/tmp-dir (str "pod-kv-test-" (UUID/randomUUID))))
          misc-table "misc-test-table"
          date-table "date-test-table"]
      (is (not (d/closed-kv? db)))
      (d/open-dbi db misc-table)
      (d/open-dbi db date-table)
      (d/transact-kv
        db
        [[:put misc-table :datalevin "Hello, world!"]
         [:put misc-table 42 {:saying "So Long, and thanks for all the fish"
                              :source "The Hitchhiker's Guide to the Galaxy"}]
         [:put date-table #inst "1991-12-25" "USSR broke apart" :instant]
         [:put date-table #inst "1989-11-09" "The fall of the Berlin Wall"
          :instant]])
      (is (= "Hello, world!" (d/get-value db misc-table :datalevin)))
      (d/transact-kv db [[:del misc-table 42]])
      (is (nil? (d/get-value db misc-table 42)))
      (is (= [[#inst "1989-11-09T00:00:00.000-00:00" "The fall of the Berlin Wall"]
              [#inst "1991-12-25T00:00:00.000-00:00" "USSR broke apart"]]
             (d/get-range db date-table [:closed (Date. 0) (Date.)] :instant)))
      (d/close-kv db))))
