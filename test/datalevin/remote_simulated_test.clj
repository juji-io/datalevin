(ns datalevin.remote-simulated-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [datalevin.core :as d]
            [datalevin.test.core :refer [server-fixture]]))

(use-fixtures :each server-fixture)

(deftest remote-simulated-reads-use-pending-datoms
  (let [conn (d/create-conn "dtlv://datalevin:datalevin@localhost/simulated-reads"
                            {:key {:db/unique :db.unique/identity}
                             :name {:db/valueType :db.type/string}})]
    (try
      (d/transact! conn [{:db/id 1 :key "one" :name "one"}
                        {:db/id 2 :key "two" :name "two"}])
      (let [pending (:db-after
                      (d/tx-data->simulated-report
                        @conn [[:db/add 1 :name "pending"]
                               [:db/retractEntity 2]
                               {:db/id 3 :key "three" :name "added"}]))
            pattern [:name]]
        (is (= {:name "pending"} (d/pull pending pattern 1)))
        (is (= {:name "added"} (d/pull pending pattern [:key "three"])))
        (is (nil? (d/pull pending pattern 2)))
        (is (= [{:name "added"} {:name "pending"} nil {:name "pending"}]
               (d/pull-many pending pattern [3 1 2 1])))
        (is (= {:name "pending"} ((d/prepare-pull pending pattern) 1)))
        (is (= #{[1 "pending"] [3 "added"]}
               (d/q '[:find ?e ?name :where [?e :name ?name]] pending)))
        (is (= 2 ((d/prepare-q pending '[:find (count ?e) . :where [?e :name]]) [])))
        (is (= {:name "one"} (d/pull @conn pattern 1)))
        (is (= {:name "two"} (d/pull @conn pattern 2)))
        (is (nil? (d/pull @conn pattern 3)))
        (is (= #{[1 "one"] [2 "two"]}
               (d/q '[:find ?e ?name :where [?e :name ?name]] @conn))))
      (finally (d/close conn)))))
