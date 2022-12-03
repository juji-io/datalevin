(ns datalevin.remote-transact-test
  (:require
   [datalevin.core :as d]
   [datalevin.datom :as dd]
   [datalevin.constants :as c]
   [datalevin.test.core :as tdc :refer [server-fixture]]
   [clojure.test :as t :refer [is are deftest testing use-fixtures]]))

(use-fixtures :each server-fixture)

(deftest datalog-larger-tx-test
  (let [dir  "dtlv://datalevin:datalevin@localhost/large-tx-test"
        end  3000
        conn (d/create-conn dir nil {:auto-entity-time? true})
        vs   (range 0 end)
        txs  (map (fn [a v] {a v}) (repeat :id) vs)]
    (d/transact! conn txs)
    (is (= (d/q '[:find (count ?e)
                  :where [?e :id]]
                @conn)
           [[end]]))
    (let [[c u] (d/q '[:find [?c ?u]
                       :in $ ?i
                       :where
                       [?e :id ?i]
                       [?e :db/created-at ?c]
                       [?e :db/updated-at ?u]]
                     @conn 1)]
      (is c)
      (is u)
      (is (= c u)))
    (d/close conn)))

(deftest simulated-tx-test
  (let [dir  "dtlv://datalevin:datalevin@localhost/simulated-tx"
        conn (d/create-conn dir
                            {:id {:db/unique    :db.unique/identity
                                  :db/valueType :db.type/long}})]
    (let [rp (d/transact! conn [{:id 1}])]
      (is (= (:tx-data rp) [(d/datom 1 :id 1)]))
      (is (= (dd/datom-tx (first (:tx-data rp))) 2)))
    (is (= (d/datoms @conn :eav) [(d/datom 1 :id 1)]))
    (is (= (:max-eid @conn) 1))
    (is (= (:max-tx @conn) 2))

    (let [rp (d/tx-data->simulated-report @conn [{:id 2}])]
      (is (= (:tx-data rp) [(d/datom 2 :id 2)]))
      (is (= (dd/datom-tx (first (:tx-data rp))) 3))
      (is (= (:max-eid (:db-after rp)) 2))
      (is (= (:max-tx (:db-after rp)) 3)))

    (is (= (d/datoms @conn :eav) [(d/datom 1 :id 1)]))
    (is (= (:max-eid @conn) 1))
    (is (= (:max-tx @conn) 2))

    (d/close conn)))

(deftest txn-test
  (let [schema {:country/short {:db/valueType :db.type/string
                                :db/unique    :db.unique/identity}
                :country/long  {:db/valueType :db.type/string}}
        conn   (d/create-conn "dtlv://datalevin:datalevin@localhost:8898/dl-test"
                              schema)
        conn1  (d/create-conn nil schema)]

    (is (= (:max-eid @conn) (:max-eid @conn1) c/e0))
    (d/transact! conn [{:country/short "RU" :country/long "Russia"}
                       {:country/short "FR" :country/long "France"}
                       {:country/short "DE" :country/long "Germany"}])
    (d/transact! conn1 [{:country/short "RU" :country/long "Russia"}
                        {:country/short "FR" :country/long "France"}
                        {:country/short "DE" :country/long "Germany"}])
    (is (= (:max-eid @conn) (:max-eid @conn1) 3))

    (d/transact! conn [{:country/short "AZ" :country/long "Azerbaijan"}])
    (d/transact! conn1 [{:country/short "AZ" :country/long "Azerbaijan"}])
    (is (= (:max-eid @conn) (:max-eid @conn1) 4))
    (is (= 4 (d/q '[:find (count ?e) . :in $ :where [?e]] (d/db conn))))

    (d/close conn)
    (d/close conn1)))
