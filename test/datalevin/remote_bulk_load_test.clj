(ns datalevin.remote-bulk-load-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.test.core :refer [server-fixture]]))

(use-fixtures :each server-fixture)

(deftest remote-bulk-load
  (doseq [wal? [false true]]
    (testing (str "bulk load with WAL " wal?)
      (let [uri (str "dtlv://datalevin:datalevin@localhost/bulk-" wal?)
            schema {:item/value {:db/valueType :db.type/long}}
            n (+ c/+wire-datom-batch-size+ 3)
            db (binding [c/*fill-db-batch-size* (inc c/+wire-datom-batch-size+)]
                 (d/init-db (map #(d/datom % :item/value (double %))
                                 (range 1 (inc n)))
                            uri schema {:wal? wal?}))
            conn (d/conn-from-db db)
            observer (d/create-conn uri)
            query '[:find (count ?e) . :where [?e :item/value]]]
        (try
          (is (= n (d/q query @conn)))
          (is (= n (d/q query @observer)))
          (is (= {:db/id 1 :item/value 1} (d/pull @conn '[*] 1)))
          (testing "fill invalidates cached queries and updates entity IDs"
            (let [filled (binding [c/*fill-db-batch-size* 2]
                           (d/fill-db @conn [(d/datom 10000 :item/value 10000.0)
                                            (d/datom 10001 :item/value 10001.0)
                                            (d/datom 10002 :item/value 10002.0)]))]
              (reset! conn filled)
              (is (= (+ n 3) (d/q query filled)))
              (is (= (+ n 3) (d/q query @observer)))
              (let [report (d/transact! conn [{:db/id -1 :item/value 42}])]
                (is (= 10003 (get (:tempids report) -1)))
                (is (= (+ n 4) (d/q query @conn))))))
          (testing "empty loads and subsequent requests work"
            (reset! conn (d/fill-db @conn []))
            (is (= (+ n 4) (d/q query @conn))))
          (finally
            (d/close observer)
            (d/close conn)))
        (let [reopened (d/create-conn uri)]
          (try
            (is (= (+ n 4) (d/q query @reopened)))
            (is (= 10000 (:item/value (d/pull @reopened '[*] 10000))))
            (finally (d/close reopened))))))))

(deftest remote-init-db-rollback
  (doseq [wal? [false true]]
    (let [uri (str "dtlv://datalevin:datalevin@localhost/bulk-init-errors-" wal?)
          schema {:item/value {:db/valueType :db.type/long}}
          conn (d/create-conn uri schema {:wal? wal? :validate-data? true})
          query '[:find ?e :where [?e :item/value]]]
      (try
        (d/transact! conn [{:db/id 1 :item/value 1}])
        (testing "later batch failure rolls back earlier batches"
          (is (thrown-with-msg? Exception #"Invalid data"
                               (binding [c/*fill-db-batch-size* 1]
                                 (d/init-db [(d/datom 100 :item/value 100)
                                             (d/datom 101 :item/value "bad")]
                                            uri))))
          (is (= #{[1]} (d/q query @conn))))
        (testing "rollback releases the writer and restores the entity counter"
          (is (= 2 (get (:tempids (d/transact! conn
                                             [{:db/id -1 :item/value 2}]))
                        -1))))
        (testing "subsequent multi-batch initialization commits"
          (let [loaded (binding [c/*fill-db-batch-size* 1]
                         (d/init-db [(d/datom 100 :item/value 100)
                                     (d/datom 101 :item/value 101)] uri))]
            (try
              (is (= #{[1] [2] [100] [101]} (d/q query loaded)))
              (is (= #{[1] [2] [100] [101]} (d/q query @conn)))
              (finally (d/close-db loaded)))))
        (finally (d/close conn))))))

(deftest remote-bulk-load-batch-errors
  (let [conn (d/create-conn "dtlv://datalevin:datalevin@localhost/bulk-errors"
                            {:item/value {:db/valueType :db.type/long}}
                            {:validate-data? true})]
    (try
      (testing "invalid batch sizes are rejected before writing"
        (is (thrown-with-msg? Exception #"Invalid fill-db batch size"
                             (binding [c/*fill-db-batch-size* 0]
                               (d/fill-db @conn [(d/datom 1 :item/value 1)])))))
      (testing "a later failed batch preserves earlier committed batches"
        (is (thrown-with-msg? Exception #"Invalid data"
                             (binding [c/*fill-db-batch-size* 1]
                               (d/fill-db @conn [(d/datom 1 :item/value 1)
                                                (d/datom 2 :item/value "bad")]))))
        (is (= #{[1]} (d/q '[:find ?e :where [?e :item/value]] @conn)))
        (is (= 2 (get (:tempids (d/transact! conn [{:db/id -1 :item/value 2}])) -1))))
      (finally (d/close conn)))))

(deftest remote-bulk-load-in-transaction
  (let [conn (d/create-conn "dtlv://datalevin:datalevin@localhost/bulk-transaction"
                            {:item/value {:db/valueType :db.type/long}})
        query '[:find ?e :where [?e :item/value]]]
    (try
      (d/transact! conn [{:db/id 1 :item/value 1}])
      (testing "bulk writes share the explicit transaction's rollback"
        (d/with-transaction [tx conn]
          (reset! tx (d/fill-db @tx [(d/datom 100 :item/value 100)]))
          (is (= #{[1] [100]} (d/q query @tx)))
          (is (= #{[1]} (d/q query @conn)))
          (d/abort-transact tx))
        (is (= #{[1]} (d/q query @conn))))
      (testing "ordinary writes use the bulk load's new entity counter"
        (d/with-transaction [tx conn]
          (reset! tx (d/fill-db @tx [(d/datom 200 :item/value 200)]))
          (is (= 201 (get (:tempids (d/transact! tx [{:db/id -1 :item/value 201}]))
                          -1))))
        (is (= #{[1] [200] [201]} (d/q query @conn))))
      (finally (d/close conn)))))
