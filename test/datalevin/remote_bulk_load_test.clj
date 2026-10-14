(ns datalevin.remote-bulk-load-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.db :as db]
            [datalevin.interface :as i]
            [datalevin.test.core :refer [server-fixture]])
  (:import [datalevin.remote DatalogStore]
           [java.util.concurrent.atomic AtomicLong]))

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

(deftest remote-init-db-returns-committed-metadata
  (doseq [wal? [false true]
          [mode batch-size datoms]
          [[:request 1 [(d/datom 100 :item/value 1)
                        (d/datom 200 :item/value 2)]]
           [:copy-in (inc c/+wire-datom-batch-size+)
            (mapv #(d/datom % :item/value %)
                  (range 1 (+ 2 c/+wire-datom-batch-size+)))]
           [:empty 1 []]]]
    (testing (str mode " import with WAL " wal?)
      (let [uri (str "dtlv://datalevin:datalevin@localhost/bulk-metadata-"
                     wal? "-" (name mode))
            db (binding [c/*fill-db-batch-size* batch-size]
                 (d/init-db datoms uri
                            {:item/value {:db/valueType :db.type/long}}
                            {:wal? wal?}))]
        (try
          (let [^DatalogStore store (:store db)
                ;; Capture before db-info itself can advance the read floor.
                floor (.get ^AtomicLong (.-read-floor-tx store))
                info (i/db-info store)]
            (is (= (select-keys info [:max-eid :max-tx])
                   (select-keys db [:max-eid :max-tx])))
            (is (= (:max-tx info) floor))
            (is (= (if (seq datoms) (:e (last datoms)) 0) (:max-eid db)))
            (is (= (+ 1 (count (partition-all batch-size datoms))) (:max-tx db)))
            (is (= (count datoms)
                   (count (d/q '[:find ?e :where [?e :item/value]] db)))))
          (finally (d/close-db db)))))))

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
        (testing "a failed batch does not realize the rest of the input"
          (let [realized (atom [])]
            (is (thrown-with-msg?
                  Exception #"Invalid data"
                  (binding [c/*fill-db-batch-size* 1]
                    (d/init-db
                      (map (fn [eid]
                             (swap! realized conj eid)
                             (d/datom eid :item/value
                                      (if (= eid 101) "bad" eid)))
                           (list 100 101 102 103))
                      uri))))
            (is (= [100 101] @realized))
            (is (= #{[1]} (d/q query @conn)))))
        (testing "invalid elements roll back earlier batches"
          (doseq [invalid [nil false {} [:db/add 101 :item/value 101]]]
            (is (thrown-with-msg?
                  clojure.lang.ExceptionInfo #"init-db expects list of Datoms"
                  (binding [c/*fill-db-batch-size* 1]
                    (d/init-db (list (d/datom 100 :item/value 100) invalid)
                               uri))))
            (is (= #{[1]} (d/q query @conn)))))
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

(deftest remote-failed-fill-invalidates-client-caches
  (binding [c/*remote-db-last-modified-check-interval-ms* 60000]
    (doseq [wal? [false true]
            batch-size [1 (inc c/+wire-datom-batch-size+)]]
      (testing (str "WAL " wal? ", batch size " batch-size)
        (let [uri (str "dtlv://datalevin:datalevin@localhost/bulk-partial-cache-"
                       wal? "-" batch-size)
              conn (d/create-conn uri
                                  {:item/value {:db/valueType :db.type/long}}
                                  {:wal? wal? :validate-data? true})
              dbv @conn
              store (:store dbv)
              datoms (conj (mapv #(d/datom % :item/value %)
                                (range 1 (inc batch-size)))
                           (d/datom (inc batch-size) :item/value "bad"))]
          (try
            (is (= 0 (d/count-datoms dbv nil :item/value nil)))
            (is (empty? (d/datoms dbv :eav)))
            (let [token (db/cache-token store)]
              (is (thrown-with-msg? Exception #"Invalid data"
                                   (binding [c/*fill-db-batch-size* batch-size]
                                     (d/fill-db dbv datoms))))
              (is (false? (db/cache-put-if-current
                            store token [:count nil :item/value nil nil] 0))
                  "an older read cannot republish its pre-import result"))
            ;; Check the warmed cache before any query can refresh metadata.
            (is (= batch-size (d/count-datoms dbv nil :item/value nil)))
            (is (= (set (range 1 (inc batch-size)))
                   (into #{} (map :e) (d/datoms dbv :eav))))
            (is (= batch-size
                   (d/q '[:find (count ?e) . :where [?e :item/value]] dbv)))
            (is (= (inc batch-size)
                   (get (:tempids (d/transact! conn [{:db/id -1 :item/value 42}]))
                        -1)))
            (is (= (inc batch-size) (d/count-datoms @conn nil :item/value nil)))
            (finally (d/close conn))))))))

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
