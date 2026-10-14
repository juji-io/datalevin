(ns datalevin.remote-tempids-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.test.core :refer [server-fixture]]))

(use-fixtures :each server-fixture)

(deftest remote-transact-tempids
  ;; Each entity contributes a datom and a tempid entry. The response also
  ;; includes :db/current-tx and :max-eid, so copy-out starts at 499 entities.
  (let [half-batch (quot c/+wire-datom-batch-size+ 2)
        sizes      [100 (- half-batch 2) (dec half-batch) half-batch
                    (inc half-batch) c/+wire-datom-batch-size+]]
    (doseq [wal? [false true]
            n sizes]
      (testing (str n " entities with WAL " wal?)
        (let [uri      (str "dtlv://datalevin:datalevin@localhost/tempids-" wal? "-" n)
              conn     (d/create-conn uri
                                      {:x/id {:db/valueType :db.type/long
                                              :db/unique    :db.unique/identity}}
                                      {:wal? wal?})
              tempid   (fn [i] (if (even? i) (- (inc i)) (str "entity-" i)))
              tx       (mapv (fn [i] {:db/id (tempid i) :x/id i}) (range n))
              expected (into {} (map (fn [i] [(tempid i) (inc i)])) (range n))
              entities (into #{} (map (fn [i] [(inc i) i])) (range n))]
          (try
            (let [report (d/transact! conn tx)
                  ids    (:tempids report)
                  after  (:db-after report)]
              (is (= expected (dissoc ids :db/current-tx)))
              (is (= (inc n) (count ids)))
              (is (= (:max-tx after) (:db/current-tx ids)))
              (is (= n (:max-eid after)))
              (is (= n (count (:tx-data report))))
              (is (= entities (into #{} (map (juxt :e :v)) (:tx-data report))))
              (is (= entities (set (d/q '[:find ?e ?id :where [?e :x/id ?id]]
                                       after)))))
            (testing "subsequent writes retain the entity counter"
              (let [report (d/transact! conn [{:db/id -1 :x/id n}])]
                (is (= (inc n) (get (:tempids report) -1)))))
            (finally (d/close conn))))))))
