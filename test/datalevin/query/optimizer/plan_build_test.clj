(ns datalevin.query.optimizer.plan-build-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [datalevin.core :as d]
   [datalevin.test.core :refer [db-fixture]]))

(use-fixtures :each db-fixture)

(deftest rule-input-binding-retains-join-column-test
  (let [schema {:store/id {:db/valueType :db.type/string
                           :db/unique :db.unique/identity}
                :store/state {:db/valueType :db.type/ref}
                :state/id {:db/valueType :db.type/string
                           :db/unique :db.unique/identity}
                :series/id {:db/valueType :db.type/string
                            :db/unique :db.unique/identity}
                :series/store {:db/valueType :db.type/ref}
                :day/d {:db/valueType :db.type/long
                        :db/unique :db.unique/identity}
                :day/snap-state {:db/valueType :db.type/ref
                                 :db/cardinality :db.cardinality/many}
                :sale/series {:db/valueType :db.type/ref}
                :sale/day {:db/valueType :db.type/ref}}
        conn (d/create-conn nil schema {:kv-opts {:inmemory? true}})
        query '[:find (count ?s) . :in $ % ?st
                :where
                [?ser :series/store ?st]
                [?s :sale/series ?ser]
                [?s :sale/day ?day]
                (snap? ?ser ?day)]]
    (try
      (d/transact! conn [{:db/id -1 :state/id "CA"}
                         {:db/id -2 :state/id "TX"}
                         {:store/id "CA_1" :store/state -1}
                         {:store/id "TX_1" :store/state -2}])
      (d/transact!
        conn
        (for [day (range 1 31)]
          (cond-> {:day/d day}
            (even? day) (assoc :day/snap-state [[:state/id "CA"]])
            (zero? (mod day 3))
            (update :day/snap-state (fnil conj []) [:state/id "TX"]))))
      (d/transact!
        conn
        (for [series (range 20)]
          {:series/id (str "S" series)
           :series/store [:store/id (if (even? series) "CA_1" "TX_1")]}))
      (d/transact!
        conn
        (for [series (range 20)
              day (range 1 31)
              :when (even? (+ series day))]
          {:sale/series [:series/id (str "S" series)]
           :sale/day [:day/d day]}))
      (let [db @conn
            store (d/q '[:find ?st . :where [?st :store/id "CA_1"]] db)]
        (doseq [rules ['[[(snap? ?series ?day)
                         [?series :series/store ?st]
                         [?st :store/state ?state]
                         [?day :day/snap-state ?state]]]
                       '[[(snap? ?series ?day)
                         [?series :series/store ?store]
                         [?store :store/state ?state]
                         [?day :day/snap-state ?state]]]]]
          (testing (pr-str rules)
            (is (= 150 (d/q query db rules store)))
            (is (= 150 (:actual-result-size
                         (d/explain {:run? true} query db rules store)))))))
      (finally
        (d/close conn)))))

(deftest repeated-attribute-skips-only-placeholder-values-test
  (let [conn (d/create-conn
               nil {:series/store {:db/valueType :db.type/ref
                                   :db/cardinality :db.cardinality/many}}
               {:kv-opts {:inmemory? true}})
        rules '[[(series-store ?series ?store)
                 [?series :series/store ?store]]]]
    (try
      (d/transact! conn [{:db/id 1 :store/id "CA"}
                         {:db/id 2 :store/id "TX"}
                         {:db/id 10 :series/store [1 2]}
                         {:db/id 11 :series/store [2]}])
      (let [db @conn]
        (testing "a constant seed still projects all values of the attribute"
          (is (= #{[10 1] [10 2]}
                 (d/q '[:find ?series ?store :in $ % ?st
                        :where [?series :series/store ?st]
                        (series-store ?series ?store)]
                      db rules 1))))
        (testing "a wildcard seed does not hide a later variable"
          (doseq [pattern ['[?series :series/store _]
                           '[?series :series/store]]]
            (is (= #{[10 1] [10 2] [11 2]}
                   (d/q {:find '[?series ?store]
                         :in '[$ %]
                         :where [pattern '(series-store ?series ?store)]}
                        db rules))))))
      (finally
        (d/close conn)))))
