(ns datalevin.bulk-load-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.interface :as i]
            [datalevin.test.core :refer [db-fixture]]
            [datalevin.util :as u]))

(use-fixtures :each db-fixture)

(deftest init-db-validates-during-loading
  (doseq [wal? [false true]]
    (testing (str "initialization with WAL " wal?)
      (let [dir   (u/tmp-dir (str "bulk-validation-" (random-uuid)))
            conn  (d/create-conn dir
                                 {:item/value {:db/valueType :db.type/long}}
                                 {:wal? wal? :validate-data? true})
            lmdb  (d/datalog-kv conn)
            query '[:find ?e :where [?e :item/value]]]
        (try
          (d/transact! conn [{:db/id 1 :item/value 1}])
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
                        dir))))
              (is (= [100 101] @realized))
              (is (= #{[1]} (d/q query @conn)))))
          (testing "invalid elements roll back earlier batches"
            (doseq [invalid [nil false {} [:db/add 101 :item/value 101]]]
              (is (thrown-with-msg?
                    clojure.lang.ExceptionInfo #"init-db expects list of Datoms"
                    (binding [c/*fill-db-batch-size* 1]
                      (d/init-db (list (d/datom 100 :item/value 100) invalid)
                                 dir))))
              (is (= #{[1]} (d/q query @conn)))))
          (finally
            (d/close conn)
            (is (i/closed-kv? lmdb))
            (u/delete-files dir)))))))
