(ns datalevin.withtxn-test
  (:require
   [datalevin.core :as d]
   [datalevin.util :as u]
   [datalevin.constants :as c]
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.test :as t :refer [is deftest testing use-fixtures]])
  (:import
   [java.util UUID]))

(use-fixtures :each db-fixture)

(deftest with-transaction-test
  (let [dir   (u/tmp-dir (str "with-tx-test-" (UUID/randomUUID)))
        conn  (d/create-conn
                dir {}
                {:kv-opts {:flags (conj c/default-env-flags :nosync)}})
        query '[:find ?c .
                :in $ ?e
                :where [?e :counter ?c]]]
    (d/transact! conn [{:db/id 1 :counter 0}])
    (is (= 0 (d/q query @conn 1)))

    (testing "new value is invisible to outside readers"
      (d/with-transaction [cn conn]
        (is (= 0 (d/q query @cn 1)))
        (d/transact! cn [{:db/id 1 :counter 1}])
        (is (= 1 (d/q query @cn 1)))
        (is (= 0 (d/q query @conn 1))))
      (is (= 1 (d/q query @conn 1))))

    (testing "abort"
      (d/with-transaction [cn conn]
        (d/transact! cn [{:db/id 1 :counter 2}])
        (is (= 2 (d/q query @cn 1)))
        (d/abort-transact cn))
      (is (= 1 (d/q query @conn 1))))

    (testing "concurrent writes do not overwrite each other"
      (let [count-f
            #(d/with-transaction [cn conn]
               (let [^long now (d/q query @cn 1)]
                 (d/transact! cn [{:db/id 1 :counter (inc now)}])
                 (d/q query @cn 1)))]
        (is (= (set [2 3 4 5])
               (set (pcalls count-f count-f count-f count-f))))))
    (d/close conn)
    (u/delete-files dir)))

(deftest with-txn-map-resize-test
  (let [dir   (u/tmp-dir (str "with-tx-resize-test-" (UUID/randomUUID)))
        conn  (d/create-conn
                dir nil
                {:kv-opts {:mapsize 1
                           :flags   (conj c/default-env-flags :nosync)}})
        query '[:find ?e .
                :in $ ?d
                :where [?e :content ?d]]
        prior "prior data"
        big   "bigger than 10 MB"]

    (d/with-transaction [cn conn]
      (d/transact! cn [{:content prior :numbers [1 2]}])
      (is (= 1 (d/q query @cn prior)))
      (d/transact! cn [{:content big
                        :numbers (range 10000000)}])
      (is (= 2 (d/q query @cn big))))

    (is (= 1 (d/q query @conn prior)))
    (is (= 2 (d/q query @conn big)))

    (is (= 4 (count (d/datoms @conn :eav))))

    (d/close conn)
    (u/delete-files dir)))
