(ns datalevin.concurrent-test
  (:require
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.test :as t :refer [is deftest testing use-fixtures]]
   [datalevin.util :as u]
   [datalevin.constants :as c]
   [datalevin.core :as d])
  (:import
   [java.util UUID Arrays Date]
   [java.lang Thread]))

(use-fixtures :each db-fixture)

(deftest with-transaction-test
  (let [dir   (u/tmp-dir (str "with-tx-test-" (UUID/randomUUID)))
        conn  (d/create-conn
                dir {}
                {:kv-opts {:flags (conj c/default-env-flags :nosync)}})
        query '[:find ?c .
                :in $ ?e
                :where [?e :counter ?c]]]
    (is (nil? (d/q query @conn 1)))

    (testing "new value is invisible to outside readers"
      (d/with-transaction [cn conn]
        (is (nil? (d/q query @cn 1)))
        (d/transact! cn [{:db/id 1 :counter 1}])
        (is (= 1 (d/q query @cn 1)))
        (is (nil? (d/q query @conn 1))))
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

(deftest large-data-concurrent-write-test
  (let [dir  (u/tmp-dir (str "large-concurrent-" (UUID/randomUUID)))
        conn (d/get-conn dir)
        d1   (apply str (repeat 1000 \a))
        d2   (apply str (repeat 1000 \a))
        tx1  [{:a d1 :b 1}]
        tx2  [{:a d2 :b 2}]
        f1   (future (d/transact! conn tx1))]
    @(future (d/transact! conn tx2))
    @f1
    (is (= 4 (count (d/datoms @conn :eav))))
    (d/close conn)))
