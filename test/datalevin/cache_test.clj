(ns datalevin.cache-test
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.db :as db]
   [datalevin.util :as u])
  (:import
   [datalevin.db DB]
   [java.util UUID]))

(deftest selective-cache-invalidation-test
  (let [dir   (str "data/cache-test-" (UUID/randomUUID))
        conn  (d/create-conn
                dir
                {:name {:db/valueType   :db.type/string
                        :db/cardinality :db.cardinality/one}
                 :age  {:db/valueType   :db.type/long
                        :db/cardinality :db.cardinality/one}}
                {:kv-opts {:flags (conj c/default-env-flags :nosync)}})
        store (.-store ^DB @conn)
        query-key [{:qfind :all} [:input]]
        keep-eid-key [:e-datoms 999]
        evict-eid-key [:e-datoms 1]
        keep-attr-key [:cardinality :age]
        evict-attr-key [:cardinality :name]]
    (try
      (db/cache-put store query-key :query)
      (db/cache-put store keep-eid-key :keep-eid)
      (db/cache-put store evict-eid-key :evict-eid)
      (db/cache-put store keep-attr-key :keep-attr)
      (db/cache-put store evict-attr-key :evict-attr)

      (is (= :query (db/cache-get store query-key)))
      (is (= :keep-eid (db/cache-get store keep-eid-key)))
      (is (= :evict-eid (db/cache-get store evict-eid-key)))
      (is (= :keep-attr (db/cache-get store keep-attr-key)))
      (is (= :evict-attr (db/cache-get store evict-attr-key)))

      (d/transact! conn [[:db/add 1 :name "Alice"]])

      (is (nil? (db/cache-get store query-key)))
      (is (nil? (db/cache-get store evict-eid-key)))
      (is (nil? (db/cache-get store evict-attr-key)))
      (is (= :keep-eid (db/cache-get store keep-eid-key)))
      (is (= :keep-attr (db/cache-get store keep-attr-key)))
      (finally
        (d/close conn)
        (u/delete-files dir)))))
