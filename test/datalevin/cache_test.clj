(ns datalevin.cache-test
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.db :as db]
   [datalevin.query]
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
        query-hit-key [:query-result {:all? false :attrs #{:name}} :parsed :inputs]
        query-keep-key [:query-result {:all? false :attrs #{:age}} :parsed :inputs]
        query-all-key [:query-result {:all? true} :parsed :inputs]
        keep-eid-key [:e-datoms 999]
        evict-eid-key [:e-datoms 1]
        keep-attr-key [:cardinality :age]
        evict-attr-key [:cardinality :name]]
    (try
      (db/cache-put store query-key :query)
      (db/cache-put store query-hit-key :query-hit)
      (db/cache-put store query-keep-key :query-keep)
      (db/cache-put store query-all-key :query-all)
      (db/cache-put store keep-eid-key :keep-eid)
      (db/cache-put store evict-eid-key :evict-eid)
      (db/cache-put store keep-attr-key :keep-attr)
      (db/cache-put store evict-attr-key :evict-attr)

      (is (= :query (db/cache-get store query-key)))
      (is (= :query-hit (db/cache-get store query-hit-key)))
      (is (= :query-keep (db/cache-get store query-keep-key)))
      (is (= :query-all (db/cache-get store query-all-key)))
      (is (= :keep-eid (db/cache-get store keep-eid-key)))
      (is (= :evict-eid (db/cache-get store evict-eid-key)))
      (is (= :keep-attr (db/cache-get store keep-attr-key)))
      (is (= :evict-attr (db/cache-get store evict-attr-key)))

      (d/transact! conn [[:db/add 1 :name "Alice"]])

      (is (nil? (db/cache-get store query-key)))
      (is (nil? (db/cache-get store query-hit-key)))
      (is (nil? (db/cache-get store query-all-key)))
      (is (= :query-keep (db/cache-get store query-keep-key)))
      (is (nil? (db/cache-get store evict-eid-key)))
      (is (nil? (db/cache-get store evict-attr-key)))
      (is (= :keep-eid (db/cache-get store keep-eid-key)))
      (is (= :keep-attr (db/cache-get store keep-attr-key)))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest query-cache-db-input-token-test
  (let [dir  (str "data/query-cache-token-test-" (UUID/randomUUID))
        conn (d/create-conn
               dir
               {:name {:db/valueType :db.type/string}}
               {:kv-opts {:flags (conj c/default-env-flags :nosync)}})]
    (try
      (let [db1 (d/db conn)]
        (d/transact! conn [{:db/id 1 :name "Alice"}])
        (let [db2 (d/db conn)]
          (is (= (#'datalevin.query/cache-input-token db1)
                 (#'datalevin.query/cache-input-token db2)))))
      (let [outside-token (#'datalevin.query/cache-input-token (d/db conn))
            inside-token-1 (d/with-transaction [cn conn]
                             (#'datalevin.query/cache-input-token (d/db cn)))
            inside-token-2 (d/with-transaction [cn conn]
                             (#'datalevin.query/cache-input-token (d/db cn)))]
        (is (not= outside-token inside-token-1))
        (is (not= inside-token-1 inside-token-2)))
      (finally
        (d/close conn)
        (u/delete-files dir)))))
