(ns datalevin.query-test
  (:require
   [datalevin.query :as sut]
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.test :refer [deftest testing is use-fixtures]]
   [datalevin.core :as d]
   [datalevin.constants :as c]
   [datalevin.util :as u])
  (:import
   [java.util UUID]))

(use-fixtures :each db-fixture)

(deftest single-encla-test
  (let [dir (u/tmp-dir (str "single-encla-test--" (UUID/randomUUID)))
        db  (-> (d/empty-db dir
                            {:aka {:db/cardinality :db.cardinality/many}}
                            {:kv-opts
                             {:flags (conj c/default-env-flags :nosync)}})
                (d/db-with [{ :db/id 1, :name "Ivan", :age 15
                             :aka    ["robot" "ai"]}
                            { :db/id 2, :name "Petr", :age 37 }
                            { :db/id 3, :name "Ivan", :age 37
                             :aka    ["bigmac"]}
                            { :db/id 4, :name "John" :age 15 }]))]

    (is (= (d/q '[:find ?e
                  :where [?e :aka "ai"]] db)
           #{[1]}))
    (is (= (d/q '[:find ?e
                  :where [?e :name]] db)
           #{[1] [2] [3] [4]}))
    (is (= (d/q '[:find  ?e ?v
                  :where
                  [?e :name "Ivan"]
                  [?e :age ?v]] db)
           #{[1 15] [3 37]}))
    (is (= (d/q '[:find  ?e1 ?e2
                  :where
                  [?e1 :name ?n]
                  [?e2 :name ?n]] db)
           #{[1 1] [2 2] [3 3] [4 4] [1 3] [3 1]}))
    (is (= (d/q '[:find  ?e ?e2 ?n
                  :where
                  [?e :name "Ivan"]
                  [?e :age ?a]
                  [?e2 :age ?a]
                  [?e2 :name ?n]] db)
           #{[1 1 "Ivan"]
             [3 3 "Ivan"]
             [1 4 "John"]
             [3 2 "Petr"]}))
    (d/close-db db)
    (u/delete-files dir)))
