(ns datalevin.test.datafy
  (:require
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.test :refer [deftest testing is use-fixtures]]
   [datalevin.datafy]
   [datalevin.util :as u]
   [datalevin.core :as d]
   [clojure.core.protocols :as cp]
   [datalevin.entity :as e])
  (:import
   [java.util UUID]))

(use-fixtures :each db-fixture)

(defn- nav [coll k]
  (cp/nav coll k (coll k)))

(defn d+n
  "Helper function to datafy/navigate for a path"
  [coll [k & ks]]
  (if (nil? k)
    coll
    (d+n (nav (cp/datafy coll) k)
         ks)))

(deftest test-navigation
  (let [schema {:ref           {:db/valueType :db.type/ref}
                :namespace/ref {:db/valueType :db.type/ref}
                :many/ref      {:db/valueType   :db.type/ref
                                :db/cardinality :db.cardinality/many}}
        dir    (u/tmp-dir (str "db-test-" (UUID/randomUUID)))
        db     (-> (d/empty-db dir schema)
                   (d/db-with [{:db/id 1 :name "Parent1"}
                               {:db/id         2 :name "Child1" :ref 1
                                :namespace/ref 1}
                               {:db/id         3 :name "GrandChild1" :ref 2
                                :namespace/ref 2}
                               {:db/id 4 :name "Master" :many/ref [1 2 3]}]))
        entity (e/entity db 3)]
    (is (= 2 (:db/id (d+n entity [:ref]))))
    (is (= 2 (:db/id (d+n entity [:namespace/ref]))))
    (is (= 1 (:db/id (d+n entity [:ref :namespace/ref]))))
    (is (= 3 (:db/id (d+n entity [:namespace/ref :ref :_ref 0
                                  :namespace/_ref 0]))))
    (is (= #{1 2 3} (set (map :db/id (d+n entity [:many/_ref 0 :many/ref])))))
    (d/close-db db)
    (u/delete-files dir)))
