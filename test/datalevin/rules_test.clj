(ns datalevin.rules-test
  (:require
   [clojure.test :refer [deftest testing are use-fixtures]]
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [datalevin.rules :as sut]
   [datalevin.core :as d]
   [datalevin.constants :as c]
   [datalevin.util :as u])
  (:import
   [java.util UUID]))

(use-fixtures :each db-fixture)

(deftest strongly-connected-components-test
  (are [graph sccs] (= (set (sut/tarjans-scc graph)) (set sccs))
    {:node        #{:unreachable}
     :reachable   #{:unreachable :reachable}
     :unreachable #{}}
    [#{:reachable} #{:node} #{:unreachable}]

    {1 [2]
     2 [3 4]
     3 [4 6]
     4 [1 5]
     5 [6]
     6 [7]
     7 [5]}
    [#{1 2 3 4} #{5 6 7}]

    {:a [:c :h]
     :b [:a :g]
     :c [:d]
     :d [:f]
     :e [:a :i]
     :f [:j]
     :g [:i]
     :h [:f :g]
     :i [:h]
     :j [:c]}
    [#{:e} #{:b} #{:a} #{:h :i :g} #{:c :j :f :d}]))

#_(deftest reachability-data-test
    (let [dir (u/tmp-dir (str "reachability-test-" (UUID/randomUUID)))
          db  (-> (d/empty-db
                    dir
                    {:node {:db/unique    :db.unique/identity
                            :db/valueType :db.type/symbol}
                     :link {:db/cardinality :db.cardinality/many
                            :db/valueType   :db.type/ref}}
                    {:kv-opts {:flags (conj c/default-env-flags :nosync)}})
                  (d/db-with [{:db/id -1 :node 'a :link -2}
                              {:db/id -2 :node 'b :link -3}
                              {:db/id -3 :node 'c :link [-3 -4]}
                              {:db/id -4 :node 'd}]))]
      (d/close-db db)
      (u/delete-files dir)))
