(ns datalevin.test.issues
  (:require
   [datalevin.core :as d]
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.test :refer [deftest testing is use-fixtures]]
   [datalevin.util :as u]))

(use-fixtures :each db-fixture)

(deftest issue-262
  (let [dir (u/tmp-dir (str "query-or-" (random-uuid)))
        db  (d/db-with (d/empty-db dir)
                       [{:attr "A"} {:attr "B"}])]
    (is (= (d/q '[:find ?a ?b
                  :where [_ :attr ?a]
                  [(vector ?a) ?b]]
                db)
           #{["A" ["A"]] ["B" ["B"]]}))
    (d/close-db db)
    (u/delete-files dir)))
