(ns datalevin.test.issues
  (:require
   [datalevin.core :as d]
   #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer        [is are deftest testing]])))


(deftest ^{:doc "CLJS `apply` + `vector` will hold onto mutable array of arguments directly"}
  issue-262
  (let [db (d/db-with (d/empty-db)
                      [{:attr "A"} {:attr "B"}])]
    (is (= (d/q '[:find ?a ?b
                  :where [_ :attr ?a]
                  [(vector ?a) ?b]]
                db)
           #{["A" ["A"]] ["B" ["B"]]}))
    (d/close-db db)))
