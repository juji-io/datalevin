(ns datalevin-bench.core-test
  (:require
   [datalevin-bench.core :as sut]
   [datalevin.core :as d]
   [datalevin.util :as u]
   [clojure.test :as t :refer [is deftest]])
  (:import
   [java.util UUID]))

(deftest correct-results-test
  (let [dir  (u/tmp-dir (str "math-test-" (UUID/randomUUID)))
        conn (sut/load-data dir)
        db   (d/db conn)]
    (is (= (time (d/q '[:find [?n ...]
                        :in $
                        :where
                        [?d :person/name "David Scott Warren"]
                        [?x :person/advised ?z]
                        [?z :dissertation/cid ?d]
                        [?y :person/advised ?a]
                        [?a :dissertation/cid ?x]
                        [?y :person/name ?n]]
                      db))
           (time (sut/run-q1 db))
           ["Dana Stewart Scott" "Hao Wang"]))
    (is (= (count (time (d/q '[:find [?n ...]
                               :in $
                               :where
                               (?x :person/advised ?a)
                               (?a :dissertation/cid ?y)
                               (?b :dissertation/cid ?x)
                               (?b :dissertation/univ ?u)
                               (?c :dissertation/cid ?y)
                               (?c :dissertation/univ ?u)
                               [?y :person/name ?n]]
                             db)))
           (count (time (sut/run-q2 db)))
           34073))
    (is (= (count (time (d/q '[:find [?n ...]
                               :in $
                               :where
                               [?x :person/advised ?a]
                               [?a :dissertation/cid ?y]
                               (?b :dissertation/cid ?x)
                               (?b :dissertation/area ?a1)
                               (?c :dissertation/cid ?y)
                               (?c :dissertation/area ?a2)
                               [(!= ?a1 ?a2)]
                               [?y :person/name ?n]]
                             db)))
           (count (time (sut/run-q3 db)))
           29317))
    (is (= (count (time (sut/run-q4 db))) 135))
    (d/close conn)
    (u/delete-files dir)))
