(ns datalevin.test.index
  (:require
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.test :refer [deftest testing is use-fixtures]]
   [datalevin.util :as u]
   [datalevin.core :as d])
  (:import
   [java.util UUID]))

(use-fixtures :each db-fixture)

(deftest test-datoms
  (let [dir  (u/tmp-dir (str "reset-test-" (UUID/randomUUID)))
        dvec #(vector (:e %) (:a %) (:v %))
        db   (-> (d/empty-db dir {:name {:db/valueType :db.type/string}
                                  :age  {:db/valueType :db.type/long}})
                 (d/db-with [ [:db/add 1 :name "Petr"]
                             [:db/add 1 :age 44]
                             [:db/add 2 :name "Ivan"]
                             [:db/add 2 :age 25]
                             [:db/add 3 :name "Sergey"]
                             [:db/add 3 :age 11] ]))]
    (testing "Main indexes, sort order"

      (is (= [[1 :name "Petr"]
              [1 :age 44]
              [2 :name "Ivan"]
              [2 :age 25]
              [3 :name "Sergey"]
              [3 :age 11]]
             (map dvec (d/datoms db :eav))))

      (is (= [[2 :name "Ivan"]
              [1 :name "Petr"]
              [3 :name "Sergey"]
              [3 :age 11]
              [2 :age 25]
              [1 :age 44] ]
             (map dvec (d/datoms db :ave)))))

    (testing "Components filtration"
      (is (= [[1 :name "Petr"]
              [1 :age 44]]
             (map dvec (d/datoms db :eav 1))))

      (is (= [ [1 :age 44] ]
             (map dvec (d/datoms db :eav 1 :age))))

      (is (= [ [3 :age 11]
              [2 :age 25]
              [1 :age 44] ]
             (map dvec (d/datoms db :ave :age)))))
    (d/close-db db)
    (u/delete-files dir)))

;; should not expect attribute in lexicographic order
;; attributes are in order of creation
(deftest test-seek-datoms
  (let [dir  (u/tmp-dir (str "seek-test-" (UUID/randomUUID)))
        dvec #(vector (:e %) (:a %) (:v %))
        db   (-> (d/empty-db dir {:name {:db/valueType :db.type/string}
                                  :age  {:db/valueType :db.type/long}})
                 (d/db-with [[:db/add 1 :name "Petr"]
                             [:db/add 1 :age 44]
                             [:db/add 2 :name "Ivan"]
                             [:db/add 2 :age 25]
                             [:db/add 3 :name "Sergey"]
                             [:db/add 3 :age 11]]))]

    (testing "Non-termination"
      (is (= (map dvec (d/seek-datoms db :ave :age 10))
             [ [3 :age 11]
              [2 :age 25]
              [1 :age 44]])))

    (testing "Closest value lookup"
      (is (= (map dvec (d/seek-datoms db :ave :name "P"))
             [
              [1 :name "Petr"]
              [3 :name "Sergey"]])))

    (testing "Exact value lookup"
      (is (= (map dvec (d/seek-datoms db :ave :name "Petr"))
             [ [1 :name "Petr"]
              [3 :name "Sergey"]])))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-search-datoms
  (let [dir  (u/tmp-dir (str "search-datoms-test-" (UUID/randomUUID)))
        dvec #(vector (:e %) (:a %) (:v %))
        db   (-> (d/empty-db dir {:name {:db/valueType :db.type/string}
                                  :age  {:db/valueType :db.type/long}})
                 (d/db-with [[:db/add 1 :name "Petr"]
                             [:db/add 1 :age 44]
                             [:db/add 2 :name "Ivan"]
                             [:db/add 2 :age 25]
                             [:db/add 3 :name "Sergey"]
                             [:db/add 3 :age 11]]))]

    (testing "Non-termination"
      (is (= (map dvec (d/search-datoms db nil :age 11))
             [ [3 :age 11]])))

    (testing "Exact value lookup"
      (is (= (map dvec (d/search-datoms db nil :name "Petr"))
             [ [1 :name "Petr"]])))

    (testing "Know value only"
      (is (= (map dvec (d/search-datoms db nil nil "Sergey"))
             [[3 :name "Sergey"]])))
    (d/close-db db)
    (u/delete-files dir)))

;; should not expect attributes in lexicographic order
(deftest test-rseek-datoms
  (let [dir  (u/tmp-dir (str "rseek-test-" (UUID/randomUUID)))
        dvec #(vector (:e %) (:a %) (:v %))
        db   (-> (d/empty-db dir {:name {:db/valueType :db.type/string}
                                  :age  {:db/valueType :db.type/long}})
                 (d/db-with [[:db/add 1 :name "Petr"]
                             [:db/add 1 :age 44]
                             [:db/add 2 :name "Ivan"]
                             [:db/add 2 :age 25]
                             [:db/add 3 :name "Sergey"]
                             [:db/add 3 :age 11]]))]

    (testing "Non-termination"
      (is (= (map dvec (d/rseek-datoms db :ave :name "Petr"))
             [ [1 :name "Petr"]
              [2 :name "Ivan"]])))

    (testing "Closest value lookup"
      (is (= (map dvec (d/rseek-datoms db :ave :age 26))
             [[2 :age 25]
              [3 :age 11]
              ])))

    (testing "Exact value lookup"
      (is (= (map dvec (d/rseek-datoms db :ave :age 25))
             [[2 :age 25]
              [3 :age 11]])))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-index-range
  (let [dir  (u/tmp-dir (str "range-test-" (UUID/randomUUID)))
        dvec #(vector (:e %) (:a %) (:v %))
        db   (d/db-with
               (d/empty-db dir {:name {:db/valueType :db.type/string}
                                :age  {:db/valueType :db.type/long}})
               [ { :db/id 1 :name "Ivan" :age 15 }
                { :db/id 2 :name "Oleg" :age 20 }
                { :db/id 3 :name "Sergey" :age 7 }
                { :db/id 4 :name "Pavel" :age 45 }
                { :db/id 5 :name "Petr" :age 20 } ])]
    (is (= (map dvec (d/index-range db :name "Pe" "S"))
           [ [5 :name "Petr"] ]))
    (is (= (map dvec (d/index-range db :name "O" "Sergey"))
           [ [2 :name "Oleg"]
            [4 :name "Pavel"]
            [5 :name "Petr"]
            [3 :name "Sergey"] ]))

    (is (= (map dvec (d/index-range db :name nil "P"))
           [ [1 :name "Ivan"]
            [2 :name "Oleg"] ]))
    (is (= (map dvec (d/index-range db :name "R" nil))
           [ [3 :name "Sergey"] ]))
    (is (= (map dvec (d/index-range db :name nil nil))
           [ [1 :name "Ivan"]
            [2 :name "Oleg"]
            [4 :name "Pavel"]
            [5 :name "Petr"]
            [3 :name "Sergey"] ]))

    (is (= (map dvec (d/index-range db :age 15 20))
           [ [1 :age 15]
            [2 :age 20]
            [5 :age 20]]))
    (is (= (map dvec (d/index-range db :age 7 45))
           [ [3 :age 7]
            [1 :age 15]
            [2 :age 20]
            [5 :age 20]
            [4 :age 45] ]))
    (is (= (map dvec (d/index-range db :age 0 100))
           [ [3 :age 7]
            [1 :age 15]
            [2 :age 20]
            [5 :age 20]
            [4 :age 45] ]))
    (d/close-db db)
    (u/delete-files dir)))
