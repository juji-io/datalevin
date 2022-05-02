(ns datalevin.storage-test
  (:require [datalevin.storage :as sut]
            [datalevin.bits :as b]
            [datalevin.util :as u]
            [datalevin.constants :as c]
            [datalevin.datom :as d]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.clojure-test :as test]
            [clojure.test.check.properties :as prop]
            [clojure.test :refer [deftest is testing]]
            [datalevin.lmdb :as lmdb])
  (:import [java.util UUID]
           [datalevin.storage Store]
           [datalevin.datom Datom]))

(deftest basic-ops-test
  (let [dir   (u/tmp-dir (str "storage-test-" (UUID/randomUUID)))
        store (sut/open dir)]
    (is (= c/gt0 (sut/max-gt store)))
    (is (= 3 (sut/max-aid store)))
    (is (= (merge c/entity-time-schema c/implicit-schema) (sut/schema store)))
    (is (= c/e0 (sut/init-max-eid store)))
    (let [a   :a/b
          v   (UUID/randomUUID)
          d   (d/datom c/e0 a v)
          s   (assoc (sut/schema store) a {:db/aid 3})
          b   :b/c
          p1  {:db/valueType :db.type/uuid}
          v1  (UUID/randomUUID)
          d1  (d/datom c/e0 b v1)
          s1  (assoc s b (merge p1 {:db/aid 4}))
          c   :c/d
          p2  {:db/valueType :db.type/ref}
          v2  (long (rand c/emax))
          d2  (d/datom c/e0 c v2)
          s2  (assoc s1 c (merge p2 {:db/aid 5}))
          dir (lmdb/dir (.-lmdb ^Store store))
          t1  (sut/last-modified store)]
      (sut/load-datoms store [d])
      (is (<= t1 (sut/last-modified store)))
      (is (= s (sut/schema store)))
      (is (= 1 (sut/datom-count store :eav)))
      (is (= 1 (sut/datom-count store :ave)))
      (is (= 0 (sut/datom-count store :vea)))
      (is (= [d] (sut/fetch store d)))
      (is (= [d] (sut/slice store :eav d d)))
      (is (= true (sut/populated? store :eav d d)))
      (is (= 1 (sut/size store :eav d d)))
      (is (= d (sut/head store :eav d d)))
      (is (= d (sut/tail store :eav d d)))
      (sut/swap-attr store b merge p1)
      (sut/load-datoms store [d1])
      (is (= s1 (sut/schema store)))
      (is (= 2 (sut/datom-count store :eav)))
      (is (= 2 (sut/datom-count store :ave)))
      (is (= 0 (sut/datom-count store :vea)))
      (is (= [] (sut/slice store :eav d (d/datom c/e0 :non-exist v1))))
      (is (= 0 (sut/size store :eav d (d/datom c/e0 :non-exist v1))))
      (is (nil? (sut/populated? store :eav d (d/datom c/e0 :non-exist v1))))
      (is (= d (sut/head store :eav d d1)))
      (is (= d1 (sut/tail store :eav d1 d)))
      (is (= 2 (sut/size store :eav d d1)))
      (is (= [d d1] (sut/slice store :eav d d1)))
      (is (= [d d1] (sut/slice store :ave d d1)))
      (is (= [d1 d] (sut/rslice store :eav d1 d)))
      (is (= [d d1] (sut/slice store :eav
                               (d/datom c/e0 a nil)
                               (d/datom c/e0 nil nil))))
      (is (= [d1 d] (sut/rslice store :eav
                                (d/datom c/e0 b nil)
                                (d/datom c/e0 nil nil))))
      (is (= 1 (sut/size-filter store :eav
                                (fn [^Datom d] (= v (.-v d)))
                                (d/datom c/e0 nil nil)
                                (d/datom c/e0 nil nil))))
      (is (= d (sut/head-filter store :eav
                                (fn [^Datom d] (= v (.-v d)))
                                (d/datom c/e0 nil nil)
                                (d/datom c/e0 nil nil))))
      (is (= d (sut/tail-filter store :eav
                                (fn [^Datom d] (= v (.-v d)))
                                (d/datom c/e0 nil nil)
                                (d/datom c/e0 nil nil))))
      (is (= [d] (sut/slice-filter store :eav
                                   (fn [^Datom d] (= v (.-v d)))
                                   (d/datom c/e0 nil nil)
                                   (d/datom c/e0 nil nil))))
      (is (= [d1 d] (sut/rslice store :ave d1 d)))
      (is (= [d d1] (sut/slice store :ave
                               (d/datom c/e0 a nil)
                               (d/datom c/e0 nil nil))))
      (is (= [d1 d] (sut/rslice store :ave
                                (d/datom c/e0 b nil)
                                (d/datom c/e0 nil nil))))
      (is (= [d] (sut/slice-filter store :ave
                                   (fn [^Datom d] (= v (.-v d)))
                                   (d/datom c/e0 nil nil)
                                   (d/datom c/e0 nil nil))))
      (sut/swap-attr store c merge p2)
      (sut/load-datoms store [d2])
      (is (= s2 (sut/schema store)))
      (is (= 3 (sut/datom-count store c/eav)))
      (is (= 3 (sut/datom-count store c/ave)))
      (is (= 1 (sut/datom-count store c/vea)))
      (is (= [d2] (sut/slice store :vea
                             (d/datom c/e0 nil v2)
                             (d/datom c/emax nil v2))))
      (sut/load-datoms store [(d/delete d)])
      (is (= 2 (sut/datom-count store c/eav)))
      (is (= 2 (sut/datom-count store c/ave)))
      (is (= 1 (sut/datom-count store c/vea)))
      (sut/close store)
      (is (sut/closed? store))
      (let [store (sut/open dir)]
        (is (= [d1] (sut/slice store :eav d1 d1)))
        (sut/load-datoms store [(d/delete d1)])
        (is (= 1 (sut/datom-count store c/eav)))
        (sut/load-datoms store [d d1])
        (is (= 3 (sut/datom-count store c/eav)))
        (sut/close store))
      (let [d     :d/e
            p3    {:db/valueType :db.type/long}
            s3    (assoc s2 d (merge p3 {:db/aid 6}))
            s4    (assoc s3 :f/g {:db/aid 7 :db/valueType :db.type/string})
            store (sut/open dir {d p3})]
        (is (= s3 (sut/schema store)))
        (sut/set-schema store {:f/g {:db/valueType :db.type/string}})
        (is (= s4 (sut/schema store)))
        (sut/close store)))
    ))

(deftest schema-test
  (let [s     {:a {:db/valueType :db.type/string}
               :b {:db/valueType :db.type/long
                   :db/unique    :db.unique/value}}
        dir   (u/tmp-dir (str "datalevin-schema-test-" (UUID/randomUUID)))
        store (sut/open dir s)
        s1    (sut/schema store)]
    (sut/close store)
    (is (sut/closed? store))
    (let [store (sut/open dir s)]
      (is (= s1 (sut/schema store)))
      (is (= {:db/unique          #{:b :db/ident},
              :db.unique/value    #{:b},
              :db.unique/identity #{:db/ident}} (sut/rschema store)))
      (sut/close store))
    (u/delete-files dir)))

(deftest giants-string-test
  (let [schema {:a {:db/valueType :db.type/string}}
        dir    (u/tmp-dir (str "datalevin-giants-str-test-" (UUID/randomUUID)))
        store  (sut/open dir schema)
        v      (apply str (repeat 10000 (UUID/randomUUID)))
        d      (d/datom c/e0 :a v)]
    (sut/load-datoms store [d])
    (is (= [d] (sut/fetch store d)))
    (is (= [d] (sut/slice store :eavt
                          (d/datom c/e0 :a c/v0)
                          (d/datom c/e0 :a c/vmax))))
    (sut/close store)
    (u/delete-files dir)))

(deftest giants-data-test
  (let [dir   (u/tmp-dir (str "datalevin-giants-data-test-" (UUID/randomUUID)))
        store (sut/open dir)
        v     (apply str (repeat 10000 (UUID/randomUUID)))
        d     (d/datom c/e0 :a v)
        d1    (d/datom (inc c/e0) :b v)]
    (sut/load-datoms store [d])
    (is (= [d] (sut/fetch store d)))
    (is (= [d] (sut/slice store :eavt
                          (d/datom c/e0 :a c/v0)
                          (d/datom c/e0 :a c/vmax))))
    (sut/close store)
    (let [store' (sut/open dir)]
      (is (sut/populated? store' :eav
                          (d/datom c/e0 :a c/v0)
                          (d/datom c/e0 :a c/vmax)))
      (is (= [d] (sut/fetch store' d)))
      (is (= [d] (sut/slice store' :eavt
                            (d/datom c/e0 :a c/v0)
                            (d/datom c/e0 :a c/vmax))))
      (sut/load-datoms store' [d1])
      (is (= 1 (sut/init-max-eid store')))
      (is (= [d1] (sut/fetch store' d1)))
      (sut/close store'))
    (u/delete-files dir)))

(deftest normal-data-test
  (let [dir   (u/tmp-dir (str "datalevin-normal-data-test-" (UUID/randomUUID)))
        store (sut/open dir)
        v     (UUID/randomUUID)
        d     (d/datom c/e0 :a v)
        d1    (d/datom (inc c/e0) :b v)]
    (sut/load-datoms store [d])
    (is (= [d] (sut/fetch store d)))
    (is (= [d] (sut/slice store :eavt
                          (d/datom c/e0 :a c/v0)
                          (d/datom c/e0 :a c/vmax))))
    (sut/close store)

    (let [store' (sut/open dir)]
      (is (sut/populated? store' :eav
                          (d/datom c/e0 :a c/v0)
                          (d/datom c/e0 :a c/vmax)))
      (is (= [d] (sut/fetch store' d)))
      (is (= [d] (sut/slice store' :eavt
                            (d/datom c/e0 :a c/v0)
                            (d/datom c/e0 :a c/vmax))))
      (sut/load-datoms store' [d1])
      (is (= 1 (sut/init-max-eid store')))
      (is (= [d1] (sut/fetch store' d1)))
      (sut/close store))
    ))

(deftest false-value-test
  (let [d     (d/datom c/e0 :a false)
        dir   (u/tmp-dir (str "storage-test-" (UUID/randomUUID)))
        store (sut/open dir)]
    (sut/load-datoms store [d])
    (is (= [d] (sut/fetch store d)))
    (sut/close store)
    (u/delete-files dir)))

(test/defspec random-data-test
  100
  (prop/for-all
    [v gen/any-printable-equatable
     a gen/keyword-ns
     e (gen/large-integer* {:min 0})]
    (let [d     (d/datom e a v)
          dir   (u/tmp-dir (str "storage-test-" (UUID/randomUUID)))
          store (sut/open dir)
          _     (sut/load-datoms store [d])
          r     (sut/fetch store d)]
      (sut/close store)
      (is (= [d] r)))))

(deftest extract-entity-test
  (let [datoms    (map #(apply d/datom %)  [[1 :name  "Petr"]
                                            [1 :aka   "Devil"]
                                            [1 :aka   "Tupen"]
                                            [2 :name  "David"]
                                            [3 :name  "Thomas"]
                                            [4 :name  "Lucy"]
                                            [5 :name  "Elizabeth"]
                                            [6 :name  "Matthew"]
                                            [7 :name  "Eunan"]
                                            [8 :name  "Kerri"]
                                            [9 :name  "Rebecca"]
                                            [1 :child 2]
                                            [1 :child 3]
                                            [2 :father 1]
                                            [3 :father 1]
                                            [6 :father 3]
                                            [10 :name  "Part A"]
                                            [11 :name  "Part A.A"]
                                            [10 :part 11]
                                            [12 :name  "Part A.A.A"]
                                            [11 :part 12]
                                            [13 :name  "Part A.A.A.A"]
                                            [12 :part 13]
                                            [14 :name  "Part A.A.A.B"]
                                            [12 :part 14]
                                            [15 :name  "Part A.B"]
                                            [10 :part 15]
                                            [16 :name  "Part A.B.A"]
                                            [15 :part 16]
                                            [17 :name  "Part A.B.A.A"]
                                            [16 :part 17]
                                            [18 :name  "Part A.B.A.B"]
                                            [16 :part 18]])
        classes   {0 #{3}, 1 #{3 4 5}, 2 #{3 7}, 3 #{3 6}}
        rclasses  {3 #{0 1 2 3}, 4 #{1}, 5 #{1}, 7 #{2}, 6 #{3}}
        ent-map   {1  1,
                   2  3,
                   3  3,
                   4  0,
                   5  0,
                   6  3,
                   7  0,
                   8  0,
                   9  0,
                   10 2,
                   11 2,
                   12 2,
                   13 0,
                   14 0,
                   15 2,
                   16 2,
                   17 0,
                   18 0}
        rentities {0 (b/bitmap [4, 5, 7, 8, 9, 13, 14, 17, 18]),
                   1 (b/bitmap [1])
                   2 (b/bitmap [10, 11, 12, 15, 16]),
                   3 (b/bitmap [2, 3, 6])}
        dir       (u/tmp-dir (str "datalevin-extract-entity-test-"
                                  (UUID/randomUUID)))
        store     (sut/open dir)]
    (sut/load-datoms store datoms)

    (is (= #{} (sut/entity-attrs store 20)))
    (is (= #{:name :aka :child} (sut/entity-attrs store 1)))
    (is (= #{:name :father} (sut/entity-attrs store 2)))

    (is (= classes (sut/classes store)))
    (is (= rclasses (sut/rclasses store)))
    (is (= (sut/rclasses store) (sut/classes->rclasses (sut/classes store))))
    (is (= ent-map (sut/entities store)))
    (is (= rentities (sut/rentities store)))
    (is (= (sut/entities store) (sut/rentities->entities (sut/rentities store))))

    (let [aids (sut/attrs->aids store #{:name :aka})]
      (is (= #{:name :aka} (sut/aids->attrs store aids))))
    (sut/close store)

    (testing "load classes"
      (let [store1 (sut/open dir)]
        (is (= classes (sut/classes store1)))
        (is (= ent-map (sut/entities store1)))
        (is (= rclasses (sut/rclasses store1)))
        (is (= rentities (sut/rentities store1))))))

  )
