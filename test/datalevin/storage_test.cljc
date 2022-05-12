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
      (sut/load-datoms store [(d/delete d)])
      (is (= 2 (sut/datom-count store c/eav)))
      (is (= 2 (sut/datom-count store c/ave)))
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

(deftest encla-links-test
  (let [schema     {:name   {:db/unique :db.unique/identity}
                    :aka    {:db/cardinality :db.cardinality/many}
                    :child  {:db/valueType :db.type/ref}
                    :father {:db/valueType :db.type/ref}
                    :part   {:db/valueType :db.type/ref}}
        datoms     (map #(apply d/datom %)  [[1 :name  "Petr"]
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
        ref-datoms (set (filter #(#{:child :father :part} (d/datom-a %)) datoms))
        n-datoms   (count datoms)
        attrs      {0 :db/ident,
                    1 :db/created-at,
                    2 :db/updated-at,
                    3 :name,
                    4 :aka,
                    5 :child,
                    6 :father,
                    7 :part}
        refs       #{7 6 5}
        classes    {0 #{3}, 1 #{3, 4, 5}, 2 #{3, 7}, 3 #{3, 6}}
        rclasses   {3 #{0 1 2 3}, 4 #{1}, 5 #{1}, 7 #{2}, 6 #{3}}
        entities   {1  1,
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
        rentities  {0 (b/bitmap [4, 5, 7, 8, 9, 13, 14, 17, 18]),
                    1 (b/bitmap [1])
                    2 (b/bitmap [10, 11, 12, 15, 16]),
                    3 (b/bitmap [2, 3, 6])}
        links      {[1 6 2]   [1 6 3],
                    [13 7 12] [0 7 2],
                    [18 7 16] [0 7 2],
                    [15 7 10] [2 7 2],
                    [17 7 16] [0 7 2],
                    [14 7 12] [0 7 2],
                    [3 6 6]   [3 6 3],
                    [12 7 11] [2 7 2],
                    [11 7 10] [2 7 2],
                    [16 7 15] [2 7 2],
                    [3 5 1]   [3 5 1],
                    [1 6 3]   [1 6 3],
                    [2 5 1]   [3 5 1]}
        dir        (u/tmp-dir (str "datalevin-extract-encla-test-"
                                   (UUID/randomUUID)))
        store      (sut/open dir schema)]
    (sut/load-datoms store datoms)
    (is (= attrs (sut/attrs store)))
    (is (= refs (sut/refs store)))

    (is (= n-datoms (sut/datom-count store :eav)))
    (is (= 13 (count ref-datoms)))

    (is (= #{(d/datom 1 :child 3) (d/datom 6 :father 3)} (sut/scan-ref-v store 3)))

    (is (= classes (sut/classes store)))
    (is (= rclasses (sut/rclasses store)))
    (is (= (sut/rclasses store) (sut/classes->rclasses (sut/classes store))))

    (is (= rentities (sut/rentities store)))
    (is (= (sut/entities store) (sut/rentities->entities (sut/rentities store))))

    (is (= links (sut/links store)))

    (sut/close store)

    (testing "load encla and links"
      (let [store1 (sut/open dir)]
        (is (= classes (sut/classes store1)))
        (is (= entities (sut/entities store1)))
        (is (= rclasses (sut/rclasses store1)))
        (is (= rentities (sut/rentities store1)))
        (is (= links (sut/links store1)))
        (sut/close store1)))
    (testing "add a datom that does not change encla"
      (let [store1 (sut/open dir)]
        (sut/load-datoms store1 [(d/datom 1 :aka "Angel")])
        (is (= (inc n-datoms) (sut/datom-count store1 :eav)))
        (is (= classes (sut/classes store1)))
        (is (= rclasses (sut/rclasses store1)))
        (is (= entities (sut/entities store1)))
        (is (= rentities (sut/rentities store1)))
        (is (= links (sut/links store1)))
        (sut/close store1)))
    (testing "delete a datom that does not change encla"
      (let [store1 (sut/open dir)]
        (sut/load-datoms store1 [(d/datom 1 :aka "Devil" -1)])
        (is (= n-datoms (sut/datom-count store1 :eav)))
        (is (= classes (sut/classes store1)))
        (is (= rclasses (sut/rclasses store1)))
        (is (= entities (sut/entities store1)))
        (is (= rentities (sut/rentities store1)))
        (is (= links (sut/links store1)))
        (sut/close store1)))
    (testing "delete a datom that changes entities"
      (let [store1 (sut/open dir)]
        (sut/load-datoms store1 [(d/datom 2 :father 1 -1)])
        (is (= (dec n-datoms) (sut/datom-count store1 :eav)))
        (is (= classes (sut/classes store1)))
        (is (= rclasses (sut/rclasses store1)))
        (is (= (assoc entities 2 0) (sut/entities store1)))
        (is (= (-> rentities
                   (update 0 #(b/bitmap-add % 2))
                   (update 3 #(b/bitmap-del % 2)))
               (sut/rentities store1)))
        (is (= (dissoc links [1 6 2]) (sut/links store1)))
        (sut/close store1)))
    (testing "add a datom that changes entities"
      (let [store1 (sut/open dir)]
        (sut/load-datoms store1 [(d/datom 2 :father 3)])
        (is (= n-datoms (sut/datom-count store1 :eav)))
        (is (= classes (sut/classes store1)))
        (is (= rclasses (sut/rclasses store1)))
        (is (= (assoc entities 2 3) (sut/entities store1)))
        (is (= (-> rentities
                   (update 3 #(b/bitmap-add % 2))
                   (update 0 #(b/bitmap-del % 2)))
               (sut/rentities store1)))
        (is (= (-> links
                   (dissoc [1 6 2])
                   (assoc [3 6 2] [3 6 3])) (sut/links store1)))
        (sut/close store1)))
    (testing "add a datom that changes classes"
      (let [store1 (sut/open dir)]
        (sut/load-datoms store1 [(d/datom 2 :child 8)])
        (is (= (inc n-datoms) (sut/datom-count store1 :eav)))
        (is (= (-> classes (assoc 4 #{3 5 6})) (sut/classes store1)))
        (is (= (-> rclasses
                   (update 5 conj 4) (update 3 conj 4) (update 6 conj 4))
               (sut/rclasses store1)))
        (is (= (assoc entities 2 4) (sut/entities store1)))
        (is (= (-> rentities
                   (assoc 4 (b/bitmap [2]))
                   (update 3 #(b/bitmap-del % 2))
                   (update 0 #(b/bitmap-del % 2)))
               (sut/rentities store1)))
        (is (= (-> links
                   (dissoc [1 6 2])
                   (assoc [3 6 2] [3 6 4] [8 5 2] [0 5 4]))
               (sut/links store1)))
        (sut/close store1)))
    (testing "delete datoms that changes classes"
      (let [store1 (sut/open dir)]
        (sut/load-datoms store1 [(d/datom 1 :child 2 -1)
                                 (d/datom 1 :child 3 -1)])
        (is (= (dec n-datoms) (sut/datom-count store1 :eav)))
        (is (= (-> classes (assoc 4 #{3 5 6} 5 #{3 4})) (sut/classes store1)))
        (is (= (-> rclasses
                   (update 5 conj 4) (update 3 conj 4 5) (update 6 conj 4)
                   (update 4 conj 5))
               (sut/rclasses store1)))
        (is (= (assoc entities 2 4 1 5) (sut/entities store1)))
        (is (= (-> rentities
                   (assoc 4 (b/bitmap [2]))
                   (update 0 #(b/bitmap-del % 2))
                   (assoc 5 (b/bitmap [1]))
                   (assoc 1 (b/bitmap)))
               (sut/rentities store1)))
        (is (= (-> links
                   (dissoc [1 6 2])
                   (assoc [3 6 2] [3 6 4] [8 5 2] [0 5 4])
                   (dissoc [2 5 1] [3 5 1])
                   )
               (sut/links store1)))
        (sut/close store1)))
    (testing "reload"
      (let [store1 (sut/open dir)]
        (is (= (dec n-datoms) (sut/datom-count store1 :eav)))
        (is (= (-> classes (assoc 4 #{3 5 6} 5 #{3 4})) (sut/classes store1)))
        (is (= (-> rclasses
                   (update 5 conj 4) (update 3 conj 4 5) (update 6 conj 4)
                   (update 4 conj 5))
               (sut/rclasses store1)))
        (is (= (assoc entities 2 4 1 5) (sut/entities store1)))
        (is (= (-> rentities
                   (assoc 4 (b/bitmap [2]))
                   (update 0 #(b/bitmap-del % 2))
                   (assoc 5 (b/bitmap [1]))
                   (assoc 1 (b/bitmap)))
               (sut/rentities store1)))
        (is (= (-> links
                   (dissoc [1 6 2])
                   (assoc [3 6 2] [3 6 4] [8 5 2] [0 5 4])
                   (dissoc [2 5 1] [3 5 1]))
               (sut/links store1)))
        (sut/close store1)))))

(deftest map-resize-encla-test
  (let [next-eid (volatile! 0)
        random-man
        (fn []
          (cond->
              {:db/id (vswap! next-eid #(inc ^long %))
               :fname (rand-nth ["Ivan" "Petr" "Sergei" "Oleg"
                                 "Yuri" "Dmitry" "Fedor" "Denis"])
               :lname (rand-nth ["Ivanov" "Petrov" "Sidorov"
                                 "Kovalev" "Kuznetsov" "Voronoi"])}
            (< 10 ^long (rand-int 100))
            (assoc :sex (rand-nth [:male :female]))
            (< 30 ^long (rand-int 100))
            (assoc :age (rand-int 100))
            (< 50 ^long (rand-int 100))
            (assoc :salary (rand-int 100000))))
        datoms   (mapcat (fn [{:keys [db/id fname lname sex age salary]}]
                           (cond-> [(d/datom id :fname fname)
                                    (d/datom id :lname lname)]
                             sex    (conj (d/datom id :sex sex))
                             age    (conj (d/datom id :age age))
                             salary (conj (d/datom id :salary salary))))
                         (take 10000 (repeatedly random-man)))
        s-dir    (u/tmp-dir (str "small-" (UUID/randomUUID)))
        s-store  (sut/open s-dir nil {:kv-opts {:mapsize 1}})
        l-dir    (u/tmp-dir (str "big-" (UUID/randomUUID)))
        l-store  (sut/open l-dir)]
    (sut/load-datoms s-store datoms)
    (sut/load-datoms l-store datoms)
    (is (= (count datoms)
           (sut/datom-count s-store :eav)
           (sut/datom-count l-store :eav)))
    (is (= (sut/classes s-store) (sut/classes l-store)))
    (is (= (sut/rclasses s-store) (sut/rclasses l-store)))
    (is (= (sut/entities s-store) (sut/entities l-store)))
    (is (= (sut/rentities s-store) (sut/rentities l-store)))
    (is (= (sut/links s-store) (sut/links l-store)))
    (is (= (sut/max-cid s-store) (sut/max-cid l-store)))))
