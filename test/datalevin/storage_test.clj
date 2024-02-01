(ns datalevin.storage-test
  (:require
   [datalevin.storage :as sut]
   [datalevin.util :as u]
   [datalevin.constants :as c]
   [datalevin.datom :as d]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.clojure-test :as test]
   [clojure.test.check.properties :as prop]
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.test :refer [deftest testing is use-fixtures]]
   [datalevin.lmdb :as lmdb]
   [clojure.string :as s])
  (:import
   [java.util UUID]
   [datalevin.storage Store]
   [datalevin.datom Datom]))

(use-fixtures :each db-fixture)

(deftest basic-ops-test
  (let [dir   (u/tmp-dir (str "storage-test-" (UUID/randomUUID)))
        store (sut/open
                dir {}
                {:kv-opts {:flags (conj c/default-env-flags :nosync)}})]
    (is (= c/g0 (sut/max-gt store)))
    (is (= 3 (sut/max-aid store)))
    (is (= (merge c/entity-time-schema c/implicit-schema)
           (sut/schema store)))
    (is (= c/e0 (sut/init-max-eid store)))
    (is (= c/tx0 (sut/max-tx store)))
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
      (is (= (inc c/tx0) (sut/max-tx store)))
      (is (<= t1 (sut/last-modified store)))
      (is (= s (sut/schema store)))
      (is (= 1 (sut/datom-count store :eav)))
      (is (= 1 (sut/datom-count store :ave)))
      (is (= 0 (sut/datom-count store :vae)))
      (is (= [d] (sut/fetch store d)))
      (is (= [d] (sut/slice store :eav d d)))
      (is (sut/populated? store :eav d d))
      (is (= 1 (sut/size store :eav d d)))
      (is (= 1 (sut/e-size store c/e0)))
      (is (= 1 (sut/a-size store a)))
      (is (= d (sut/head store :eav d d)))
      (is (= d (sut/tail store :eav d d)))
      (sut/swap-attr store b merge p1)
      (sut/load-datoms store [d1])
      (is (= (+ 2 c/tx0) (sut/max-tx store)))
      (is (= s1 (sut/schema store)))
      (is (= 2 (sut/datom-count store :eav)))
      (is (= 2 (sut/datom-count store :ave)))
      (is (= 0 (sut/datom-count store :vae)))
      (is (= [] (sut/slice store :eav d (d/datom c/e0 :non-exist v1))))
      (is (= 0 (sut/size store :eav d (d/datom c/e0 :non-exist v1))))
      (is (nil? (sut/populated? store :eav d (d/datom c/e0 :non-exist v1))))
      (is (= d (sut/head store :eav d d1)))
      (is (= d1 (sut/tail store :eav d1 d)))
      (is (= 2 (sut/size store :eav d d1)))
      (is (= 2 (sut/e-size store c/e0)))
      (is (= 1 (sut/a-size store b)))
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
                                (fn [^Datom d] (when (= v (.-v d)) d))
                                (d/datom c/e0 nil nil)
                                (d/datom c/e0 nil nil))))
      (is (= d (sut/tail-filter store :eav
                                (fn [^Datom d]
                                  (when (= v (.-v d)) d))
                                (d/datom c/e0 nil nil)
                                (d/datom c/e0 nil nil))))
      (is (= [d] (sut/slice-filter store :eav
                                   (fn [^Datom d]
                                     (when (= v (.-v d)) d))
                                   (d/datom c/e0 nil nil)
                                   (d/datom c/e0 nil nil))))
      (is (= [c/e0] (map #(aget ^objects % 0)
                         (sut/ave-tuples store a nil nil (constantly true)))))
      (is (= [c/e0] (map #(aget ^objects % 0)
                         (sut/ave-tuples store b v1 v1))))
      (is (= [d1 d] (sut/rslice store :ave d1 d)))
      (is (= [d d1] (sut/slice store :ave
                               (d/datom c/e0 a nil)
                               (d/datom c/e0 nil nil))))
      (is (= [d1 d] (sut/rslice store :ave
                                (d/datom c/e0 b nil)
                                (d/datom c/e0 nil nil))))
      (is (= [d] (sut/slice-filter store :ave
                                   (fn [^Datom d]
                                     (when (= v (.-v d)) d))
                                   (d/datom c/e0 nil nil)
                                   (d/datom c/e0 nil nil))))
      (sut/swap-attr store c merge p2)
      (sut/load-datoms store [d2])
      (is (= (+ 3 c/tx0) (sut/max-tx store)))
      (is (= s2 (sut/schema store)))
      (is (= 3 (sut/datom-count store c/eav)))
      (is (= 3 (sut/datom-count store c/ave)))
      (is (= 1 (sut/datom-count store c/vae)))
      (is (= 3 (sut/e-size store c/e0)))
      (is (= 1 (sut/a-size store c)))
      (is (= 1 (sut/v-size store v2)))
      (is (= [d2] (sut/slice store :vae
                             (d/datom c/e0 c v2)
                             (d/datom c/emax c v2))))
      (sut/load-datoms store [(d/delete d)])
      (is (= (+ 4 c/tx0) (sut/max-tx store)))
      (is (= 2 (sut/datom-count store c/eav)))
      (is (= 2 (sut/datom-count store c/ave)))
      (is (= 1 (sut/datom-count store c/vae)))
      (sut/close store)
      (is (sut/closed? store))
      (let [store (sut/open dir {}
                            {:kv-opts {:flags (conj c/default-env-flags :nosync)}})]
        (is (= (+ 4 c/tx0) (sut/max-tx store)))
        (is (= [d1] (sut/slice store :eav d1 d1)))
        (sut/load-datoms store [(d/delete d1)])
        (is (= (+ 5 c/tx0) (sut/max-tx store)))
        (is (= 1 (sut/datom-count store c/eav)))
        (sut/load-datoms store [d d1])
        (is (= (+ 6 c/tx0) (sut/max-tx store)))
        (is (= 3 (sut/datom-count store c/eav)))
        (sut/close store))
      (let [d     :d/e
            p3    {:db/valueType :db.type/long}
            s3    (assoc s2 d (merge p3 {:db/aid 6}))
            s4    (assoc s3 :f/g {:db/aid 7 :db/valueType :db.type/string})
            store (sut/open dir {d p3}
                            {:kv-opts {:flags (conj c/default-env-flags :nosync)}})]
        (is (= (+ 6 c/tx0) (sut/max-tx store)))
        (is (= s3 (sut/schema store)))
        (sut/set-schema store {:f/g {:db/valueType :db.type/string}})
        (is (= s4 (sut/schema store)))
        (sut/close store)))
    (u/delete-files dir)))

(deftest schema-test
  (let [s     {:a {:db/valueType :db.type/string}
               :b {:db/valueType :db.type/long}}
        dir   (u/tmp-dir (str "datalevin-schema-test-" (UUID/randomUUID)))
        store (sut/open
                dir s
                {:kv-opts {:flags (conj c/default-env-flags :nosync)}})
        s1    (sut/schema store)]
    (sut/close store)
    (is (sut/closed? store))
    (let [store (sut/open dir s)]
      (is (= s1 (sut/schema store)))
      (sut/close store))
    (u/delete-files dir)))

(deftest giants-string-test
  (let [schema {:a {:db/valueType :db.type/string}}
        dir    (u/tmp-dir (str "datalevin-giants-str-test-" (UUID/randomUUID)))
        store  (sut/open
                 dir schema
                 {:kv-opts {:flags (conj c/default-env-flags :nosync)}})
        v      (apply str (repeat 100 (UUID/randomUUID)))
        d      (d/datom c/e0 :a v)]
    (sut/load-datoms store [d])
    (is (= [d] (sut/fetch store d)))
    (is (= [d] (sut/slice store :eav
                          (d/datom c/e0 :a c/v0)
                          (d/datom c/e0 :a c/vmax))))
    (sut/close store)
    (u/delete-files dir)))

(deftest giants-data-test
  (let [dir   (u/tmp-dir (str "datalevin-giants-data-test-" (UUID/randomUUID)))
        store (sut/open
                dir nil
                {:kv-opts {:flags (conj c/default-env-flags :nosync)}})
        v     (apply str (repeat 100 (UUID/randomUUID)))
        d     (d/datom c/e0 :a v)
        d1    (d/datom (inc c/e0) :b v)]
    (sut/load-datoms store [d])
    (is (= [d] (sut/fetch store d)))
    (is (= [d] (sut/slice store :eav
                          (d/datom c/e0 :a c/v0)
                          (d/datom c/e0 :a c/vmax))))
    (sut/close store)
    (let [store' (sut/open dir nil
                           {:kv-opts {:flags (conj c/default-env-flags :nosync)}})]
      (is (sut/populated? store' :eav
                          (d/datom c/e0 :a c/v0)
                          (d/datom c/e0 :a c/vmax)))
      (is (= [d] (sut/fetch store' d)))
      (is (= [d] (sut/slice store' :eav
                            (d/datom c/e0 :a c/v0)
                            (d/datom c/e0 :a c/vmax))))
      (sut/load-datoms store' [d1])
      (is (= 1 (sut/init-max-eid store')))
      (is (= [d1] (sut/fetch store' d1)))
      (sut/close store'))
    (u/delete-files dir)))

(deftest normal-data-test
  (let [dir   (u/tmp-dir (str "datalevin-normal-data-test-" (UUID/randomUUID)))
        store (sut/open
                dir nil
                {:kv-opts {:flags (conj c/default-env-flags :nosync)}})
        v     (UUID/randomUUID)
        d     (d/datom c/e0 :a v)
        d1    (d/datom (inc c/e0) :b v)]
    (sut/load-datoms store [d])
    (is (= [d] (sut/fetch store d)))
    (is (= [d] (sut/slice store :eav
                          (d/datom c/e0 :a c/v0)
                          (d/datom c/e0 :a c/vmax))))
    (sut/close store)

    (let [store' (sut/open dir nil
                           {:kv-opts {:flags (conj c/default-env-flags :nosync)}})]
      (is (sut/populated? store' :eav
                          (d/datom c/e0 :a c/v0)
                          (d/datom c/e0 :a c/vmax)))
      (is (= [d] (sut/fetch store' d)))
      (is (= [d] (sut/slice store' :eav
                            (d/datom c/e0 :a c/v0)
                            (d/datom c/e0 :a c/vmax))))
      (sut/load-datoms store' [d1])
      (is (= 1 (sut/init-max-eid store')))
      (is (= [d1] (sut/fetch store' d1)))
      (sut/close store))
    (u/delete-files dir)))

(deftest false-value-test
  (let [d     (d/datom c/e0 :a false)
        dir   (u/tmp-dir (str "storage-test-" (UUID/randomUUID)))
        store (sut/open dir nil
                        {:kv-opts {:flags (conj c/default-env-flags :nosync)}})]
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
          store (sut/open dir {}
                          {:kv-opts
                           {:flags (conj c/default-env-flags :nosync)}})
          _     (sut/load-datoms store [d])
          r     (sut/fetch store d)]
      (sut/close store)
      (u/delete-files dir)
      (is (= [d] r)))))

(deftest eav-scan-v-test
  (let [d0      (d/datom 0 :a 10)
        d1      (d/datom 5 :a 1)
        d2      (d/datom 5 :b "5b")
        d3      (d/datom 8 :a 7)
        d4      (d/datom 8 :b "8b")
        d5      (d/datom 10 :b "10b")
        d6      (d/datom 10 :c :c10)
        d7      (d/datom 10 :d "Tom")
        d8      (d/datom 10 :d "Jerry")
        d9      (d/datom 10 :d "Mick")
        d10     (d/datom 10 :e "nice")
        d11     (d/datom 10 :e "good")
        d12     (d/datom 12 :f 2.2)
        tuples0 [(object-array [0]) (object-array [5]) (object-array [8])]
        tuples1 [(object-array [:none 0])
                 (object-array [:nada 8])
                 (object-array [:zero 10])]
        tuples2 [(object-array [8]) (object-array [5]) (object-array [8])]
        tuples3 [(object-array [10]) (object-array [5]) (object-array [10])]
        tuples4 [(object-array [10]) (object-array [0])]
        dir     (u/tmp-dir (str "storage-test-" (UUID/randomUUID)))
        store   (sut/open dir
                          {:a {}
                           :b {:db/valueType   :db.type/string
                               :db/cardinality :db.cardinality/many}
                           :c {:db/valueType :db.type/keyword}
                           :d {:db/cardinality :db.cardinality/many
                               :db/valueType   :db.type/string}
                           :e {:db/cardinality :db.cardinality/many
                               :db/valueType   :db.type/string}}
                          {:kv-opts
                           {:flags (conj c/default-env-flags :nosync)}})]
    (sut/load-datoms store [d0 d1 d2 d3 d4 d5 d6 d7 d8 d9 d10 d11 d12])
    (is (= [[5 1 "5b"] [8 7 "8b"]]
           (mapv vec (sut/eav-scan-v store tuples0 0 [:b :a]
                                     [(constantly true)
                                      (constantly true)]))))
    (is (= [[5 1 "5b"] [8 7 "8b"]]
           (mapv vec (sut/eav-scan-v store tuples0 0 [:b :a] [nil nil]))))
    (is (= [[5 1 "5b"] [8 7 "8b"]]
           (mapv vec (sut/eav-scan-v store tuples0 0 [:a :b]
                                     [odd? #(s/ends-with? % "b")]))))
    (is (= [[5 1 "5b"] [8 7 "8b"]]
           (mapv vec (sut/eav-scan-v store tuples0 0 [:a :b] [odd? nil]))))
    (is (= [[5 1 "5b"] [8 7 "8b"]]
           (mapv vec (sut/eav-scan-v store tuples0 0 [:b :a]
                                     [(constantly true) odd?]))))
    (is (= []
           (mapv vec (sut/eav-scan-v store tuples0 0 [:b :a]
                                     [(constantly true) even?]))))
    (is (= [[0 10]]
           (mapv vec (sut/eav-scan-v store tuples0 0 [:a] [even?]))))
    (is (= [[:none 0 10]]
           (mapv vec (sut/eav-scan-v store tuples1 1 [:a] [even?]))))
    (is (= [[:none 0 10] [:nada 8 7]]
           (mapv vec (sut/eav-scan-v store tuples1 1 [:a] [nil]))))
    (is (= [[:zero 10 "10b" :c10]]
           (mapv vec (sut/eav-scan-v store tuples1 1 [:b :c] [nil nil]))))
    (is (= [[:zero 10 "Jerry"] [:zero 10 "Mick"] [:zero 10 "Tom"]]
           (mapv vec (sut/eav-scan-v store tuples1 1 [:d] [nil]))))
    (is (= [[:zero 10 "Tom"]]
           (mapv vec (sut/eav-scan-v store tuples1 1 [:d] [#(< (count %) 4)]))))
    (is (= [[:zero 10 :c10 "Tom"]]
           (mapv vec (sut/eav-scan-v store tuples1 1 [:c :d]
                                     [nil #(< (count %) 4)]))))
    (is (= [[8 7 "8b"] [5 1 "5b"] [8 7 "8b"]]
           (mapv vec (sut/eav-scan-v store tuples2 0 [:b :a] [nil nil]))))
    (is (= [[10 :c10 "Jerry"] [10 :c10 "Mick"] [10 :c10 "Tom"]
            [10 :c10 "Jerry"] [10 :c10 "Mick"] [10 :c10 "Tom"]]
           (mapv vec (sut/eav-scan-v store tuples3 0 [:c :d] [nil nil]))))
    (is (= [[10 "Jerry" "good"] [10 "Jerry" "nice"]
            [10 "Mick" "good"] [10 "Mick" "nice"]
            [10 "Tom" "good"] [10 "Tom" "nice"]]
           (mapv vec (sut/eav-scan-v store tuples4 0 [:e :d] [nil nil]))))
    (sut/close store)
    (u/delete-files dir)))

(deftest vae-scan-e-test
  (let [d0      (d/datom 0 :a 1)
        d1      (d/datom 5 :a 1)
        d2      (d/datom 5 :b 2)
        d3      (d/datom 8 :a 7)
        d4      (d/datom 8 :b 2)
        d5      (d/datom 8 :a 3)
        tuples0 [(object-array [1]) (object-array [2]) (object-array [3])]
        tuples1 [(object-array [:none 7])
                 (object-array [:nada 2])
                 (object-array [:zero 2])]
        dir     (u/tmp-dir (str "storage-test-" (UUID/randomUUID)))
        store   (sut/open dir
                          {:a {:db/valueType   :db.type/ref
                               :db/cardinality :db.cardinality/many}
                           :b {:db/valueType :db.type/ref}}
                          {:kv-opts
                           {:flags (conj c/default-env-flags :nosync)}})]
    (sut/load-datoms store [d0 d1 d2 d3 d4 d5])
    (is (= []
           (mapv vec (sut/vae-scan-e store tuples0 0 :c))))
    (is (= [[1 0] [1 5] [3 8]]
           (mapv vec (sut/vae-scan-e store tuples0 0 :a))))
    (is (= [[2 5] [2 8]]
           (mapv vec (sut/vae-scan-e store tuples0 0 :b))))
    (is (= [[:none 7 8]]
           (mapv vec (sut/vae-scan-e store tuples1 1 :a))))
    (is (= [[:nada 2 5] [:nada 2 8] [:nada 2 5] [:nada 2 8]]
           (mapv vec (sut/vae-scan-e store tuples1 1 :b))))
    (sut/close store)
    (u/delete-files dir)))
