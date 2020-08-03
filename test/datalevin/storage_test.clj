(ns datalevin.storage-test
  (:require [datalevin.storage :as sut]
            [datalevin.bits :as b]
            [datalevin.constants :as c]
            [datalevin.datom :as d]
            [clojure.set :as set]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.clojure-test :as test]
            [clojure.test.check.properties :as prop]
            [clojure.test :refer [deftest is use-fixtures]]
            [datalevin.lmdb :as lmdb])
  (:import [java.util UUID]
           [datalevin.storage Store]
           [datalevin.lmdb LMDB]
           [datalevin.datom Datom]))

(def ^:dynamic ^Store store nil)

(defn store-test-fixture
  [f]
  (let [dir (str "/tmp/store-test-" (UUID/randomUUID))]
    (with-redefs [store (sut/open nil dir)]
      (f)
      (sut/close store)
      (b/delete-files dir))))

(use-fixtures :each store-test-fixture)

(deftest basic-ops-test
  (is (= c/gt0 (sut/max-gt store)))
  (is (= 1 (sut/max-aid store)))
  (is (= c/implicit-schema (sut/schema store)))
  (is (= c/e0 (sut/init-max-eid store)))
  (let [a   :a/b
        v   (UUID/randomUUID)
        d   (d/datom c/e0 a v)
        s   (assoc (sut/schema store) a {:db/aid 1})
        b   :b/c
        p1  {:db/valueType :db.type/uuid}
        v1  (UUID/randomUUID)
        d1  (d/datom c/e0 b v1)
        s1  (assoc s b (merge p1 {:db/aid 2}))
        c   :c/d
        p2  {:db/valueType :db.type/ref}
        v2  (long (rand c/emax))
        d2  (d/datom c/e0 c v2)
        s2  (assoc s1 c (merge p2 {:db/aid 3}))
        dir (.-dir ^LMDB (.-lmdb store))]
    (sut/load-datoms store [d])
    (is (= s (sut/schema store)))
    (is (= 1 (sut/datom-count store :eav)))
    (is (= 1 (sut/datom-count store :aev)))
    (is (= 1 (sut/datom-count store :ave)))
    (is (= 0 (sut/datom-count store :vae)))
    (is (= [d] (sut/fetch store d)))
    (is (= [d] (sut/slice store :eav d d)))
    (is (= true (sut/populated? store :eav d d)))
    (is (= 1 (sut/size store :eav d d)))
    (is (= d (sut/head store :eav d d)))
    (sut/swap-attr store b merge p1)
    (sut/load-datoms store [d1])
    (is (= s1 (sut/schema store)))
    (is (= 2 (sut/datom-count store :eav)))
    (is (= 2 (sut/datom-count store :aev)))
    (is (= 2 (sut/datom-count store :ave)))
    (is (= 0 (sut/datom-count store :vae)))
    (is (= [] (sut/slice store :eav d (d/datom c/e0 :non-exist v1))))
    (is (= 0 (sut/size store :eav d (d/datom c/e0 :non-exist v1))))
    (is (nil? (sut/populated? store :eav d (d/datom c/e0 :non-exist v1))))
    (is (= d (sut/head store :eav d d1)))
    (is (= 2 (sut/size store :eav d d1)))
    (is (= [d d1] (sut/slice store :eav d d1)))
    (is (= [d d1] (sut/slice store :aev d d1)))
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
    (is (= [d] (sut/slice-filter store :eav
                                 (fn [^Datom d] (= v (.-v d)))
                                 (d/datom c/e0 nil nil)
                                 (d/datom c/e0 nil nil))))
    (is (= [d1 d] (sut/rslice store :aev d1 d)))
    (is (= [d d1] (sut/slice store :aev
                             (d/datom c/e0 a nil)
                             (d/datom c/e0 nil nil))))
    (is (= [d1 d] (sut/rslice store :aev
                              (d/datom c/e0 b nil)
                              (d/datom c/e0 nil nil))))
    (is (= [d] (sut/slice-filter store :aev
                                 (fn [^Datom d] (= v (.-v d)))
                                 (d/datom c/e0 nil nil)
                                 (d/datom c/e0 nil nil))))
    (sut/swap-attr store c merge p2)
    (sut/load-datoms store [d2])
    (is (= s2 (sut/schema store)))
    (is (= 3 (sut/datom-count store c/eav)))
    (is (= 3 (sut/datom-count store c/aev)))
    (is (= 3 (sut/datom-count store c/ave)))
    (is (= 1 (sut/datom-count store c/vae)))
    (is (= [d2] (sut/slice store :vae
                           (d/datom c/e0 nil v2)
                           (d/datom c/emax nil v2))))
    (sut/load-datoms store [(d/delete d)])
    (is (= 2 (sut/datom-count store c/eav)))
    (is (= 2 (sut/datom-count store c/aev)))
    (is (= 2 (sut/datom-count store c/ave)))
    (is (= 1 (sut/datom-count store c/vae)))
    (sut/close store)
    (is (sut/closed? store))
    (let [store (sut/open nil dir)]
      (is (= [d1] (sut/slice store :eav d1 d1)))
      (sut/load-datoms store [(d/delete d1)])
      (is (= 1 (sut/datom-count store c/eav)))
      (sut/load-datoms store [d d1])
      (is (= 3 (sut/datom-count store c/eav))))
    (sut/close store)
    (let [d     :d/e
          p3    {:db/valueType :db.type/long}
          s3    (assoc s2 d (merge p3 {:db/aid 4}))
          s4    (assoc s3 :f/g {:db/aid 5 :db/valueType :db.type/string})
          store (sut/open {d p3} dir)]
      (is (= s3 (sut/schema store)))
      (sut/set-schema store {:f/g {:db/valueType :db.type/string}})
      (is (= s4 (sut/schema store))))))

(deftest schema-test
  (let [s     {:a {:db/valueType :db.type/string}
               :b {:db/valueType :db.type/long}}
        dir   (str "/tmp/datalevin-schema-test-" (UUID/randomUUID))
        store (sut/open s dir)
        s1    (sut/schema store)]
    (sut/close store)
    (is (sut/closed? store))
    (let [store (sut/open s dir)]
      (is (= s1 (sut/schema store))))
    ))
