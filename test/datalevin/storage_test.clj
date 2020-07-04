(ns datalevin.storage-test
  (:require [datalevin.storage :as sut]
            [datalevin.datom :as d]
            [datalevin.bits :as b]
            [taoensso.timbre :as log]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.clojure-test :as test]
            [clojure.test.check.properties :as prop]
            [datalevin.constants :as c]
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
    (with-redefs [store (sut/open dir)]
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
        d   (d/datom c/e0 a v c/tx0)
        s   (assoc (sut/schema store) a {:db/aid 1})
        b   :b/c
        p   {:db/valueType :db.type/uuid
             :db/index     true}
        v1  (UUID/randomUUID)
        d1  (d/datom c/e0 b v1 c/tx0)
        s1  (assoc s b (merge p {:db/aid 2}))
        dir (.-dir ^LMDB (.-lmdb store))]
    (sut/insert store d)
    (is (= s (sut/schema store)))
    (is (= 1 (sut/datom-count store c/eav)))
    (is (= 1 (sut/datom-count store c/aev)))
    (is (= 0 (sut/datom-count store c/ave)))
    (is (= 0 (sut/datom-count store c/vae)))
    (sut/swap-attr store b merge p)
    (sut/insert store d1)
    (is (= s1 (sut/schema store)))
    (is (= 2 (sut/datom-count store c/eav)))
    (is (= 2 (sut/datom-count store c/aev)))
    (is (= 1 (sut/datom-count store c/ave)))
    (is (= 1 (sut/datom-count store c/vae)))
    (is (= [d d1] (sut/slice store :eav d d1)))
    (is (= [d1 d] (sut/rslice store :eav d1 d)))
    (is (= [d d1] (sut/slice store :eav
                             (d/datom c/e0 a nil)
                             (d/datom c/e0 nil nil))))
    (is (= [d1 d] (sut/rslice store :eav
                             (d/datom c/e0 b nil)
                             (d/datom c/e0 nil nil))))
    (is (= [d] (sut/slice-filter store :eav
                                 (fn [^Datom d] (= v (.-v d)))
                                 (d/datom c/e0 nil nil)
                                 (d/datom c/e0 nil nil))))
    (sut/delete store d)
    (is (= 1 (sut/datom-count store c/eav)))
    (is (= 1 (sut/datom-count store c/aev)))
    (is (= 1 (sut/datom-count store c/ave)))
    (is (= 1 (sut/datom-count store c/vae)))
    (sut/close store)
    (let [store (sut/open dir)]
      (is (= [d1] (sut/slice store :eav d1 d1))))))
