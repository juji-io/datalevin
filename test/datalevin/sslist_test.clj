(ns datalevin.sslist-test
  (:require [datalevin.sslist :as sut]
            [datalevin.bits :as b]
            [taoensso.nippy :as nippy]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.clojure-test :as test]
            [clojure.test.check.properties :as prop]
            [clojure.test :refer [deftest is]])
  (:import
   [datalevin.sslist SparseShortArrayList]
   [org.eclipse.collections.impl.list.mutable.primitive ShortArrayList]))

(deftest basic-ops-test
  (let [ssl (sut/sparse-short-arraylist)]
    (sut/set ssl 42 99)
    (sut/set ssl 88 888)
    (sut/set ssl 1000 2)
    (sut/set ssl 2000 0)
    (is (= (seq (.toArray ^ShortArrayList (.-items ^SparseShortArrayList ssl)))
           [99 888 2 0]))
    (is (nil? (sut/get ssl 99)))
    (is (= (sut/get ssl 42) 99))
    (is (= (sut/get ssl 1000) 2))
    (is (= (sut/get ssl 2000) 0))
    (sut/set ssl 1000 3)
    (is (= (sut/get ssl 1000) 3))))

(deftest roundtrip-test
  (let [ssl (sut/sparse-short-arraylist)
        bf  (b/allocate-buffer 16384)]
    (sut/set ssl 42 99)
    (sut/set ssl 88 888)
    (sut/set ssl 1000 2)
    (sut/set ssl 2000 0)
    (b/put-buffer bf ssl)
    (.flip bf)
    (let [ssl1 (b/read-buffer bf)]
      (is (= (sut/get ssl1 42) 99))
      (is (= (sut/get ssl1 1000) 2))
      (is (= (sut/get ssl1 2000) 0)))))
