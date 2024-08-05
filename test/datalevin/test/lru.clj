(ns datalevin.test.lru
  (:require
   [clojure.test :as t :refer [is are deftest]]
   [datalevin.util :as u])
  (:import [datalevin.utl LRUCache]))

(deftest test-put
  (let [tgt (System/currentTimeMillis)
        l   (LRUCache. 2 tgt)]
    (is (= (.target l) tgt))
    (is (nil? (.get l :a)))
    (.put l :a 1)
    (is (= (.get l :a) 1))
    (.put l :b 2)
    (is (= (.get l :a) 1))
    (is (= (.get l :b) 2))
    (.put l :c 3)
    (is (nil? (.get l :a))) ;; :a get evicted on third insert
    (is (= (.get l :b) 2))
    (is (= (.get l :c) 3))
    (.put l :b 4)
    (is (= (.get l :b) 4))
    (is (= (.get l :c) 3))
    (.put l :d 5)
    (is (= (.get l :c) 3))
    (is (= (.get l :d) 5))
    (is (nil? (.get l :b))) ;; :b get evicted because :c is accessed more recently
    ))

(deftest test-remove
  (let [l (LRUCache. 2)]
    (is (nil? (.get l :a)))
    (.put l :a 1)
    (is (= (.get l :a) 1))
    (.put l :b 2)
    (is (= (.get l :a) 1))
    (is (= (.get l :b) 2))
    (.remove l :b)
    (is (nil? (.get l :b)))
    (is (= (.get l :a) 1))
    (.put l :b 4)
    (is (= (.get l :b) 4))
    (is (= (.get l :a) 1))
    (.put l :d 5)
    (is (nil? (.get l :b)));; :b get evicted because :a is accessed more recently
    (is (= (.get l :d) 5))))
