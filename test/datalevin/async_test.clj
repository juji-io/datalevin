(ns datalevin.async-test
  (:require
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.test :refer [deftest is are use-fixtures testing]]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.clojure-test :as test]
   [clojure.test.check.properties :as prop]
   [datalevin.core :as d]
   [datalevin.async :as a]
   )
  (:import
   [datalevin.async IAsyncWork]
   [java.util UUID]))

(use-fixtures :each db-fixture)

(defrecord Work1 [num]
  IAsyncWork
  (work-key [_] :work1)
  (do-work [_]  num)
  (pre-batch [_])
  (post-batch [_])
  (batch-size [_] 3))

(defrecord Work2 [num]
  IAsyncWork
  (work-key [_] :work2)
  (do-work [_]  num)
  (pre-batch [_])
  (post-batch [_])
  (batch-size [_] 5))

(defrecord Work3 [num]
  IAsyncWork
  (work-key [_] :work3)
  (do-work [_]  num)
  (pre-batch [_])
  (post-batch [_])
  (batch-size [_] 10))

(deftest async-setup-test
  (let [executor (a/new-async-executor)
        res1     (shuffle (range 3000))
        res2     (shuffle (range 2000))
        res3     (shuffle (range 1000))]
    (a/start executor)
    (let [futs1 (pmap #(a/exec executor (Work1. %)) res1)
          futs2 (pmap #(a/exec executor (Work2. %)) res2)
          futs3 (pmap #(a/exec executor (Work3. %)) res3)]
      (is (= res1 (mapv #(deref %) futs1)))
      (is (= res2 (mapv #(deref %) futs2)))
      (is (= res3 (mapv #(deref %) futs3))))
    (a/stop executor)))
