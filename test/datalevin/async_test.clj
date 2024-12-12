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
  (batch-limit [_] 3))

(defrecord Work2 [num]
  IAsyncWork
  (work-key [_] :work2)
  (do-work [_]  num)
  (pre-batch [_])
  (post-batch [_])
  (batch-limit [_] 5))

(defrecord Work3 [num]
  IAsyncWork
  (work-key [_] :work3)
  (do-work [_]  num)
  (pre-batch [_])
  (post-batch [_])
  (batch-limit [_] 10))

(deftest basic-async-setup-test
  (let [executor (a/get-executor)
        res1     (shuffle (range 3000))
        res2     (shuffle (range 2000))
        res3     (shuffle (range 1000))]
    (let [futs1 (pmap #(a/exec executor (Work1. %)) res1)
          futs2 (pmap #(a/exec executor (Work2. %)) res2)
          futs3 (pmap #(a/exec executor (Work3. %)) res3)]
      (is (= res1 (mapv #(deref %) futs1)))
      (is (= res2 (mapv #(deref %) futs2)))
      (is (= res3 (mapv #(deref %) futs3))))
    (a/shutdown-executor)))

(defrecord ErrWork []
  IAsyncWork
  (work-key [_] :err-work)
  (do-work [_]  (/ 1 0))
  (pre-batch [_])
  (post-batch [_])
  (batch-limit [_] 10))

(defrecord ErrPreWork []
  IAsyncWork
  (work-key [_] :err-pre-work)
  (do-work [_] :something)
  (pre-batch [_] (/ 1 0))
  (post-batch [_])
  (batch-limit [_] 10))

(deftest exception-test
  (let [executor (a/get-executor)
        fut1     (a/exec executor (ErrWork.))
        fut2     (a/exec executor (ErrPreWork.))]
    (is (thrown? Exception (deref fut1)))
    (is (= :something (deref fut2)))
    (a/shutdown-executor)))
