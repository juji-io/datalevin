(ns datalevin.async-test
  (:require
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.test :refer [deftest is are use-fixtures testing]]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.clojure-test :as test]
   [clojure.test.check.properties :as prop]
   [datalevin.core :as d]
   [datalevin.util :as u]
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
  (batch-limit [_] 3)
  (last-only? [_] false))

(defrecord Work2 [num]
  IAsyncWork
  (work-key [_] :work2)
  (do-work [_]  num)
  (pre-batch [_])
  (post-batch [_])
  (batch-limit [_] 5)
  (last-only? [_] false))

(defrecord Work3 [num]
  IAsyncWork
  (work-key [_] :work3)
  (do-work [_]  num)
  (pre-batch [_])
  (post-batch [_])
  (batch-limit [_] 10)
  (last-only? [_] false))

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
  (batch-limit [_] 10)
  (last-only? [_] false))

(defrecord ErrPreWork []
  IAsyncWork
  (work-key [_] :err-pre-work)
  (do-work [_] :something)
  (pre-batch [_] (/ 1 0))
  (post-batch [_])
  (batch-limit [_] 10)
  (last-only? [_] false))

(deftest exception-test
  (let [executor (a/get-executor)
        fut1     (a/exec executor (ErrWork.))
        fut2     (a/exec executor (ErrPreWork.))]
    (is (thrown? Exception (deref fut1)))
    (is (= :something (deref fut2)))
    (a/shutdown-executor)))

(defrecord LastWork [num]
  IAsyncWork
  (work-key [_] :last-work)
  (do-work [_] num)
  (pre-batch [_])
  (post-batch [_])
  (batch-limit [_] 10)
  (last-only? [_] true))

(deftest last-work-test
  (let [executor   (a/get-executor)
        input      (vec (shuffle (range 1000)))
        last-input (last input)
        futs       (mapv #(a/exec executor (LastWork. %)) input)
        res        (mapv #(deref %) futs)
        indices    (conj (->> (for [d (distinct res)] (u/index-of #{d} res))
                              (drop 1)
                              (map dec)
                              vec)
                         (dec (count res)))]
    ;; (println "input->" input "res->" re-seq)
    (is (not= input res))
    (is (< (count (distinct res)) (count (distinct input))))
    (is (= last-input (peek res)))
    (is (every? (fn [[i r]] (= i r))
                (map (fn [x y] [x y])
                     (for [i indices] (get input i))
                     (for [i indices] (get res i)))))
    (a/shutdown-executor)))
