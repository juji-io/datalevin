(ns datalevin.async-test
  (:require
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.test :refer [deftest is are use-fixtures testing]]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.clojure-test :as test]
   [clojure.test.check.properties :as prop]
   [datalevin.core :as d]
   [datalevin.util :as u]
   [datalevin.async :as a])
  (:import
   [datalevin.async IAsyncWork WorkItem]
   [java.util UUID]))

(use-fixtures :each db-fixture)

(defrecord Work1 [num]
  IAsyncWork
  (work-key [_] :work1)
  (do-work [_]  num)
  (combine [_] nil)
  (callback [_] nil))

(defrecord Work2 [num]
  IAsyncWork
  (work-key [_] :work2)
  (do-work [_]  num)
  (combine [_] nil)
  (callback [_] nil))

(defrecord Work3 [num]
  IAsyncWork
  (work-key [_] :work3)
  (do-work [_]  num)
  (combine [_] nil)
  (callback [_] nil))

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
  (combine [_] nil)
  (callback [_] nil))

(deftest exception-test
  (let [executor (a/get-executor)
        fut1     (a/exec executor (ErrWork.))]
    (is (thrown? Exception (deref fut1)))
    (a/shutdown-executor)))

(defn- last-combine [coll] (last coll))

(defrecord LastCombineWork [num]
  IAsyncWork
  (work-key [_] :last-combine-work)
  (do-work [_] num)
  (combine [_] last-combine)
  (callback [_] nil))

(deftest last-combine-work-test
  (let [executor   (a/get-executor)
        input      (vec (shuffle (range 1000)))
        last-input (last input)
        futs       (mapv #(a/exec executor (LastCombineWork. %)) input)
        res        (mapv #(deref %) futs)
        indices    (conj (->> (for [d (distinct res)] (u/index-of #{d} res))
                              (drop 1)
                              (map dec)
                              vec)
                         (dec (count res)))]
    (is (not= input res))
    (is (< (count (distinct res)) (count (distinct input))))
    (is (= last-input (peek res)))
    (is (every? (fn [[i r]] (= i r))
                (map (fn [x y] [x y])
                     (for [i indices] (get input i))
                     (for [i indices] (get res i)))))
    (a/shutdown-executor)))

(declare concat-combine)

(defrecord ConcatCombineWork [v]
  IAsyncWork
  (work-key [_] :combine-work)
  (do-work [_] v)
  (combine [_] concat-combine)
  (callback [_] nil))

(defn- concat-combine
  [coll]
  (assoc (first coll) :v
         (into [] (comp (map #(.-v ^ConcatCombineWork %)) cat) coll)))

(deftest concat-combine-work-test
  (let [executor (a/get-executor)
        get-v    #(vec (shuffle (range (inc ^int (rand-int %)))))
        input    (mapv get-v (range 2 10))
        futs     (mapv #(a/exec executor (ConcatCombineWork. %)) input)
        res      (mapv #(deref %) futs)
        dist-res (->> res
                      (partition-by identity)
                      (mapv first)
                      flatten)]
    (is (not= input res))
    (is (= (flatten input) dist-res))
    (a/shutdown-executor)))

(defrecord CallbackWork [num cb]
  IAsyncWork
  (work-key [_] :cb-work)
  (do-work [_] num)
  (combine [_] nil)
  (callback [_] cb))

(deftest same-callback-test
  (let [executor (a/get-executor)
        input    (vec (shuffle (range 100)))
        sum      (atom 0)
        cb       (fn [x] (swap! sum #(+ ^long % ^long x)))
        futs     (mapv #(a/exec executor (CallbackWork. % cb)) input)]
    (is (= input (mapv deref futs)))
    (is (= @sum 4950))
    (a/shutdown-executor)))

(deftest diff-callback-test
  (let [executor (a/get-executor)
        total    (atom 0)
        input    (range 100)
        cb1      (fn [_] (swap! total inc))
        cb2      (fn [_] (swap! total dec))
        futs     (map #(a/exec executor (CallbackWork. %1 %2))
                      input (interleave (repeat 100 cb1) (repeat 100 cb2)))]
    (is (= input (mapv deref futs)))
    (is (= @total 0))
    (a/shutdown-executor)))
