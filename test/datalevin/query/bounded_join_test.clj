(ns datalevin.query.bounded-join-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.join :as join]
            [datalevin.relation :as relation])
  (:import [java.math BigDecimal BigInteger]
           [java.util Random]))

(defn- rel [variables rows]
  (relation/relation! (zipmap variables (range)) (mapv object-array rows)))

(defn- budget [maximum check!]
  (let [active (atom {}) requests (atom []) retained (atom #{}) next-id (atom 0)]
    {:active active :requests requests :retained retained
     :hooks {:check! check!
             :reserve! (fn [rows bytes]
                         (swap! requests conj [rows bytes])
                         (when (> (+' bytes (reduce +' 0 (vals @active))) maximum)
                           (throw (ex-info "quota" {:error :query-source/limit})))
                         (let [id (swap! next-id inc)] (swap! active assoc id bytes) id))
             :retain! #(swap! retained conj %)
             :release! #(swap! active dissoc %)}}))

(deftest exact-admission-matches-shared-join-key-and-bag-semantics
  (let [random (Random. 271828)
        key-values [nil 1 1N (BigInteger. "1") 2 (BigDecimal. "1.0") (BigDecimal. "1.00")
              0.0 -0.0 Double/NaN (float 1) "one" :one (byte-array [1]) (byte-array [1])
              [(byte-array [1])] {:id (byte-array [2])}]]
    (dotimes [case-index 100]
      (let [row #(vector (nth key-values (.nextInt random (count key-values))) (.nextInt random 3)
                        (.nextInt random 5))
            multiple? (even? case-index)
            a (rel ['?a '?b '?left] (repeatedly (+ 1 (.nextInt random 20)) row))
            b (rel (if multiple? ['?a '?b '?right] ['?a '?extra '?right])
                   (repeatedly (+ 1 (.nextInt random 20)) row))
            expected (join/hash-join a b)
            {:keys [hooks active requests retained]} (budget 10000000 (fn []))
            actual (join/hash-join-bounded a b hooks)]
        (is (= (:attrs expected) (:attrs actual)))
        (is (= (frequencies (map vec (:tuples expected))) (frequencies (map vec (:tuples actual)))))
        (is (= (count (:tuples actual)) (first (last @requests))))
        (is (= 1 (count @active)))
        (is (= (set (keys @active)) @retained))))))

(deftest Cartesian-products-and-zero-column-relations-are-admitted-exactly
  (doseq [[left right] [[(rel [] [[] []]) (rel [] [[] [] []])]
                        [(rel ['?x] [[1] [1]]) (rel ['?y] [[:a] [:b]])]
                        [(rel ['?x] []) (rel ['?y] [[:a]])]
                        [(rel [] [[]]) (relation/relation! {} nil)]]]
    (let [{:keys [hooks requests]} (budget 1000000 (fn []))
          actual (join/hash-join-bounded left right hooks)
          expected (join/hash-join left right)]
      (is (= (frequencies (map vec (:tuples expected))) (frequencies (map vec (:tuples actual)))))
      (is (= (count (:tuples actual)) (first (last @requests)))))))

(deftest output-capacity-is-rejected-before-materializing-a-many-to-many-join
  (let [left (rel ['?x '?left] (map #(vector 1 %) (range 100)))
        right (rel ['?x '?right] (map #(vector 1 %) (range 100)))
        {:keys [hooks active retained requests]} (budget 100000 (fn []))
        failure (try (join/hash-join-bounded left right hooks) nil
                     (catch clojure.lang.ExceptionInfo e (ex-data e)))]
    (is (= :query-source/limit (:error failure)))
    (is (= 10000 (first (last @requests))))
    (is (empty? @active))
    (is (empty? @retained))))

(deftest cancellation-during-index-construction-unwinds-before-output-reservation
  (let [checks (atom 0)
        {:keys [hooks active retained]} (budget 1000000
                                         #(when (= 20 (swap! checks inc))
                                            (throw (ex-info "cancelled" {:error :query-source/cancelled}))))
        failure (try (join/hash-join-bounded (rel ['?x] (map vector (range 100)))
                                            (rel ['?x] (map vector (range 100))) hooks) nil
                     (catch clojure.lang.ExceptionInfo e (ex-data e)))]
    (is (= :query-source/cancelled (:error failure)))
    (is (empty? @active))
    (is (empty? @retained))))
