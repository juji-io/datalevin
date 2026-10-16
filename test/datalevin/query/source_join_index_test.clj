(ns datalevin.query.source-join-index-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.join :as join]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source-test :refer [error-data]])
  (:import [java.util Map]))

(deftest retained-join-indexes-preserve-keys-and-buckets
  (let [captured (atom nil)
        tables [[{'?a 1.0M '?b nil '?tag :first}
                 {'?a 1.0M '?b nil '?tag :second}
                 {'?a 1.00M '?b nil '?tag :other}
                 {'?a 1.0M '?b "present" '?tag :present}]
                [{'?tag :first '?c 1} {'?tag :second '?c 2}]
                [{'?independent :a} {'?independent :b}]]]
    (rt/with-runtime {}
      (fn [runtime]
        (let [[composite single cross :as steps]
              (join/prepare! runtime {'?a 1.0M '?b nil} tables)]
          (reset! captured [runtime steps])
          (is (= (take 2 (first tables))
                 (vec (join/candidates composite {'?a 1.0M '?b nil}))))
          (is (= [(nth (first tables) 2)]
                 (vec (join/candidates composite {'?a 1.00M '?b nil}))))
          (is (nil? (join/candidates composite {'?a 1M '?b nil})))
          (is (= [{'?tag :second '?c 2}]
                 (vec (join/candidates single {'?tag :second}))))
          (is (identical? (last tables) (join/candidates cross {})))
          (is (pos? (long (:retained-bytes (control/usage (:control runtime)))))))))
    (let [[runtime steps] @captured]
      (is (every? zero? (vals (control/usage (:control runtime)))))
      (doseq [step (butlast steps)] (is (.isEmpty ^Map (:index step)))))))

(deftest index-quota-rejects-before-hashing
  (let [hashes (atom 0) captured (atom nil)
        key (reify Object (hashCode [_] (swap! hashes inc) 1))
        error (error-data
                #(rt/with-runtime {:limits {:retained-bytes 4000}}
                   (fn [runtime]
                     (reset! captured runtime)
                     (join/prepare! runtime {'?key key}
                                    [(vec (repeat 10 {'?key key}))]))))]
    (is (= :query-source/limit (:error error)))
    (is (= :retained-bytes (:category error)))
    (is (zero? @hashes))
    (is (every? zero? (vals (control/usage (:control @captured)))))))

(deftest partial-index-build-observes-cancellation-and-releases-state
  (let [hashes (atom 0) captured (atom nil)
        error (error-data
                #(rt/with-runtime {}
                   (fn [runtime]
                     (reset! captured runtime)
                     (let [key (reify Object
                                 (hashCode [_]
                                   (when (= 3 (swap! hashes inc))
                                     (control/cancel! (:control runtime)
                                       {:error :query-source/cancelled :phase :execution}))
                                   1))]
                       (join/prepare! runtime {'?key key}
                                      [(vec (repeat 100 {'?key key}))])))))]
    (is (= :query-source/cancelled (:error error)))
    (is (<= 3 (long @hashes) 4))
    (is (empty? @(:cleanup @captured)))
    (is (every? zero? (vals (control/usage (:control @captured)))))))

(deftest failed-probe-clears-index-and-ledger
  (let [captured (atom nil)
        key (reify Object (hashCode [_] (throw (Exception. "private key"))))
        error (error-data
                #(rt/with-runtime {}
                   (fn [runtime]
                     (let [step (first (join/prepare! runtime {'?a 1 '?b 2}
                                        [[{'?a 1 '?b 2}]]))]
                       (reset! captured [runtime step])
                       (join/candidates step {'?a key '?b 2})))))]
    (is (= :query-source/provider (:error error)))
    (is (not (.contains (pr-str error) "private key")))
    (let [[runtime step] @captured]
      (is (.isEmpty ^Map (:index step)))
      (is (every? zero? (vals (control/usage (:control runtime))))))))
