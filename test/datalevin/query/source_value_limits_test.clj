(ns datalevin.query.source-value-limits-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.query.source :as source]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.program :as program]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :as value]
            [datalevin.query.source-planning-test :as fixture]))

(defn- error [f] (try (f) nil (catch clojure.lang.ExceptionInfo e (ex-data e))))
(defn- nested [wrap n leaf] (reduce (fn [x _] (wrap x)) leaf (range n)))

(deftest value-walks-reject-deep-inputs-with-a-resource-error
  (doseq [wrap [vector list #(hash-map :value %)]]
    (let [deep (nested wrap 5000 1)]
      (doseq [walk [value/value-charge value/copy-charge value/owned-copy value/portable?
                    value/execution-value! #(value/diagnostic-copy % (fn []))
                    #(value/matches-type? :data %)
                    #(value/disjoint-constant? :data % (fn []))]]
        (is (= :query-source/limit (:error (error #(walk deep)))))))))

(deftest depth-admission-preserves-normal-values-at-the-boundary
  (doseq [leaf [1 1N 1M 1/3 (byte-array [1])]
          wrap [vector list #(hash-map :value %)]]
    (let [x (nested wrap 128 leaf)]
      (is (pos? (value/copy-charge x)))
      (is (= (value/copy-charge x) (value/value-charge (value/owned-copy x)))))))

(deftest metadata-and-retained-vector-parents-obey-the-same-depth-bound
  (let [deep (nested vector 5000 1)
        x (with-meta 'leaf {:hidden deep})
        parent [1 deep] view (subvec parent 0 1)]
    (doseq [x [x view]
            walk [value/value-charge #(value/metadata-charge % :descriptor (fn [_]))]]
      (is (= :query-source/limit (:error (error #(walk x))))))
    (is (= :query-source/limit (:error (error #(value/portable? x)))))
    (is (= :query-source/limit (:error (error #(value/owned-copy x)))))
    ;; A copied view sheds its hidden parent. Diagnostic copying drops metadata.
    (is (= [1] (value/owned-copy view)))
    (is (pos? (value/copy-charge view)))
    (is (= 'leaf (value/diagnostic-copy x (fn []))))
    (is (nil? (meta (value/diagnostic-copy x (fn [])))))))

(deftest deep-custom-hooks-are-not-invoked-after-the-depth-limit
  (let [calls (atom 0)
        custom (reify value/IQuerySourceValue
                 (-logical-type [_] (swap! calls inc) {:kind :custom})
                 (-retained-bytes [_] (swap! calls inc) 64))
        x (nested vector 5000 custom)]
    (is (= :query-source/limit (:error (error #(value/value-charge x)))))
    (is (= :query-source/limit (:error (error #(value/owned-copy x)))))
    (is (zero? @calls))))

(deftest nested-logical-tuples-are-bounded-before-jvm-stack-exhaustion
  (let [type (nested #(hash-map :kind :tuple :elements [%]) 5000 :long)
        x (nested vector 5000 1)]
    (is (= {:error :query-source/limit :phase :types}
           (error #(value/matches-type? type x))))))

(deftest cancellation-and-timeout-still-precede-value-admission
  (doseq [category [:query-source/cancelled :query/timeout]
          walk [#(value/value-charge %1 (fn [_] (%2))) value/owned-copy value/portable?
                value/diagnostic-copy value/execution-value! #(value/matches-type? :data %1 %2)]]
    (let [x (nested vector 5000 1) ctl (control/create)]
      (try
        (control/cancel! ctl {:error category :phase :execution})
        (is (= category (:error (error #(walk x (fn [] (control/check-active! ctl)))))))
        (finally (control/close! ctl))))))

(deftest public-batch-and-binding-sizing-propagate-depth-resource-failures
  (let [x (nested vector 5000 1)
        column {:variable '?key :type :data :nullable? false}]
    (is (= :query-source/limit
           (:error (error #(source/batch-charge-bytes [column] [[x]])))))
    (is (= :query-source/limit
           (:error (error #(source/binding-charge-bytes [column] [{:binding-id 0 :values [x]}])))))))

(deftest deep-query-inputs-fail-before-providers-and-unwind-preparation
  (let [events (atom []) saved (atom nil) x (nested vector 5000 1)]
    (is (= :query-source/limit
           (:error (error #(rt/with-runtime {}
                             (fn [runtime]
                               (reset! saved runtime)
                               (program/query! runtime '[:find ?v :in $s ?key :where [$s ?e :row/value ?v]]
                                 [(fixture/provider events {}) x])))))))
    (is (empty? @events))
    (is (every? zero? (vals (control/usage (:control @saved)))))))
