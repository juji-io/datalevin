(ns datalevin.query.source-mapped-conformance-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.core :as d]
            [datalevin.query.source :as source]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.fake :as fixture]
            [datalevin.query.source.mapped-conformance :as conformance]))

(deftest mapped-values-and-result-shapes
  (conformance/values! fixture/source))

(deftest mapped-dependencies-and-association
  (conformance/dependencies! fixture/source))

(deftest mapped-rules-branches-and-many-valued-attributes
  (conformance/regions! fixture/source))

(deftest mapped-structural-explanations
  (conformance/explanations! fixture/source))

(deftest native-and-mapped-rule-composition
  (conformance/native! fixture/source))

(deftest independent-caller-seeds-and-private-scope-boundaries
  (conformance/caller-seeds! fixture/source))

(deftest pinned-versions-and-fresh-invocations
  (conformance/lifecycle! fixture/source fixture/replace-facts!))

(deftest owned-input-batches-and-results
  (conformance/ownership! fixture/source))

(deftest mapped-envelope-and-lifecycle-failures
  (conformance/failures! fixture/source))

(deftest modeled-setup-and-page-requests-share-the-invocation-ledger
  (let [info (fixture/info {:row/value {:entity-type :long :value-type :string}})
        facts [[1 :row/value "one"] [2 :row/value "two"]]
        query '{:find [?value] :in [$s] :where [[$s _ :row/value ?value]]
                :query-source {:limits {:batch-rows 1}}}]
    (doseq [maximum [0 1 4]]
      (let [captured (atom nil)
            p (fixture/source info facts {:setup-request? true :page-requests? true
                                           :hooks {:acquire #(reset! captured (:control %))}})
            result (try (d/explain {:run? true}
                          (assoc-in query [:query-source :limits :external-requests] maximum) p)
                        (catch clojure.lang.ExceptionInfo e (ex-data e)))
            requests (filter #(= :request (first %)) @(:events p))]
        (if (= maximum 4)
          (do (is (= 2 (:result-rows result)))
              (is (= 3 (get-in result [:cursors 0 :metrics :external-requests]))))
          (do (is (= :query-source/limit (:error result)))
              (is (= :external-requests (:category result)))))
        (is (= maximum (count requests)))
        (is (= maximum (:external-requests (control/usage @captured))))
        (is (every? zero? (vals (dissoc (control/usage @captured) :external-requests))))
        (is (= :closed (try (source/check-active! @captured) nil
                           (catch clojure.lang.ExceptionInfo e (:state (ex-data e))))))))))
