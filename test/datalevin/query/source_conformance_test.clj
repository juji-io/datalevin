(ns datalevin.query.source-conformance-test
  (:require [clojure.test :refer [deftest]]
            [datalevin.query.source.conformance :as conformance]
            [datalevin.query.source.fake :as fake]))

(deftest tuple-execution-matrix
  (conformance/tuple-semantics! fake/source))

(deftest eav-execution-matrix
  (conformance/eav-semantics! fake/source))

(deftest native-execution-matrix
  (conformance/native-semantics! fake/source))

(deftest lifecycle-execution-matrix
  (conformance/lifecycle! fake/source #(reset! (:rows %1) %2)))

(deftest key-execution-matrix
  (conformance/key-semantics! fake/source))
