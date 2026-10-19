(ns datalevin.query.source-conformance-test
  (:require [clojure.test :refer [deftest]]
            [datalevin.query.source.conformance :as conformance]
            [datalevin.query.source.fake :as fake]))

(deftest tuple-execution-matrix
  (conformance/tuple-semantics! (conformance/source-constructor fake/source)))

(deftest eav-execution-matrix
  (conformance/eav-semantics! (conformance/source-constructor fake/source)))

(deftest native-execution-matrix
  (conformance/native-semantics! (conformance/source-constructor fake/source)))

(deftest lifecycle-execution-matrix
  (conformance/lifecycle! (conformance/source-constructor fake/source)
                          #(conformance/replace-rows fake/replace-facts! %1 %2)))

(deftest key-execution-matrix
  (conformance/key-semantics! (conformance/source-constructor fake/source)))
