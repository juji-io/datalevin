(ns datalevin.util-test
  (:require [datalevin.util :as sut]
            [datalevin.constants :as c]
            [clojure.test :refer [deftest is]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.clojure-test :as test]
            [clojure.test.check.properties :as prop])
  (:import [java.util Arrays]
           [java.nio ByteBuffer]))

(test/defspec base64-test
  100
  (prop/for-all [^bytes k (gen/not-empty gen/bytes)]
                (Arrays/equals k
                               ^bytes (sut/decode-base64
                                        (sut/encode-base64 k)))))

(test/defspec transite-string-test
  100
  (prop/for-all [k gen/any-equatable]
                (= k (sut/read-transit-string (sut/write-transit-string k)))))
