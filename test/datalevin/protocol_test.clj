(ns datalevin.protocol-test
  (:require [datalevin.protocol :as sut]
            [datalevin.constants :as c]
            [datalevin.bits :as b]
            [clojure.test :refer [deftest is]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.clojure-test :as test]
            [clojure.test.check.properties :as prop])
  (:import [java.nio ByteBuffer]))

(test/defspec transite-bf-test
  100
  (prop/for-all [k gen/any-equatable]
                (let [bf (ByteBuffer/allocateDirect c/+default-buffer-size+)]
                  (sut/write-transit-bf bf k)
                  (.flip bf)
                  (= k (sut/read-transit-bf bf)))))

(test/defspec message-bf-test
  100
  (prop/for-all
    [v gen/any-equatable]
    (let [^ByteBuffer bf (b/allocate-buffer 16384)]
      (sut/write-message-bf bf v)
      (let [pos (.position bf)]
        (.flip bf)
        (is (= pos (.getInt bf)))
        (is (= v (sut/read-transit-bf bf)))))))
