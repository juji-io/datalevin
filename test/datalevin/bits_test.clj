(ns datalevin.bits-test
  (:require [datalevin.bits :as sut]
            [datalevin.datom :as d]
            [taoensso.nippy :as nippy]
            [taoensso.timbre :as log]
            [clojure.test :refer [deftest is]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.clojure-test :as test]
            [clojure.test.check.properties :as prop])
  (:import [java.util Arrays]
           [java.nio ByteBuffer]))

(def bf (ByteBuffer/allocateDirect 16384))

(deftest datom-test
  (let [d1 (d/datom 1 :pet/name "Mr. Kitty")]
    (.clear ^ByteBuffer bf)
    (sut/put-buffer bf d1 :datom)
    (.flip ^ByteBuffer bf)
    (is (= d1 (nippy/thaw (nippy/freeze d1))))
    (is (= d1 (sut/read-buffer bf :datom)))))

(test/defspec datom-generative-test
  100
  (prop/for-all [e gen/large-integer
                 a gen/keyword-ns
                 v gen/any-equatable
                 t gen/large-integer]
                (let [d (d/datom e a v e)]
                  (.clear ^ByteBuffer bf)
                  (sut/put-buffer bf d :datom)
                  (.flip ^ByteBuffer bf)
                  (is (= d (sut/read-buffer bf :datom))))))

(test/defspec bytes-generative-test
  100
  (prop/for-all [^bytes k (gen/not-empty gen/bytes)]
                (.clear ^ByteBuffer bf)
                (sut/put-buffer bf k :bytes)
                (.flip ^ByteBuffer bf)
                (Arrays/equals k ^bytes (sut/read-buffer bf :bytes))))

(test/defspec byte-generative-test
  100
  (prop/for-all [k gen/byte]
                (.clear ^ByteBuffer bf)
                (sut/put-buffer bf k :byte)
                (.flip ^ByteBuffer bf)
                (= k (sut/read-buffer bf :byte))))

(test/defspec data-generative-test
  100
  (prop/for-all [k gen/any-equatable]
                (.clear ^ByteBuffer bf)
                (sut/put-buffer bf k :data)
                (.flip ^ByteBuffer bf)
                (= k (sut/read-buffer bf :data))))

(test/defspec long-generative-test
  100
  (prop/for-all [k gen/large-integer]
                (.clear ^ByteBuffer bf)
                (sut/put-buffer bf k :long)
                (.flip ^ByteBuffer bf)
                (= k (sut/read-buffer bf :long))))
