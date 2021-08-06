(ns datalevin.protocol-test
  (:require [datalevin.protocol :as sut]
            [datalevin.constants :as c]
            [datalevin.bits :as b]
            [clojure.test :refer [deftest testing is]]
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

(deftest segment-messages-test
  (let [src-arr         (byte-array 200)
        ^ByteBuffer src (ByteBuffer/wrap src-arr)
        ^ByteBuffer dst (b/allocate-buffer 200)
        msg1            {:text "this is the first message" :value 888} ; 61 bytes
        msg2            {:text "the second message"}                   ; 40 bytes
        sink            (atom [])
        handler         (fn [msg] (swap! sink conj msg))
        write2dst       (fn [^long n]
                          (.put dst (.array src)
                                (+ (.arrayOffset src) (.position src)) n)
                          (.position src (+ (.position src) n)))]

    (sut/write-message-bf src msg1)
    (sut/write-message-bf src msg2)
    (.flip src)

    (testing "less than header length available"
      (write2dst 2)
      (sut/segment-messages dst handler)
      (is (= (.position dst) 2))
      (is (empty? @sink)))

    (testing "less than message length available"
      (write2dst 10)
      (sut/segment-messages dst handler)
      (is (= (.position dst) 12))
      (is (empty? @sink)))

    (testing "first message available"
      (write2dst 60)
      (sut/segment-messages dst handler)
      (is (= (.position dst) 11))
      (is (= (.limit dst) 200))
      (is (= (count @sink) 1))
      (is (= msg1 (first @sink))))

    (testing "second message still not available"
      (write2dst 10)
      (sut/segment-messages dst handler)
      (is (= (.position dst) 21))
      (is (= (.limit dst) 200))
      (is (= (count @sink) 1))
      (is (= msg1 (first @sink))))

    (testing "second message available"
      (write2dst 19)
      (sut/segment-messages dst handler)
      (is (= (.position dst) 0))
      (is (= (.limit dst) 200))
      (is (= (count @sink) 2))
      (is (= msg2 (second @sink))))


    ))
