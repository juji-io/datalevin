(ns datalevin.protocol-test
  (:require [datalevin.protocol :as sut]
            [datalevin.constants :as c]
            [datalevin.bits :as b]
            [datalevin.util :as u]
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

(test/defspec transit-message-bf-test
  100
  (prop/for-all
    [v gen/any-equatable]
    (let [^ByteBuffer bf (b/allocate-buffer 16384)]
      (sut/write-message-bf bf v)
      (let [pos (.position bf)]
        (.flip bf)
        (is (= 1 (short (.get bf))))
        (is (= pos (.getInt bf)))
        (is (= v (sut/read-transit-bf bf)))))))

(defn- write2dst
  [^ByteBuffer src ^ByteBuffer dst ^long n]
  (.put dst (.array src) (+ (.arrayOffset src) (.position src)) n)
  (.position src (+ (.position src) n)))

(deftest segment-messages-test
  (let [src-arr         (byte-array 200)
        ^ByteBuffer src (ByteBuffer/wrap src-arr)
        ^ByteBuffer dst (b/allocate-buffer 200)
        msg1            {:text "this is the first message" :value 888} ; 62 bytes
        msg2            {:text "the second message"}                   ; 41 bytes
        sink            (atom [])
        handler         (fn [type msg]
                          (swap! sink conj (sut/read-value type msg)))]

    (sut/write-message-bf src msg1)
    (sut/write-message-bf src msg2)
    (.flip src)

    (testing "less than header length available"
      (write2dst src dst 2)
      (sut/segment-messages dst handler)
      (is (= (.position dst) 2))
      (is (empty? @sink)))

    (testing "less than message length available"
      (write2dst src dst 10)
      (sut/segment-messages dst handler)
      (is (= (.position dst) 12))
      (is (empty? @sink)))

    (testing "first message available"
      (write2dst src dst 60)
      (sut/segment-messages dst handler)
      (is (= (.position dst) 10))
      (is (= (.limit dst) 200))
      (is (= (count @sink) 1))
      (is (= msg1 (first @sink))))

    (testing "second message still not available"
      (write2dst src dst 10)
      (sut/segment-messages dst handler)
      (is (= (.position dst) 20))
      (is (= (.limit dst) 200))
      (is (= (count @sink) 1))
      (is (= msg1 (first @sink))))

    (testing "second message available"
      (write2dst src dst 21)
      (sut/segment-messages dst handler)
      (is (= (.position dst) 0))
      (is (= (.limit dst) 200))
      (is (= (count @sink) 2))
      (is (= msg2 (second @sink))))))

(deftest receive-one-messages-test
  (let [src-arr         (byte-array 200)
        ^ByteBuffer src (ByteBuffer/wrap src-arr)
        ^ByteBuffer dst (b/allocate-buffer 200)
        msg1            {:text "this is the first message" :value 888} ; 62 bytes
        msg2            {:text "the second message"}                   ; 41 bytes
        ]

    (sut/write-message-bf src msg1)
    (sut/write-message-bf src msg2)
    (.flip src)

    (testing "less than header length available"
      (write2dst src dst 2)
      (is (nil? (sut/receive-one-message dst)))
      (is (= (.position dst) 2)))

    (testing "less than message length available"
      (write2dst src dst 10)
      (is (nil? (sut/receive-one-message dst)))
      (is (= (.position dst) 12)))

    (testing "first message available"
      (write2dst src dst 60)
      (is (= msg1 (sut/receive-one-message dst)))
      (is (= (.position dst) 10))
      (is (= (.limit dst) 200)))))
