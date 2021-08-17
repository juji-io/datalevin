(ns datalevin.server-test
  (:require [datalevin.server :as sut]
            [datalevin.bits :as b]
            [datalevin.protocol :as p]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.clojure-test :as test]
            [clojure.test.check.properties :as prop]
            [clojure.test :refer [is deftest testing]])
  (:import [java.nio ByteBuffer]))

(deftest password-test
  (let [s  (sut/salt)
        s1 (sut/salt)
        p  "superDoperSecret"
        p1 "superdopersecret"
        h  (sut/password-hashing p s)
        h1 (sut/password-hashing p s1)]
    (is (sut/password-matches? p h s))
    (is (sut/password-matches? p h1 s1))
    (is (not (sut/password-matches? p1 h s)))
    (is (not (sut/password-matches? p h s1)))
    (is (not (sut/password-matches? p h1 s)))))

(deftest segment-messages-test
  (let [src-arr         (byte-array 200)
        ^ByteBuffer src (ByteBuffer/wrap src-arr)
        ^ByteBuffer dst (b/allocate-buffer 200)
        msg1            {:text "this is the first message" :value 888} ; 62 bytes
        msg2            {:text "the second message"}                   ; 41 bytes
        sink            (atom [])
        handler         (fn [type msg]
                          (swap! sink conj (p/read-value type msg)))]

    (p/write-message-bf src msg1)
    (p/write-message-bf src msg2)
    (.flip src)

    (testing "less than header length available"
      (b/buffer-transfer src dst 2)
      (sut/segment-messages dst handler)
      (is (= (.position dst) 2))
      (is (empty? @sink)))

    (testing "less than message length available"
      (b/buffer-transfer src dst 10)
      (sut/segment-messages dst handler)
      (is (= (.position dst) 12))
      (is (empty? @sink)))

    (testing "first message available"
      (b/buffer-transfer src dst 60)
      (sut/segment-messages dst handler)
      (is (= (.position dst) 10))
      (is (= (.limit dst) 200))
      (is (= (count @sink) 1))
      (is (= msg1 (first @sink))))

    (testing "second message still not available"
      (b/buffer-transfer src dst 10)
      (sut/segment-messages dst handler)
      (is (= (.position dst) 20))
      (is (= (.limit dst) 200))
      (is (= (count @sink) 1))
      (is (= msg1 (first @sink))))

    (testing "second message available"
      (b/buffer-transfer src dst 21)
      (sut/segment-messages dst handler)
      (is (= (.position dst) 0))
      (is (= (.limit dst) 200))
      (is (= (count @sink) 2))
      (is (= msg2 (second @sink))))))
