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
