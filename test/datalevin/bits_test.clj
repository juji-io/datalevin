(ns datalevin.bits-test
  (:require [datalevin.bits :as sut]
            [datalevin.datom :as d]
            [taoensso.nippy :as nippy]
            [clojure.test :refer [deftest is]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.clojure-test :as test]
            [clojure.test.check.properties :as prop])
  (:import [java.nio ByteBuffer]))

(deftest datom-test
  (let [bf (ByteBuffer/allocateDirect 512)
        d1 (d/datom 1 :name "Mr. Kitty")]
    (sut/put-buffer bf d1 :datom)
    (.flip bf)
    (is (= d1 (nippy/thaw (nippy/freeze d1))))
    (is (= d1 (sut/read-buffer bf :datom)))))

;;TODO generative test
