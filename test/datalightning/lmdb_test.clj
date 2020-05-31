(ns datalightning.lmdb-test
  (:require [datalightning.lmdb :as sut]
            [datalightning.util :as util]
            [clojure.test :refer [deftest is use-fixtures]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.clojure-test :as test]
            [clojure.test.check.properties :as prop]
            [taoensso.nippy :as nippy]
            [taoensso.timbre :as log])
  (:import [java.util UUID]))

(def ^:dynamic lmdb nil)

(defn lmdb-test-fixture
  "test using a default lmdb env, with two dbi 'a' and 'b'"
  [f]
  (let [dir (str "/tmp/lmdb-test-" (UUID/randomUUID))]
    (with-redefs [lmdb (sut/open-lmdb dir)]
      (sut/open-dbi lmdb "a")
      (sut/open-dbi lmdb "b")
      (f)
      (sut/close lmdb)
      (util/delete-files dir))))

(use-fixtures :each lmdb-test-fixture)

(deftest basic-ops-test
  (sut/transact lmdb
                [[:put "a" 1 2]
                 [:put "a" 'a 1]
                 [:put "a" :datalightning ["hello" "world"]]
                 [:put "b" 2 3]
                 [:put "b" :a 4]])
  (is (= 2 (sut/get-long lmdb "a" 1)))
  (is (nil? (sut/get-long lmdb "a" 2)))
  (is (nil? (sut/get-long lmdb "b" 1)))
  (is (= 1 (sut/get-long lmdb "a" 'a)))
  (is (= ["hello" "world"] (sut/get-data lmdb "a" :datalightning)))
  (is (= 3 (sut/get-long lmdb "b" 2)))
  (is (= 4 (sut/get-long lmdb "b" :a)))
  (sut/transact lmdb [[:del "a" 1]])
  (is (nil? (sut/get-long lmdb "a" 1)))
  (is (thrown-with-msg? Exception #"open-dbi" (sut/get-raw lmdb "c" 1)))
  (sut/transact lmdb [[:put "a" 1 (range 1000)]])
  (is (= (range 1000) (sut/get-data lmdb "a" 1)))

  )
