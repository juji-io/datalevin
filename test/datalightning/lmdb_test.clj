(ns datalightning.lmdb-test
  (:require [datalightning.lmdb :as sut]
            [datalightning.util :as util]
            [clojure.test :refer [deftest is use-fixtures]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.clojure-test :as test]
            [clojure.test.check.properties :as prop]
            [taoensso.nippy :as nippy]
            [taoensso.timbre :as log])
  (:import [java.util UUID Arrays]))

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
                 [:put "a" 5 {}]
                 [:put "a" :datalightning ["hello" "world"]]
                 [:put "b" 2 3]
                 [:put "b" (byte-array [0x41 0x42]) :bk]
                 [:put "b" [-1 -235254457N] 5]
                 [:put "b" :a 4]
                 [:put "b" :bv (byte-array [0x41 0x42 0x43])]
                 [:put-long-key "b" 1 :long]
                 [:put-long-val "b" :long 1]
                 [:put-long-kv "b" 2 3]])
  ;; data
  (is (= 2 (sut/get-data lmdb "a" 1)))
  (is (nil? (sut/get-data lmdb "a" 2)))
  (is (nil? (sut/get-data lmdb "b" 1)))
  (is (= 5 (sut/get-data lmdb "b" [-1 -235254457N])))
  (is (= 1 (sut/get-data lmdb "a" 'a)))
  (is (= {} (sut/get-data lmdb "a" 5)))
  (is (= ["hello" "world"] (sut/get-data lmdb "a" :datalightning)))
  (is (= 3 (sut/get-data lmdb "b" 2)))
  (is (= 4 (sut/get-data lmdb "b" :a)))
  (is (= :bk (sut/get-data lmdb "b" (byte-array [0x41 0x42]))))

  ;; bytes
  (is (Arrays/equals ^bytes (byte-array [0x41 0x42 0x43])
                     ^bytes (sut/get-bytes lmdb "b" :bv)))

  ;; long
  (is (= :long (sut/get-data lmdb "b" 1 :long)))
  (is (= 1 (sut/get-long lmdb "b" :long)))
  (is (= 3 (sut/get-long lmdb "b" 2 :long)))

  ;; del
  (sut/transact lmdb [[:del "a" 1]])
  (is (nil? (sut/get-data lmdb "a" 1)))

  ;; non-existent dbi
  (is (thrown-with-msg? Exception #"open-dbi" (sut/get-raw lmdb "c" 1)))

  ;; larger val
  (sut/transact lmdb [[:put "a" 1 (range 1000)]])
  (is (= (range 1000) (sut/get-data lmdb "a" 1)))

  ;; key overflow
  (is (thrown? java.lang.AssertionError
               (sut/transact lmdb [[:put "a" (range 1000) 1]]))))

;; generative tests

(defn- data-size-less-than?
  [limit data]
  (< (alength ^bytes (nippy/fast-freeze data)) limit))

(test/defspec data-ops-generative-test
  100
  (prop/for-all [k (gen/such-that (partial data-size-less-than? 500)
                                  gen/any-equatable)
                 v gen/any-equatable]
                (let [_      (sut/transact lmdb [[:put "a" k v]])
                      put-ok (= v (sut/get-data lmdb "a" k))
                      _      (sut/transact lmdb [[:del "a" k]])
                      del-ok (nil? (sut/get-data lmdb "a" k))]
                  (and put-ok del-ok))))

(test/defspec bytes-ops-generative-test
  100
  (prop/for-all [^bytes k (gen/not-empty gen/bytes)
                 ^bytes v (gen/not-empty gen/bytes)]
                (let [_      (sut/transact lmdb [[:put "a" k v]])
                      put-ok (Arrays/equals v
                                            ^bytes (sut/get-bytes lmdb "a" k))
                      _      (sut/transact lmdb [[:del "a" k]])
                      del-ok (nil? (sut/get-bytes lmdb "a" k))]
                  (and put-ok del-ok))))

(test/defspec long-ops-generative-test
  100
  (prop/for-all [^long k gen/large-integer
                 ^long v gen/large-integer]
                (let [_      (sut/transact lmdb [[:put-long-kv "a" k v]])
                      put-ok (= v ^long (sut/get-long lmdb "a" k :long))
                      _      (sut/transact lmdb [[:del-long-key "a" k]])
                      del-ok (nil? (sut/get-long lmdb "a" k))]
                  (and put-ok del-ok))))
