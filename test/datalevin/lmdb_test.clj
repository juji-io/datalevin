(ns datalevin.lmdb-test
  (:require [datalevin.lmdb :as sut]
            [datalevin.bits :as b]
            [datalevin.constants :as c]
            [datalevin.datom :as d]
            [clojure.test :refer [deftest is use-fixtures]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.clojure-test :as test]
            [clojure.test.check.properties :as prop]
            [taoensso.nippy :as nippy]
            [taoensso.timbre :as log])
  (:import [java.util UUID Arrays]
           [org.lmdbjava CursorIterable$KeyVal]))

(def ^:dynamic lmdb nil)

(defn lmdb-test-fixture
  [f]
  (let [dir (str "/tmp/lmdb-test-" (UUID/randomUUID))]
    (with-redefs [lmdb (sut/open-lmdb dir)]
      (sut/open-dbi lmdb "a")
      (sut/open-dbi lmdb "b")
      (sut/open-dbi lmdb "c" Long/BYTES Long/BYTES)
      (f)
      (sut/close lmdb)
      (b/delete-files dir))))

(use-fixtures :each lmdb-test-fixture)

(deftest basic-ops-test
  ;; transact
  (sut/transact lmdb
                [[:put "a" 1 2]
                 [:put "a" 'a 1]
                 [:put "a" 5 {}]
                 [:put "a" :annunaki/enki true :attr :data]
                 [:put "a" :datalevin ["hello" "world"]]
                 [:put "a" 42 (d/datom 1 :a/b {:id 4}) :long :datom]
                 [:put "b" 2 3]
                 [:put "b" (byte 0x01) #{1 2} :byte :data]
                 [:put "b" (byte-array [0x41 0x42]) :bk :bytes :data]
                 [:put "b" [-1 -235254457N] 5]
                 [:put "b" :a 4]
                 [:put "b" :bv (byte-array [0x41 0x42 0x43]) :data :bytes]
                 [:put "b" 1 :long :long :data]
                 [:put "b" :long 1 :data :long]
                 [:put "b" 2 3 :long :long]])

  ;; get
  (is (= 2 (sut/get-value lmdb "a" 1)))
  (is (= true (sut/get-value lmdb "a" :annunaki/enki :attr :data)))
  (is (= (d/datom 1 :a/b {:id 4}) (sut/get-value lmdb "a" 42 :long :datom)))
  (is (nil? (sut/get-value lmdb "a" 2)))
  (is (nil? (sut/get-value lmdb "b" 1)))
  (is (= 5 (sut/get-value lmdb "b" [-1 -235254457N])))
  (is (= 1 (sut/get-value lmdb "a" 'a)))
  (is (= {} (sut/get-value lmdb "a" 5)))
  (is (= ["hello" "world"] (sut/get-value lmdb "a" :datalevin)))
  (is (= 3 (sut/get-value lmdb "b" 2)))
  (is (= 4 (sut/get-value lmdb "b" :a)))
  (is (= #{1 2} (sut/get-value lmdb "b" (byte 0x01) :byte)))
  (is (= :bk (sut/get-value lmdb "b" (byte-array [0x41 0x42]) :bytes)))
  (is (Arrays/equals ^bytes (byte-array [0x41 0x42 0x43])
                     ^bytes (sut/get-value lmdb "b" :bv :data :bytes)))
  (is (= :long (sut/get-value lmdb "b" 1 :long :data)))
  (is (= 1 (sut/get-value lmdb "b" :long :data :long)))
  (is (= 3 (sut/get-value lmdb "b" 2 :long :long)))

  ;; del
  (sut/transact lmdb [[:del "a" 1]])
  (is (nil? (sut/get-value lmdb "a" 1)))

  ;; non-existent dbi
  (is (thrown-with-msg? Exception #"open-dbi" (sut/get-value lmdb "z" 1)))

  ;; handle val overflow automatically
  (sut/transact lmdb [[:put "c" 1 (range 1000)]])
  (is (= (range 1000) (sut/get-value lmdb "c" 1)))

  ;; key overflow throws instead
  (is (thrown? java.lang.AssertionError
               (sut/transact lmdb [[:put "a" (range 1000) 1]]))))

(deftest get-first-test
  (let [ks  (shuffle (range 0 1000))
        vs  (map inc ks)
        txs (map (fn [k v] [:put "c" k v :long :long]) ks vs)]
    (sut/transact lmdb txs)
    (is (= [0 1] (sut/get-first lmdb "c" [:all] :long :long)))
    (is (= [0 nil] (sut/get-first lmdb "c" [:all] :long :ignore)))
    (is (= [999 1000] (sut/get-first lmdb "c" [:all-back] :long :long)))
    (is (= [9 10] (sut/get-first lmdb "c" [:at-least 9] :long :long)))
    (is (= [10 11] (sut/get-first lmdb "c" [:greater-than 9] :long :long)))
    (sut/transact lmdb [[:put "a" 0xff 1 :byte]
                        [:put "a" 0xee 2 :byte]
                        [:put "a" 0x11 3 :byte]])
    (is (= 3 (sut/get-first lmdb "a" [:all] :byte :data true)))
    (is (= 1 (sut/get-first lmdb "a" [:all-back] :byte :data true)))))

(deftest get-range-test
  (let [ks  (shuffle (range 0 1000))
        vs  (map inc ks)
        txs (map (fn [k v] [:put "c" k v :long :long]) ks vs)
        res (sort-by first (map (fn [k v] [k v]) ks vs))]
    (sut/transact lmdb txs)
    (is (= res (sut/get-range lmdb "c" [:all] :long :long)))
    (is (= res
           (sut/get-range lmdb "c" [:less-than Long/MAX_VALUE] :long :long)))
    (is (= (take 10 res)
           (sut/get-range lmdb "c" [:at-most 9] :long :long)))
    (is (= (->> res (drop 10) (take 100))
           (sut/get-range lmdb "c" [:closed 10 109] :long :long)))
    (is (= (->> res (drop 990))
           (sut/get-range lmdb "c" [:at-least 990] :long :long)))
    (is (= (->> res (drop 10) (take 100) (map second))
           (sut/get-range lmdb "c" [:closed 10 109] :long :long true)))))

(deftest get-some-test
  (let [ks  (shuffle (range 0 1000))
        vs  (map inc ks)
        txs (map (fn [k v] [:put "c" k v :long :long]) ks vs)
        pred (fn [^CursorIterable$KeyVal kv]
               (let [k (-> kv (.key) (b/read-buffer :long))]
                 (when (> ^long k 15) (-> kv (.val) (b/read-buffer :long)))))]
    (sut/transact lmdb txs)
    (is (= 17 (sut/get-some lmdb pred "c" [:all] :long :long)))))

(deftest multi-threads-get-value-test
  (let [ks (shuffle (range 0 1000))
        vs (map inc ks)
        txs (map (fn [k v] [:put "a" k v :long :long]) ks vs)]
    (sut/transact lmdb txs)
    (is (= vs (pmap #(sut/get-value lmdb "a" % :long :long) ks)))))

;; generative tests

(test/defspec datom-ops-generative-test
  100
  (prop/for-all [k gen/large-integer
                 e gen/large-integer
                 a gen/keyword-ns
                 v gen/any-equatable]
                (let [d      (d/datom e a v e)
                      _      (sut/transact lmdb [[:put "a" k d :long :datom]])
                      put-ok (= d (sut/get-value lmdb "a" k :long :datom))
                      _      (sut/transact lmdb [[:del "a" k :long]])
                      del-ok (nil? (sut/get-value lmdb "a" k :long))]
                  (and put-ok del-ok))))

(defn- data-size-less-than?
  [^long limit data]
  (< (alength ^bytes (nippy/fast-freeze data)) limit))

(test/defspec data-ops-generative-test
  100
  (prop/for-all [k (gen/such-that (partial data-size-less-than? c/+max-key-size+)
                                  gen/any-equatable)
                 v (gen/such-that (partial data-size-less-than? c/+default-val-size+)
                                  gen/any-equatable)]
                (let [_      (sut/transact lmdb [[:put "a" k v]])
                      put-ok (= v (sut/get-value lmdb "a" k))
                      _      (sut/transact lmdb [[:del "a" k]])
                      del-ok (nil? (sut/get-value lmdb "a" k))]
                  (and put-ok del-ok))))

(test/defspec bytes-ops-generative-test
  100
  (prop/for-all [^bytes k (gen/not-empty gen/bytes)
                 ^bytes v (gen/not-empty gen/bytes)]
                (let [_      (sut/transact lmdb [[:put "a" k v :bytes :bytes]])
                      put-ok (Arrays/equals v
                                            ^bytes
                                            (sut/get-value
                                             lmdb "a" k :bytes :bytes))
                      _      (sut/transact lmdb [[:del "a" k :bytes]])
                      del-ok (nil? (sut/get-value lmdb "a" k :bytes))]
                  (and put-ok del-ok))))

(test/defspec long-ops-generative-test
  100
  (prop/for-all [^long k gen/large-integer
                 ^long v gen/large-integer]
                (let [_      (sut/transact lmdb [[:put "a" k v :long :long]])
                      put-ok (= v ^long (sut/get-value lmdb "a" k :long :long))
                      _      (sut/transact lmdb [[:del "a" k :long]])
                      del-ok (nil? (sut/get-value lmdb "a" k))]
                  (and put-ok del-ok))))
