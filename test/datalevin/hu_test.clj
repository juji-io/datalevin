(ns datalevin.hu-test
  (:require
   [datalevin.hu :as sut]
   [datalevin.util :as u]
   [datalevin.bits :as b]
   [datalevin.buffer :as bf]
   [clojure.test :refer [deftest testing is are]]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.clojure-test :as test]
   [clojure.test.check.properties :as prop]
   [datalevin.constants :as c])
  (:import
   [java.util Arrays]
   [java.nio ByteBuffer]
   [org.eclipse.collections.impl.map.mutable UnifiedMap]
   [datalevin.hu TableKey TableEntry]))

(deftest encoding-test
  (let [^longs freqs0 (long-array [1 1 1 1 1 1 1 1])
        ^longs freqs1 (long-array [8 6 2 3 4 7 11 9 8 1 3])
        ^longs freqs2 (long-array [5 2 7 2 1 1 1 2 4 5])
        ^shorts data0 (short-array (range 8))
        ^shorts data1 (short-array (range 11))
        ^shorts data2 (short-array (range 10))]
    (testing "level tree construction"
      (are [freqs levels] (= (seq (sut/create-levels (alength freqs) freqs))
                             levels)
        freqs0 [3 3 3 3 3 3 3 3]
        freqs1 [3 3 5 5 4 3 3 3 3 4 4]
        freqs2 [3 3 2 4 5 5 4 4 3 3]))
    (testing "code construction"
      (are [freqs results] (= (let [n (alength freqs)
                                    lens (byte-array n)
                                    codes (int-array n)]
                                (sut/create-codes n lens codes freqs)
                                [(vec codes) (vec lens)])
                              results)
        freqs0 [[0 1 2 3 4 5 6 7] [3 3 3 3 3 3 3 3]]
        freqs1 [[0 1 8 9 5 3 4 5 6 14 15] [3 3 5 5 4 3 3 3 3 4 4]]
        freqs2 [[0 1 1 8 18 19 10 11 6 7] [3 3 2 4 5 5 4 4 3 3]]))
    (testing "decoding table construction"
      (are [freqs results]
          (= (let [n (alength freqs)
                   lens (byte-array n)
                   codes (int-array n)
                   _ (sut/create-codes n lens codes freqs)
                   ^UnifiedMap tables (sut/create-decode-tables lens codes 2)
                   keys (.keySet tables)]
               (into {}
                     (mapv (fn [^TableKey k]
                             [[(.-prefix k) (.-len k)]
                              (mapv (fn [^TableEntry entry]
                                      (let [^TableKey link (.-link entry)]
                                        [(.-decoded entry)
                                         [(.-prefix link) (.-len link)]]))
                                    (.get tables k))])
                           keys)))
             results)
        freqs0 {[0 0] [[nil [0 2]] [nil [1 2]] [nil [2 2]] [nil [3 2]]],
                [0 1] [[0 [0 0]] [1 [0 0]] [2 [0 0]] [3 [0 0]]],
                [0 2] [[0 [0 1]] [0 [1 1]] [1 [0 1]] [1 [1 1]]],
                [1 1] [[4 [0 0]] [5 [0 0]] [6 [0 0]] [7 [0 0]]],
                [1 2] [[2 [0 1]] [2 [1 1]] [3 [0 1]] [3 [1 1]]],
                [2 2] [[4 [0 1]] [4 [1 1]] [5 [0 1]] [5 [1 1]]],
                [3 2] [[6 [0 1]] [6 [1 1]] [7 [0 1]] [7 [1 1]]]}
        freqs1 {[0 0] [[nil [0 2]] [nil [1 2]] [nil [2 2]] [nil [3 2]]],
                [0 1] [[0 [0 0]] [1 [0 0]] [nil [2 3]] [5 [0 0]]],
                [0 2] [[0 [0 1]] [0 [1 1]] [1 [0 1]] [1 [1 1]]],
                [1 1] [[6 [0 0]] [7 [0 0]] [8 [0 0]] [nil [7 3]]],
                [1 2] [[nil [4 4]] [4 [0 0]] [5 [0 1]] [5 [1 1]]],
                [2 2] [[6 [0 1]] [6 [1 1]] [7 [0 1]] [7 [1 1]]],
                [2 3] [[2 [0 0]] [3 [0 0]] [4 [0 1]] [4 [1 1]]],
                [3 2] [[8 [0 1]] [8 [1 1]] [9 [0 0]] [10 [0 0]]],
                [4 4] [[2 [0 1]] [2 [1 1]] [3 [0 1]] [3 [1 1]]],
                [7 3] [[9 [0 1]] [9 [1 1]] [10 [0 1]] [10 [1 1]]]}
        freqs2 {[0 0] [[nil [0 2]] [2 [0 0]] [nil [2 2]] [nil [3 2]]],
                [0 1] [[0 [0 0]] [1 [0 0]] [2 [0 1]] [2 [1 1]]],
                [0 2] [[0 [0 1]] [0 [1 1]] [1 [0 1]] [1 [1 1]]],
                [1 1] [[nil [4 3]] [nil [5 3]] [8 [0 0]] [9 [0 0]]],
                [2 2] [[3 [0 0]] [nil [9 4]] [6 [0 0]] [7 [0 0]]],
                [3 2] [[8 [0 1]] [8 [1 1]] [9 [0 1]] [9 [1 1]]],
                [4 3] [[3 [0 1]] [3 [1 1]] [4 [0 0]] [5 [0 0]]],
                [5 3] [[6 [0 1]] [6 [1 1]] [7 [0 1]] [7 [1 1]]],
                [9 4] [[4 [0 1]] [4 [1 1]] [5 [0 1]] [5 [1 1]]]}))))

(def freqs (repeatedly 65536 #(rand-int 1000000)))
(def ht    (sut/new-hu-tucker (long-array (map inc freqs))))

(test/defspec order-preservation-test
  1000
  (prop/for-all
    [bs1 (gen/such-that #(< 1 (alength ^bytes %) c/+max-key-size+)
                        gen/bytes)
     bs2 (gen/such-that #(< 1 (alength ^bytes %) c/+max-key-size+)
                        gen/bytes)]
    (let [^ByteBuffer src1 (bf/allocate-buffer c/+max-key-size+)
          ^ByteBuffer src2 (bf/allocate-buffer c/+max-key-size+)
          ^ByteBuffer dst1 (bf/allocate-buffer c/+max-key-size+)
          ^ByteBuffer dst2 (bf/allocate-buffer c/+max-key-size+)]
      (b/put-buffer src1 bs1 :bytes)
      (b/put-buffer src2 bs2 :bytes)
      (.flip src1)
      (.flip src2)
      (sut/encode ht src1 dst1)
      (sut/encode ht src2 dst2)
      (.flip ^ByteBuffer src1)
      (.flip ^ByteBuffer src2)
      (.flip ^ByteBuffer dst1)
      (.flip ^ByteBuffer dst2)
      (is (u/same-sign? (bf/compare-buffer src1 src2)
                        (bf/compare-buffer dst1 dst2))))))

(test/defspec encode-decode-round-trip-test
  1000
  (prop/for-all
    [^bytes bs (gen/such-that #(< 0 (alength ^bytes %) c/+max-key-size+)
                              gen/bytes)]
    (let [^ByteBuffer src (bf/allocate-buffer c/+max-key-size+)
          ^ByteBuffer dst (bf/allocate-buffer c/+max-key-size+)
          ^ByteBuffer res (bf/allocate-buffer c/+max-key-size+)]
      (b/put-bytes src bs)
      (.flip src)
      (sut/encode ht src dst)
      (.flip dst)
      (sut/decode ht dst res)
      (.flip res)
      (.rewind dst)
      (is (Arrays/equals bs ^bytes (b/get-bytes res))))))
