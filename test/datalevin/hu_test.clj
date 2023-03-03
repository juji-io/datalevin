(ns datalevin.hu-test
  (:require
   [datalevin.hu :as sut]
   [datalevin.util :as u]
   [datalevin.bits :as b]
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

(deftest basic-ops-test
  (let [^longs freqs0 (long-array [1 1 1 1 1 1 1 1])
        ^longs freqs1 (long-array [8 6 2 3 4 7 11 9 8 1 3])
        ^longs freqs2 (long-array [5 2 7 2 1 1 1 2 4 5])
        ^longs freqs3 (long-array [7 3 4 1 3 2 4 3 5 3 6])
        ^shorts data0 (short-array (range 8))
        ^shorts data1 (short-array (range 11))
        ^shorts data2 (short-array (range 10))
        ^shorts data3 (short-array (range 11))]
    (testing "level tree construction"
      (are [freqs levels] (= (seq (sut/create-levels freqs)) levels)
        freqs0 [3 3 3 3 3 3 3 3]
        freqs1 [3 3 5 5 4 3 3 3 3 4 4]
        freqs2 [3 3 2 4 5 5 4 4 3 3]
        freqs3 [3 4 4 4 4 4 4 3 3 3 3]))
    (testing "code construction"
      (are [freqs results] (= (let [n (alength freqs)
                                    lens (byte-array n)
                                    codes (int-array n)]
                                (sut/create-codes lens codes freqs)
                                [(vec codes) (vec lens)])
                              results)
        freqs0 [[0 1 2 3 4 5 6 7] [3 3 3 3 3 3 3 3]]
        freqs1 [[0 1 8 9 5 3 4 5 6 14 15] [3 3 5 5 4 3 3 3 3 4 4]]
        freqs2 [[0 1 1 8 18 19 10 11 6 7] [3 3 2 4 5 5 4 4 3 3]]
        freqs3 [[0 2 3 4 5 6 7 4 5 6 7] [3 4 4 4 4 4 4 3 3 3 3]]
        ))
    (testing "decoding table construction"
      (are [freqs results]
          (= (let [n (alength freqs)
                   lens (byte-array n)
                   codes (int-array n)
                   _ (sut/create-codes lens codes freqs)
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
                [9 4] [[4 [0 1]] [4 [1 1]] [5 [0 1]] [5 [1 1]]]}
        freqs3 {[2 2] [[7 [0 1]] [7 [1 1]] [8 [0 1]] [8 [1 1]]],
                [0 0] [[nil [0 2]] [nil [1 2]] [nil [2 2]] [nil [3 2]]],
                [2 3] [[3 [0 1]] [3 [1 1]] [4 [0 1]] [4 [1 1]]],
                [3 3] [[5 [0 1]] [5 [1 1]] [6 [0 1]] [6 [1 1]]],
                [1 1] [[7 [0 0]] [8 [0 0]] [9 [0 0]] [10 [0 0]]],
                [1 3] [[1 [0 1]] [1 [1 1]] [2 [0 1]] [2 [1 1]]],
                [0 2] [[0 [0 1]] [0 [1 1]] [1 [0 0]] [2 [0 0]]],
                [1 2] [[3 [0 0]] [4 [0 0]] [5 [0 0]] [6 [0 0]]],
                [3 2] [[9 [0 1]] [9 [1 1]] [10 [0 1]] [10 [1 1]]],
                [0 1] [[0 [0 0]] [nil [1 3]] [nil [2 3]] [nil [3 3]]]}))
    (testing "encoding"
      (are [freqs data bytes]
          (let [^ByteBuffer src (b/allocate-buffer 64)
                ^ByteBuffer dst (b/allocate-buffer 64)
                ht (sut/new-hu-tucker freqs 2)
                size (alength data)]
            (dotimes [i size] (b/put-short src (aget data i)))
            (.flip src)
            (sut/encode ht src dst)
            (.flip dst)
            (let [^bytes results (b/get-bytes dst)]
              (println "results=>" (b/hexify results))
              (Arrays/equals results bytes)))
        freqs0 data0 (byte-array [5 57 119])
        freqs1 data1 (byte-array [5 9 87 46 239])
        freqs2 data2 (byte-array [5 137 78 175 112])
        freqs3 data3 (byte-array [4 104 172 242 238])))
    (testing "round trip"
      (are [freqs data]
          (let [^ByteBuffer src (b/allocate-buffer 64)
                ^ByteBuffer src1 (b/allocate-buffer 64)
                ^ByteBuffer dst (b/allocate-buffer 64)
                ^ByteBuffer res (b/allocate-buffer 64)
                ht (sut/new-hu-tucker freqs 2)
                size (alength data)]
            (dotimes [i size] (b/put-short src (aget data i)))
            (.flip src)
            (dotimes [i size] (b/put-short src1 (aget data i)))
            (b/put-short src1 0)
            (.flip src1)
            (sut/encode ht src dst)
            (.flip dst)
            (sut/decode ht dst res)
            (.flip res)
            (.rewind dst)
            (.rewind src)
            (.rewind res)
            (or (= 0 (b/compare-buffer src res))
                (= 0 (b/compare-buffer src1 res))))
        freqs0 data0
        freqs1 data1
        freqs2 data2
        freqs3 data3))))

#_(let [freqs (repeatedly 65536 #(rand-int 1000000))
       ht    (sut/new-hu-tucker (long-array (map inc freqs)))]
   (test/defspec preserve-order-test
     100
     (prop/for-all
       [bs1 (gen/such-that #(< 1 (alength ^bytes %) c/+max-key-size+)
                           gen/bytes)
        bs2 (gen/such-that #(< 1 (alength ^bytes %) c/+max-key-size+)
                           gen/bytes)]
       (let [^ByteBuffer src1 (b/allocate-buffer c/+max-key-size+)
             ^ByteBuffer src2 (b/allocate-buffer c/+max-key-size+)
             ^ByteBuffer dst1 (b/allocate-buffer c/+max-key-size+)
             ^ByteBuffer dst2 (b/allocate-buffer c/+max-key-size+)]
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
         (is (u/same-sign? (b/compare-buffer src1 src2)
                           (b/compare-buffer dst1 dst2)))))))

(let [freqs (repeatedly 65536 #(rand-int 1000000))
      ht    (sut/new-hu-tucker (long-array (map inc freqs)))]
  (test/defspec round-trip-generative-test
    3
    (prop/for-all
      [^bytes bs (gen/such-that #(< 1 (alength ^bytes %) c/+max-key-size+)
                                gen/bytes)]
      (let [^ByteBuffer src (b/allocate-buffer c/+max-key-size+)
            ^ByteBuffer dst (b/allocate-buffer c/+max-key-size+)
            ^ByteBuffer res (b/allocate-buffer c/+max-key-size+)
            n+1             (inc (alength bs))
            ^bytes bs0      (byte-array n+1)]
        (System/arraycopy bs 0 bs0 0 (alength bs))
        (aset bs0 ^long (dec n+1) (byte 0))
        (b/put-bytes src bs)
        (.flip src)
        (sut/encode ht src dst)
        (.flip dst)
        (sut/decode ht dst res)
        (.flip res)
        (println "bs =>" (b/hexify bs))
        (let [^bytes bs1 (b/get-bytes res)]
          (println "bs1 =>" (b/hexify bs1))
          (is (or (Arrays/equals bs bs1)
                  (Arrays/equals bs0 bs1))))))))
