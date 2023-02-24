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
   [java.nio ByteBuffer]
   [datalevin.hu DecodeNode]
   [datalevin.utl OptimalCodeLength]))

(deftest coding-test
  (let [^longs freqs0 (long-array [1 1 1 1 1 1 1 1])
        ^longs freqs1 (long-array [8 6 2 3 4 7 11 9 8 1 3])
        ^longs freqs2 (long-array [5 2 7 2 1 1 1 2 4 5])]
    (testing "level tree construction"
      (are [freqs levels] (= (seq (OptimalCodeLength/generate freqs)) levels)
        freqs0 [3 3 3 3 3 3 3 3]
        freqs1 [3 3 5 5 4 3 3 3 3 4 4]
        freqs2 [3 3 2 4 5 5 4 4 3 3]))
    (testing "code construction"
      (are [freqs results] (= (let [n (alength freqs)
                                    lens (byte-array n)
                                    codes (int-array n)]
                                (sut/create-codes lens codes freqs)
                                (vec codes))
                              results)
        freqs0 [0 1 2 3 4 5 6 7]
        freqs1 [0 1 8 9 5 3 4 5 6 14 15]
        freqs2 [0 1 1 8 18 19 10 11 6 7]))
    #_(testing "decoding table construction"
        (are [freqs results]
            (= (let [n (alength freqs)
                     lens (byte-array n)
                     codes (int-array n)]
                 (sut/create-codes lens codes freqs)
                 (sut/create-decode-table lens codes 2))
               results)
          freqs0 {}
          freqs1 {}
          freqs2 {}))))

#_(let [freqs (repeatedly 65536 #(rand-int 10000000))
        ht    (sut/new-hu-tucker (long-array (map inc freqs)))]
    (test/defspec preserve-order-test
      100
      (prop/for-all
        [bs1 (gen/such-that #(< 0 (alength ^bytes %) c/+max-key-size+)
                            gen/bytes)
         bs2 (gen/such-that #(< 0 (alength ^bytes %) c/+max-key-size+)
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
