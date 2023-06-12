(ns datalevin.hu-test
  (:require
   [datalevin.hu :as sut]
   [datalevin.util :as u]
   [datalevin.bits :as b]
   [datalevin.buffer :as bf]
   [datalevin.constants :as c]
   [clojure.test :refer [deftest testing is are]]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.clojure-test :as test]
   [clojure.test.check.properties :as prop]
   [taoensso.nippy :as nippy])
  (:import
   [java.util Arrays]
   [java.nio ByteBuffer]))

(deftest encoding-test
  (let [^longs freqs0 (long-array [1 1 1 1 1 1 1 1])
        ^longs freqs1 (long-array [8 6 2 3 4 7 11 9 8 1 3])
        ^longs freqs2 (long-array [5 2 7 2 1 1 1 2 4 5])]
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
        freqs2 [[0 1 1 8 18 19 10 11 6 7] [3 3 2 4 5 5 4 4 3 3]]))))

(test/defspec order-preservation-test
  1000
  (let [freqs            (repeatedly 65536 #(rand-int 1000000))
        ht               (sut/new-hu-tucker (long-array (map inc freqs)))
        ^ByteBuffer src1 (bf/allocate-buffer c/+max-key-size+)
        ^ByteBuffer src2 (bf/allocate-buffer c/+max-key-size+)
        ^ByteBuffer dst1 (bf/allocate-buffer c/+max-key-size+)
        ^ByteBuffer dst2 (bf/allocate-buffer c/+max-key-size+)]
    (prop/for-all
      [bs1 (gen/such-that #(< 0 (alength ^bytes %) c/+max-key-size+)
                          gen/bytes)
       bs2 (gen/such-that #(< 0 (alength ^bytes %) c/+max-key-size+)
                          gen/bytes)]
      (.clear src1)
      (.clear src2)
      (.clear dst1)
      (.clear dst2)
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
  (let [freqs (repeatedly 65536 #(rand-int 1000000))
        ht    (sut/new-hu-tucker (long-array (map inc freqs)))
        ;; ht      (nippy/fast-thaw (nippy/fast-freeze ht-orig))

        ^ByteBuffer src (bf/allocate-buffer c/+max-key-size+)
        ^ByteBuffer dst (bf/allocate-buffer c/+max-key-size+)
        ^ByteBuffer res (bf/allocate-buffer c/+max-key-size+)]
    (prop/for-all
      [^bytes bs (gen/such-that #(< 0 (alength ^bytes %) c/+max-key-size+)
                                gen/bytes)]
      (.clear src)
      (.clear dst)
      (.clear res)
      (b/put-bytes src bs)
      (.flip src)
      (sut/encode ht src dst)
      (.flip dst)
      (sut/decode ht dst res)
      (.flip res)
      (is (Arrays/equals bs ^bytes (b/get-bytes res))))))

(comment

  (def freqs (repeatedly c/compress-sample-size #(rand-int 1000000)))
  (def hu (sut/new-hu-tucker (long-array (map inc freqs))))
  (require '[clj-memory-meter.core :as mm])
  (mm/measure hu)
  ;; => "10.8 MiB"

  )
