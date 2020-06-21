(ns datalevin.bits-test
  (:require [datalevin.bits :as sut]
            [datalevin.datom :as d]
            [taoensso.nippy :as nippy]
            [taoensso.timbre :as log]
            [clojure.test :refer [deftest is]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.clojure-test :as test]
            [clojure.test.check.properties :as prop]
            [datalevin.constants :as c])
  (:import [java.util Arrays]
           [java.nio ByteBuffer]
           [datalevin.datom Datom]
           [datalevin.bits DatomIndexable]))

(def ^ByteBuffer bf (ByteBuffer/allocateDirect 16384))

(deftest datom-test
  (let [d1 (d/datom 1 :pet/name "Mr. Kitty")]
    (.clear bf)
    (sut/put-buffer bf d1 :datom)
    (.flip bf)
    (is (= d1 (nippy/thaw (nippy/freeze d1))))
    (is (= d1 (sut/read-buffer bf :datom)))))

(test/defspec bytes-generative-test
  1000
  (prop/for-all [^bytes k (gen/not-empty gen/bytes)]
                (.clear bf)
                (sut/put-buffer bf k :bytes)
                (.flip bf)
                (Arrays/equals k ^bytes (sut/read-buffer bf :bytes))))

(test/defspec byte-generative-test
  1000
  (prop/for-all [k gen/byte]
                (.clear bf)
                (sut/put-buffer bf k :byte)
                (.flip bf)
                (= k (sut/read-buffer bf :byte))))

(test/defspec data-generative-test
  1000
  (prop/for-all [k gen/any-equatable]
                (.clear bf)
                (sut/put-buffer bf k :data)
                (.flip bf)
                (= k (sut/read-buffer bf :data))))

(test/defspec long-generative-test
  1000
  (prop/for-all [k gen/large-integer]
                (.clear bf)
                (sut/put-buffer bf k :long)
                (.flip bf)
                (= k (sut/read-buffer bf :long))))

(test/defspec datom-generative-test
  1000
  (prop/for-all [e gen/large-integer
                 a gen/keyword-ns
                 v gen/any-equatable
                 t gen/large-integer]
                (let [d (d/datom e a v t)]
                  (.clear bf)
                  (sut/put-buffer bf d :datom)
                  (.flip bf)
                  (is (= d (sut/read-buffer bf :datom))))))

(def ^Datom d (d/datom 123456 :anunnaki/character "Enki"))
(def ^DatomIndexable di (sut/datom-indexable d))

(def ^ByteBuffer bf1 (ByteBuffer/allocateDirect 16384))

(defn- bf-compare
  "Jave ByteBuffer compareTo is byte-wise signed comparison, not good"
  [^ByteBuffer bf1 ^ByteBuffer bf2]
  (loop [i 0 j 0]
    (let [v1  (short (bit-and (.get bf1) (short 0xFF)))
          v2  (short (bit-and (.get bf2) (short 0xFF)))
          res (- v1 v2)]
      (if (not (zero? res))
        res
        (cond
          (and (= (.limit bf1) i)
               (= (.limit bf2) j)) 0
          (and (not= (.limit bf1) i)
               (= (.limit bf2) j)) 1
          (and (= (.limit bf1) i)
               (not= (.limit bf2) j)) -1
          :else (recur (inc i) (inc j)))))))

(test/defspec eavt-generative-test
  1000
  (prop/for-all
   [e (gen/large-integer* {:min c/e0})
    a gen/keyword-ns
    v gen/any-equatable
    t (gen/large-integer* {:min c/tx0 :max c/txmax})]
   (let [_                   (.clear ^ByteBuffer bf)
         _                   (sut/put-buffer bf di :eavt)
         _                   (.flip ^ByteBuffer bf)
         ^Datom d1           (d/datom e a v t)
         ^DatomIndexable di1 (sut/datom-indexable d1)
         _                   (.clear ^ByteBuffer bf1)
         _                   (sut/put-buffer bf1 di1 :eavt)
         _                   (.flip ^ByteBuffer bf1)
         ^long  res          (bf-compare bf bf1)
         e-d                 (.-e d)
         e-d1                (.-e d1)
         ^String a-d         (str (.-a d))
         ^String a-d1        (str (.-a d1))
         ^bytes v-d          (nippy/freeze (.-v d))
         ^bytes v-d1         (nippy/freeze (.-v d1))
         tx-d                (.-tx d)
         tx-d1               (.-tx d1)]
     (if (= e-d e-d1)
       (if (= (.compareTo a-d a-d1) 0)
         (if (= (Arrays/compare v-d v-d1) 0)
           (if (= tx-d tx-d1)
             (is (= (bf-compare bf bf1) 0))
             (if (< tx-d tx-d1)
               (is (< res 0))
               (is (> res 0))))
           (if (< (Arrays/compare v-d v-d1) 0)
             (is (< res 0))
             (is (> res 0))))
         (if (< (.compareTo a-d a-d1) 0)
           (is (< res 0))
           (is (> res 0))))
       (if (< e-d e-d1)
         (is (< res 0))
         (is (> res 0)))))))

(test/defspec aevt-generative-test
  1000
  (prop/for-all
   [e (gen/large-integer* {:min c/e0})
    a gen/keyword-ns
    v gen/any-equatable
    t (gen/large-integer* {:min c/tx0 :max c/txmax})]
   (let [_                   (.clear ^ByteBuffer bf)
         _                   (sut/put-buffer bf di :aevt)
         _                   (.flip ^ByteBuffer bf)
         ^Datom d1           (d/datom e a v t)
         ^DatomIndexable di1 (sut/datom-indexable d1)
         _                   (.clear ^ByteBuffer bf1)
         _                   (sut/put-buffer bf1 di1 :aevt)
         _                   (.flip ^ByteBuffer bf1)
         ^long  res          (bf-compare bf bf1)
         e-d                 (.-e d)
         e-d1                (.-e d1)
         ^String a-d         (str (.-a d))
         ^String a-d1        (str (.-a d1))
         ^bytes v-d          (nippy/freeze (.-v d))
         ^bytes v-d1         (nippy/freeze (.-v d1))
         tx-d                (.-tx d)
         tx-d1               (.-tx d1)]
     (if (= (.compareTo a-d a-d1) 0)
       (if (= e-d e-d1)
         (if (= (Arrays/compare v-d v-d1) 0)
           (if (= tx-d tx-d1)
             (is (= (bf-compare bf bf1) 0))
             (if (< tx-d tx-d1)
               (is (< res 0))
               (is (> res 0))))
           (if (< (Arrays/compare v-d v-d1) 0)
             (is (< res 0))
             (is (> res 0))))
         (if (< e-d e-d1)
           (is (< res 0))
           (is (> res 0))))
       (if (< (.compareTo a-d a-d1) 0)
         (is (< res 0))
         (is (> res 0)))))))

(test/defspec avet-generative-test
  1000
  (prop/for-all
   [e (gen/large-integer* {:min c/e0})
    a gen/keyword-ns
    v gen/any-equatable
    t (gen/large-integer* {:min c/tx0 :max c/txmax})]
   (let [_                   (.clear ^ByteBuffer bf)
         _                   (sut/put-buffer bf di :avet)
         _                   (.flip ^ByteBuffer bf)
         ^Datom d1           (d/datom e a v t)
         ^DatomIndexable di1 (sut/datom-indexable d1)
         _                   (.clear ^ByteBuffer bf1)
         _                   (sut/put-buffer bf1 di1 :avet)
         _                   (.flip ^ByteBuffer bf1)
         ^long  res          (bf-compare bf bf1)
         e-d                 (.-e d)
         e-d1                (.-e d1)
         ^String a-d         (str (.-a d))
         ^String a-d1        (str (.-a d1))
         ^bytes v-d          (nippy/freeze (.-v d))
         ^bytes v-d1         (nippy/freeze (.-v d1))
         tx-d                (.-tx d)
         tx-d1               (.-tx d1)]
     (if (= (.compareTo a-d a-d1) 0)
       (if (= (Arrays/compare v-d v-d1) 0)
         (if (= e-d e-d1)
           (if (= tx-d tx-d1)
             (is (= (bf-compare bf bf1) 0))
             (if (< tx-d tx-d1)
               (is (< res 0))
               (is (> res 0))))
           (if (< e-d e-d1)
             (is (< res 0))
             (is (> res 0))))
         (if (< (Arrays/compare v-d v-d1) 0)
           (is (< res 0))
           (is (> res 0))))
       (if (< (.compareTo a-d a-d1) 0)
         (is (< res 0))
         (is (> res 0)))))))

(test/defspec vaet-generative-test
  1000
  (prop/for-all
   [e (gen/large-integer* {:min c/e0})
    a gen/keyword-ns
    v gen/any-equatable
    t (gen/large-integer* {:min c/tx0 :max c/txmax})]
   (let [_                   (.clear ^ByteBuffer bf)
         _                   (sut/put-buffer bf di :vaet)
         _                   (.flip ^ByteBuffer bf)
         ^Datom d1           (d/datom e a v t)
         ^DatomIndexable di1 (sut/datom-indexable d1)
         _                   (.clear ^ByteBuffer bf1)
         _                   (sut/put-buffer bf1 di1 :vaet)
         _                   (.flip ^ByteBuffer bf1)
         ^long  res          (bf-compare bf bf1)
         e-d                 (.-e d)
         e-d1                (.-e d1)
         ^String a-d         (str (.-a d))
         ^String a-d1        (str (.-a d1))
         ^bytes v-d          (nippy/freeze (.-v d))
         ^bytes v-d1         (nippy/freeze (.-v d1))
         tx-d                (.-tx d)
         tx-d1               (.-tx d1)]
     (if (= (Arrays/compare v-d v-d1) 0)
       (if (= (.compareTo a-d a-d1) 0)
         (if (= e-d e-d1)
           (if (= tx-d tx-d1)
             (is (= (bf-compare bf bf1) 0))
             (if (< tx-d tx-d1)
               (is (< res 0))
               (is (> res 0))))
           (if (< e-d e-d1)
             (is (< res 0))
             (is (> res 0))))
         (if (< (.compareTo a-d a-d1) 0)
           (is (< res 0))
           (is (> res 0))))
       (if (< (Arrays/compare v-d v-d1) 0)
         (is (< res 0))
         (is (> res 0)))))))
