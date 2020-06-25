(ns datalevin.bits-test
  (:require [datalevin.bits :as sut]
            [datalevin.datom :as d]
            [datalevin.constants :as c]
            [taoensso.nippy :as nippy]
            [taoensso.timbre :as log]
            [clojure.test :refer [deftest is]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.clojure-test :as test]
            [clojure.test.check.properties :as prop])
  (:import [java.util Arrays UUID]
           [java.nio ByteBuffer]
           [datalevin.datom Datom]
           [datalevin.bits Indexable]))

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

(test/defspec attr-generative-test
  1000
  (prop/for-all [k gen/keyword-ns]
                (.clear bf)
                (sut/put-buffer bf k :attr)
                (.flip bf)
                (= k (sut/read-buffer bf :attr))))

;; test indexing preserves the order of values

(def e 123456)
(def a 235)

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
               (= (.limit bf2) j))    0
          (and (not= (.limit bf1) i)
               (= (.limit bf2) j))    1
          (and (= (.limit bf1) i)
               (not= (.limit bf2) j)) -1
          :else                       (recur (inc i) (inc j)))))))

(test/defspec keyword-eav-generative-test
  1000
  (prop/for-all
   [e1 (gen/large-integer* {:min c/e0})
    a1 gen/nat
    v  gen/keyword-ns
    v1 gen/keyword-ns]
   (let [^Indexable d  (sut/indexable e a v :db.type/keyword)
         _             (.clear ^ByteBuffer bf)
         _             (sut/put-buffer bf d :eav)
         _             (.flip ^ByteBuffer bf)
         ^Indexable d1 (sut/indexable e1 a1 v1 :db.type/keyword)
         _             (.clear ^ByteBuffer bf1)
         _             (sut/put-buffer bf1 d1 :eav)
         _             (.flip ^ByteBuffer bf1)
         ^long  res    (bf-compare bf bf1)
         v-cmp         (compare v v1)]
     (if (= e e1)
       (if (= a a1)
         (if (= v-cmp 0)
           (is (= res 0))
           (if (< v-cmp 0)
             (is (< res 0))
             (is (> res 0))))
         (if (< ^int a ^int a1)
           (is (< res 0))
           (is (> res 0))))
       (if (< ^long e ^long e1)
         (is (< res 0))
         (is (> res 0)))))))

(test/defspec keyword-aev-generative-test
  1000
  (prop/for-all
   [e1 (gen/large-integer* {:min c/e0})
    a1 gen/nat
    v  gen/keyword-ns
    v1 gen/keyword-ns]
   (let [^Indexable d  (sut/indexable e a v :db.type/keyword)
         _             (.clear ^ByteBuffer bf)
         _             (sut/put-buffer bf d :aev)
         _             (.flip ^ByteBuffer bf)
         ^Indexable d1 (sut/indexable e1 a1 v1 :db.type/keyword)
         _             (.clear ^ByteBuffer bf1)
         _             (sut/put-buffer bf1 d1 :aev)
         _             (.flip ^ByteBuffer bf1)
         ^long  res    (bf-compare bf bf1)
         v-cmp         (compare v v1)]
     (if (= a a1)
       (if (= e e1)
         (if (= v-cmp 0)
           (is (= res 0))
           (if (< v-cmp 0)
             (is (< res 0))
             (is (> res 0))))
         (if (< ^long e ^long e1)
           (is (< res 0))
           (is (> res 0))))
       (if (< ^int a ^int a1)
         (is (< res 0))
         (is (> res 0)))))))

(test/defspec keyword-ave-generative-test
  1000
  (prop/for-all
   [e1 (gen/large-integer* {:min c/e0})
    a1 gen/nat
    v1 gen/keyword-ns
    v gen/keyword-ns]
   (let [^Indexable d  (sut/indexable e a v :db.type/keyword)
         _             (.clear ^ByteBuffer bf)
         _             (sut/put-buffer bf d :ave)
         _             (.flip ^ByteBuffer bf)
         ^Indexable d1 (sut/indexable e1 a1 v1 :db.type/keyword)
         _             (.clear ^ByteBuffer bf1)
         _             (sut/put-buffer bf1 d1 :ave)
         _             (.flip ^ByteBuffer bf1)
         ^long res     (bf-compare bf bf1)
         v-cmp         (compare v v1)]
     (if (= a a1)
       (if (= v-cmp 0)
         (if (= e e1)
           (is (= res 0))
           (if (< ^long e ^long e1)
             (is (< res 0))
             (is (> res 0))))
         (if (< v-cmp 0)
           (is (< res 0))
           (is (> res 0))))
       (if (< ^int a ^int a1)
         (is (< res 0))
         (is (> res 0)))))))

(test/defspec keyword-vae-generative-test
  1000
  (prop/for-all
   [e1 (gen/large-integer* {:min c/e0})
    a1 gen/nat
    v1 gen/keyword-ns
    v gen/keyword-ns]
   (let [^Indexable d  (sut/indexable e a v :db.type/keyword)
         _             (.clear ^ByteBuffer bf)
         _             (sut/put-buffer bf d :vae)
         _             (.flip ^ByteBuffer bf)
         ^Indexable d1 (sut/indexable e1 a1 v1 :db.type/keyword)
         _             (.clear ^ByteBuffer bf1)
         _             (sut/put-buffer bf1 d1 :vae)
         _             (.flip ^ByteBuffer bf1)
         ^long  res    (bf-compare bf bf1)
         v-cmp         (compare v v1)]
     (if (= v-cmp 0)
       (if (= a a1)
         (if (= e e1)
           (is (= res 0))
           (if (< ^long e ^long e1)
             (is (< res 0))
             (is (> res 0))))
         (if (< ^int a ^int a1)
           (is (< res 0))
           (is (> res 0))))
       (if (< v-cmp 0)
         (is (< res 0))
         (is (> res 0)))))))

(test/defspec symbol-eav-generative-test
  1000
  (prop/for-all
   [e1 (gen/large-integer* {:min c/e0})
    a1 gen/nat
    v1 gen/symbol-ns
    v gen/symbol-ns]
   (let [^Indexable d  (sut/indexable e a v :db.type/symbol)
         _             (.clear ^ByteBuffer bf)
         _             (sut/put-buffer bf d :eav)
         _             (.flip ^ByteBuffer bf)
         ^Indexable d1 (sut/indexable e1 a1 v1 :db.type/symbol)
         _             (.clear ^ByteBuffer bf1)
         _             (sut/put-buffer bf1 d1 :eav)
         _             (.flip ^ByteBuffer bf1)
         ^long  res    (bf-compare bf bf1)
         v-cmp         (compare v v1)]
     (if (= e e1)
       (if (= a a1)
         (if (= v-cmp 0)
           (is (= res 0))
           (if (< v-cmp 0)
             (is (< res 0))
             (is (> res 0))))
         (if (< ^int a ^int a1)
           (is (< res 0))
           (is (> res 0))))
       (if (< ^long e ^long e1)
         (is (< res 0))
         (is (> res 0)))))))

(test/defspec symbol-aev-generative-test
  1000
  (prop/for-all
   [e1 (gen/large-integer* {:min c/e0})
    a1 gen/nat
    v1 gen/symbol-ns
    v  gen/symbol-ns]
   (let [^Indexable d  (sut/indexable e a v :db.type/symbol)
         _             (.clear ^ByteBuffer bf)
         _             (sut/put-buffer bf d :aev)
         _             (.flip ^ByteBuffer bf)
         ^Indexable d1 (sut/indexable e1 a1 v1 :db.type/symbol)
         _             (.clear ^ByteBuffer bf1)
         _             (sut/put-buffer bf1 d1 :aev)
         _             (.flip ^ByteBuffer bf1)
         ^long  res    (bf-compare bf bf1)
         v-cmp         (compare v v1)]
     (if (= a a1)
       (if (= e e1)
         (if (= v-cmp 0)
           (is (= res 0))
           (if (< v-cmp 0)
             (is (< res 0))
             (is (> res 0))))
         (if (< ^long e ^long e1)
           (is (< res 0))
           (is (> res 0))))
       (if (< ^int a ^int a1)
         (is (< res 0))
         (is (> res 0)))))))

(test/defspec symbol-ave-generative-test
  1000
  (prop/for-all
   [e1 (gen/large-integer* {:min c/e0})
    a1 gen/nat
    v1 gen/symbol-ns
    v  gen/symbol-ns]
   (let [^Indexable d  (sut/indexable e a v :db.type/symbol)
         _             (.clear ^ByteBuffer bf)
         _             (sut/put-buffer bf d :ave)
         _             (.flip ^ByteBuffer bf)
         ^Indexable d1 (sut/indexable e1 a1 v1 :db.type/symbol)
         _             (.clear ^ByteBuffer bf1)
         _             (sut/put-buffer bf1 d1 :ave)
         _             (.flip ^ByteBuffer bf1)
         ^long  res    (bf-compare bf bf1)
         v-cmp         (compare v v1)]
     (if (= a a1)
       (if (= v-cmp 0)
         (if (= e e1)
           (is (= res 0))
           (if (< ^long e ^long e1)
             (is (< res 0))
             (is (> res 0))))
         (if (< v-cmp 0)
           (is (< res 0))
           (is (> res 0))))
       (if (< ^int a ^int a1)
         (is (< res 0))
         (is (> res 0)))))))

(test/defspec symbol-vae-generative-test
  1000
  (prop/for-all
   [e1 (gen/large-integer* {:min c/e0})
    a1 gen/nat
    v1 gen/symbol-ns
    v  gen/symbol-ns]
   (let [^Indexable d  (sut/indexable e a v :db.type/symbol)
         _             (.clear ^ByteBuffer bf)
         _             (sut/put-buffer bf d :vae)
         _             (.flip ^ByteBuffer bf)
         ^Indexable d1 (sut/indexable e1 a1 v1 :db.type/symbol)
         _             (.clear ^ByteBuffer bf1)
         _             (sut/put-buffer bf1 d1 :vae)
         _             (.flip ^ByteBuffer bf1)
         ^long  res    (bf-compare bf bf1)
         v-cmp         (compare v v1)]
     (if (= v-cmp 0)
       (if (= a a1)
         (if (= e e1)
           (is (= res 0))
           (if (< ^long e ^long e1)
             (is (< res 0))
             (is (> res 0))))
         (if (< ^int a ^int a1)
           (is (< res 0))
           (is (> res 0))))
       (if (< v-cmp 0)
         (is (< res 0))
         (is (> res 0)))))))

(test/defspec string-eav-generative-test
  1000
  (prop/for-all
   [e1 (gen/large-integer* {:min c/e0})
    a1 gen/nat
    v1 gen/string
    v gen/string]
   (let [^Indexable d  (sut/indexable e a v :db.type/string)
         _             (.clear ^ByteBuffer bf)
         _             (sut/put-buffer bf d :eav)
         _             (.flip ^ByteBuffer bf)
         ^Indexable d1 (sut/indexable e1 a1 v1 :db.type/string)
         _             (.clear ^ByteBuffer bf1)
         _             (sut/put-buffer bf1 d1 :eav)
         _             (.flip ^ByteBuffer bf1)
         ^long  res    (bf-compare bf bf1)
         v-cmp         (compare v v1)]
     (if (= e e1)
       (if (= a a1)
         (if (= v-cmp 0)
           (is (= res 0))
           (if (< v-cmp 0)
             (is (< res 0))
             (is (> res 0))))
         (if (< ^int a ^int a1)
           (is (< res 0))
           (is (> res 0))))
       (if (< ^long e ^long e1)
         (is (< res 0))
         (is (> res 0)))))))

(test/defspec string-aev-generative-test
  1000
  (prop/for-all
   [e1 (gen/large-integer* {:min c/e0})
    a1 gen/nat
    v1 gen/string
    v gen/string]
   (let [^Indexable d  (sut/indexable e a v :db.type/string)
         _             (.clear ^ByteBuffer bf)
         _             (sut/put-buffer bf d :aev)
         _             (.flip ^ByteBuffer bf)
         ^Indexable d1 (sut/indexable e1 a1 v1 :db.type/string)
         _             (.clear ^ByteBuffer bf1)
         _             (sut/put-buffer bf1 d1 :aev)
         _             (.flip ^ByteBuffer bf1)
         ^long  res    (bf-compare bf bf1)
         v-cmp         (compare v v1)]
     (if (= a a1)
       (if (= e e1)
         (if (= v-cmp 0)
           (is (= res 0))
           (if (< v-cmp 0)
             (is (< res 0))
             (is (> res 0))))
         (if (< ^long e ^long e1)
           (is (< res 0))
           (is (> res 0))))
       (if (< ^int a ^int a1)
         (is (< res 0))
         (is (> res 0)))))))

(test/defspec string-ave-generative-test
  1000
  (prop/for-all
   [e1 (gen/large-integer* {:min c/e0})
    a1 gen/nat
    v1 gen/string
    v gen/string]
   (let [^Indexable d  (sut/indexable e a v :db.type/string)
         _             (.clear ^ByteBuffer bf)
         _             (sut/put-buffer bf d :ave)
         _             (.flip ^ByteBuffer bf)
         ^Indexable d1 (sut/indexable e1 a1 v1 :db.type/string)
         _             (.clear ^ByteBuffer bf1)
         _             (sut/put-buffer bf1 d1 :ave)
         _             (.flip ^ByteBuffer bf1)
         ^long  res    (bf-compare bf bf1)
         v-cmp         (compare v v1)]
     (if (= a a1)
       (if (= v-cmp 0)
         (if (= e e1)
           (is (= res 0))
           (if (< ^long e ^long e1)
             (is (< res 0))
             (is (> res 0))))
         (if (< v-cmp 0)
           (is (< res 0))
           (is (> res 0))))
       (if (< ^int a ^int a1)
         (is (< res 0))
         (is (> res 0)))))))

(test/defspec string-vae-generative-test
  1000
  (prop/for-all
   [e1 (gen/large-integer* {:min c/e0})
    a1 gen/nat
    v1 gen/string
    v gen/string]
   (let [^Indexable d  (sut/indexable e a v :db.type/string)
         _             (.clear ^ByteBuffer bf)
         _             (sut/put-buffer bf d :vae)
         _             (.flip ^ByteBuffer bf)
         ^Indexable d1 (sut/indexable e1 a1 v1 :db.type/string)
         _             (.clear ^ByteBuffer bf1)
         _             (sut/put-buffer bf1 d1 :vae)
         _             (.flip ^ByteBuffer bf1)
         ^long  res    (bf-compare bf bf1)
         v-cmp         (compare v v1)]
     (if (= v-cmp 0)
       (if (= a a1)
         (if (= e e1)
           (is (= res 0))
           (if (< ^long e ^long e1)
             (is (< res 0))
             (is (> res 0))))
         (if (< ^int a ^int a1)
           (is (< res 0))
           (is (> res 0))))
       (if (< v-cmp 0)
         (is (< res 0))
         (is (> res 0)))))))

(test/defspec boolean-eav-generative-test
  1000
  (prop/for-all
   [e1 (gen/large-integer* {:min c/e0})
    a1 gen/nat
    v1 gen/boolean
    v gen/boolean]
   (let [^Indexable d  (sut/indexable e a v :db.type/boolean)
         _             (.clear ^ByteBuffer bf)
         _             (sut/put-buffer bf d :eav)
         _             (.flip ^ByteBuffer bf)
         ^Indexable d1 (sut/indexable e1 a1 v1 :db.type/boolean)
         _             (.clear ^ByteBuffer bf1)
         _             (sut/put-buffer bf1 d1 :eav)
         _             (.flip ^ByteBuffer bf1)
         ^long  res    (bf-compare bf bf1)
         v-cmp         (compare v v1)]
     (if (= e e1)
       (if (= a a1)
         (if (= v-cmp 0)
           (is (= res 0))
           (if (< v-cmp 0)
             (is (< res 0))
             (is (> res 0))))
         (if (< ^int a ^int a1)
           (is (< res 0))
           (is (> res 0))))
       (if (< ^long e ^long e1)
         (is (< res 0))
         (is (> res 0)))))))

(test/defspec boolean-aev-generative-test
  1000
  (prop/for-all
   [e1 (gen/large-integer* {:min c/e0})
    a1 gen/nat
    v1 gen/boolean
    v  gen/boolean]
   (let [^Indexable d  (sut/indexable e a v :db.type/boolean)
         _             (.clear ^ByteBuffer bf)
         _             (sut/put-buffer bf d :aev)
         _             (.flip ^ByteBuffer bf)
         ^Indexable d1 (sut/indexable e1 a1 v1 :db.type/boolean)
         _             (.clear ^ByteBuffer bf1)
         _             (sut/put-buffer bf1 d1 :aev)
         _             (.flip ^ByteBuffer bf1)
         ^long  res    (bf-compare bf bf1)
         v-cmp         (compare v v1)]
     (if (= a a1)
       (if (= e e1)
         (if (= v-cmp 0)
           (is (= res 0))
           (if (< v-cmp 0)
             (is (< res 0))
             (is (> res 0))))
         (if (< ^long e ^long e1)
           (is (< res 0))
           (is (> res 0))))
       (if (< ^int a ^int a1)
         (is (< res 0))
         (is (> res 0)))))))

(test/defspec boolean-ave-generative-test
  1000
  (prop/for-all
   [e1 (gen/large-integer* {:min c/e0})
    a1 gen/nat
    v1 gen/boolean
    v  gen/boolean]
   (let [^Indexable d  (sut/indexable e a v :db.type/boolean)
         _             (.clear ^ByteBuffer bf)
         _             (sut/put-buffer bf d :ave)
         _             (.flip ^ByteBuffer bf)
         ^Indexable d1 (sut/indexable e1 a1 v1 :db.type/boolean)
         _             (.clear ^ByteBuffer bf1)
         _             (sut/put-buffer bf1 d1 :ave)
         _             (.flip ^ByteBuffer bf1)
         ^long  res    (bf-compare bf bf1)
         v-cmp         (compare v v1)]
     (if (= a a1)
       (if (= v-cmp 0)
         (if (= e e1)
           (is (= res 0))
           (if (< ^long e ^long e1)
             (is (< res 0))
             (is (> res 0))))
         (if (< v-cmp 0)
           (is (< res 0))
           (is (> res 0))))
       (if (< ^int a ^int a1)
         (is (< res 0))
         (is (> res 0)))))))

(test/defspec boolean-vae-generative-test
  1000
  (prop/for-all
   [e1 (gen/large-integer* {:min c/e0})
    a1 gen/nat
    v1 gen/boolean
    v  gen/boolean]
   (let [^Indexable d  (sut/indexable e a v :db.type/boolean)
         _             (.clear ^ByteBuffer bf)
         _             (sut/put-buffer bf d :vae)
         _             (.flip ^ByteBuffer bf)
         ^Indexable d1 (sut/indexable e1 a1 v1 :db.type/boolean)
         _             (.clear ^ByteBuffer bf1)
         _             (sut/put-buffer bf1 d1 :vae)
         _             (.flip ^ByteBuffer bf1)
         ^long  res    (bf-compare bf bf1)
         v-cmp         (compare v v1)]
     (if (= v-cmp 0)
       (if (= a a1)
         (if (= e e1)
           (is (= res 0))
           (if (< ^long e ^long e1)
             (is (< res 0))
             (is (> res 0))))
         (if (< ^int a ^int a1)
           (is (< res 0))
           (is (> res 0))))
       (if (< v-cmp 0)
         (is (< res 0))
         (is (> res 0)))))))

(test/defspec long-eav-generative-test
  1000
  (prop/for-all
   [e1 (gen/large-integer* {:min c/e0})
    a1 gen/nat
    v1 gen/large-integer
    v  gen/large-integer]
   (let [^Indexable d  (sut/indexable e a v :db.type/long)
         _             (.clear ^ByteBuffer bf)
         _             (sut/put-buffer bf d :eav)
         _             (.flip ^ByteBuffer bf)
         ^Indexable d1 (sut/indexable e1 a1 v1 :db.type/long)
         _             (.clear ^ByteBuffer bf1)
         _             (sut/put-buffer bf1 d1 :eav)
         _             (.flip ^ByteBuffer bf1)
         ^long  res    (bf-compare bf bf1)
         v-cmp         (compare v v1)]
     (if (= e e1)
       (if (= a a1)
         (if (= v-cmp 0)
           (is (= res 0))
           (if (< v-cmp 0)
             (is (< res 0))
             (is (> res 0))))
         (if (< ^int a ^int a1)
           (is (< res 0))
           (is (> res 0))))
       (if (< ^long e ^long e1)
         (is (< res 0))
         (is (> res 0)))))))

(test/defspec long-aev-generative-test
  1000
  (prop/for-all
   [e1 (gen/large-integer* {:min c/e0})
    a1 gen/nat
    v1 gen/large-integer
    v  gen/large-integer]
   (let [^Indexable d  (sut/indexable e a v :db.type/long)
         _             (.clear ^ByteBuffer bf)
         _             (sut/put-buffer bf d :aev)
         _             (.flip ^ByteBuffer bf)
         ^Indexable d1 (sut/indexable e1 a1 v1 :db.type/long)
         _             (.clear ^ByteBuffer bf1)
         _             (sut/put-buffer bf1 d1 :aev)
         _             (.flip ^ByteBuffer bf1)
         ^long  res    (bf-compare bf bf1)
         v-cmp         (compare v v1)]
     (if (= a a1)
       (if (= e e1)
         (if (= v-cmp 0)
           (is (= res 0))
           (if (< v-cmp 0)
             (is (< res 0))
             (is (> res 0))))
         (if (< ^long e ^long e1)
           (is (< res 0))
           (is (> res 0))))
       (if (< ^int a ^int a1)
         (is (< res 0))
         (is (> res 0)))))))

(test/defspec long-ave-generative-test
  1000
  (prop/for-all
   [e1 (gen/large-integer* {:min c/e0})
    a1 gen/nat
    v1 gen/large-integer
    v gen/large-integer]
   (let [^Indexable d  (sut/indexable e a v :db.type/long)
         _             (.clear ^ByteBuffer bf)
         _             (sut/put-buffer bf d :ave)
         _             (.flip ^ByteBuffer bf)
         ^Indexable d1 (sut/indexable e1 a1 v1 :db.type/long)
         _             (.clear ^ByteBuffer bf1)
         _             (sut/put-buffer bf1 d1 :ave)
         _             (.flip ^ByteBuffer bf1)
         ^long  res    (bf-compare bf bf1)
         v-cmp         (compare v v1)]
     (if (= a a1)
       (if (= v-cmp 0)
         (if (= e e1)
           (is (= res 0))
           (if (< ^long e ^long e1)
             (is (< res 0))
             (is (> res 0))))
         (if (< v-cmp 0)
           (is (< res 0))
           (is (> res 0))))
       (if (< ^int a ^int a1)
         (is (< res 0))
         (is (> res 0)))))))

(test/defspec long-vae-generative-test
  1000
  (prop/for-all
   [e1 (gen/large-integer* {:min c/e0})
    a1 gen/nat
    v1 gen/large-integer
    v  gen/large-integer]
   (let [^Indexable d  (sut/indexable e a v :db.type/long)
         _             (.clear ^ByteBuffer bf)
         _             (sut/put-buffer bf d :vae)
         _             (.flip ^ByteBuffer bf)
         ^Indexable d1 (sut/indexable e1 a1 v1 :db.type/long)
         _             (.clear ^ByteBuffer bf1)
         _             (sut/put-buffer bf1 d1 :vae)
         _             (.flip ^ByteBuffer bf1)
         ^long  res    (bf-compare bf bf1)
         v-cmp         (compare v v1)]
     (if (= v-cmp 0)
       (if (= a a1)
         (if (= e e1)
           (is (= res 0))
           (if (< ^long e ^long e1)
             (is (< res 0))
             (is (> res 0))))
         (if (< ^int a ^int a1)
           (is (< res 0))
           (is (> res 0))))
       (if (< v-cmp 0)
         (is (< res 0))
         (is (> res 0)))))))

(test/defspec double-eav-generative-test
  1000
  (prop/for-all
   [e1 (gen/large-integer* {:min c/e0})
    a1 gen/nat
    v1 (gen/double* {:NaN? false})
    v (gen/double* {:NaN? false})]
   (let [^Indexable d  (sut/indexable e a v :db.type/double)
         _             (.clear ^ByteBuffer bf)
         _             (sut/put-buffer bf d :eav)
         _             (.flip ^ByteBuffer bf)
         ^Indexable d1 (sut/indexable e1 a1 v1 :db.type/double)
         _             (.clear ^ByteBuffer bf1)
         _             (sut/put-buffer bf1 d1 :eav)
         _             (.flip ^ByteBuffer bf1)
         ^long  res    (bf-compare bf bf1)
         v-cmp         (compare v v1)]
     (if (= e e1)
       (if (= a a1)
         (if (= v-cmp 0)
           (is (= res 0))
           (if (< v-cmp 0)
             (is (< res 0))
             (is (> res 0))))
         (if (< ^int a ^int a1)
           (is (< res 0))
           (is (> res 0))))
       (if (< ^long e ^long e1)
         (is (< res 0))
         (is (> res 0)))))))

(test/defspec double-aev-generative-test
  1000
  (prop/for-all
   [e1 (gen/large-integer* {:min c/e0})
    a1 gen/nat
    v1 (gen/double* {:NaN? false})
    v (gen/double* {:NaN? false})]
   (let [^Indexable d  (sut/indexable e a v :db.type/double)
         _             (.clear ^ByteBuffer bf)
         _             (sut/put-buffer bf d :aev)
         _             (.flip ^ByteBuffer bf)
         ^Indexable d1 (sut/indexable e1 a1 v1 :db.type/double)
         _             (.clear ^ByteBuffer bf1)
         _             (sut/put-buffer bf1 d1 :aev)
         _             (.flip ^ByteBuffer bf1)
         ^long  res    (bf-compare bf bf1)
         v-cmp         (compare v v1)]
     (if (= a a1)
       (if (= e e1)
         (if (= v-cmp 0)
           (is (= res 0))
           (if (< v-cmp 0)
             (is (< res 0))
             (is (> res 0))))
         (if (< ^long e ^long e1)
           (is (< res 0))
           (is (> res 0))))
       (if (< ^int a ^int a1)
         (is (< res 0))
         (is (> res 0)))))))

(test/defspec double-ave-generative-test
  1000
  (prop/for-all
   [e1 (gen/large-integer* {:min c/e0})
    a1 gen/nat
    v1 (gen/double* {:NaN? false})
    v (gen/double* {:NaN? false})]
   (let [^Indexable d  (sut/indexable e a v :db.type/double)
         _             (.clear ^ByteBuffer bf)
         _             (sut/put-buffer bf d :ave)
         _             (.flip ^ByteBuffer bf)
         ^Indexable d1 (sut/indexable e1 a1 v1 :db.type/double)
         _             (.clear ^ByteBuffer bf1)
         _             (sut/put-buffer bf1 d1 :ave)
         _             (.flip ^ByteBuffer bf1)
         ^long  res    (bf-compare bf bf1)
         v-cmp         (compare v v1)]
     (if (= a a1)
       (if (= v-cmp 0)
         (if (= e e1)
           (is (= res 0))
           (if (< ^long e ^long e1)
             (is (< res 0))
             (is (> res 0))))
         (if (< v-cmp 0)
           (is (< res 0))
           (is (> res 0))))
       (if (< ^int a ^int a1)
         (is (< res 0))
         (is (> res 0)))))))

(test/defspec double-vae-generative-test
  1000
  (prop/for-all
   [e1 (gen/large-integer* {:min c/e0})
    a1 gen/nat
    v1 (gen/double* {:NaN? false})
    v (gen/double* {:NaN? false})]
   (let [^Indexable d  (sut/indexable e a v :db.type/double)
         _             (.clear ^ByteBuffer bf)
         _             (sut/put-buffer bf d :vae)
         _             (.flip ^ByteBuffer bf)
         ^Indexable d1 (sut/indexable e1 a1 v1 :db.type/double)
         _             (.clear ^ByteBuffer bf1)
         _             (sut/put-buffer bf1 d1 :vae)
         _             (.flip ^ByteBuffer bf1)
         ^long  res    (bf-compare bf bf1)
         v-cmp         (compare v v1)]
     (if (= v-cmp 0)
       (if (= a a1)
         (if (= e e1)
           (is (= res 0))
           (if (< ^long e ^long e1)
             (is (< res 0))
             (is (> res 0))))
         (if (< ^int a ^int a1)
           (is (< res 0))
           (is (> res 0))))
       (if (< v-cmp 0)
         (is (< res 0))
         (is (> res 0)))))))

;; TODO test-check doesn't have a float generator yet
