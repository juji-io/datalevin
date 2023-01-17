(ns datalevin.bits-test
  (:require
   [datalevin.bits :as sut]
   [datalevin.sparselist :as sl]
   [datalevin.datom :as d]
   [datalevin.constants :as c]
   [clojure.test :refer [deftest is]]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.clojure-test :as test]
   [clojure.test.check.properties :as prop]
   [datalevin.util :as u]
   [datalevin.bits :as b])
  (:import
   [java.util Arrays Date]
   [java.util.concurrent Semaphore]
   [java.nio ByteBuffer]
   [java.nio.charset StandardCharsets]
   [org.joda.time DateTime]
   [org.roaringbitmap RoaringBitmap]
   [datalevin.sparselist SparseIntArrayList]
   [datalevin.bits Indexable Retrieved]))

;; binary index preserves the order of values

(def e 123456)
(def a 235)

(defn- bf-compare
  "Jave ByteBuffer compareTo is byte-wise signed comparison, not good"
  [^ByteBuffer bf1 ^ByteBuffer bf2]
  (loop []
    (let [v1  (short (bit-and (.get bf1) (short 0xFF)))
          v2  (short (bit-and (.get bf2) (short 0xFF)))
          res (- v1 v2)]
      (if (not (zero? res))
        res
        (let [r1 (.remaining bf1)
              r2 (.remaining bf2)]
          (cond
            (= r1 r2 0)             0
            (and (< 0 r1) (= 0 r2)) 1
            (and (= 0 r1) (< 0 r2)) -1
            :else                   (recur)))))))

;; buffer read/write

(defn- bytes-size-less-than?
  [^long limit ^bytes bs]
  (< (alength bs) limit))

(defn- string-size-less-than?
  [^long limit ^String s]
  (< (alength (.getBytes s StandardCharsets/UTF_8)) limit))

(test/defspec data-generative-test
  100
  (prop/for-all [k gen/any-equatable]
                (let [^ByteBuffer bf (sut/allocate-buffer 16384)]
                  (.clear bf)
                  (sut/put-buffer bf k :data)
                  (.flip bf)
                  (= k (sut/read-buffer bf :data)))))

(test/defspec string-generative-test
  100
  (prop/for-all [k (gen/such-that (partial string-size-less-than? c/+val-bytes-wo-hdr+)
                                  gen/string)]
                (let [^ByteBuffer bf (sut/allocate-buffer 16384)]
                  (.clear bf)
                  (sut/put-buffer bf k :string)
                  (.flip bf)
                  (= k (sut/read-buffer bf :string)))))

(test/defspec int-generative-test
  100
  (prop/for-all [k gen/int]
                (let [^ByteBuffer bf (sut/allocate-buffer 16384)]
                  (.clear bf)
                  (sut/put-buffer bf k :int)
                  (.flip bf)
                  (= k (sut/read-buffer bf :int)))))

(test/defspec int-int-generative-test
  100
  (prop/for-all [k1 gen/int
                 k2 gen/int]
                (let [^ByteBuffer bf (sut/allocate-buffer 16384)]
                  (.clear bf)
                  (sut/put-buffer bf [k1 k2] :int-int)
                  (.flip bf)
                  (= [k1 k2] (sut/read-buffer bf :int-int)))))

(test/defspec doc-info-generative-test
  100
  (prop/for-all [k1 gen/small-integer
                 k2 gen/small-integer
                 k3 (gen/not-empty (gen/list-distinct gen/small-integer))]
                (let [^ByteBuffer bf (sut/allocate-buffer 16384)
                      ^ints ar       (int-array k3)]
                  (.clear bf)
                  (sut/put-buffer bf [k1 k2 ar] :doc-info)
                  (.flip bf)
                  (let [[k1' k2' ar'] (sut/read-buffer bf :doc-info)]
                    (is (and (= k1 k1')
                             (= k2 k2')
                             (Arrays/equals ar ^ints ar')))))))

(test/defspec term-info-generative-test
  100
  (prop/for-all [k1 gen/int
                 k2 (gen/double* {:NaN? false})
                 k3 (gen/vector gen/int)
                 k4 (gen/vector gen/int)]
                (let [^ByteBuffer bf         (sut/allocate-buffer 16384)
                      k2                     (float k2)
                      k3                     (sort k3)
                      ^SparseIntArrayList sl (sl/sparse-arraylist k3 k4)]
                  (.clear bf)
                  (sut/put-buffer bf [k1 k2 sl] :term-info)
                  (.flip bf)
                  (= [k1 k2 sl] (sut/read-buffer bf :term-info)))))

(test/defspec long-generative-test
  100
  (prop/for-all [k gen/large-integer]
                (let [^ByteBuffer bf (sut/allocate-buffer 16384)]
                  (.clear bf)
                  (sut/put-buffer bf k :long)
                  (.flip bf)
                  (= k (sut/read-buffer bf :long)))))

(test/defspec double-generative-test
  100
  (prop/for-all [k (gen/double* {:NaN? false})]
                (let [^ByteBuffer bf (sut/allocate-buffer 16384)]
                  (.clear bf)
                  (sut/put-buffer bf k :double)
                  (.flip bf)
                  (= k (sut/read-buffer bf :double)))))

(test/defspec float-generative-test
  100
  (prop/for-all [k (gen/double* {:NaN? false})]
                (let [f              (float k)
                      ^ByteBuffer bf (sut/allocate-buffer 16384)]
                  (.clear bf)
                  (sut/put-buffer bf f :float)
                  (.flip bf)
                  (= f (sut/read-buffer bf :float)))))

(def gen-bigint (gen/such-that
                  #(= clojure.lang.BigInt (type %))
                  gen/size-bounded-bigint))

(defn bigint-test
  [i j]
  (let [^ByteBuffer bf  (sut/allocate-buffer 816384)
        ^ByteBuffer bf1 (sut/allocate-buffer 816384)
        ^BigInteger m   (.toBigInteger ^clojure.lang.BigInt i)
        ^BigInteger n   (.toBigInteger ^clojure.lang.BigInt j)]
    (.clear bf)
    (sut/put-buffer bf m :bigint)
    (.flip bf)
    (.clear bf1)
    (sut/put-buffer bf1 n :bigint)
    (.flip bf1)
    (if (< (.compareTo m n) 0)
      (is (< (bf-compare bf bf1) 0))
      (if (= (.compareTo m n) 0)
        (is (= (bf-compare bf bf1) 0))
        (is (> (bf-compare bf bf1) 0))))
    (.rewind bf)
    (.rewind bf1)
    (let [^BigInteger m1 (sut/read-buffer bf :bigint)
          ^BigInteger n1 (sut/read-buffer bf1 :bigint)]
      (is (= (.compareTo m m1) 0))
      (is (= (.compareTo n n1) 0))
      (if (< (.compareTo m n) 0)
        (is (< (.compareTo m1 n1) 0))
        (if (= (.compareTo m n) 0)
          (is (= (.compareTo m1 n1) 0))
          (is (> (.compareTo m1 n1) 0)))))))

(test/defspec bigint-generative-test
  100
  (prop/for-all [i gen-bigint
                 j gen-bigint]
                (bigint-test i j)))

(defn bigdec-test
  [vi vj si sj]
  (let [^ByteBuffer bf  (sut/allocate-buffer 816384)
        ^ByteBuffer bf1 (sut/allocate-buffer 816384)
        ^BigInteger vi  (.toBigInteger ^clojure.lang.BigInt vi)
        ^BigInteger vj  (.toBigInteger ^clojure.lang.BigInt vj)
        ^BigDecimal m   (BigDecimal. vi ^int si)
        ^BigDecimal n   (BigDecimal. vj ^int sj)]
    (.clear bf)
    (sut/put-buffer bf m :bigdec)
    (.flip bf)
    (.clear bf1)
    (sut/put-buffer bf1 n :bigdec)
    (.flip bf1)
    (if (< (.compareTo m n) 0)
      (is (< (bf-compare bf bf1) 0))
      (if (= (.compareTo m n) 0)
        (is (= (bf-compare bf bf1) 0))
        (is (> (bf-compare bf bf1) 0))))
    (.rewind bf)
    (.rewind bf1)
    (let [^BigDecimal m1 (sut/read-buffer bf :bigdec)
          ^BigDecimal n1 (sut/read-buffer bf1 :bigdec)]
      (is (= (.compareTo m m1) 0))
      (is (= (.compareTo n n1) 0))
      (if (< (.compareTo m n) 0)
        (is (< (.compareTo m1 n1) 0))
        (if (= (.compareTo m n) 0)
          (is (= (.compareTo m1 n1) 0))
          (is (> (.compareTo m1 n1) 0)))))))

(test/defspec bigdec-generative-test
  100
  (prop/for-all [vi gen-bigint
                 vj gen-bigint
                 si gen/small-integer
                 sj gen/small-integer]
                (bigdec-test vi vj si sj)))

(test/defspec bytes-generative-test
  100
  (prop/for-all [^bytes k (gen/not-empty gen/bytes)]
                (let [^ByteBuffer bf (sut/allocate-buffer 16384)]
                  (.clear bf)
                  (sut/put-buffer bf k :bytes)
                  (.flip bf)
                  (Arrays/equals k ^bytes (sut/read-buffer bf :bytes)))))

(test/defspec byte-generative-test
  100
  (prop/for-all [k gen/byte]
                (let [^ByteBuffer bf (sut/allocate-buffer 16384)]
                  (.clear bf)
                  (sut/put-buffer bf k :byte)
                  (.flip bf)
                  (= k (sut/read-buffer bf :byte)))))

(test/defspec keyword-generative-test
  100
  (prop/for-all [k gen/keyword-ns]
                (let [^ByteBuffer bf (sut/allocate-buffer 16384)]
                  (.clear bf)
                  (sut/put-buffer bf k :keyword)
                  (.flip bf)
                  (= k (sut/read-buffer bf :keyword)))))

(test/defspec symbol-generative-test
  100
  (prop/for-all [k gen/symbol-ns]
                (let [^ByteBuffer bf (sut/allocate-buffer 16384)]
                  (.clear bf)
                  (sut/put-buffer bf k :symbol)
                  (.flip bf)
                  (= k (sut/read-buffer bf :symbol)))))

(test/defspec boolean-generative-test
  100
  (prop/for-all [k gen/boolean]
                (let [^ByteBuffer bf (sut/allocate-buffer 16384)]
                  (.clear bf)
                  (sut/put-buffer bf k :boolean)
                  (.flip bf)
                  (= k (sut/read-buffer bf :boolean)))))

(test/defspec instant-generative-test
  100
  (prop/for-all [k gen/int]
                (let [^ByteBuffer bf (sut/allocate-buffer 16384)
                      d              (Date. ^long k)]
                  (.clear bf)
                  (sut/put-buffer bf d :instant)
                  (.flip bf)
                  (= d (sut/read-buffer bf :instant)))))

(test/defspec instant-compare-test
  100
  (prop/for-all [k gen/int
                 k1 gen/int]
                (let [^ByteBuffer bf  (sut/allocate-buffer (inc Long/BYTES))
                      ^ByteBuffer bf1 (sut/allocate-buffer (inc Long/BYTES))
                      d               (Date. ^long k)
                      d1              (Date. ^long k1)
                      sign            (fn [^long diff]
                                        (cond
                                          (< 0 diff) 1
                                          (= 0 diff) 0
                                          (< diff 0) -1))]
                  (.clear bf)
                  (sut/put-buffer bf d :instant)
                  (.flip bf)
                  (.clear bf1)
                  (sut/put-buffer bf1 d1 :instant)
                  (.flip bf1)
                  (= (sign (- ^long k ^long k1)) (sign (bf-compare bf bf1))))))

(test/defspec uuid-generative-test
  100
  (prop/for-all [k gen/uuid]
                (let [^ByteBuffer bf (sut/allocate-buffer 16384)]
                  (.clear bf)
                  (sut/put-buffer bf k :uuid)
                  (.flip bf)
                  (= k (sut/read-buffer bf :uuid)))))

(deftest datom-test
  (let [d1             (d/datom 1 :pet/name "Mr. Kitty")
        ^ByteBuffer bf (sut/allocate-buffer 16384)]
    (.clear bf)
    (sut/put-buffer bf d1 :datom)
    (.flip bf)
    (is (= d1 (sut/deserialize (sut/serialize d1))))
    (is (= d1 (sut/read-buffer bf :datom)))))

(test/defspec datom-generative-test
  100
  (prop/for-all [e gen/large-integer
                 a gen/keyword-ns
                 v gen/any-equatable
                 t gen/large-integer]
                (let [^ByteBuffer bf (sut/allocate-buffer 16384)
                      d              (d/datom e a v t)]
                  (.clear bf)
                  (sut/put-buffer bf d :datom)
                  (.flip bf)
                  (is (= d (sut/read-buffer bf :datom))))))

(test/defspec attr-generative-test
  100
  (prop/for-all [k gen/keyword-ns]
                (let [^ByteBuffer bf (sut/allocate-buffer 16384)]
                  (.clear bf)
                  (sut/put-buffer bf k :attr)
                  (.flip bf)
                  (= k (sut/read-buffer bf :attr)))))

;; extrema bounds

(defn test-extrema
  [v d dmin dmax]
  (let [^ByteBuffer bf  (sut/allocate-buffer 16384)
        ^ByteBuffer bf1 (sut/allocate-buffer 16384)]
    (.clear bf)
    (sut/put-buffer bf d :eav)
    (.flip bf)
    (.clear bf1)
    (sut/put-buffer bf1 dmin :eav)
    (.flip bf1)
    (is (>= (bf-compare bf bf1) 0)
        (do
          (.rewind bf)
          (.rewind bf1)
          (str "v: " v
               " d: " (sut/hexify (sut/get-bytes bf))
               " dmin: " (sut/hexify (sut/get-bytes bf1)))))
    (.clear bf1)
    (sut/put-buffer bf1 dmax :eav)
    (.flip bf1)
    (.rewind bf)
    (is (<= (bf-compare bf bf1) 0))
    (.clear bf)
    (sut/put-buffer bf d :ave)
    (.flip bf)
    (.clear bf1)
    (sut/put-buffer bf1 dmin :ave)
    (.flip bf1)
    ;; TODO deal with occasional fail here, basically, empty character
    #_(is (>=(bf-compare bf bf1) 0)
          (do
            (.rewind bf)
            (.rewind bf1)
            (str "v: " v
                 " d: " (sut/hexify (sut/get-bytes bf))
                 " dmin: " (sut/hexify (sut/get-bytes bf1)))))
    (.clear bf1)
    (sut/put-buffer bf1 dmax :ave)
    (.flip bf1)
    (.rewind bf)
    (is (<= (bf-compare bf bf1) 0))))

(test/defspec keyword-extrema-generative-test
  100
  (prop/for-all
   [v  gen/keyword-ns]
   (test-extrema v
                 (sut/indexable e a v :db.type/keyword)
                 (sut/indexable e a c/v0 :db.type/keyword)
                 (sut/indexable e a c/vmax :db.type/keyword))))

(test/defspec symbol-extrema-generative-test
  100
  (prop/for-all
    [v  gen/symbol-ns]
    (test-extrema v
                  (sut/indexable e a v :db.type/symbol)
                  (sut/indexable e a c/v0 :db.type/symbol)
                  (sut/indexable e a c/vmax :db.type/symbol))))

;; null character "^@" is a special case
(test/defspec string-extrema-generative-test
  100
  (prop/for-all
    [v  (gen/such-that #(and (string-size-less-than? c/+val-bytes-wo-hdr+ %)
                             (not= "^@" %))
                       gen/string)]
    (test-extrema v
                  (sut/indexable e a v :db.type/string)
                  (sut/indexable e a c/v0 :db.type/string)
                  (sut/indexable e a c/vmax :db.type/string))))

(test/defspec boolean-extrema-generative-test
  5
  (prop/for-all
   [v  gen/boolean]
   (test-extrema v
                 (sut/indexable e a v :db.type/boolean)
                 (sut/indexable e a c/v0 :db.type/boolean)
                 (sut/indexable e a c/vmax :db.type/boolean))))

(test/defspec long-extrema-generative-test
  100
  (prop/for-all
   [v  gen/large-integer]
   (test-extrema v
                 (sut/indexable e a v :db.type/long)
                 (sut/indexable e a c/v0 :db.type/long)
                 (sut/indexable e a c/vmax :db.type/long))))

(test/defspec double-extrema-generative-test
  100
  (prop/for-all
    [v (gen/double* {:NaN? false})]
    (test-extrema v
                  (sut/indexable e a v :db.type/double)
                  (sut/indexable e a c/v0 :db.type/double)
                  (sut/indexable e a c/vmax :db.type/double))))

(test/defspec float-extrema-generative-test
  100
  (prop/for-all
    [v (gen/double* {:NaN? false})]
    (let [f (float v)]
      (test-extrema f
                    (sut/indexable e a f :db.type/float)
                    (sut/indexable e a c/v0 :db.type/float)
                    (sut/indexable e a c/vmax :db.type/float)))))

(test/defspec ref-extrema-generative-test
  100
  (prop/for-all
    [v  gen/nat]
    (test-extrema v
                  (sut/indexable e a v :db.type/ref)
                  (sut/indexable e a c/v0 :db.type/ref)
                  (sut/indexable e a c/vmax :db.type/ref))))

(test/defspec uuid-extrema-generative-test
  100
  (prop/for-all
    [v  gen/uuid]
    (test-extrema v
                  (sut/indexable e a v :db.type/uuid)
                  (sut/indexable e a c/v0 :db.type/uuid)
                  (sut/indexable e a c/vmax :db.type/uuid))))

(test/defspec bigint-extrema-generative-test
  100
  (prop/for-all
    [i  gen-bigint]
    (let [^BigInteger v (.toBigInteger ^clojure.lang.BigInt i)]
      (test-extrema v
                    (sut/indexable e a v :db.type/bigint)
                    (sut/indexable e a c/v0 :db.type/bigint)
                    (sut/indexable e a c/vmax :db.type/bigint)))))

(test/defspec bigdec-extrema-generative-test
  100
  (prop/for-all
    [i  gen-bigint
     s gen/small-integer]
    (let [^BigInteger n (.toBigInteger ^clojure.lang.BigInt i)
          ^BigDecimal v (BigDecimal. n ^int s)]
      (test-extrema v
                    (sut/indexable e a v :db.type/bigdec)
                    (sut/indexable e a c/v0 :db.type/bigdec)
                    (sut/indexable e a c/vmax :db.type/bigdec)))))

;; orders

(defn- ave-test
  [v e1 a1 v1 ^Indexable d ^Indexable d1]
  (let [^ByteBuffer bf  (sut/allocate-buffer 16384)
        ^ByteBuffer bf1 (sut/allocate-buffer 16384)
        _               (.clear ^ByteBuffer bf)
        _               (sut/put-buffer bf d :ave)
        _               (.flip ^ByteBuffer bf)
        _               (.clear ^ByteBuffer bf1)
        _               (sut/put-buffer bf1 d1 :ave)
        _               (.flip ^ByteBuffer bf1)
        ^long res       (bf-compare bf bf1)
        v-cmp           (compare v v1)]
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
        (is (> res 0))))
    (.rewind ^ByteBuffer bf)
    (let [^Retrieved r (sut/read-buffer bf :ave)]
      (is (= e (.-e r)))
      (is (= a (.-a r)))
      (is (= v (.-v r))))
    (.rewind ^ByteBuffer bf1)
    (let [^Retrieved r (sut/read-buffer bf1 :ave)]
      (is (= e1 (.-e r)))
      (is (= a1 (.-a r)))
      (is (= v1 (.-v r))))))

(defn- eav-test
  [v e1 a1 v1 ^Indexable d ^Indexable d1]
  (let [^ByteBuffer bf  (sut/allocate-buffer 16384)
        ^ByteBuffer bf1 (sut/allocate-buffer 16384)
        _               (.clear ^ByteBuffer bf)
        _               (sut/put-buffer bf d :eav)
        _               (.flip ^ByteBuffer bf)
        _               (.clear ^ByteBuffer bf1)
        _               (sut/put-buffer bf1 d1 :eav)
        _               (.flip ^ByteBuffer bf1)
        ^long  res      (bf-compare bf bf1)
        v-cmp           (compare v v1)]
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
        (is (> res 0))))
    (.rewind ^ByteBuffer bf)
    (let [^Retrieved r (sut/read-buffer bf :eav)]
      (is (= e (.-e r)))
      (is (= a (.-a r)))
      (is (= v (.-v r))))
    (.rewind ^ByteBuffer bf1)
    (let [^Retrieved r (sut/read-buffer bf1 :eav)]
      (is (= e1 (.-e r)))
      (is (= a1 (.-a r)))
      (is (= v1 (.-v r))))))

(defn- vea-test
  [v e1 a1 ^Indexable d ^Indexable d1]
  (let [^ByteBuffer bf  (sut/allocate-buffer 16384)
        ^ByteBuffer bf1 (sut/allocate-buffer 16384)
        _               (.clear ^ByteBuffer bf)
        _               (sut/put-buffer bf d :vea)
        _               (.flip ^ByteBuffer bf)
        _               (.clear ^ByteBuffer bf1)
        _               (sut/put-buffer bf1 d1 :vea)
        _               (.flip ^ByteBuffer bf1)
        ^long  res      (bf-compare bf bf1)]
    (if (= e e1)
      (if (= a a1)
        (is (= res 0))
        (if (< ^int a ^int a)
          (is (< res 0))
          (is (> res 0))))
      (if (< ^long e ^long e1)
        (is (< res 0))
        (is (> res 0))))
    (.rewind ^ByteBuffer bf)
    (let [^Retrieved r (sut/read-buffer bf :vea)]
      (is (= e (.-e r)))
      (is (= a (.-a r)))
      (is (= v (.-v r))))
    (.rewind ^ByteBuffer bf1)
    (let [^Retrieved r (sut/read-buffer bf1 :vea)]
      (is (= e1 (.-e r)))
      (is (= a1 (.-a r)))
      (is (= v (.-v r))))))

(test/defspec vea-generative-test
  100
  (prop/for-all
    [e1 (gen/large-integer* {:min c/e0})
     a1 gen/nat
     v  (gen/large-integer* {:min c/e0})]
    (vea-test v e1 a1
              (sut/indexable e a v :db.type/ref)
              (sut/indexable e1 a1 v :db.type/ref))))

(test/defspec instant-eav-generative-test
  100
  (prop/for-all
    [e1 (gen/large-integer* {:min c/e0})
     a1 gen/nat
     v1 gen/int
     v gen/int]
    (let [v'  (Date. ^long v)
          v1' (Date. ^long v1)]
      (eav-test v' e1 a1 v1'
                (sut/indexable e a v' :db.type/instant)
                (sut/indexable e1 a1 v1' :db.type/instant)))))

(test/defspec instant-vea-generative-test
  100
  (prop/for-all
    [e1 (gen/large-integer* {:min c/e0})
     a1 gen/nat
     v1 gen/int
     v gen/int]
    (let [v'  (Date. ^long v)
          v1' (Date. ^long v1)]
      (ave-test v' e1 a1 v1'
                (sut/indexable e a v' :db.type/instant)
                (sut/indexable e1 a1 v1' :db.type/instant)))))

(test/defspec keyword-eav-generative-test
  100
  (prop/for-all
    [e1 (gen/large-integer* {:min c/e0})
     a1 gen/nat
     v1 gen/keyword-ns
     v gen/keyword-ns]
    (eav-test v e1 a1 v1
              (sut/indexable e a v :db.type/keyword)
              (sut/indexable e1 a1 v1 :db.type/keyword))))

(test/defspec keyword-ave-generative-test
  100
  (prop/for-all
    [e1 (gen/large-integer* {:min c/e0})
     a1 gen/nat
     v1 gen/keyword-ns
     v gen/keyword-ns]
    (ave-test v e1 a1 v1
              (sut/indexable e a v :db.type/keyword)
              (sut/indexable e1 a1 v1 :db.type/keyword))))

(test/defspec symbol-eav-generative-test
  100
  (prop/for-all
    [e1 (gen/large-integer* {:min c/e0})
     a1 gen/nat
     v1 gen/symbol-ns
     v gen/symbol-ns]
    (eav-test v e1 a1 v1
              (sut/indexable e a v :db.type/symbol)
              (sut/indexable e1 a1 v1 :db.type/symbol))))

(test/defspec symbol-ave-generative-test
  100
  (prop/for-all
    [e1 (gen/large-integer* {:min c/e0})
     a1 gen/nat
     v1 gen/symbol-ns
     v  gen/symbol-ns]
    (ave-test v e1 a1 v1
              (sut/indexable e a v :db.type/symbol)
              (sut/indexable e1 a1 v1 :db.type/symbol))))

(test/defspec string-eav-generative-test
  100
  (prop/for-all
    [e1 (gen/large-integer* {:min c/e0})
     a1 gen/nat
     v1 gen/string
     v  gen/string]
    (eav-test v e1 a1 v1
              (sut/indexable e a v :db.type/string)
              (sut/indexable e1 a1 v1 :db.type/string))))

(test/defspec string-ave-generative-test
  100
  (prop/for-all
    [e1 (gen/large-integer* {:min c/e0})
     a1 gen/nat
     v1 gen/string
     v  gen/string]
    (ave-test v e1 a1 v1
              (sut/indexable e a v :db.type/string)
              (sut/indexable e1 a1 v1 :db.type/string))))

(test/defspec bigint-eav-generative-test
  100
  (prop/for-all
    [e1 (gen/large-integer* {:min c/e0})
     a1 gen/nat
     i1 gen-bigint
     i  gen-bigint]
    (let [^BigInteger v1 (.toBigInteger ^clojure.lang.BigInt i1)
          ^BigInteger v  (.toBigInteger ^clojure.lang.BigInt i)]
      (eav-test v e1 a1 v1
                (sut/indexable e a v :db.type/bigint)
                (sut/indexable e1 a1 v1 :db.type/bigint)))))

(test/defspec bigint-eav-generative-test
  100
  (prop/for-all
    [e1 (gen/large-integer* {:min c/e0})
     a1 gen/nat
     i1 gen-bigint
     i  gen-bigint]
    (let [^BigInteger v1 (.toBigInteger ^clojure.lang.BigInt i1)
          ^BigInteger v  (.toBigInteger ^clojure.lang.BigInt i)]
      (ave-test v e1 a1 v1
                (sut/indexable e a v :db.type/bigint)
                (sut/indexable e1 a1 v1 :db.type/bigint)))))

(test/defspec bigdec-eav-generative-test
  100
  (prop/for-all
    [e1 (gen/large-integer* {:min c/e0})
     a1 gen/nat
     i1 gen-bigint
     i  gen-bigint]
    (let [^BigInteger u1 (.toBigInteger ^clojure.lang.BigInt i1)
          ^BigInteger u  (.toBigInteger ^clojure.lang.BigInt i)
          v1             (BigDecimal. u1 -10)
          v              (BigDecimal. u -10)]
      (eav-test v e1 a1 v1
                (sut/indexable e a v :db.type/bigdec)
                (sut/indexable e1 a1 v1 :db.type/bigdec)))))

(test/defspec bigdec-ave-generative-test
  100
  (prop/for-all
    [e1 (gen/large-integer* {:min c/e0})
     a1 gen/nat
     i1 gen-bigint
     i  gen-bigint]
    (let [^BigInteger u1 (.toBigInteger ^clojure.lang.BigInt i1)
          ^BigInteger u  (.toBigInteger ^clojure.lang.BigInt i)
          v1             (BigDecimal. u1 -10)
          v              (BigDecimal. u -10)]
      (ave-test v e1 a1 v1
                (sut/indexable e a v :db.type/bigdec)
                (sut/indexable e1 a1 v1 :db.type/bigdec)))))

(test/defspec string-ave-generative-test
  100
  (prop/for-all
    [e1 (gen/large-integer* {:min c/e0})
     a1 gen/nat
     v1 gen/string
     v  gen/string]
    (ave-test v e1 a1 v1
              (sut/indexable e a v :db.type/string)
              (sut/indexable e1 a1 v1 :db.type/string))))

(test/defspec boolean-eav-generative-test
  100
  (prop/for-all
    [e1 (gen/large-integer* {:min c/e0})
     a1 gen/nat
     v1 gen/boolean
     v  gen/boolean]
    (eav-test v e1 a1 v1
              (sut/indexable e a v :db.type/boolean)
              (sut/indexable e1 a1 v1 :db.type/boolean))))

(test/defspec boolean-ave-generative-test
  100
  (prop/for-all
    [e1 (gen/large-integer* {:min c/e0})
     a1 gen/nat
     v1 gen/boolean
     v  gen/boolean]
    (ave-test v e1 a1 v1
              (sut/indexable e a v :db.type/boolean)
              (sut/indexable e1 a1 v1 :db.type/boolean))))

(test/defspec long-eav-generative-test
  100
  (prop/for-all
    [e1 (gen/large-integer* {:min c/e0})
     a1 gen/nat
     v1 gen/large-integer
     v  gen/large-integer]
    (eav-test v e1 a1 v1
              (sut/indexable e a v :db.type/long)
              (sut/indexable e1 a1 v1 :db.type/long))))


(test/defspec long-ave-generative-test
  100
  (prop/for-all
    [e1 (gen/large-integer* {:min c/e0})
     a1 gen/nat
     v1 gen/large-integer
     v  gen/large-integer]
    (ave-test v e1 a1 v1
              (sut/indexable e a v :db.type/long)
              (sut/indexable e1 a1 v1 :db.type/long))))

(test/defspec double-eav-generative-test
  100
  (prop/for-all
    [e1 (gen/large-integer* {:min c/e0})
     a1 gen/nat
     v1 (gen/double* {:NaN? false})
     v  (gen/double* {:NaN? false})]
    (eav-test v e1 a1 v1
              (sut/indexable e a v :db.type/double)
              (sut/indexable e1 a1 v1 :db.type/double))))

(test/defspec double-ave-generative-test
  100
  (prop/for-all
    [e1 (gen/large-integer* {:min c/e0})
     a1 gen/nat
     v1 (gen/double* {:NaN? false})
     v  (gen/double* {:NaN? false})]
    (ave-test v e1 a1 v1
              (sut/indexable e a v :db.type/double)
              (sut/indexable e1 a1 v1 :db.type/double))))

(test/defspec float-ave-generative-test
  100
  (prop/for-all
    [e1 (gen/large-integer* {:min c/e0})
     a1 gen/nat
     v1 (gen/double* {:NaN? false})
     v  (gen/double* {:NaN? false})]
    (let [f1 (float v1)
          f  (float v)]
      (ave-test f e1 a1 f1
                (sut/indexable e a f :db.type/float)
                (sut/indexable e1 a1 f1 :db.type/float)))))

(test/defspec uuid-eav-generative-test
  50
  (prop/for-all
    [v  gen/uuid]
    (let [^ByteBuffer bf (sut/allocate-buffer 16384)
          ^Indexable d   (sut/indexable e a v :db.type/uuid)
          _              (.clear ^ByteBuffer bf)
          _              (sut/put-buffer bf d :eav)
          _              (.flip ^ByteBuffer bf)
          ^Retrieved r   (sut/read-buffer bf :eav)]
      (is (= e (.-e r)))
      (is (= a (.-a r)))
      (is (= v (.-v r))))))

(test/defspec uuid-ave-generative-test
  50
  (prop/for-all
    [v  gen/uuid]
    (let [^ByteBuffer bf (sut/allocate-buffer 16384)
          ^Indexable d   (sut/indexable e a v :db.type/uuid)
          _              (.clear ^ByteBuffer bf)
          _              (sut/put-buffer bf d :ave)
          _              (.flip ^ByteBuffer bf)
          ^Retrieved r   (sut/read-buffer bf :ave)]
      (is (= e (.-e r)))
      (is (= a (.-a r)))
      (is (= v (.-v r))))))

(test/defspec bytes-eav-generative-test
  50
  (prop/for-all
    [v  (gen/such-that (partial bytes-size-less-than? c/+val-bytes-wo-hdr+)
                       gen/bytes)]
    (let [^ByteBuffer bf (sut/allocate-buffer 16384)
          ^Indexable d   (sut/indexable e a v :db.type/bytes)
          _              (.clear ^ByteBuffer bf)
          _              (sut/put-buffer bf d :eav)
          _              (.flip ^ByteBuffer bf)
          ^Retrieved r   (sut/read-buffer bf :eav)]
      (is (= e (.-e r)))
      (is (= a (.-a r)))
      (is (Arrays/equals ^bytes v ^bytes (.-v r))))))

(test/defspec bytes-ave-generative-test
  50
  (prop/for-all
    [v  (gen/such-that (partial bytes-size-less-than? c/+val-bytes-wo-hdr+)
                       gen/bytes)]
    (let [^ByteBuffer bf (sut/allocate-buffer 16384)
          ^Indexable d   (sut/indexable e a v :db.type/bytes)
          _              (.clear ^ByteBuffer bf)
          _              (sut/put-buffer bf d :ave)
          _              (.flip ^ByteBuffer bf)
          ^Retrieved r   (sut/read-buffer bf :ave)]
      (is (= e (.-e r)))
      (is (= a (.-a r)))
      (is (Arrays/equals ^bytes v ^bytes (.-v r))))))

(defn data-size-less-than?
  [^long limit data]
  (< (alength ^bytes (sut/serialize data)) limit))

(test/defspec data-eav-generative-test
  50
  (prop/for-all
    [v  (gen/such-that (partial data-size-less-than? c/+val-bytes-wo-hdr+)
                       gen/any-equatable)]
    (let [^ByteBuffer bf (sut/allocate-buffer 16384)
          ^Indexable d   (sut/indexable e a v nil)
          _              (.clear ^ByteBuffer bf)
          _              (sut/put-buffer bf d :eav)
          _              (.flip ^ByteBuffer bf)
          ^Retrieved r   (sut/read-buffer bf :eav)]
      (is (= e (.-e r)))
      (is (= a (.-a r)))
      (is (= v (.-v r))))))

(test/defspec data-ave-generative-test
  50
  (prop/for-all
    [v  (gen/such-that (partial data-size-less-than? c/+val-bytes-wo-hdr+)
                       gen/any-equatable)]
    (let [^ByteBuffer bf (sut/allocate-buffer 16384)
          ^Indexable d   (sut/indexable e a v nil)
          _              (.clear ^ByteBuffer bf)
          _              (sut/put-buffer bf d :ave)
          _              (.flip ^ByteBuffer bf)
          ^Retrieved r   (sut/read-buffer bf :ave)]
      (is (= e (.-e r)))
      (is (= a (.-a r)))
      (is (= v (.-v r))))))

(deftest bitmap-roundtrip-test
  (let [rr  (RoaringBitmap/bitmapOf (int-array [1 2 3 1000]))
        rr1 (sut/bitmap [1 2 3 1000])
        bf  (sut/allocate-buffer 16384)]
    (is (= rr rr1))
    (is (= 1000 (.select rr 3)))
    (is (= (.getCardinality rr) 4))
    (sut/put-buffer bf rr :bitmap)
    (.flip bf)
    (let [^RoaringBitmap rr1 (sut/read-buffer bf :bitmap)]
      (is (.equals rr rr1)))
    (sut/bitmap-add rr 4)
    (is (= 4 (.select rr 3)))
    (sut/bitmap-del rr 4)
    (is (= 1000 (.select rr 3)))))

(deftest data-serialize-test
  ;; TODO somehow this doesn't work in graal
  (when-not (u/graal?)
    (let [d1  (DateTime.)
          bs1 (sut/serialize d1)]
      (is (instance? org.joda.time.DateTime (sut/deserialize bs1))))
    (let [d  (Semaphore. 1)
          bs (sut/serialize d)]
      (is (not (instance? java.util.concurrent.Semaphore (sut/deserialize bs))))
      (binding [c/*data-serializable-classes* #{"java.util.concurrent.Semaphore"}]
        (is (instance? java.util.concurrent.Semaphore (sut/deserialize bs)))))))

(test/defspec base64-test
  100
  (prop/for-all [^bytes k (gen/not-empty gen/bytes)]
                (Arrays/equals k
                               ^bytes (sut/decode-base64
                                        (sut/encode-base64 k)))))
