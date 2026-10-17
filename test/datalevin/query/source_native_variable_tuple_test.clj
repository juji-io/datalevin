(ns datalevin.query.source-native-variable-tuple-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.bits :as bits]
            [datalevin.constants :as c]
            [datalevin.datom :as datom]
            [datalevin.query.source.native-value :as native-value]
            [datalevin.query.source.value :as value]
            [datalevin.query.source-native-value-test :as fixture]
            [datalevin.query.source-test :refer [category]]
            [taoensso.nippy :as nippy]
            [taoensso.nippy.impl :as impl]
            [taoensso.nippy.schema :as schema])
  (:import [java.nio ByteBuffer]
           [java.util Arrays Date UUID]))

(defn- tuple-type [element] {:kind :native-homogeneous-tuple :element element})

(defn- prepare [bytes element]
  (native-value/prepare-giant (ByteBuffer/wrap bytes) (tuple-type element) 1 :test/value))

(defn- prefix ^ByteBuffer []
  (let [buffer (ByteBuffer/allocate 8192)]
    (.put buffer schema/id-prefixed-custom-md)
    (.putShort buffer (short (impl/coerce-custom-type-id :datalevin/datom)))
    (.putLong buffer 1)
    (nippy/freeze-to-bb! buffer :test/value)
    buffer))

(deftest variable-tuple-layouts
  (doseq [[element xs]
          {:long [Long/MIN_VALUE -1 0 1 Long/MAX_VALUE]
           :boolean [true false] :float [(float 1.25) (float -2.5)]
           :double [0.0 1.25] :bigint [(biginteger 42) (biginteger -42)]
           :bigdec [1.00M -123456789.0001M] :instant [(Date. 123456)]
           :uuid [(UUID/randomUUID)] :keyword [:test/value :other]
           :symbol ['test/value 'other] :string ["" "a" "λ😀"]}
          n [1 2 3 31 128 255]
          :let [x (vec (take n (cycle xs)))
                type (tuple-type element)]]
    (doseq [freeze [bits/serialize #(nippy/freeze % {:compressor nil})]]
      (let [{:keys [decode retained-bytes]} (prepare (freeze (datom/datom 1 :test/value x)) element)]
        (is (= x (decode)))
        (is (>= (long retained-bytes) (long (value/value-charge x))))))
    (let [indexable (bits/indexable 1 2 x [element] 1)]
      (when-not (bits/giant? indexable)
        (let [raw (ByteBuffer/wrap (bits/indexable-bytes indexable))
              prepared (native-value/prepare-inline raw type)]
          (is (zero? (.position raw)))
          (is (= x ((:decode prepared)))))))))

(deftest variable-tuple-preflight-rejects-shape-type-and-truncation
  (doseq [x [[] (vec (range 256)) [nil] [1 nil] [1 "wrong"] (list 1 2) 1]]
    (is (= :query-source/invalid
           (category #(prepare (bits/serialize (datom/datom 1 :test/value x)) :long)))
        (pr-str x)))
  (let [bytes (bits/serialize (datom/datom 1 :test/value [1 2 3]))]
    (dotimes [n (alength bytes)]
      (is (= :query-source/invalid
             (category #(prepare (Arrays/copyOf bytes n) :long)))))
    (is (= :query-source/invalid
           (category #(prepare (Arrays/copyOf bytes (inc (alength bytes))) :long)))))
  (doseq [n [-1 0 256 Integer/MAX_VALUE]]
    (let [buffer (prefix)]
      (.put buffer (byte 21)) ; Nippy vec-lg
      (.putInt buffer n)
      (.flip buffer)
      (is (= :query-source/invalid
             (category #(native-value/prepare-giant buffer (tuple-type :long) 1 :test/value)))))))

(deftest inline-variable-tuple-framing-is-checked-before-decoding
  (doseq [[element xs] [[:long [1 2]] [:string ["" "a"]]]]
    (let [bytes (bits/indexable-bytes (bits/indexable 1 2 xs [element] 1))
          type (tuple-type element)]
      (doseq [[position replacement]
              (concat [[6 0] [6 3] [5 c/type-boolean]]
                      (when (= element :string)
                        [[7 1] [9 1] [10 0] [11 100] [12 100]]))]
        (let [bad (aclone bytes)]
          (aset-byte bad position (byte replacement))
          (is (= :query-source/invalid
                 (category #(native-value/prepare-inline (ByteBuffer/wrap bad) type))))))
      (dotimes [n (alength bytes)]
        (is (= :query-source/invalid
               (category #(native-value/prepare-inline
                            (ByteBuffer/wrap (Arrays/copyOf bytes n)) type)))))
      (doseq [[header expected] [[c/type-custom :query-source/unsupported]
                                [c/type-homo-tuple :query-source/unsupported]
                                [c/type-bytes :query-source/invalid]]]
        (let [bad (aclone bytes)]
          (aset-byte bad 5 (byte header))
          (is (= expected
                 (category #(native-value/prepare-inline (ByteBuffer/wrap bad) type)))))))))

(deftest inline-tuple-counts-and-component-lengths-are-unsigned
  (doseq [[x types type]
          [[(vec (repeat 128 nil)) (vec (repeat 128 :long))
            {:kind :tuple :elements (vec (repeat 128 :long))}]
           [[(.repeat "x" 200)] [:string] (tuple-type :string)]
           [[(.repeat "x" 200) ""] [:string :string]
            {:kind :tuple :elements [:string :string]}]]]
    (let [indexable (bits/indexable 1 2 x types 1)
          raw (ByteBuffer/wrap (bits/indexable-bytes indexable))]
      (is (not (bits/giant? indexable)))
      (is (= x ((:decode (native-value/prepare-inline raw type))))))))

(deftest variable-tuple-caches-are-bounded-and-charged-per-occurrence
  (let [bytes (fn [id]
                (let [buffer (prefix)]
                  (.put buffer schema/id-vec-2)
                  (.put buffer schema/id-cached-md)
                  (.putShort buffer (short id))
                  (nippy/freeze-to-bb! buffer (.repeat "λ" 1000))
                  (.put buffer schema/id-cached-md)
                  (.putShort buffer (short id))
                  (nippy/freeze-to-bb! buffer 1)
                  (.flip buffer)
                  (bits/get-bytes buffer)))
        {:keys [decode retained-bytes]} (prepare (bytes 0) :string)]
    (is (= (vec (repeat 2 (.repeat "λ" 1000))) (decode)))
    (is (>= (long retained-bytes) (long (value/value-charge (decode)))))
    (is (= :query-source/invalid (category #(prepare (bytes 1) :string))))
    (is (= :query-source/unsupported (category #(prepare (bytes Short/MAX_VALUE) :string)))))
  (let [buffer (prefix)]
    (.put buffer (byte 21))
    (.putInt buffer 255)
    (dotimes [i 255]
      (.put buffer schema/id-cached-md)
      (.putShort buffer (short i))
      (nippy/freeze-to-bb! buffer (long i)))
    (nippy/freeze-to-bb! buffer 1)
    (.flip buffer)
    (is (= (vec (range 255))
           ((:decode (native-value/prepare-giant buffer (tuple-type :long) 1 :test/value)))))))

(deftest unsupported-components-and-cancellation-never-reach-readers
  (reset! fixture/decoded-custom-values 0)
  (doseq [x [[(fixture/->UnexpectedValue)] [[1]]
             (with-meta [1] {:hidden (fixture/->UnexpectedValue)})]]
    (is (= :query-source/unsupported
           (category #(prepare (bits/serialize (datom/datom 1 :test/value x)) :long)))))
  (is (zero? @fixture/decoded-custom-values))
  (let [bytes (bits/serialize (datom/datom 1 :test/value (vec (range 255))))]
    (is (= :query-source/cancelled
           (category #(native-value/prepare-giant
                        (ByteBuffer/wrap bytes) (tuple-type :long) 1 :test/value
                        (fn [] (value/fail! :query-source/cancelled :execution))))))
    (is (= :query-source/unsupported
           (category #(native-value/giant-value
                        (ByteBuffer/wrap bytes) (tuple-type :long) 1 :test/value))))))
