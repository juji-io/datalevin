(ns datalevin.query.source-native-value-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.bits :as bits]
            [datalevin.buffer :as buffer]
            [datalevin.constants :as c]
            [datalevin.datom :as datom]
            [datalevin.index :as index]
            [datalevin.query.source.native-value :as native-value]
            [datalevin.query.source-test :refer [category]]
            [taoensso.nippy :as nippy]
            [taoensso.nippy.impl :as impl]
            [taoensso.nippy.schema :as schema])
  (:import [java.io DataInput DataOutput]
           [java.nio ByteBuffer]
           [java.util ArrayList Arrays Date UUID]))

(defn- decode [bytes type]
  (native-value/giant-value (ByteBuffer/wrap bytes) type 1 :test/value))

(deftest scalar-datom-layouts
  (doseq [[type values]
          {:long [0 1 -1 128 -128 32768 -32768 2147483648 -2147483648
                  Long/MAX_VALUE Long/MIN_VALUE]
           :boolean [true false]
           :float [(float 1.25)] :double [0.0 1.25]
           :bigint [42N (biginteger -42)] :bigdec [1.00M -123456789.0001M]
           :instant [(Date. 123456)] :uuid [(UUID/randomUUID)]
           :keyword [:test/value (keyword "test" (apply str (repeat 600 "a")))]
           :symbol ['test/value (symbol "test" (apply str (repeat 600 "a")))]
           :string ["" "small" (apply str (repeat 200 "a"))
                    (apply str (repeat 200 "λ😀"))
                    (apply str (repeat 20000 "λ😀"))]}
          value values
          freeze [bits/serialize #(nippy/freeze % {:compressor nil})]]
    (is (= value (decode (freeze (datom/datom 1 :test/value value)) type))
        (str type " " (class value)))))

(defn- tuple-type [elements] {:kind :tuple :elements elements})

(deftest fixed-tuple-datom-layouts
  (doseq [[types value]
          [[[:long :string] [42 "λ😀"]]
           [[:keyword :symbol :long] [:test/value 'test/value nil]]
           [[:boolean :float :double :bigint :bigdec :instant :uuid]
            [false (float 1.25) 2.0 (biginteger 42) 1.00M (Date. 12) (UUID/randomUUID)]]
           [(vec (repeat 100 :long)) (vec (range 100))]
           [(vec (repeat 255 :string)) (vec (repeat 255 "value"))]]]
    (doseq [freeze [bits/serialize #(nippy/freeze % {:compressor nil})]]
      (is (= value (decode (freeze (datom/datom 1 :test/value value))
                           (tuple-type types)))))
    (let [indexable (bits/indexable 1 2 value types 1)]
      (when-not (bits/giant? indexable)
        (is (= value (native-value/inline-value
                       (ByteBuffer/wrap (bits/indexable-bytes indexable))
                       (tuple-type types))))))))

(deftest wide-tuple-encoding-with-a-cold-buffer-pool
  ;; Normal typed tuple writes must be independent of earlier pool borrowers.
  (let [borrowed (ArrayList.)]
    (.drainTo buffer/array-buffers borrowed)
    (try
      (let [value (vec (range 100))
            encoded (bits/indexable 1 2 value (vec (repeat 100 :long)) 7)]
        (is (bits/giant? encoded))
        (is (= 7 (bits/avg->giant-id (ByteBuffer/wrap (bits/indexable-bytes encoded))))))
      (finally (run! buffer/return-array-buffer borrowed)))))

(deftest tuple-preflight-rejects-malformed-and-generic-components
  (let [type (tuple-type [:long :string])
        bytes (bits/serialize (datom/datom 1 :test/value [42 "value"]))]
    (doseq [length (range (alength bytes))]
      (is (= :query-source/invalid
             (category #(decode (Arrays/copyOf bytes (int length)) type)))))
    (doseq [bad-type [(tuple-type [:long :string :long])
                     (tuple-type [:string :long])]]
      (is (= :query-source/invalid (category #(decode bytes bad-type))))))
  (doseq [value [[1 {:not "a scalar"}] [1 ["nested"]]]]
    (is (= :query-source/unsupported
           (category #(decode (bits/serialize (datom/datom 1 :test/value value))
                              (tuple-type [:long :string]))))))
  (let [bytes (bits/indexable-bytes (bits/indexable 1 2 [42 "value"] [:long :string] 1))
        type (tuple-type [:long :string])]
    ;; A generic/custom/nested component must never reach bits' fallback reader.
    (doseq [header [c/type-custom c/type-hete-tuple 99]]
      (let [bad (aclone bytes)]
        (aset-byte bad 6 (byte header))
        (is (= :query-source/unsupported
               (category #(native-value/inline-value (ByteBuffer/wrap bad) type))))))
    (doseq [[position replacement] [[5 3] [6 c/type-string]
                                   [(- (alength bytes) 5) 0]
                                   [(- (alength bytes) 4) 100]]]
      (let [bad (aclone bytes)]
        (aset-byte bad position (byte replacement))
        (is (= :query-source/invalid
               (category #(native-value/inline-value (ByteBuffer/wrap bad) type))))))))

(deftest giant-buffers-preserve-layout-and-borrowed-position
  (let [d (datom/datom 1 :test/value (apply str (repeat 4000 "large value")))
        raw (bits/serialize d)]
    (doseq [threshold [0 Long/MAX_VALUE]]
      (let [bytes (:value (binding [c/*giants-zstd-threshold* threshold]
                             (index/encode-giant-datom d)))
            buffer (doto (ByteBuffer/allocateDirect (+ 16 (alength ^bytes bytes)))
                     (.position 8) (.put ^bytes bytes) (.flip) (.position 8))]
        (is (= (alength raw) (index/giant-datom-raw-size buffer)))
        (is (= (:v d) (native-value/giant-value (index/giant-datom-buffer buffer)
                                               :string 1 :test/value)))
        (is (= 8 (.position buffer)))
        (when (zero? threshold)
          (is (< (alength ^bytes bytes) (alength raw))))))))

(deftest bounded-scalar-caches
  (let [bytes (fn [cache-id]
                (let [buffer (ByteBuffer/allocate 100)]
                  (.put buffer schema/id-prefixed-custom-md)
                  (.putShort buffer (short (impl/coerce-custom-type-id :datalevin/datom)))
                  (.putLong buffer 1)
                  (.put buffer schema/id-cached-md)
                  (.putShort buffer (short cache-id))
                  (nippy/freeze-to-bb! buffer :test/value)
                  (.put buffer schema/id-cached-md)
                  (.putShort buffer (short cache-id))
                  (nippy/freeze-to-bb! buffer 1)
                  (.flip buffer)
                  (bits/get-bytes buffer)))]
    (is (= :test/value (decode (bytes 0) :keyword)))
    (is (= :query-source/invalid (category #(decode (bytes 0) :string))))
    (is (= :query-source/unsupported (category #(decode (bytes Short/MAX_VALUE) :keyword))))))

(deftype UnexpectedValue [])
(def decoded-custom-values (atom 0))
(nippy/extend-freeze UnexpectedValue ::unexpected
  [x ^DataOutput out] (.writeInt out (if x 1 0)))
(nippy/extend-thaw ::unexpected
  [^DataInput in]
  (swap! decoded-custom-values inc)
  (.readInt in))

(deftest tuple-preflight-bounds-counts-caches-and-custom-readers
  (reset! decoded-custom-values 0)
  (is (= :query-source/unsupported
         (category #(decode (bits/serialize (datom/datom 1 :test/value [1 (UnexpectedValue.)]))
                            (tuple-type [:long :string])))))
  (is (zero? @decoded-custom-values))
  (doseq [length [-1 0 Integer/MAX_VALUE]]
    (let [buffer (ByteBuffer/allocate 100)]
      (.put buffer schema/id-prefixed-custom-md)
      (.putShort buffer (short (impl/coerce-custom-type-id :datalevin/datom)))
      (.putLong buffer 1)
      (nippy/freeze-to-bb! buffer :test/value)
      #_{:clj-kondo/ignore [:unresolved-var]}
      (.put buffer schema/id-vec-lg)
      (.putInt buffer length)
      (.flip buffer)
      (is (= :query-source/invalid
             (category #(native-value/giant-value buffer (tuple-type [:long :string])
                                                  1 :test/value))))))
  (let [bytes (fn [cache-id]
                (let [buffer (ByteBuffer/allocate 100)]
                  (.put buffer schema/id-prefixed-custom-md)
                  (.putShort buffer (short (impl/coerce-custom-type-id :datalevin/datom)))
                  (.putLong buffer 1)
                  (nippy/freeze-to-bb! buffer :test/value)
                  (.put buffer schema/id-vec-2)
                  (.put buffer schema/id-cached-md)
                  (.putShort buffer (short cache-id))
                  (nippy/freeze-to-bb! buffer "cached")
                  (.put buffer schema/id-cached-md)
                  (.putShort buffer (short cache-id))
                  (nippy/freeze-to-bb! buffer 1)
                  (.flip buffer)
                  (bits/get-bytes buffer)))]
    (is (= ["cached" "cached"] (decode (bytes 0) (tuple-type [:string :string]))))
    (is (= :query-source/invalid
           (category #(decode (bytes 0) (tuple-type [:string :long])))))
    (is (= :query-source/invalid
           (category #(decode (bytes 4) (tuple-type [:string :string])))))
    (is (= :query-source/unsupported
           (category #(decode (bytes Short/MAX_VALUE) (tuple-type [:string :string])))))))

(deftest preflight-rejects-before-invoking-readers
  (reset! decoded-custom-values 0)
  (doseq [value [(UnexpectedValue.) {:nested "value"}
                (with-meta 'value {:payload (UnexpectedValue.)})]]
    (is (= :query-source/unsupported
           (category #(decode (bits/serialize (datom/datom 1 :test/value value)) :string)))))
  (is (zero? @decoded-custom-values))
  (let [bytes (bits/serialize (datom/datom 1 :test/value "value"))]
    (doseq [length (range (alength bytes))]
      (is (= :query-source/invalid
             (category #(decode (Arrays/copyOf bytes length) :string)))))
    (doseq [[eid attribute type] [[2 :test/value :string] [1 :test/other :string]
                                 [1 :test/value :long]]]
      (is (= :query-source/invalid
             (category #(native-value/giant-value (ByteBuffer/wrap bytes) type eid attribute)))))
    (is (= :query-source/invalid
           (category #(decode (Arrays/copyOf bytes (inc (alength bytes))) :string)))))
  (doseq [length [-1 Integer/MAX_VALUE]]
    (let [buffer (ByteBuffer/allocate 100)]
      (.put buffer schema/id-prefixed-custom-md)
      (.putShort buffer (short (impl/coerce-custom-type-id :datalevin/datom)))
      (.putLong buffer 1)
      (nippy/freeze-to-bb! buffer :test/value)
      (.put buffer schema/id-str-lg)
      (.putInt buffer length)
      (.flip buffer)
      (is (= :query-source/invalid
             (category #(native-value/giant-value buffer :string 1 :test/value))))))
  (let [bytes (nippy/freeze (datom/datom 1 :test/value (apply str (repeat 10000 "a"))))]
    (is (= :query-source/unsupported (category #(decode bytes :string))))))
