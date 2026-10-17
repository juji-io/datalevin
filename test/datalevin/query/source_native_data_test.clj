(ns datalevin.query.source-native-data-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.bits :as bits]
            [datalevin.datom :as datom]
            [datalevin.query.source.native-value :as native-value]
            [datalevin.query.source.value :as value]
            [datalevin.query.source-test :refer [category]]
            [taoensso.nippy :as nippy]
            [taoensso.nippy.impl :as impl]
            [taoensso.nippy.schema :as schema])
  (:import [java.io DataInput DataOutput]
           [java.nio ByteBuffer]
           [java.time Duration Instant Period]
           [java.util Arrays Date UUID]))

(defn- prepare [bytes]
  (native-value/prepare-giant (ByteBuffer/wrap bytes) :data 1 :test/value))

(defn- decode [bytes] ((:decode (prepare bytes))))

(defn- prefix ^ByteBuffer [^ByteBuffer buffer attribute]
  (.put buffer schema/id-prefixed-custom-md)
  (.putShort buffer (short (impl/coerce-custom-type-id :datalevin/datom)))
  (.putLong buffer 1)
  (nippy/freeze-to-bb! buffer attribute)
  buffer)

(defn cached-dag
  "Encoded vector of successively doubled cached subtrees; no expanded values
  are constructed. Also used by the integrated quota regression."
  ^bytes [attribute levels]
  (let [buffer (prefix (ByteBuffer/allocate 8192) attribute)]
    (.put buffer (byte 21)) ; Nippy vec-lg
    (.putInt buffer levels)
    (dotimes [i levels]
      (.put buffer schema/id-cached-md)
      (.putShort buffer (short i))
      (.put buffer schema/id-vec-2)
      (dotimes [_ 2]
        (if (zero? i)
          (nippy/freeze-to-bb! buffer nil)
          (do (.put buffer schema/id-cached-md)
              (.putShort buffer (short (dec i)))))))
    (nippy/freeze-to-bb! buffer 1)
    (.flip buffer)
    (bits/get-bytes buffer)))

(deftest portable-data-layouts-and-conservative-charges
  (doseq [x [true false (byte 42) (short 42) (int 42) 0 42 -42 42N (biginteger 42) 1.00M
             \λ 1/2 -7/3 9223372036854775808/3
             (float 1.5) 2.5 "λ😀" :test/key 'test/symbol (Date. 12) (UUID/randomUUID)
             Instant/MIN Instant/MAX (Instant/ofEpochSecond -1 123456789)
             (Duration/ofSeconds Long/MIN_VALUE) (Duration/ofSeconds Long/MAX_VALUE 999999999)
             (Period/of Integer/MIN_VALUE Integer/MAX_VALUE -12)
             [] {} #{} '() [1 nil :key ["nested"]]
             (with-meta [1 2] {:source ["local"]})
             (with-meta 'value {:source :test})
             {:key [1 2] [nil :composite] #{"a" "b"} :list '(nil true "value")}
             (vec (repeat 400 nil)) (apply list (range 400))
             (zipmap (range 400) (repeat nil)) (set (range 400))
             (sorted-map) (sorted-set) (sorted-map :a 1/2 :b [\x (short 2)])
             (with-meta (sorted-set 1/2 2/3) {:source :sorted})
             (into (sorted-map) (map (fn [i] [i [\λ 1/2]])) (range 400))
             (into (sorted-set) (range 400))]
          freeze [bits/serialize #(nippy/freeze % {:compressor nil})]]
    (let [bytes (freeze (datom/datom 1 :test/value x))
          {:keys [decode retained-bytes]} (prepare bytes)
          decoded (decode)]
      (is (= x decoded))
      (is (= (class x) (class decoded)))
      (is (= (meta x) (meta decoded)))
      (is (>= (long retained-bytes) (long (value/value-charge decoded)))))
    (let [indexable (bits/indexable 1 2 x :db.type/data 1)]
      (when-not (bits/giant? indexable)
        (let [{:keys [decode retained-bytes]}
              (native-value/prepare-inline (ByteBuffer/wrap (bits/indexable-bytes indexable)) :data)]
          (is (= x (decode)))
          (is (>= (long retained-bytes) (long (value/value-charge x)))))))))

(deftest legacy-inline-data-keeps-the-borrowed-buffer-position
  (let [x {:nested [nil (with-meta 'value {:source :test})]}
        bytes (nippy/freeze x {:compressor nil})
        raw (doto (ByteBuffer/allocate (+ 6 (alength ^bytes bytes)))
              (.putInt 2) (.put ^bytes bytes) (.put (byte 0)) (.put (byte 1))
              (.flip))
        prepared (native-value/prepare-inline raw :data)]
    (is (= 0 (.position raw)))
    (is (= x ((:decode prepared))))
    (is (= 0 (.position raw)))))

(defn- time-payload ^bytes [tag ^long seconds ^long nanos]
  (.array (doto (ByteBuffer/allocate 13)
            (.put (byte tag)) (.putLong seconds) (.putInt (int nanos)))))

(deftest temporal-layouts-check-lengths-types-and-constructor-errors
  (doseq [tag [schema/id-time-instant schema/id-time-duration schema/id-time-period]]
    (let [bytes (time-payload tag 0 0)]
      (dotimes [length (alength bytes)]
        (is (= :query-source/invalid
               (category #(native-value/prepare-data
                            (ByteBuffer/wrap (Arrays/copyOf bytes length)) (fn []))))))
      (is (= :query-source/invalid
             (category #(native-value/prepare-data
                          (ByteBuffer/wrap (Arrays/copyOf bytes (inc (alength bytes)))) (fn [])))))))
  (doseq [[tag seconds nanos]
          [[schema/id-time-instant Long/MAX_VALUE 0]
           [schema/id-time-instant Long/MIN_VALUE 0]
           [schema/id-time-duration Long/MAX_VALUE Integer/MAX_VALUE]
           [schema/id-time-duration Long/MIN_VALUE Integer/MIN_VALUE]]]
    (let [{:keys [decode]} (native-value/prepare-data
                            (ByteBuffer/wrap (time-payload tag seconds nanos)) (fn []))]
      (is (= :query-source/invalid (category decode)))))
  ;; Nippy normalizes nanosecond adjustments. Preserve its decoding semantics.
  (doseq [tag [schema/id-time-instant schema/id-time-duration]
          nanos [-1 1000000000 Integer/MAX_VALUE]]
    (let [bytes (time-payload tag 0 nanos)
          {:keys [decode]} (native-value/prepare-data (ByteBuffer/wrap bytes) (fn []))]
      (is (= (nippy/thaw-from-bb! (ByteBuffer/wrap bytes)) (decode)))))
  (is (= :query-source/invalid
         (category #(native-value/prepare-giant
                      (ByteBuffer/wrap (bits/serialize (datom/datom 1 :test/value Instant/EPOCH)))
                      :instant 1 :test/value)))))

(deftest counts-truncation-and-depth-are-checked-before-decoding
  (let [bytes (bits/serialize (datom/datom 1 :test/value {:nested [1 nil "value"]}))]
    (dotimes [length (alength bytes)]
      (is (= :query-source/invalid (category #(decode (Arrays/copyOf bytes length))))))
    (is (= :query-source/invalid
           (category #(decode (Arrays/copyOf bytes (inc (alength bytes))))))))
  (doseq [tag [21 23 30 20 28 31 29] ; includes current/legacy sorted collections
          length [-1 Integer/MAX_VALUE]]
    (let [buffer (prefix (ByteBuffer/allocate 100) :test/value)]
      (.put buffer (byte tag))
      (.putInt buffer length)
      (nippy/freeze-to-bb! buffer 1)
      (.flip buffer)
      (is (= :query-source/invalid
             (category #(native-value/prepare-giant buffer :data 1 :test/value))))))
  (let [nested (fn [n] (nth (iterate vector 1) n))]
    (is (= (nested 64) (decode (bits/serialize (datom/datom 1 :test/value (nested 64))))))
    (is (= :query-source/unsupported
           (category #(decode (bits/serialize (datom/datom 1 :test/value (nested 65)))))))))

(deftype UnexpectedData [])
(def decoded-custom-values (atom 0))
(nippy/extend-freeze UnexpectedData ::unexpected
  [x ^DataOutput out] (.writeInt out (if x 1 0)))
(nippy/extend-thaw ::unexpected
  [^DataInput in] (swap! decoded-custom-values inc) (.readInt in))

(deftest unsupported-data-never-invokes-readers
  (reset! decoded-custom-values 0)
  (doseq [x [(UnexpectedData.) {:nested (UnexpectedData.)} (with-meta [1] {:hidden (UnexpectedData.)})
             (sorted-map :a (UnexpectedData.))
             clojure.lang.PersistentQueue/EMPTY]]
    (is (= :query-source/unsupported
           (category #(decode (bits/serialize (datom/datom 1 :test/value x)))))))
  (is (zero? @decoded-custom-values))
  (is (= :query-source/invalid
         (category #(decode (bits/serialize (datom/datom 1 :test/value nil)))))))

(defn malformed-sorted-data
  "Well-sized sorted-map payload whose keys cannot be compared."
  ^bytes []
  (let [buffer (ByteBuffer/allocate 100)]
    (.put buffer (byte 31))
    (.putInt buffer 2)
    (doseq [x [:key 1 "incomparable" 2]] (nippy/freeze-to-bb! buffer x))
    (.flip buffer)
    (bits/get-bytes buffer)))

(deftest malformed-portable-data-is-invalid
  (let [prepared (native-value/prepare-data (ByteBuffer/wrap (malformed-sorted-data)) (fn []))]
    ;; The layout is bounded, but construction must report a structural error.
    (is (= :query-source/invalid (category (:decode prepared)))))
  (doseq [x [\λ (byte 2) (short 3) 2/3 (sorted-map :a 1 :b 2) (sorted-set 1 2)]
          :let [bytes (bits/serialize x)]
          length (range (alength bytes))]
    (is (= :query-source/invalid
           (category #(native-value/prepare-data
                        (ByteBuffer/wrap (Arrays/copyOf bytes (int length))) (fn []))))))
  (doseq [[numerator-length denominator-length denominator]
          [[0 1 1] [-1 1 1] [Integer/MAX_VALUE 1 1] [1 0 1]
           [1 -1 1] [1 Integer/MAX_VALUE 1] [1 1 0]]]
    (let [buffer (doto (ByteBuffer/allocate 11)
                   (.put (byte 70)) (.putInt numerator-length) (.put (byte 1))
                   (.putInt denominator-length) (.put (byte denominator)) (.flip))]
      (is (= :query-source/invalid
             (category #(native-value/prepare-data buffer (fn [])))))))
  ;; The old map tag counts individual key/value slots, so odd counts reject.
  (let [buffer (doto (ByteBuffer/allocate 8) (.put (byte 29)) (.putInt 3)
                 (.put (byte 0)) (.put (byte 0)) (.put (byte 0)) (.flip))]
    (is (= :query-source/invalid
           (category #(native-value/prepare-data buffer (fn [])))))))

(deftest legacy-sorted-map-slot-count
  (let [bytes (bits/serialize (sorted-map :a 1 :b 2))
        buffer (ByteBuffer/wrap bytes)]
    (.put buffer 0 (byte 29))
    (.putInt buffer 1 4)
    (let [decoded ((:decode (native-value/prepare-data buffer (fn []))))]
      (is (sorted? decoded))
      (is (= (sorted-map :a 1 :b 2) decoded)))))

(deftest caches-count-expanded-occurrences-and-cannot-hide-depth
  (let [small (prepare (cached-dag :test/value 6))
        large (prepare (cached-dag :test/value 40))]
    (is (>= (long (:retained-bytes small)) (long (value/value-charge ((:decode small))))))
    (is (> (long (:retained-bytes large)) 1000000000000)))
  (is (= Long/MAX_VALUE (:retained-bytes (prepare (cached-dag :test/value 60)))))
  (is (= :query-source/unsupported (category #(prepare (cached-dag :test/value 64)))))
  (let [buffer (prefix (ByteBuffer/allocate 100) :test/value)]
    (.put buffer schema/id-cached-md)
    (.putShort buffer (short 0))
    (.put buffer schema/id-vec-2)
    (.put buffer schema/id-cached-md)
    (.putShort buffer (short 0))
    (nippy/freeze-to-bb! buffer nil)
    (nippy/freeze-to-bb! buffer 1)
    (.flip buffer)
    (is (= :query-source/invalid
           (category #(native-value/prepare-giant buffer :data 1 :test/value))))))

(deftest generic-cache-entry-boundary
  (let [bytes (fn [n]
                (let [buffer (prefix (ByteBuffer/allocate 8192) :test/value)]
                  (.put buffer (byte 21)) ; Nippy vec-lg
                  (.putInt buffer n)
                  (dotimes [i n]
                    (.put buffer schema/id-cached-md)
                    (.putShort buffer (short i))
                    (nippy/freeze-to-bb! buffer (long i)))
                  (nippy/freeze-to-bb! buffer 1)
                  (.flip buffer)
                  (bits/get-bytes buffer)))]
    (is (= (vec (range 256)) (decode (bytes 256))))
    (is (= :query-source/unsupported (category #(prepare (bytes 257)))))))

(deftest large-preflight-is-interruptible
  (let [bytes (bits/serialize (datom/datom 1 :test/value (vec (repeat 5000 nil))))
        checks (atom 0)]
    (is (= :query-source/cancelled
           (category #(native-value/prepare-giant
                        (ByteBuffer/wrap bytes) :data 1 :test/value
                        (fn []
                          (when (= 3 (swap! checks inc))
                            (value/fail! :query-source/cancelled :execution)))))))
    (is (= 3 @checks))))
