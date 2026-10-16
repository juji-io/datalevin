(ns datalevin.query.source-native-metadata-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.bits :as bits]
            [datalevin.datom :as datom]
            [datalevin.query.source.native-value :as native-value]
            [datalevin.query.source.value :as value]
            [datalevin.query.source-native-data-test :as fixture]
            [datalevin.query.source-test :refer [category]]
            [taoensso.nippy :as nippy]
            [taoensso.nippy.impl :as impl]
            [taoensso.nippy.schema :as schema])
  (:import [java.nio ByteBuffer]
           [java.util Arrays]))

(defn- prepare [^bytes bytes type]
  (native-value/prepare-giant (ByteBuffer/wrap bytes) type 1 :test/value))

(defn- prefix ^ByteBuffer [^ByteBuffer buffer attribute]
  (.put buffer schema/id-prefixed-custom-md)
  (.putShort buffer (short (impl/coerce-custom-type-id :datalevin/datom)))
  (.putLong buffer 1)
  (nippy/freeze-to-bb! buffer attribute)
  buffer)

(defn cached-metadata-datom
  "Small wire fixture with exponentially expanding metadata, without allocating
  that expansion. Used by native scan quota tests as well as preflight tests."
  ^bytes [attribute x levels]
  (let [buffer (prefix (ByteBuffer/allocate 8192) attribute)]
    (.put buffer (byte 25)) ; metadata
    (.put buffer (byte 30)) (.putInt buffer 1) ; map, one pair
    (nippy/freeze-to-bb! buffer :expanded)
    (.put buffer (byte 21)) (.putInt buffer levels) ; vector
    (dotimes [i levels]
      (.put buffer schema/id-cached-md) (.putShort buffer (short i))
      (.put buffer schema/id-vec-2)
      (dotimes [_ 2]
        (if (zero? i)
          (nippy/freeze-to-bb! buffer nil)
          (do (.put buffer schema/id-cached-md) (.putShort buffer (short (dec i)))))))
    (nippy/freeze-to-bb! buffer x)
    (nippy/freeze-to-bb! buffer 1)
    (.flip buffer)
    (bits/get-bytes buffer)))

(deftest typed-metadata-layouts-preserve-values-and-conservative-charges
  (let [m {:source ["local" nil] :items (vec (repeat 300 nil))}
        symbol (with-meta 'test/symbol m)]
    (doseq [[type x] [[:symbol symbol]
                      [{:kind :tuple :elements [:long :symbol]} (with-meta [1 symbol] m)]
                      [{:kind :tuple :elements [:long :symbol]} [nil symbol]]
                      [{:kind :native-homogeneous-tuple :element :symbol}
                       (with-meta [symbol 'other] m)]]
            freeze [bits/serialize #(nippy/freeze % {:compressor nil})]]
      (let [bytes (freeze (datom/datom 1 :test/value x))
            {:keys [decode retained-bytes]} (prepare bytes type)
            result (decode)]
        (is (= x result))
        (is (= (meta x) (meta result)))
        (when (vector? x) (is (= (mapv meta x) (mapv meta result))))
        (is (>= (long retained-bytes) (long (value/value-charge result))))
        ;; Convenience decoding cannot bypass the additional reservation.
        (is (= :query-source/unsupported
               (category #(native-value/giant-value (ByteBuffer/wrap bytes) type 1 :test/value))))))))

(deftest metadata-does-not-bypass-declared-types-or-custom-reader-preflight
  (let [type {:kind :tuple :elements [:long :symbol]}]
    (doseq [x [(with-meta ['wrong 'value] {:m 1})
               (with-meta [1 'value 'extra] {:m 1})
               (with-meta {:a 1} {:m 1})
               (with-meta 'value {:m 1})]]
      (is (= :query-source/invalid
             (category #(prepare (bits/serialize (datom/datom 1 :test/value x)) type))))))
  (reset! fixture/decoded-custom-values 0)
  (doseq [[type x] [[:symbol (with-meta 'value {:hidden (fixture/->UnexpectedData)})]
                    [{:kind :tuple :elements [:long :symbol]}
                     [1 (with-meta 'value {:hidden (fixture/->UnexpectedData)})]]
                    [{:kind :native-homogeneous-tuple :element :long}
                     (with-meta [1 2] {:hidden (byte-array [1 2])})]]]
    (is (= :query-source/unsupported
           (category #(prepare (bits/serialize (datom/datom 1 :test/value x)) type)))))
  (is (zero? @fixture/decoded-custom-values)))

(deftest metadata-counts-truncation-and-depth-are-bounded
  (let [type {:kind :tuple :elements [:long :symbol]}
        bytes (bits/serialize (datom/datom 1 :test/value (with-meta [1 'value] {:nested [1 nil]})))]
    (dotimes [n (alength bytes)]
      (is (= :query-source/invalid (category #(prepare (Arrays/copyOf bytes n) type)))))
    (doseq [metadata [42 [1 2] :wrong]]
      (let [buffer (prefix (ByteBuffer/allocate 100) :test/value)]
        (.put buffer (byte 25))
        (nippy/freeze-to-bb! buffer metadata)
        (nippy/freeze-to-bb! buffer [1 'value])
        (nippy/freeze-to-bb! buffer 1)
        (.flip buffer)
        (is (= :query-source/invalid
               (category #(native-value/prepare-giant buffer type 1 :test/value)))))))
  (doseq [depth [60 65]
          :let [x (with-meta 'value {:nested (nth (iterate vector nil) depth)})
                bytes (bits/serialize (datom/datom 1 :test/value x))]]
    (if (= depth 60)
      (is (= (meta x) (meta ((:decode (prepare bytes :symbol))))))
      (is (= :query-source/unsupported (category #(prepare bytes :symbol)))))))

(deftest cached-metadata-is-charged-at-every-expanded-occurrence
  (doseq [[type x] [[:symbol 'value]
                    [{:kind :tuple :elements [:long :symbol]} [1 'value]]
                    [{:kind :native-homogeneous-tuple :element :long} [1 2]]]]
    (let [small (prepare (cached-metadata-datom :test/value x 6) type)
          large (prepare (cached-metadata-datom :test/value x 40) type)]
      (is (>= (long (:retained-bytes small)) (long (value/value-charge ((:decode small))))))
      (is (> (long (:retained-bytes large)) 1000000000000))
      (is (= Long/MAX_VALUE (:retained-bytes (prepare (cached-metadata-datom :test/value x 60) type))))
      (is (= :query-source/unsupported
             (category #(prepare (cached-metadata-datom :test/value x 64) type)))))))

(deftest metadata-caches-cannot-bypass-component-types-or-drop-charges
  (let [symbol (with-meta 'shared {:items (vec (repeat 300 nil))})
        cached (nippy/cache symbol)
        x (with-meta [cached cached] {:shared cached})
        bytes (bits/serialize (datom/datom 1 :test/value x))
        {:keys [decode retained-bytes]}
        (prepare bytes {:kind :tuple :elements [:symbol :symbol]})
        result (decode)]
    (is (= [symbol symbol] result))
    (is (= {:shared symbol} (meta result)))
    (is (= (meta symbol) (meta (first result))))
    (is (>= (long retained-bytes) (long (value/value-charge result))))
    (is (= :query-source/invalid
           (category #(prepare bytes {:kind :tuple :elements [:long :symbol]}))))))

(deftest large-typed-metadata-preflight-is-interruptible
  (let [x (with-meta 'value {:items (vec (repeat 5000 nil))})
        bytes (bits/serialize (datom/datom 1 :test/value x))
        checks (atom 0)]
    (is (= :query-source/cancelled
           (category #(native-value/prepare-giant
                        (ByteBuffer/wrap bytes) :symbol 1 :test/value
                        (fn []
                          (when (= 3 (swap! checks inc))
                            (value/fail! :query-source/cancelled :execution)))))))
    (is (= 3 @checks))))
