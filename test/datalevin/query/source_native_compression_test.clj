(ns datalevin.query.source-native-compression-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.bits :as bits]
            [datalevin.datom :as datom]
            [datalevin.query.source :as source]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.native-compression :as compression]
            [datalevin.query.source.native-value :as native-value]
            [datalevin.query.source-native-data-test :as fixture]
            [datalevin.query.source-test :refer [category error-data]]
            [taoensso.nippy :as nippy]
            [taoensso.nippy.compression :as codecs])
  (:import [java.nio ByteBuffer]
           [java.util Arrays]
           [taoensso.nippy.compression SnappyCompressor]))

(def snappy-compressor (SnappyCompressor. false))

(defn oversized-envelope
  "Invalid body advertising a huge expansion: quota checks must precede codecs."
  ^bytes [codec]
  (case codec
    :lz4 (.array (doto (ByteBuffer/allocate 9)
                   (.putInt 0x4e505908) (.putInt Integer/MAX_VALUE) (.put (byte 0))))
    :snappy (byte-array (map unchecked-byte [0x4e 0x50 0x59 1 255 255 255 255 7 0]))))

(defn- decode [ctl ^ByteBuffer buffer type]
  (compression/with-buffer ctl buffer #(native-value/scratch-bytes type %)
    (fn [raw]
      (let [{:keys [decode retained-bytes]}
            (native-value/prepare-giant raw type 1 :test/value)
            token (source/reserve! ctl {:retained-bytes retained-bytes})]
        (try (decode)
             (finally (source/release! token)))))))

(deftest compressed-and-raw-values-preserve-borrowed-buffers
  (doseq [compressor [nil codecs/lz4-compressor snappy-compressor]
          x ["" (.repeat "λ😀" 4000) {:items (vec (repeat 300 [1 "two" nil]))}]
          allocate [#(ByteBuffer/allocate %) #(ByteBuffer/allocateDirect %)]
          :let [d (datom/datom 1 :test/value x)
                bytes (nippy/freeze d {:compressor compressor})
                ^ByteBuffer buffer (doto ^ByteBuffer (allocate (+ 16 (alength bytes)))
                                     (.position 8) (.put bytes) (.flip) (.position 8))
                limit (.limit buffer)
                ctl (control/create)]]
    (try
      (is (= x (decode ctl buffer :data)))
      (is (= 8 (.position buffer)))
      (is (= limit (.limit buffer)))
      (is (zero? (:retained-bytes (control/usage ctl))))
      (finally (control/close! ctl))))
  (let [ctl (control/create)
        bytes (bits/serialize (datom/datom 1 :test/value "raw"))
        raw (ByteBuffer/wrap bytes)]
    (try
      (is (= "raw" (decode ctl raw :string)))
      (is (zero? (.position raw)))
      (finally (control/close! ctl)))))

(deftest expansion-reservations-survive-consumer-work-and-release-on-failure
  (doseq [compressor [codecs/lz4-compressor snappy-compressor]]
    (let [x (.repeat "x" 10000)
          bytes (nippy/freeze x {:compressor compressor})
          ctl (control/create)
          outer (source/reserve! ctl {:retained-bytes 1000})
          visit #(compression/with-buffer ctl (ByteBuffer/wrap bytes) (constantly 0) %)]
      (try
        (is (= x (visit (fn [buffer]
                         (is (> (:retained-bytes (control/usage ctl)) 80000))
                         (bits/deserialize-bf buffer)))))
        (is (= 1000 (:retained-bytes (control/usage ctl))))
        (is (thrown-with-msg? Exception #"consumer failed"
                              (visit (fn [_] (throw (Exception. "consumer failed"))))))
        (is (= 1000 (:retained-bytes (control/usage ctl))))
        (is (= :query-source/cancelled
               (category #(visit (fn [_]
                                   (control/cancel! ctl {:error :query-source/cancelled :phase :execution})
                                   (source/check-active! ctl))))))
        (is (= 1000 (:retained-bytes (control/usage ctl))))
        (finally (source/release! outer) (control/close! ctl))))))

(deftest advertised-expansion-is-checked-before-copying-or-decompressing
  (doseq [codec [:lz4 :snappy]]
    (let [ctl (control/create {:limits {:retained-bytes 100000}})]
      (try
        (let [data (error-data #(decode ctl (ByteBuffer/wrap (oversized-envelope codec)) :data))]
          (is (= :query-source/limit (:error data)))
          (is (= :retained-bytes (:category data)))
          (is (> (long (:attempted data)) Integer/MAX_VALUE))
          (is (zero? (:retained-bytes (control/usage ctl)))))
        (control/cancel! ctl {:error :query-source/cancelled :phase :execution})
        (is (= :query-source/cancelled
               (category #(decode ctl (ByteBuffer/wrap (oversized-envelope codec)) :data))))
        (finally (control/close! ctl))))))

(deftest malformed-envelopes-and-unsupported-compressors-reject
  (let [ctl (control/create)]
    (try
      (doseq [compressor [codecs/lz4-compressor snappy-compressor]
              :let [bytes (nippy/freeze (datom/datom 1 :test/value "value") {:compressor compressor})]]
        (dotimes [n (alength bytes)]
          (is (= :query-source/invalid
                 (category #(decode ctl (ByteBuffer/wrap (Arrays/copyOf bytes n)) :string)))
              (str "truncated at " n)))
        (is (zero? (:retained-bytes (control/usage ctl)))))
      (doseq [size [-1 0 1 100]]
        (let [bytes (nippy/freeze (datom/datom 1 :test/value "value") {:compressor codecs/lz4-compressor})]
          (.putInt (ByteBuffer/wrap bytes) 4 size)
          (is (= :query-source/invalid (category #(decode ctl (ByteBuffer/wrap bytes) :string))))))
      (doseq [tail [[255 255 255 255 8] [128 128 128 128 128] [255] [0]]]
        (let [bytes (byte-array (map unchecked-byte (concat [0x4e 0x50 0x59 1] tail)))]
          (is (= :query-source/invalid (category #(decode ctl (ByteBuffer/wrap bytes) :data))))))
      (doseq [tag [2 11 20 127]]
        (let [bytes (byte-array [0x4e 0x50 0x59 tag 0])]
          (is (= :query-source/unsupported (category #(decode ctl (ByteBuffer/wrap bytes) :data))))))
      (is (zero? (:retained-bytes (control/usage ctl))))
      (finally (control/close! ctl)))))

(deftest compressed-values-still-preflight-before-custom-readers
  (reset! fixture/decoded-custom-values 0)
  (doseq [compressor [codecs/lz4-compressor snappy-compressor]]
    (let [ctl (control/create)
          bytes (nippy/freeze (datom/datom 1 :test/value {:hidden (fixture/->UnexpectedData)})
                              {:compressor compressor})]
      (try
        (is (= :query-source/unsupported (category #(decode ctl (ByteBuffer/wrap bytes) :data))))
        (is (zero? @fixture/decoded-custom-values))
        (is (zero? (:retained-bytes (control/usage ctl))))
        (finally (control/close! ctl))))))
