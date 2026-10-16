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
  (:import [com.github.luben.zstd ZstdCompressCtx]
           [java.io ByteArrayOutputStream]
           [java.nio ByteBuffer ByteOrder]
           [java.util.zip CRC32]
           [java.util Arrays]
           [org.tukaani.xz LZMA2Options XZOutputStream]
           [taoensso.nippy.compression SnappyCompressor]))

(def snappy-compressor (SnappyCompressor. false))
(def compressors [codecs/lz4-compressor snappy-compressor
                  codecs/zstd-compressor codecs/lzma2-compressor])

(defn oversized-envelope
  "Invalid body advertising a huge expansion: quota checks must precede codecs."
  ^bytes [codec]
  (case codec
    (:lz4 :lzma2) (.array (doto (ByteBuffer/allocate 9)
                           (.putInt (if (= codec :lz4) 0x4e505908 0x4e50590b))
                           (.putInt Integer/MAX_VALUE) (.put (byte 0))))
    :snappy (byte-array (map unchecked-byte [0x4e 0x50 0x59 1 255 255 255 255 7 0]))
    :zstd (byte-array (map unchecked-byte
                          [0x4e 0x50 0x59 20 0x28 0xb5 0x2f 0xfd 0xa0 255 255 255 127 0]))))

(defn- decode [ctl ^ByteBuffer buffer type]
  (compression/with-buffer ctl buffer #(native-value/scratch-bytes type %)
    (fn [raw]
      (let [{:keys [decode retained-bytes]}
            (native-value/prepare-giant raw type 1 :test/value)
            token (source/reserve! ctl {:retained-bytes retained-bytes})]
        (try (decode)
             (finally (source/release! token)))))))

(deftest compressed-and-raw-values-preserve-borrowed-buffers
  (doseq [compressor (cons nil compressors)
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
  (doseq [compressor compressors]
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
  (doseq [codec [:lz4 :snappy :zstd :lzma2]]
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
      (doseq [compressor compressors
              :let [bytes (nippy/freeze (datom/datom 1 :test/value "value") {:compressor compressor})]]
        (dotimes [n (alength bytes)]
          (is (= :query-source/invalid
                 (category #(decode ctl (ByteBuffer/wrap (Arrays/copyOf bytes n)) :string)))
              (str "truncated at " n)))
        (is (zero? (:retained-bytes (control/usage ctl)))))
      (doseq [compressor [codecs/lz4-compressor codecs/lzma2-compressor]
              size [-1 0 1 100]]
        (let [bytes (nippy/freeze (datom/datom 1 :test/value "value") {:compressor compressor})]
          (.putInt (ByteBuffer/wrap bytes) 4 size)
          (is (= :query-source/invalid (category #(decode ctl (ByteBuffer/wrap bytes) :string))))))
      (doseq [tail [[255 255 255 255 8] [128 128 128 128 128] [255] [0]]]
        (let [bytes (byte-array (map unchecked-byte (concat [0x4e 0x50 0x59 1] tail)))]
          (is (= :query-source/invalid (category #(decode ctl (ByteBuffer/wrap bytes) :data))))))
      (doseq [tag [2 127]]
        (let [bytes (byte-array [0x4e 0x50 0x59 tag 0])]
          (is (= :query-source/unsupported (category #(decode ctl (ByteBuffer/wrap bytes) :data))))))
      (is (zero? (:retained-bytes (control/usage ctl))))
      (finally (control/close! ctl)))))

(deftest compressed-values-still-preflight-before-custom-readers
  (reset! fixture/decoded-custom-values 0)
  (doseq [compressor compressors]
    (let [ctl (control/create)
          bytes (nippy/freeze (datom/datom 1 :test/value {:hidden (fixture/->UnexpectedData)})
                              {:compressor compressor})]
      (try
        (is (= :query-source/unsupported (category #(decode ctl (ByteBuffer/wrap bytes) :data))))
        (is (zero? @fixture/decoded-custom-values))
        (is (zero? (:retained-bytes (control/usage ctl))))
        (finally (control/close! ctl))))))

(defn- zstd-raw-envelope ^bytes [^bytes raw width single?]
  ;; Independent uncompressed-block fixture exercises all frame size encodings,
  ;; including encodings the default Nippy compressor does not choose.
  (let [size (alength raw)
        width (long width)
        buffer (doto (ByteBuffer/allocate (+ size 64 (* 3 (quot size 131072))))
                 (.putInt 0x4e505914) (.order ByteOrder/LITTLE_ENDIAN)
                 (.putInt (unchecked-int 0xfd2fb528))
                 (.put (unchecked-byte (+ (if single? 32 0) (case width 1 0 2 64 4 128 8 192)))))]
    (when-not single? (.put buffer (byte 0)))
    (case width
      1 (.put buffer (unchecked-byte size))
      2 (.putShort buffer (unchecked-short (- size 256)))
      4 (.putInt buffer size)
      8 (.putLong buffer size))
    (loop [offset 0]
      (let [length (min 131072 (- size offset))
            last? (= (+ offset length) size)
            header (+ (bit-shift-left length 3) (if last? 1 0))]
        (doseq [shift [0 8 16]]
          (.put buffer (unchecked-byte (bit-shift-right header (long shift)))))
        (.put buffer raw offset length)
        (when-not last? (recur (+ offset length)))))
    (Arrays/copyOf (.array buffer) (.position buffer))))

(deftest zstd-size-encodings-multiple-blocks-and-frame-boundaries
  (let [ctl (control/create)]
    (try
      (doseq [[size width single?] [[10 1 true] [400 2 true] [400 2 false]
                                   [400 4 true] [400 4 false] [400 8 true] [400 8 false]
                                   [300000 4 true]]
              :let [x (.repeat "x" size)
                    raw (bits/serialize (datom/datom 1 :test/value x))
                    bytes (zstd-raw-envelope raw width single?)]]
        (is (= x (decode ctl (ByteBuffer/wrap bytes) :string)))
        (is (zero? (:retained-bytes (control/usage ctl)))))
      (let [raw (bits/serialize (datom/datom 1 :test/value "checksum"))
            ^bytes frame (with-open [ctx (ZstdCompressCtx.)]
                           (.setChecksum ctx true)
                           (.compress ctx raw))
            bytes (.array (doto (ByteBuffer/allocate (+ 4 (alength frame)))
                            (.putInt 0x4e505914) (.put frame)))
            corrupted (aclone bytes)]
        (is (= "checksum" (decode ctl (ByteBuffer/wrap bytes) :string)))
        (aset-byte corrupted (dec (alength corrupted))
                   (unchecked-byte (bit-xor 1 (aget corrupted (dec (alength corrupted))))))
        (doseq [bad [corrupted
                     (Arrays/copyOf bytes (inc (alength bytes)))
                     (.array (doto (ByteBuffer/allocate (+ (alength bytes) (alength frame)))
                               (.put bytes) (.put frame)))]]
          (is (= :query-source/invalid (category #(decode ctl (ByteBuffer/wrap bad) :string))))))
      (doseq [[error header] [[:query-source/unsupported [0 0]] ; unknown size
                              [:query-source/unsupported [0x21 1 1]] ; external dictionary
                              [:query-source/unsupported [0xe0 0 0 0 128 0 0 0 0]] ; > JVM array
                              [:query-source/unsupported [0xe0 0 0 0 0 0 0 0 128]] ; uint64
                              [:query-source/invalid [0x28 1]]]] ; reserved flag
        (let [bytes (byte-array (map unchecked-byte
                                    (concat [0x4e 0x50 0x59 20 0x28 0xb5 0x2f 0xfd] header)))]
          (is (= error (category #(decode ctl (ByteBuffer/wrap bytes) :string))))))
      (is (zero? (:retained-bytes (control/usage ctl))))
      (finally (control/close! ctl)))))

(defn oversized-dictionary-envelope
  "A valid tiny XZ stream requesting a 128 MiB decoder dictionary."
  ^bytes []
  (let [bytes (nippy/freeze (datom/datom 1 :test/value "dictionary")
                            {:compressor codecs/lzma2-compressor})
        ;; Nippy header + raw length + XZ stream header = 20 bytes.
        block 20
        length (* 4 (inc (bit-and 0xff (aget bytes block))))
        crc (CRC32.)]
    (assert (= [0 0x21 1] (mapv #(bit-and 0xff (aget bytes (+ block (long %)))) [1 2 3])))
    (aset-byte bytes (+ block 4) (byte 30))
    (.update crc bytes block (- length 4))
    (.putInt (doto (ByteBuffer/wrap bytes) (.order ByteOrder/LITTLE_ENDIAN))
             (+ block length -4) (unchecked-int (.getValue crc)))
    bytes))

(deftest xz-dictionary-allocation-is-charged-and-released-on-errors
  (let [valid (nippy/freeze (datom/datom 1 :test/value "dictionary")
                           {:compressor codecs/lzma2-compressor})
        ;; The default dictionary fits; the modified 128 MiB dictionary cannot.
        ctl (control/create {:limits {:retained-bytes 1048576}})
        outer (source/reserve! ctl {:retained-bytes 1000})]
    (try
      (let [data (error-data #(decode ctl (ByteBuffer/wrap (oversized-dictionary-envelope)) :string))]
        (is (= :query-source/limit (:error data)))
        (is (= :retained-bytes (:category data)))
        (is (>= (long (:attempted data)) 134217728))
        (is (= 1000 (:retained-bytes (control/usage ctl)))))
      ;; Truncation after the dictionary was allocated must release it as well.
      (is (= :query-source/invalid
             (category #(decode ctl (ByteBuffer/wrap (Arrays/copyOf valid (- (alength valid) 2))) :string))))
      (is (= 1000 (:retained-bytes (control/usage ctl))))
      (is (= "dictionary" (decode ctl (ByteBuffer/wrap valid) :string)))
      (is (= 1000 (:retained-bytes (control/usage ctl))))
      (finally (source/release! outer) (control/close! ctl)))))

(deftest xz-partial-construction-releases-dictionary-when-next-array-exceeds-quota
  (let [bytes (nippy/freeze (datom/datom 1 :test/value (.repeat "x" 2000))
                           {:compressor codecs/lzma2-compressor})
        ctl (control/create {:limits {:retained-bytes 550000}})]
    (try
      ;; Expansion plus the 256 KiB dictionary fits; the range decoder does not.
      (let [data (error-data #(decode ctl (ByteBuffer/wrap bytes) :string))]
        (is (= :query-source/limit (:error data)))
        (is (<= 590000 (long (:attempted data)) 650000))
        (is (zero? (:retained-bytes (control/usage ctl)))))
      (finally (control/close! ctl)))))

(deftest xz-multiple-blocks-release-dictionaries-and-reject-trailing-streams
  (let [x (.repeat "x" 2000)
        raw (bits/serialize (datom/datom 1 :test/value x))
        stream (ByteArrayOutputStream.)
        ctl (control/create {:limits {:retained-bytes 1048576}})]
    (with-open [xz (XZOutputStream. stream (LZMA2Options. 0))]
      (dotimes [offset (alength raw)]
        (.write xz raw offset 1)
        ;; Enough blocks to exhaust the quota if dictionaries are retained.
        (when (zero? (long (mod offset 200))) (.endBlock xz))))
    (let [body (.toByteArray stream)
          bytes (.array (doto (ByteBuffer/allocate (+ 8 (alength body)))
                          (.putInt 0x4e50590b) (.putInt (alength raw)) (.put body)))]
      (try
        (is (= x (decode ctl (ByteBuffer/wrap bytes) :string)))
        (doseq [bad [(Arrays/copyOf bytes (+ 4 (alength bytes))) ; XZ stream padding
                     (.array (doto (ByteBuffer/allocate (+ (alength bytes) (alength body)))
                               (.put bytes) (.put body)))]]
          (is (= :query-source/invalid (category #(decode ctl (ByteBuffer/wrap bad) :string)))))
        (is (zero? (:retained-bytes (control/usage ctl))))
        (finally (control/close! ctl))))))
