(ns ^:no-doc datalevin.query.source.native-compression
  "Bound legacy Nippy expansion before copying or decompressing native values."
  (:require [datalevin.query.source :as source]
            [datalevin.query.source.value :refer [fail!]])
  (:import [java.io ByteArrayInputStream IOException]
           [java.nio ByteBuffer ByteOrder]
           [java.util IdentityHashMap]
           [io.airlift.compress Decompressor]
           [io.airlift.compress.lz4 Lz4Decompressor]
           [io.airlift.compress.snappy SnappyDecompressor]
           [io.airlift.compress.zstd ZstdDecompressor]
           [org.tukaani.xz ArrayCache SingleXZInputStream]))

(defn- need! [^ByteBuffer buffer ^long n]
  (when-not (<= 0 n (.remaining buffer))
    (fail! :query-source/invalid :native-value)))

(defn- unsigned! ^long [^ByteBuffer buffer ^long width]
  (need! buffer width)
  (case width
    0 0
    1 (bit-and 0xff (.get buffer))
    2 (bit-and 0xffff (.getShort buffer))
    4 (bit-and 0xffffffff (.getInt buffer))
    8 (.getLong buffer)))

(defn- zstd-header [^ByteBuffer raw]
  (let [buffer (doto (.duplicate raw) (.order ByteOrder/LITTLE_ENDIAN))]
    (need! buffer 5)
    (when-not (= (unchecked-int 0xfd2fb528) (.getInt buffer))
      (fail! :query-source/invalid :native-value))
    (let [descriptor (unsigned! buffer 1)
          single? (bit-test descriptor 5)
          size-width (case (bit-shift-right descriptor 6)
                       0 (if single? 1 0) 1 2 2 4 3 8)
          dictionary-width (case (bit-and descriptor 3) 0 0 1 1 2 2 3 4)]
      (when (bit-test descriptor 3) (fail! :query-source/invalid :native-value))
      (when-not single? (unsigned! buffer 1)) ; window descriptor, no allocation
      (when-not (zero? (unsigned! buffer dictionary-width))
        (fail! :query-source/unsupported :native-value))
      ;; Standard Nippy writers declare the size. Unknown-size streams need a
      ;; separate bounded streaming contract, as do frames beyond JVM arrays.
      (when (zero? size-width) (fail! :query-source/unsupported :native-value))
      (let [size (unsigned! buffer size-width)
            size (if (= size-width 2) (+ size 256) size)]
        (when-not (<= 0 size Integer/MAX_VALUE)
          (fail! :query-source/unsupported :native-value))
        {:size size :blocks buffer :checksum? (bit-test descriptor 2)}))))

(defn- validate-zstd-frame! [control {:keys [blocks checksum?]}]
  ;; A size belongs to one frame. Reject concatenation/trailing bytes rather
  ;; than letting a second frame expand against the first frame's reservation.
  (let [^ByteBuffer buffer (.duplicate ^ByteBuffer blocks)]
    (loop []
      (source/check-active! control)
      (need! buffer 3)
      (let [header (bit-or (unsigned! buffer 1)
                           (bit-shift-left (unsigned! buffer 1) 8)
                           (bit-shift-left (unsigned! buffer 1) 16))
            kind (bit-and 3 (bit-shift-right header 1))
            size (bit-shift-right header 3)
            stored (if (= kind 1) 1 size)]
        (when (or (= kind 3) (> size 131072))
          (fail! :query-source/invalid :native-value))
        (need! buffer stored)
        (.position buffer (int (+ (.position buffer) stored)))
        (if (bit-test header 0)
          (do
            (when checksum?
              (need! buffer 4)
              (.position buffer (+ (.position buffer) 4)))
            (when (.hasRemaining buffer)
              (fail! :query-source/invalid :native-value)))
          (recur))))))

(defn- snappy-size ^long [^ByteBuffer buffer]
  ;; Read the bounded uint32 varint ourselves: the codec's array helper would
  ;; require copying the borrowed payload before its expansion is reserved.
  (loop [shift 0, size 0]
    (when-not (.hasRemaining buffer) (fail! :query-source/invalid :native-value))
    (let [b (bit-and 0xff (.get buffer))]
      (when (and (= shift 28) (> b 7))
        (fail! :query-source/invalid :native-value))
      (let [size (bit-or size (bit-shift-left (bit-and b 0x7f) shift))]
        (if (zero? (bit-and b 0x80)) size
            (recur (+ shift 7) size))))))

(defn- envelope [^ByteBuffer raw]
  (let [buffer (.duplicate raw)
        p (.position buffer)]
    (when (and (<= 3 (.remaining buffer))
               (= 0x4e (.get buffer p))
               (= 0x50 (.get buffer (+ p 1)))
               (= 0x59 (.get buffer (+ p 2))))
      (when (< (.remaining buffer) 4) (fail! :query-source/invalid :native-value))
      (.position buffer (+ p 4))
      (case (.get buffer (+ p 3))
        0 nil
        (8 11) (do
                 (need! buffer 4)
                 {:codec (if (= 8 (.get buffer (+ p 3))) :lz4 :lzma2)
                  :size (.getInt buffer) :buffer buffer})
        1 {:codec :snappy :size (snappy-size (.duplicate buffer)) :buffer buffer}
        20 (assoc (zstd-header buffer) :codec :zstd :buffer buffer)
        (fail! :query-source/unsupported :native-value)))))

(defn- with-xz-arrays [control f]
  ;; XZ obtains its dictionary and range-decoder buffers through ArrayCache.
  ;; Reserve before each allocation, discard on return, and also cover arrays
  ;; abandoned by a failed constructor. Never use the process-wide array cache.
  (let [arrays (IdentityHashMap.)
        allocate (fn [size width new-array]
                   (let [size (long size)]
                     (when (neg? size) (fail! :query-source/invalid :native-value))
                     (let [token (source/reserve! control
                                   {:retained-bytes (+ 64 (* size (long width)))})]
                       (try
                         (let [buffer (new-array size)]
                           (.put arrays buffer token)
                           buffer)
                         (catch Throwable e (source/release! token) (throw e))))))
        cache (proxy [ArrayCache] []
                (getByteArray [size _fill] (allocate size 1 byte-array))
                (getIntArray [size _fill] (allocate size 4 int-array))
                (putArray [buffer]
                  (when-let [token (.remove arrays buffer)]
                    (source/release! token))))]
    (try (f cache)
         (finally
           (run! source/release! (.values arrays))
           (.clear arrays)))))

(defn- expand-xz! [control ^bytes input ^bytes output]
  (with-xz-arrays control
    (fn [cache]
      (try
        (with-open [in (ByteArrayInputStream. input)
                    xz (SingleXZInputStream. in Integer/MAX_VALUE ^ArrayCache cache)]
          (loop [offset 0]
            (source/check-active! control)
            (if (= offset (alength output))
              (when-not (and (= -1 (.read xz)) (zero? (.available in)))
                (fail! :query-source/invalid :native-value))
              (let [n (.read xz output (int offset) (int (min 65536 (- (alength output) offset))))]
                (when-not (pos? n) (fail! :query-source/invalid :native-value))
                (recur (+ offset n))))))
        (catch IOException _ (fail! :query-source/invalid :native-value))))))

(defn- expand ^ByteBuffer [control {:keys [codec size buffer] :as envelope}]
  (when (= codec :zstd) (validate-zstd-frame! control envelope))
  (let [^ByteBuffer buffer buffer
        input (byte-array (.remaining buffer))
        output (byte-array (long size))]
    (.get buffer input)
    (if (= codec :lzma2)
      (expand-xz! control input output)
      (let [^Decompressor decoder (case codec :lz4 (Lz4Decompressor.)
                                        :snappy (SnappyDecompressor.) :zstd (ZstdDecompressor.))
            actual (try (.decompress decoder input 0 (alength input) output 0 (alength output))
                        (catch RuntimeException _ (fail! :query-source/invalid :native-value)))]
        (when-not (= actual (long size)) (fail! :query-source/invalid :native-value))))
    (ByteBuffer/wrap output)))

(defn with-buffer
  "Visit raw Nippy bytes, expanding supported legacy envelopes under the query
  ledger. scratch-bytes bounds the consumer's preflight state at expanded size.
  The reservation covers expansion and scalar decoding through the callback;
  the consumer must separately reserve any layout-derived collection charge.
  The borrowed input buffer's position and limit are preserved."
  [control buffer scratch-bytes f]
  (source/check-active! control)
  (if-let [{:keys [size codec] :as envelope} (envelope buffer)]
    (let [size (long size)]
      (when-not (pos? size) (fail! :query-source/invalid :native-value))
      (let [token (source/reserve! control
                    ;; Zstd tables/literal scratch and XZ filter/metadata objects
                    ;; fit in 256 KiB. XZ's variable arrays are charged separately.
                    {:retained-bytes (+ 4096 (if (#{:zstd :lzma2} codec) 262144 0)
                                       (.remaining ^ByteBuffer (:buffer envelope))
                                       (* 8 size) (long (scratch-bytes size)))})]
        (try
          (let [expanded (expand control envelope)]
            (source/check-active! control)
            (f expanded))
          (finally (source/release! token)))))
    (f (.duplicate ^ByteBuffer buffer))))
