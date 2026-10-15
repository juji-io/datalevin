(ns ^:no-doc datalevin.query.source.native-compression
  "Bound legacy Nippy expansion before copying or decompressing native values."
  (:require [datalevin.query.source :as source]
            [datalevin.query.source.value :refer [fail!]])
  (:import [java.nio ByteBuffer]
           [io.airlift.compress Decompressor]
           [io.airlift.compress.lz4 Lz4Decompressor]
           [io.airlift.compress.snappy SnappyDecompressor]))

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
        8 (do
            (when (< (.remaining buffer) 4) (fail! :query-source/invalid :native-value))
            {:codec :lz4 :size (.getInt buffer) :buffer buffer})
        1 {:codec :snappy :size (snappy-size (.duplicate buffer)) :buffer buffer}
        (fail! :query-source/unsupported :native-value)))))

(defn- expand ^ByteBuffer [{:keys [codec size buffer]}]
  (let [^ByteBuffer buffer buffer
        input (byte-array (.remaining buffer))
        output (byte-array (long size))
        ;; These decoders are stateless; their small scratch objects are covered
        ;; by the fixed reservation. No thread-local expanded buffers survive.
        ^Decompressor decoder (case codec :lz4 (Lz4Decompressor.) :snappy (SnappyDecompressor.))]
    (.get buffer input)
    (let [actual (try (.decompress decoder input 0 (alength input) output 0 (alength output))
                      (catch RuntimeException _ (fail! :query-source/invalid :native-value)))]
      (when-not (= actual (long size)) (fail! :query-source/invalid :native-value)))
    (ByteBuffer/wrap output)))

(defn with-buffer
  "Visit raw Nippy bytes, expanding supported legacy envelopes under the query
  ledger. scratch-bytes bounds the consumer's preflight state at expanded size.
  The reservation covers expansion and scalar decoding through the callback;
  the consumer must separately reserve any layout-derived collection charge.
  The borrowed input buffer's position and limit are preserved."
  [control buffer scratch-bytes f]
  (source/check-active! control)
  (if-let [{:keys [size] :as envelope} (envelope buffer)]
    (let [size (long size)]
      (when-not (pos? size) (fail! :query-source/invalid :native-value))
      (let [token (source/reserve! control
                    {:retained-bytes (+ 4096 (.remaining ^ByteBuffer (:buffer envelope))
                                       (* 8 size) (long (scratch-bytes size)))})]
        (try
          (let [expanded (expand envelope)]
            (source/check-active! control)
            (f expanded))
          (finally (source/release! token)))))
    (f (.duplicate ^ByteBuffer buffer))))
