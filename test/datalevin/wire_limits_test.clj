(ns datalevin.wire-limits-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.bits :as b]
            [datalevin.constants :as c]
            [datalevin.protocol :as p]
            [datalevin.protocol.context :as context]
            [datalevin.server.connection :as connection]
            [datalevin.server.handlers :as handlers])
  (:import [com.github.luben.zstd Zstd ZstdCompressCtx]
           [datalevin.io ScratchBufferPool WireCompression]
           [java.nio ByteBuffer]
           [java.nio.channels SelectionKey]))

(defn- codec-field [^WireCompression codec ^String field-name]
  (let [field (.getDeclaredField WireCompression field-name)]
    (.setAccessible field true)
    (.get field codec)))

(defn- body ^ByteBuffer [kind expected ^bytes compressed]
  ;; Nonzero offsets catch mistakes when passing header positions into JNI.
  (let [size (+ 11 4 (alength compressed))
        bf (if (= kind :direct) (ByteBuffer/allocateDirect size) (ByteBuffer/allocate size))]
    (.position bf 11)
    (.putInt bf (int expected))
    (.put bf compressed)
    (.flip bf)
    (.position bf 11)
    (if (= kind :read-only) (.asReadOnlyBuffer bf) bf)))

(deftest invalid-zstd-headers-and-declared-sizes-allocate-no-direct-scratch
  (let [raw (byte-array 512)
        packed (Zstd/compress raw)
        unknown (with-open [compressor (ZstdCompressCtx.)]
                  (.setContentSize compressor false)
                  (.compress compressor raw))
        malformed (byte-array (map unchecked-byte [0x28 0xb5 0x2f 0xfd 0xff 0]))]
    (doseq [kind [:heap :direct :read-only]
            [expected bytes maximum]
            [[4194304 (byte-array 6) 8388608]
             [4194304 malformed 8388608]
             [512 unknown 1024]
             [513 packed 1024]
             [512 packed 511]
             [Integer/MAX_VALUE packed 1024]
             [-1 packed 1024]
             [0 (byte-array (map unchecked-byte [0x50 0x2a 0x4d 0x18 0 0 0 0])) 1024]]]
      (testing (str kind " expected=" expected " limit=" maximum)
        (with-open [codec (WireCompression.)]
          (is (thrown? IllegalArgumentException (.decompress codec (body kind expected bytes) (int maximum))))
          ;; This directly guards the reported retained-buffer failure without
          ;; allocating a large test payload or depending on GC/MXBean timing.
          (doseq [field ["raw" "input" "decompressor"]]
            (is (nil? (codec-field codec field)))))))))

(deftest default-java-receive-limit-cannot-be-bypassed-with-a-length-prefix
  (with-open [codec (WireCompression.)]
    (is (thrown? IllegalArgumentException
                 (.decompress codec (body :direct (inc WireCompression/DEFAULT_MAX_MESSAGE_SIZE)
                                          (byte-array 6)))))
    (is (nil? (codec-field codec "raw")))
    (is (nil? (codec-field codec "input")))))

(deftest truncated-and-concatenated-frames-never-allocate-output
  (let [packed (Zstd/compress (byte-array 512))
        truncated (byte-array (butlast packed))
        concatenated (byte-array (concat packed packed))]
    (doseq [kind [:heap :direct :read-only]
            bytes [truncated concatenated]]
      (with-open [codec (WireCompression.)]
        (is (thrown? RuntimeException (.decompress codec (body kind 512 bytes) 1024)))
        (is (nil? (codec-field codec "raw")))
        (is (nil? (codec-field codec "decompressor")))))))

(deftest receive-limits-cover-raw-and-compressed-payloads-and-buffer-growth
  (let [opts {:compression :zstd :compression-threshold 0 :max-message-size 512}
        packed (Zstd/compress (byte-array 513))]
    (doseq [kind [:heap :direct :read-only]]
      (is (thrown? IllegalArgumentException
                   (p/read-value (bit-or (long c/message-format-nippy) c/message-flag-zstd)
                                 (body kind 513 packed) opts))))
    (is (= :protocol/message-too-large
           (:error (try (p/read-value c/message-format-nippy (byte-array 513) opts)
                        (catch Exception e (ex-data e))))))
    (doseq [extract? [false true]]
      (let [bf (doto (ByteBuffer/allocate 16)
                 (.put (byte c/message-format-nippy))
                 (.putInt Integer/MAX_VALUE))
            holder (volatile! bf)]
        (is (= :protocol/message-too-large
               (:error (try
                         (if extract?
                           (p/extract-message bf p/read-value (fn [& _]) opts)
                           (p/receive-one-message! holder opts))
                         (catch Exception e (ex-data e))))))
        (is (identical? bf @holder))
        (is (= 16 (.capacity ^ByteBuffer @holder)))))
    (doseq [extract? [false true]]
      (let [bf (doto (ByteBuffer/allocate 16)
                 (.put (unchecked-byte (bit-or (long c/message-format-nippy) c/message-flag-zstd)))
                 (.putInt 517))
            holder (volatile! bf)]
        (is (thrown-with-msg? Exception #"without negotiated support"
                             (if extract?
                               (p/extract-message bf p/read-value (fn [& _]) {:max-message-size 512})
                               (p/receive-one-message! holder {:max-message-size 512}))))
        (is (identical? bf @holder))))
    ;; The wire limit includes compressed bytes too, even for direct Java use.
    (with-open [codec (WireCompression.)]
      (is (thrown? IllegalArgumentException (.decompress codec (body :direct 512 (byte-array 513)) 512)))
      (is (nil? (codec-field codec "raw")))
      (is (nil? (codec-field codec "input"))))))

(deftest valid-frames-at-the-limit-remain-readable-with-bounded-growth
  (doseq [kind [:heap :direct :read-only]
          [maximum sizes] [[512 [257 512]] [20000 [12000 19000]]]]
    (with-open [codec (WireCompression.)]
      (doseq [size sizes]
        (let [raw (byte-array size)
              decoded (.decompress codec (body kind size (Zstd/compress raw)) (int maximum))]
          (is (java.util.Arrays/equals raw ^bytes (b/get-bytes decoded)))
          (is (<= (.capacity ^ByteBuffer (codec-field codec "raw")) (long maximum)))))))
  (let [value {:payload (apply str (repeat 200 "x"))}
        encoded (doto (ByteBuffer/allocate 1024) (p/write-message-bf value) (.flip))
        opts {:max-message-size (- (.remaining encoded) c/message-header-size)}
        holder (volatile! (ByteBuffer/allocate 16))]
    ;; A larger cached allocation must not bypass this receive limit.
    (.releaseBuffer (ScratchBufferPool/shared)
                    (ByteBuffer/allocateDirect (* 2 1024 1024)))
    (.put ^ByteBuffer @holder (doto (.duplicate encoded) (.limit 5)))
    (.position encoded 5)
    (is (nil? (p/receive-one-message! holder opts)))
    (is (= (+ (p/message-size-limit opts) c/message-header-size)
           (.capacity ^ByteBuffer @holder)))
    (.put ^ByteBuffer @holder encoded)
    (is (= value (p/receive-one-message! holder opts)))))

(defn- registration-key ^SelectionKey [state]
  (doto (proxy [SelectionKey] []
          (isValid [] true))
    (.attach state)))

(deftest compression-requires-a-live-session-and-keeps-the-local-receive-limit
  (doseq [live? [false true]]
    (let [state (volatile! {:wire-opts (p/default-wire-opts {:max-message-size 512})})
          reply (atom nil)
          deps {:get-client (fn [_ _] (when live? {:username "known-user"}))
                :write-message (fn [_ message] (reset! reply message))}
          capabilities (p/local-wire-capabilities (p/client-wire-opts {:wire-compression :zstd
                                                                      :max-message-size 999999}))]
      (handlers/set-client-id deps nil (registration-key state)
                              {:client-id (random-uuid) :wire-capabilities capabilities})
      (is (= :set-client-id-ok (:type @reply)))
      (is (= (if live? [:zstd] []) (get-in @reply [:wire-capabilities :compression])))
      (is (= 512 (p/message-size-limit (:wire-opts @state))))
      (when-not live?
        (with-open [^java.io.Closeable ctx (context/create)]
          (context/with-context ctx
            (context/with-compression [codec]
              ;; A shared codec may already own scratch from an earlier request.
              ;; Rejection must leave that scratch unchanged.
              (let [raw (codec-field codec "raw")
                    input (codec-field codec "input")]
                (is (thrown-with-msg? Exception #"without negotiated support"
                                     (p/read-value (bit-or (long c/message-format-nippy) c/message-flag-zstd)
                                                   (body :direct 4194304 (byte-array 6))
                                                   (:wire-opts @state))))
                (is (identical? raw (codec-field codec "raw")))
                (is (identical? input (codec-field codec "input")))))))))))

(deftest receive-policy-is-local-and-validated-before-acquiring-resources
  (doseq [maximum [0 -1 nil 1.5 Integer/MAX_VALUE]]
    (is (thrown? Exception (p/client-wire-opts {:max-message-size maximum})))
    (is (thrown? Exception (connection/limits {:max-message-size maximum}))))
  (let [opts (p/client-wire-opts {:max-message-size 1024 :wire-compression :zstd})
        peer (assoc (p/local-wire-capabilities opts) :max-message-size Integer/MAX_VALUE)]
    (is (= 1024 (p/message-size-limit (p/negotiate-wire-opts peer opts))))))
