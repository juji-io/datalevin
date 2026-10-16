(ns ^:no-doc datalevin.test-adapter.rust
  "A synchronous, bounded Nippy process bridge. No reference engine calls."
  (:require
   [datalevin.bits :refer [->Indexable ->Retrieved ->CustomReference]]
   [datalevin.util :refer [raise]]
   [datalevin.buffer :as buffer]
   [taoensso.nippy :as nippy]
   [datalevin.test-adapter :as adapter]
   [datalevin.test-adapter.bits :as bits])
  (:import
   [java.io DataInputStream DataOutputStream]
   [java.lang ProcessBuilder ProcessBuilder$Redirect]
   [java.math BigDecimal BigInteger]
   [datalevin.bits Indexable]
   [java.nio ByteBuffer ByteOrder]
   [java.util Date UUID]
   [org.roaringbitmap RoaringBitmap]
   [java.util.concurrent Callable ExecutionException ExecutorService Executors
    ThreadFactory TimeUnit TimeoutException]))

(def ^:private ^:const max-frame-bytes (* 16 1024 1024))

(deftype ^:private DbHandle [session id])

(defn- read-frame [^DataInputStream input]
  (let [size (.readInt input)]
    (when-not (<= 1 size max-frame-bytes)
      (raise "Invalid Rust adapter frame size" {:size size}))
    (let [bytes (byte-array size)]
      (.readFully input bytes)
      (nippy/fast-thaw bytes))))

(defn- write-frame! [^DataOutputStream output value]
  (let [bytes (try
                (nippy/fast-freeze value)
                (catch Exception error
                  (adapter/unsupported! :adapter (:op value)
                                        (str "Rust adapter cannot encode the request: "
                                             (ex-message error)))))
        size (alength bytes)]
    (when (> size max-frame-bytes)
      (adapter/unsupported! :adapter (:op value)
                            "Rust adapter request exceeds the frame limit"))
    (.writeInt output size)
    (.write output bytes)
    (.flush output)))

(defn- stop-peer! [{:keys [process input output worker stopped]}]
  (when (compare-and-set! stopped false true)
    ;; Kill before closing streams: another thread may be blocked in read/write.
    (.destroy ^Process process)
    (when-not (.waitFor ^Process process 100 TimeUnit/MILLISECONDS)
      (.destroyForcibly ^Process process)
      (.waitFor ^Process process 1 TimeUnit/SECONDS))
    (.shutdownNow ^ExecutorService worker)
    (doseq [stream [input output]]
      (try (.close ^java.io.Closeable stream) (catch Exception _)))))

(defn- exchange! [{:keys [worker timeout-ms stopped] :as peer} f]
  (locking peer
    (when @stopped
      (raise "Rust adapter process is closed" {}))
    (let [task (.submit ^ExecutorService worker ^Callable (bound-fn [] (f)))]
      (try
        (.get task (long timeout-ms) TimeUnit/MILLISECONDS)
        (catch TimeoutException error
          (stop-peer! peer)
          (throw (ex-info "Rust adapter request timed out"
                          {:timeout-ms timeout-ms} error)))
        (catch ExecutionException error
          (let [cause (.getCause error)]
            (if (adapter/unsupported-data cause)
              ;; Request encoding/size checks fail before writing any bytes.
              (throw cause)
              (do
                (stop-peer! peer)
                (throw (ex-info "Rust adapter transport failed" {} cause))))))))))

(defn- argument [session operation value]
  (if (instance? DbHandle value)
    (if (identical? session (.-session ^DbHandle value))
      {:kind :db :id (.-id ^DbHandle value)}
      (raise "Database belongs to a different Rust adapter session"
                      {:operation operation}))
    {:kind :value :value value}))

(defn- result-value [session operation response]
  (when-not (map? response)
    (raise "Rust adapter response must be a map" {:operation operation}))
  (case (:status response)
    :ok
    (let [{:keys [kind id] :as result} (:result response)
          expects-db? (contains? #{:empty-db :db-with} operation)]
      (cond
        (and expects-db? (= :db kind) (int? id) (pos? (long id)))
        (DbHandle. session id)

        (and (not expects-db?) (= :value kind) (contains? result :value))
        (:value result)

        :else (raise "Invalid Rust adapter result descriptor"
                              {:operation operation :result result})))
    :unsupported
    (adapter/unsupported! :rust operation
                          (or (:message response) "Unsupported Rust operation"))
    :error
    (let [{:keys [class message data]} (:error response)]
      (when-not (and (string? class) (string? message))
        (raise "Invalid Rust adapter error descriptor" {:response response}))
      (throw (case class
               "java.lang.IllegalArgumentException" (IllegalArgumentException. message)
               "java.lang.IllegalStateException" (IllegalStateException. message)
               "java.lang.NumberFormatException" (NumberFormatException. message)
               "java.lang.AssertionError" (AssertionError. ^Object message)
               "java.lang.ArrayIndexOutOfBoundsException" (ArrayIndexOutOfBoundsException. message)
               "java.lang.NegativeArraySizeException" (NegativeArraySizeException. message)
               "java.io.IOException" (java.io.IOException. message)
               "clojure.lang.ExceptionInfo" (ex-info message (or data {}))
               (adapter/unsupported! :adapter operation
                                     (str "Exception class is not mapped: " class)))))
    (raise "Invalid Rust adapter response status" {:response response})))

(defn- invoke-peer! [peer session operation arguments]
  (let [request {:op operation
                 :args (mapv #(argument session operation %) arguments)}
        response (exchange! peer
                            #(do (write-frame! (:output peer) request)
                                 (read-frame (:input peer))))]
    (result-value session operation response)))

(defn- serialization-value! [value]
  ;; Datalevin excludes Class values before calling its codec. This is the JVM
  ;; binding's value policy; it does not invoke the reference serializer.
  (when (instance? Class value)
    (raise "Unfreezable type: java.lang.Class" {})))

(defn- serialize-value! ^bytes [peer session value]
  (serialization-value! value)
  (invoke-peer! peer session :bits/serialize [value]))

(defn- big-endian-buffer! [^ByteBuffer buffer]
  (when-not (= ByteOrder/BIG_ENDIAN (.order buffer))
    (throw (IllegalArgumentException.
             (str "ByteBuffer must use BIG_ENDIAN order for DataInput/DataOutput semantics (have "
                  (.order buffer) ").")))))

(defn- serialize-buffer! [peer session ^ByteBuffer buffer value]
  (serialization-value! value)
  (big-endian-buffer! buffer)
  ;; ByteBuffer.put checks capacity/read-only status before changing position.
  ;; Rust performs serialization; the host only copies its output to JVM memory.
  (.put buffer ^bytes (invoke-peer! peer session :bits/serialize [value])))

(defn- acquire-serialized-buffer! ^ByteBuffer [peer session value]
  (let [bytes (serialize-value! peer session value)
        owned (buffer/get-array-buffer (alength bytes))]
    (try
      (big-endian-buffer! owned)
      (.put owned bytes)
      (.flip owned)
      (catch Throwable error
        (buffer/return-array-buffer owned)
        (throw error)))))

(defn- measure-size [peer session value]
  ;; Preserve the original allocation estimate, including its fixed-width
  ;; integer/byte-array cases. Other values are measured using real Rust output.
  (cond
    (bytes? value) (inc (alength ^bytes value))
    (instance? ByteBuffer value) (.remaining ^ByteBuffer value)
    (int? value) 8
    (instance? Byte value) 1
    :else (alength (serialize-value! peer session value))))

(defn- deserialize-buffer! [peer session ^ByteBuffer buffer]
  (let [limit (.limit buffer)]
    (try
      (when-not (= ByteOrder/BIG_ENDIAN (.order buffer))
        (adapter/unsupported! :adapter :bits/deserialize-bf
                              "Little-endian buffer decode errors are not mapped yet"))
      (invoke-peer! peer session :bits/deserialize [(bits/get-bytes buffer)])
      ;; Matches the storage API even on codec errors and support gaps. Returned
      ;; values own their bytes; no borrowed buffer crosses the process boundary.
      (finally (.position buffer limit)))))

(defn- put-integer! [peer session ^ByteBuffer buffer value value-type]
  ;; Match the binding's casts before touching memory. The selected Rust codec
  ;; produces the bytes; the JVM only enforces its buffer bounds and mutability.
  (let [integer (case value-type
                  :byte (unchecked-byte value)
                  :short (short value)
                  :int (int value)
                  :id (long value))]
    (.put buffer ^bytes (invoke-peer! peer session :bits/encode-integer
                                     [integer value-type (= ByteOrder/LITTLE_ENDIAN
                                                            (.order buffer))]))))

(defn- read-integer! [peer session ^ByteBuffer buffer value-type]
  (let [width (case value-type :byte 1 :short 2 :int 4 :id 8)
        bytes (bits/get-bytes buffer width)]
    (invoke-peer! peer session :bits/decode-integer
                  [bytes value-type (= ByteOrder/LITTLE_ENDIAN (.order buffer))])))

(defn- put-ints! [peer session ^ByteBuffer buffer value sorted?]
  (let [_ (alength ^ints value)
        words (invoke-peer! peer session :bits/encode-ints
                            [value sorted? (= ByteOrder/LITTLE_ENDIAN (.order buffer))])]
    (doseq [^bytes word words] (.put buffer word))))

(defn- bitmap-member [member]
  ;; The primitive bitmap API requires Number, rather than Character coercion.
  (when (some? member) (.longValue ^Number member))
  (int member))

(defn- mutate-bitmap! [peer session operation ^RoaringBitmap bitmap member]
  (let [member (bitmap-member member)]
    (.getLongCardinality bitmap)
    (let [^RoaringBitmap result (invoke-peer! peer session operation [bitmap member])]
      ;; Rust decides the new contents. Copy that owned value into the existing
      ;; JVM wrapper so callers retain the original bitmap's identity.
      (.clear bitmap)
      (.or bitmap result)
      bitmap)))

(defn- put-bitmap! [peer session ^ByteBuffer buffer ^RoaringBitmap bitmap]
  (.getLongCardinality bitmap)
  (let [{:keys [header containers]} (invoke-peer! peer session :bits/encode-bitmap [bitmap])
        little? (= ByteOrder/LITTLE_ENDIAN (.order buffer))
        target (if little? buffer (.slice buffer))]
    (doseq [^bytes field header] (.put target field))
    (doseq [fields containers]
      (let [view (.slice target)]
        (doseq [^bytes field fields] (.put view field))
        (.position target (+ (.position target) (.position view)))))
    (when-not little? (.position buffer (+ (.position buffer) (.position target))))
    nil))

(defn- read-ints! [peer session ^ByteBuffer buffer sorted?]
  (let [little? (= ByteOrder/LITTLE_ENDIAN (.order buffer))
        {:keys [size compressed?]}
        (invoke-peer! peer session :bits/ints-header [(bits/get-bytes buffer 4) little?])
        ;; Match the JVM array length cast before checking adapter transport limits.
        _ (when (> size Integer/MAX_VALUE)
            (throw (NegativeArraySizeException. (str (unchecked-int size)))))
        size (int size)]
    (when (> size (quot (- max-frame-bytes 1024) 4))
      (adapter/unsupported! :adapter :bits/read-buffer "Integer array exceeds the adapter frame limit"))
    (let [words (byte-array (* size 4))]
      (dotimes [i size] (.get buffer words (* i 4) 4))
      (invoke-peer! peer session :bits/decode-ints [words compressed? sorted? little?]))))

(defn- put-scalar! [peer session ^ByteBuffer buffer value value-type]
  (let [negative? (when (= value-type :long)
                    (if (keyword? value)
                      (case value
                        :db.value/sysMin true
                        :db.value/sysMax false
                        (raise "Expecting long, got keyword" value {}))
                      (neg? (long value))))]
    (.put buffer ^bytes (invoke-peer! peer session :bits/scalar-header
                                     [nil value-type (boolean negative?)]))
    ;; Header writes precede payload casts/validation, as in the original API.
    (let [payload (case value-type
                    :long (long value)
                    :float (float value)
                    :double (double value)
                    :boolean (boolean value)
                    :instant (if (inst? value) (long (inst-ms value))
                                 (raise "Expect an inst? value" {:value value}))
                    :instant-pre-06 (.getTime ^Date value)
                    :uuid (do (.getMostSignificantBits ^UUID value) value))
          bytes ^bytes (invoke-peer! peer session :bits/encode-scalar
                                      [payload value-type (= ByteOrder/LITTLE_ENDIAN
                                                             (.order buffer))])]
      (if (= value-type :uuid)
        (do (.put buffer bytes 0 8) (.put buffer bytes 8 8))
        (.put buffer bytes)))))

(defn- read-scalar! [peer session ^ByteBuffer buffer value-type]
  (bits/get-bytes buffer 1)
  (let [width (case value-type :boolean 1 :float 4 :uuid 16 8)
        bytes (byte-array width)]
    (if (= value-type :uuid)
      (do (.get buffer bytes 0 8) (.get buffer bytes 8 8))
      (.get buffer bytes))
    (invoke-peer! peer session :bits/decode-scalar
                  [bytes value-type (= ByteOrder/LITTLE_ENDIAN (.order buffer))])))

(defn- put-variable! [peer session ^ByteBuffer buffer value value-type]
  (.put buffer ^bytes (invoke-peer! peer session :bits/variable-header [value-type]))
  (let [payload (case value-type
                  :string (.toCharArray ^String value)
                  :bytes (let [_ (alength ^bytes value)] value)
                  :bigint (.toByteArray ^BigInteger value)
                  (:keyword :symbol)
                  (let [name (.toCharArray ^String (name value))
                        namespace (namespace value)]
                    [name (some-> ^String namespace .toCharArray)]))]
    (.put buffer ^bytes (invoke-peer! peer session :bits/encode-variable [payload value-type]))))

(defn- read-variable! [peer session ^ByteBuffer buffer value-type]
  (bits/get-bytes buffer 1)
  (let [bytes (if (= value-type :bigint)
                (let [prefix (bits/get-bytes buffer 1)
                      length (int (invoke-peer! peer session :bits/bigint-length [prefix]))
                      bytes (byte-array (inc length))]
                  (aset-byte bytes 0 (aget prefix 0))
                  (.get buffer bytes 1 length)
                  bytes)
                (bits/get-bytes buffer))]
    (invoke-peer! peer session :bits/decode-variable [bytes value-type])))

(defn- put-attr! [peer session ^ByteBuffer buffer value]
  ;; String conversion and substring are JVM value operations. Rust encodes
  ;; their UTF-16 units and checks the byte limit before the first buffer write.
  (let [^String text (subs (str value) 1)]
    (.put buffer ^bytes (invoke-peer! peer session :bits/encode-attr [(.toCharArray text)]))))

(defn- declared-type! [value-type]
  (when (qualified-keyword? value-type)
    (raise "Custom storage types require a declared DBI"
           {:error :custom-type/undeclared :type value-type})))

(defn- put-decimal! [peer session ^ByteBuffer buffer value]
  (.put buffer ^bytes (invoke-peer! peer session :bits/variable-header [:bigdec]))
  (let [^BigDecimal decimal value
        fields (invoke-peer! peer session :bits/encode-bigdec
                             [(.toByteArray (.unscaledValue decimal))
                              (.scale decimal) (= ByteOrder/LITTLE_ENDIAN (.order buffer))])]
    ;; Rust supplies field writes and any terminal codec error. Apply them in
    ;; order so buffer overflow/read-only errors precede later validation errors.
    (doseq [field fields]
      (.put buffer ^bytes (result-value session :bits/put-buffer field)))
    buffer))

(defn- read-decimal! [peer session ^ByteBuffer buffer]
  (bits/get-bytes buffer 1)
  (let [approximate (bits/get-bytes buffer 8)
        prefix (bits/get-bytes buffer 1)
        length (int (invoke-peer! peer session :bits/bigint-length [prefix]))
        unscaled (byte-array (inc length))]
    (aset-byte unscaled 0 (aget prefix 0))
    (.get buffer unscaled 1 length)
    ;; An empty integer fails before the separator/scale reads in the source API.
    (invoke-peer! peer session :bits/decode-variable [unscaled :bigint])
    (let [separator (bits/get-bytes buffer 1)
          scale (bits/get-bytes buffer 4)
          bytes (byte-array (+ length 14))]
      (System/arraycopy approximate 0 bytes 0 8)
      (System/arraycopy unscaled 0 bytes 8 (alength unscaled))
      (System/arraycopy separator 0 bytes (+ length 9) 1)
      (System/arraycopy scale 0 bytes (+ length 10) 4)
      (invoke-peer! peer session :bits/decode-bigdec
                    [bytes (= ByteOrder/LITTLE_ENDIAN (.order buffer))]))))

(defn- tuple-input [value value-type]
  (if (#{:db.value/sysMin :db.value/sysMax} value)
    value
    (do
      (when (and (keyword? value) (not= value-type :keyword))
        (if (= value-type :long)
          (raise "Expecting long, got keyword" value {})
          (raise "Expect other data types, got keyword instead: " value {})))
      (case value-type
        :long (long value)
        :int (clojure.lang.RT/intCast value)
        :float (float value)
        :double (double value)
        :boolean (boolean value)
        :instant (Date. (.getTime ^Date value))
        :uuid (do (.getMostSignificantBits ^UUID value) value)
        :string (.toCharArray ^String value)
        :bytes (let [_ (alength ^bytes value)] value)
        :bigint (do (.toByteArray ^BigInteger value) value)
        :bigdec (do (.unscaledValue ^BigDecimal value) value)
        (:keyword :symbol) [(.toCharArray ^String (name value))
                           (some-> ^String (namespace value) .toCharArray)]))))

(defn- tuple-component [errors values index value-type heterogeneous?]
  (let [failed (fn [error lookup?]
                 (let [index (dec (count (swap! errors conj error)))]
                   {:error (long index) :lookup? lookup?}))
        input (try {:value (nth values index)}
                   (catch Exception error (failed error true)))]
    (if (contains? input :error)
      input
      (try
        {:value (if (and heterogeneous? (nil? (:value input)))
                  nil
                  (tuple-input (:value input) value-type))}
        (catch Exception error (failed error false))))))

(defn- put-tuple! [peer session ^ByteBuffer buffer values types]
  (.put buffer ^bytes (invoke-peer! peer session :bits/tuple-header [types]))
  (let [homogeneous? (= 1 (count types))
        count (if homogeneous? (count values) (count types))
        errors (atom [])
        components (mapv #(tuple-component errors values %
                                            (nth types (if homogeneous? 0 %))
                                            (not homogeneous?)) (range count))
        fields (invoke-peer! peer session :bits/encode-tuple
                             [types components (long count)
                              (= ByteOrder/LITTLE_ENDIAN (.order buffer))])]
    (doseq [field fields]
      (cond
        (:empty field)
        (raise "Cannot store an empty homogeneous tuple"
                 {:error :data/validation :value values :tuple-type (first types)})
        (contains? field :host-error) (throw (nth @errors (:host-error field)))
        (contains? field :too-large)
        (raise "The maximal tuple element size is\n                              255 bytes"
                 {:too-large (nth values (:too-large field))})
        :else (.put buffer ^bytes (result-value session :bits/put-buffer field))))))

(defn- replay-reads! [^ByteBuffer buffer reads]
  (doseq [[operation amount width] reads]
    (case operation
      :bytes (bits/get-bytes buffer amount)
      :position (.position buffer (int amount))
      :limit (.limit buffer (int amount))
      :mark (.mark buffer)
      :reset (.reset buffer)
      :absolute (case (int width)
                  1 (.get buffer (int amount))
                  4 (.getInt buffer (int amount))
                  8 (.getLong buffer (int amount))))))

(defn- read-tuple! [peer session ^ByteBuffer buffer types]
  (let [bytes (bits/get-bytes (doto (.duplicate buffer) (.position 0)))
        {:keys [reads result]} (invoke-peer! peer session :bits/decode-tuple
                                             [bytes (long (.position buffer))
                                              (= 1 (count types))
                                              (= ByteOrder/LITTLE_ENDIAN (.order buffer))])]
    ;; Rust decides framing, field widths and cursor movement. Replay on the
    ;; caller's buffer so JVM bounds exceptions and mark invalidation stay exact.
    (replay-reads! buffer reads)
    (result-value session :bits/read-buffer result)))

(defn- read-bitmap! [peer session ^ByteBuffer buffer]
  (let [view (.slice buffer)
        {:keys [reads result]} (invoke-peer! peer session :bits/decode-bitmap
                                             [(bits/get-bytes (.duplicate view))])]
    (replay-reads! view reads)
    (result-value session :bits/read-buffer result)))

(defn- raw-index-type [value-type]
  (case value-type
    :db.type/string :string :db.type/keyword :keyword :db.type/symbol :symbol
    :db.type/bigint :bigint :db.type/bigdec :bigdec :db.type/bytes :bytes
    :db.type/long :long :db.type/ref :long :db.type/float :float :db.type/double :double
    :db.type/boolean :boolean :db.type/instant :instant :db.type/uuid :uuid
    value-type))

(defn- index-input [errors value value-type]
  (cond
    (#{:db.value/sysMin :db.value/sysMax} value) {:value value}
    (vector? value-type)
    (let [types (mapv raw-index-type value-type)
          homogeneous? (= 1 (count types))
          count (if homogeneous? (count value) (count types))]
      {:elements (mapv #(tuple-component errors value % (nth types (if homogeneous? 0 %))
                                         (not homogeneous?)) (range count))
       :count (long count)})
    :else
    {:value (case value-type
              :db.type/long (tuple-input value :long)
              ;; Fixed values are cast only when written, after the AVG prefix.
              (:db.type/ref :db.type/float :db.type/double :db.type/boolean
                            :db.type/instant :db.type/uuid) nil
              :db.type/bytes (when (some? value) (tuple-input value :bytes))
              (:db.type/string :db.type/keyword :db.type/symbol :db.type/bigint :db.type/bigdec)
              (tuple-input value (raw-index-type value-type))
              (do (serialization-value! value) value))}))

(defn- index-field-bytes! [session operation field errors values types]
  (cond
    (:empty field) (raise "Cannot store an empty homogeneous tuple"
                          {:error :data/validation :value values :tuple-type (first types)})
    (contains? field :host-error) (throw (nth @errors (:host-error field)))
    (contains? field :too-large)
    (raise "The maximal tuple element size is\n                              255 bytes"
             {:too-large (nth values (:too-large field))})
    :else (result-value session operation field)))

(defn- indexable! [peer session entity attribute value value-type max-giant]
  (when (map? value-type)
    (adapter/unsupported! :adapter :bits/indexable "Custom index callbacks need a selected-backend adapter"))
  (let [errors (atom [])
        {:keys [header bytes cut? failure]} (invoke-peer! peer session :bits/indexable
                                                         [value-type (index-input errors value value-type)])]
    (when failure
      (index-field-bytes! session :bits/indexable failure errors value
                          (when (vector? value-type) (mapv raw-index-type value-type))))
    ;; Inline byte values retain the input array alias; giant prefixes own a copy.
    (->Indexable entity attribute value header
                 (if (and (= value-type :db.type/bytes) (not cut?) (not (keyword? value))) value bytes)
                 (if cut? max-giant 0))))

(defn- put-avg! [peer session ^ByteBuffer buffer ^Indexable value]
  (let [attribute (int (.-a value))
        header (.-f value)
        bytes (.-b value)
        errors (atom [])
        component (if bytes {:value nil}
                    (try
                      {:value (tuple-input (.-v value)
                                           (case (short header)
                                             (-64 -63 -8) :long -62 :int -11 :float -10 :double
                                             -9 :instant -7 :uuid -3 :boolean))}
                      (catch Exception error (swap! errors conj error) {:error 0})))
        fields (invoke-peer! peer session :bits/encode-avg
                             [attribute header bytes component (.-g value)
                              (= ByteOrder/LITTLE_ENDIAN (.order buffer))])]
    (doseq [field fields]
      (.put buffer ^bytes (index-field-bytes! session :bits/put-buffer field errors nil nil)))
    buffer))

(defn- indexable-bytes! [peer session value ^ByteBuffer buffer]
  (.clear buffer)
  (put-avg! peer session buffer value)
  ;; Retain the original heap-array/slice behavior, including direct-buffer errors.
  (java.util.Arrays/copyOfRange (.array buffer) 0 (.position buffer)))

(defn- avg-value [{:keys [kind value]}]
  (case kind :value value :custom (->CustomReference value)))

(defn- read-avg! [peer session ^ByteBuffer buffer mode]
  (let [limit (.limit buffer)
        bytes (bits/get-bytes (doto (.duplicate buffer) (.position 0)))
        {:keys [reads result]} (invoke-peer! peer session :bits/decode-avg
                                             [bytes (long (.position buffer)) mode
                                              (= ByteOrder/LITTLE_ENDIAN (.order buffer))])]
    (try
      (replay-reads! buffer reads)
      (let [value (result-value session :bits/read-buffer result)]
        (if (:retrieved? value)
          (->Retrieved nil (:a value) (avg-value (:v value)) (:g value))
          (avg-value value)))
      (finally (.limit buffer limit)))))

(defrecord RustBackend [peer session info]
  adapter/Backend
  (invoke! [_ operation arguments]
    (case operation
      :bits/serialize
      (serialize-value! peer session (first arguments))

      :bits/bitmap
      (let [values (first arguments)
            _ (count values)]
        (invoke-peer! peer session operation [(int-array (map bitmap-member values))]))

      (:bits/bitmap-add :bits/bitmap-del)
      (apply mutate-bitmap! peer session operation arguments)

      :bits/acquire-serialized-bf
      (acquire-serialized-buffer! peer session (first arguments))

      :bits/measure-size
      (measure-size peer session (first arguments))

      :bits/indexable (apply indexable! peer session arguments)
      :bits/giant? (invoke-peer! peer session :bits/giant? [(.-g ^Indexable (first arguments))])
      :bits/indexable-bytes
      (if (= 2 (count arguments))
        (indexable-bytes! peer session (first arguments) (second arguments))
        (let [buffer (buffer/get-array-buffer)]
          (try (indexable-bytes! peer session (first arguments) buffer)
               (finally (buffer/return-array-buffer buffer)))))
      :bits/avg->aid (read-avg! peer session (first arguments) :aid)
      :bits/avg->giant-id (read-avg! peer session (first arguments) :giant-id)
      :bits/avg->inline-value (read-avg! peer session (first arguments) :inline)
      :bits/avg->r (read-avg! peer session (first arguments) :r)

      :bits/serialize-bf
      (let [[buffer value] arguments]
        (serialize-buffer! peer session buffer value))

      :bits/deserialize-bf
      (deserialize-buffer! peer session (first arguments))

      :bits/put-buffer
      (let [[^ByteBuffer buffer value value-type] arguments]
        (case value-type
          :avg (put-avg! peer session buffer value)
          :attr (put-attr! peer session buffer value)
          :ints (put-ints! peer session buffer value false)
          :bitmap (put-bitmap! peer session buffer value)
          :doc-info (let [[id count positions] value]
                      (put-integer! peer session buffer id :int)
                      (put-integer! peer session buffer count :short)
                      (put-ints! peer session buffer positions false))
          :pos-info (let [[positions offsets] value]
                      (put-ints! peer session buffer positions true)
                      (put-ints! peer session buffer offsets true))
          (:data nil) (serialize-buffer! peer session buffer value)
          :bigdec (put-decimal! peer session buffer value)
          (:string :bytes :keyword :symbol :bigint)
          (put-variable! peer session buffer value value-type)
          (:long :float :double :boolean :instant :instant-pre-06 :uuid)
          (put-scalar! peer session buffer value value-type)
          (:byte :short :int :id) (put-integer! peer session buffer value value-type)
          :int-int (let [[first-value second-value] value]
                     (put-integer! peer session buffer first-value :int)
                     (put-integer! peer session buffer second-value :int))
          :raw (cond
                 (bytes? value) (.put buffer ^bytes value)
                 (instance? ByteBuffer value) (.put buffer (.duplicate ^ByteBuffer value))
                 :else (raise "Raw value must be bytes or ByteBuffer, got "
                               (type value) {:input value}))
          :term-info
          (adapter/unsupported! :adapter operation
                                (str "Rust buffer codec is not implemented for " value-type))
          (if (vector? value-type)
            (put-tuple! peer session buffer value value-type)
            (do (declared-type! value-type)
                (serialize-buffer! peer session buffer value)))))

      :bits/read-buffer
      (let [[^ByteBuffer buffer value-type] arguments]
        (case value-type
          :avg (read-avg! peer session buffer :avg)
          :attr (invoke-peer! peer session :bits/decode-attr [(bits/get-bytes buffer)])
          :ints (read-ints! peer session buffer false)
          :bitmap (read-bitmap! peer session buffer)
          :doc-info [(read-integer! peer session buffer :int)
                     (read-integer! peer session buffer :short)
                     (read-ints! peer session buffer false)]
          :pos-info [(read-ints! peer session buffer true)
                     (read-ints! peer session buffer true)]
          (:data nil) (deserialize-buffer! peer session buffer)
          :bigdec (read-decimal! peer session buffer)
          (:string :bytes :keyword :symbol :bigint)
          (read-variable! peer session buffer value-type)
          (:long :float :double :boolean :instant :instant-pre-06 :uuid)
          (read-scalar! peer session buffer value-type)
          (:byte :short :int :id) (read-integer! peer session buffer value-type)
          :int-int [(read-integer! peer session buffer :int)
                    (read-integer! peer session buffer :int)]
          :raw (bits/get-bytes buffer)
          (:term-info :query-term-info)
          (adapter/unsupported! :adapter operation
                                (str "Rust buffer codec is not implemented for " value-type))
          (if (vector? value-type)
            (read-tuple! peer session buffer value-type)
            (do (declared-type! value-type)
                (deserialize-buffer! peer session buffer)))))

      (invoke-peer! peer session operation arguments)))
  (stop! [_] (stop-peer! peer))
  (backend-info [_] info))

(defn start
  "Start an already-built peer using an argv vector, without a shell."
  ([command] (start command 30000))
  ([command timeout-ms]
   (when-not (and (vector? command) (seq command) (every? string? command)
                  (int? timeout-ms) (pos? (long timeout-ms)))
     (raise "Expected a peer command vector and positive timeout" {}))
   (let [process (.start (doto (ProcessBuilder. ^java.util.List command)
                          (.redirectError ProcessBuilder$Redirect/INHERIT)))
         worker (Executors/newSingleThreadExecutor
                  (reify ThreadFactory
                    (newThread [_ task]
                      (doto (Thread. task "datalevin-test-adapter-io")
                        (.setDaemon true)))))
         peer {:process process
               :input (DataInputStream. (.getInputStream process))
               :output (DataOutputStream. (.getOutputStream process))
               :worker worker :timeout-ms timeout-ms :stopped (atom false)}]
     (try
       (let [hello (exchange! peer #(read-frame (:input peer)))]
         (when-not (and (= "datalevin-test-adapter" (:protocol hello))
                        (= 1 (:version hello)) (= :rust (:backend hello)))
           (raise "Incompatible Rust adapter handshake" {:hello hello}))
         (->RustBackend peer (Object.)
                        {:backend :rust :runtime :rust :command command :peer hello}))
       (catch Throwable error
         (stop-peer! peer)
         (throw error))))))
