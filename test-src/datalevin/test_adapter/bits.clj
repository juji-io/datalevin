(ns ^:no-doc datalevin.test-adapter.bits
  "Selected-backend serialization for the original Clojure codec tests."
  (:require
   [datalevin.buffer :as buffer]
   [datalevin.test-adapter :as adapter])
  (:import
   [java.nio ByteBuffer]))

(defn serialize ^bytes [value]
  (adapter/call! :bits/serialize [value]))

(defn deserialize [^bytes bytes]
  (adapter/call! :bits/deserialize [bytes]))

(defn encode-base64 [^bytes bytes]
  (adapter/call! :bits/encode-base64 [bytes]))

(defn decode-base64 ^bytes [^String encoded]
  (adapter/call! :bits/decode-base64 [encoded]))

(defn encode-bigint ^bytes [value]
  (adapter/call! :bits/encode-bigint [value]))

(defn decode-bigint [^bytes bytes]
  (adapter/call! :bits/decode-bigint [bytes]))

(defn serialize-bf [buffer value]
  (adapter/call! :bits/serialize-bf [buffer value]))

(defn deserialize-bf [buffer]
  (adapter/call! :bits/deserialize-bf [buffer]))

(defn get-bytes
  "Copy a JVM buffer's bytes before its owner reuses it. No codec is involved."
  (^bytes [^ByteBuffer buffer]
   (get-bytes buffer (.remaining buffer)))
  (^bytes [^ByteBuffer buffer size]
   (let [bytes (byte-array size)]
     (.get buffer bytes)
     bytes)))

(defn put-buffer
  ([buffer value] (put-buffer buffer value :data))
  ([buffer value value-type]
   (adapter/call! :bits/put-buffer [buffer value value-type])))

(defn put-bf
  "Clear, write through the selected backend, and flip. Nil leaves the buffer alone."
  [^ByteBuffer buffer value value-type]
  (when-some [value value]
    (.clear buffer)
    (put-buffer buffer value value-type)
    (.flip buffer)))

(defn read-buffer
  ([buffer] (read-buffer buffer :data))
  ([buffer value-type]
   (adapter/call! :bits/read-buffer [buffer value-type])))

(defn acquire-serialized-bf
  "Acquire a JVM heap buffer containing the selected codec's bytes, ready to read.
  Return it with buffer/return-array-buffer after synchronous consumption."
  ^ByteBuffer [value]
  (adapter/call! :bits/acquire-serialized-bf [value]))

(defmacro with-serialized-bf
  "Consume a serialized buffer within body and return it even when body throws.
  Neither the buffer nor a lazy reader of it may escape this scope."
  [[sym value] & body]
  `(let [~(with-meta sym (assoc (meta sym) :tag 'java.nio.ByteBuffer))
         (acquire-serialized-bf ~value)]
     (try
       ~@body
       (finally (buffer/return-array-buffer ~sym)))))

(defn measure-size [value]
  (adapter/call! :bits/measure-size [value]))
