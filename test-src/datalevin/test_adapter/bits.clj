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
