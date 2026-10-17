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

(defn bitmap
  ([] (adapter/call! :bits/bitmap []))
  ([values] (adapter/call! :bits/bitmap [values])))

(defn bitmap-add [bitmap member]
  (adapter/call! :bits/bitmap-add [bitmap member]))

(defn bitmap-del [bitmap member]
  (adapter/call! :bits/bitmap-del [bitmap member]))

(defn bitmap? [value]
  (instance? org.roaringbitmap.RoaringBitmap value))

(defn bitmap-and [left right]
  (adapter/call! :bits/bitmap-and [left right]))

(defn bitmap-or [left right]
  (adapter/call! :bits/bitmap-or [left right]))

(defn bitmaps-and [bitmaps]
  (adapter/call! :bits/bitmaps-and [bitmaps]))

(defn bitmaps-or [bitmaps]
  (adapter/call! :bits/bitmaps-or [bitmaps]))

(defn bitmap-empty? [bitmap]
  (adapter/call! :bits/bitmap-empty? [bitmap]))

(defn bitmap64
  ([] (adapter/call! :bits/bitmap64 []))
  ([members] (adapter/call! :bits/bitmap64 [members])))

(defn bitmap64-add [bitmap ^long member]
  (adapter/call! :bits/bitmap64-add [bitmap member]))

(defn bitmap64-contains? [bitmap ^long member]
  (adapter/call! :bits/bitmap64-contains? [bitmap member]))

(defn bitmap64-cardinality ^long [bitmap]
  (adapter/call! :bits/bitmap64-cardinality [bitmap]))

(defn bitmap64-min ^long [bitmap]
  (adapter/call! :bits/bitmap64-min [bitmap]))

(defn bitmap64-max ^long [bitmap]
  (adapter/call! :bits/bitmap64-max [bitmap]))

(defn bitmap64->longs ^longs [bitmap]
  (adapter/call! :bits/bitmap64->longs [bitmap]))

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

(defn buffer-reader
  "Capture a storage type; dispatch to the active backend when the reader is called."
  [value-type]
  (fn [buffer] (read-buffer buffer value-type)))

(defn type-size [value-type]
  (adapter/call! :bits/type-size [value-type]))

(defn valid-data? [value value-type]
  (adapter/call! :bits/valid-data? [value value-type]))

(defn indexable [entity attribute value value-type max-giant]
  (adapter/call! :bits/indexable [entity attribute value value-type max-giant]))

(defn giant? [value]
  (adapter/call! :bits/giant? [value]))

(defn indexable-bytes
  (^bytes [value] (adapter/call! :bits/indexable-bytes [value]))
  (^bytes [value buffer] (adapter/call! :bits/indexable-bytes [value buffer])))

(defn avg->aid [buffer]
  (adapter/call! :bits/avg->aid [buffer]))

(defn avg->giant-id [buffer]
  (adapter/call! :bits/avg->giant-id [buffer]))

(defn avg->inline-value [buffer]
  (adapter/call! :bits/avg->inline-value [buffer]))

(defn avg->r [buffer]
  (adapter/call! :bits/avg->r [buffer]))

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
