(ns datalevin.protocol
  "Client/Server protocol"
  (:require [datalevin.bits :as b]
            [datalevin.constants :as c]
            [datalevin.util :as u]
            [cognitect.transit :as transit]
            [taoensso.nippy :as nippy])
  (:import [java.io DataInput DataOutput]
           [java.util Arrays UUID Date Base64]
           [java.nio ByteBuffer]
           [java.nio.charset StandardCharsets]
           [java.lang String Character]
           [datalevin.io ByteBufferInputStream ByteBufferOutputStream]
           ))

(defn read-transit-bf
  "Read from a ByteBuffer containing transit+json encoded bytes,
  return a Clojure value. Consumes the entire buffer"
  [^ByteBuffer bf]
  (try
    (transit/read (transit/reader (ByteBufferInputStream. bf) :json))
    (catch Exception e
      (u/raise "Unable to read transit from ByteBuffer:" (ex-message e) {}))))

(defn write-transit-bf
  "Write a Clojure value as transit+json encoded bytes into a ByteBuffer"
  [^ByteBuffer bf v]
  (try
    (transit/write (transit/writer (ByteBufferOutputStream. bf) :json) v)
    (catch Exception e
      (u/raise "Unable to write transit to ByteBuffer:" (ex-message e) {}))))

(defn- write-value-bf
  [bf type msg]
  (case (short type)
    1 (write-transit-bf bf msg)))

(defn- read-value-bf
  [bf type]
  (case (short type)
    1 (read-transit-bf bf)))

(defn read-value
  [type bs]
  (case (short type)
    1 (u/read-transit-bytes bs)))

(defn write-message-bf
  "Write a message to a ByteBuffer. First byte is type, then four bytes
  length of the whole message (include header), followed by message value"
  ([bf msg]
   (write-message-bf bf msg c/message-type-transit))
  ([^ByteBuffer bf msg type]
   (let [start-pos (.position bf)]
     (.position bf (+ c/message-header-size start-pos))
     (write-value-bf bf type msg)
     (let [end-pos (.position bf)]
       (.position bf start-pos)
       (.put bf ^byte (unchecked-byte type))
       (.putInt bf (- end-pos start-pos))
       (.position bf end-pos)))))

(defn segment-messages
  "Segment the content of read buffer into messages, and call msg-handler
  on each. The messages are byte arrays, so message parsing is done in the
  msg-handler, which ideally is handled by a worker thread, so main-event loop
  is not blocked by slow parsing."
  [^ByteBuffer read-bf msg-handler]
  (loop []
    (let [pos (.position read-bf)]
      (when (> pos c/message-header-size)
        (.flip read-bf)
        (let [available (.limit read-bf)
              type      (.get read-bf)
              length    (.getInt read-bf)]
          (if (< available length)
            (doto read-bf
              (.limit (.capacity read-bf))
              (.position pos))
            (let [ba (byte-array (- length c/message-header-size))]
              (.get read-bf ba)
              (msg-handler type ba)
              (if (= available length)
                (.clear read-bf)
                (do (.compact read-bf)
                    (recur))))))))))

(defn receive-one-message
  "Consume one message from the read-bf and return it. If there is not
  enough data for one message, return nil. Prepare the buffer for write."
  [^ByteBuffer read-bf]
  (let [pos (.position read-bf)]
    (when (> pos c/message-header-size)
      (.flip read-bf)
      (let [available (.limit read-bf)
            type      (.get read-bf)
            length    (.getInt read-bf)]
        (if (< available length)
          (do (doto read-bf
                (.limit (.capacity read-bf))
                (.position pos))
              nil)
          (let [msg (read-value-bf (.slice read-bf) type)]
            (if (= available length)
              (.clear read-bf)
              (doto read-bf
                (.position length)
                (.compact)))
            msg))))))
