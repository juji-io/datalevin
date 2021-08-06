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
  return a Clojure value"
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

(defn write-message-bf
  "Write message to a ByteBuffer. First four bytes are length
  of the message (including itself), followed by transit encoded bytes"
  [^ByteBuffer bf m]
  (let [start-pos (.position bf)]
    (.position bf (+ c/message-header-size start-pos))
    (write-transit-bf bf m)
    (let [end-pos (.position bf)]
      (.position bf start-pos)
      (.putInt bf end-pos)
      (.position bf end-pos))))

(defn segment-messages
  "Segment the content of read buffer into messages, and call msg-handler
  on each"
  [^ByteBuffer read-bf msg-handler]
  (loop []
    (let [pos (.position read-bf)]
      (when (> pos c/message-header-size)
        (.flip read-bf)
        (if (< (.remaining read-bf) (.getInt read-bf))
          (doto read-bf
            (.limit (.capacity read-bf))
            (.position pos))
          (let [msg (read-transit-bf read-bf)]
            (msg-handler msg)
            (if-not (.hasRemaining read-bf)
              (.clear read-bf)
              (do (.compact read-bf)
                  (recur)))))))))
