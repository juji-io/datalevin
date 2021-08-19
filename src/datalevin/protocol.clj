(ns datalevin.protocol
  "Shared code of client/server"
  (:require [datalevin.bits :as b]
            [datalevin.constants :as c]
            [datalevin.util :as u]
            [datalevin.datom :as d]
            [cognitect.transit :as transit]
            [clojure.string :as s]
            [taoensso.nippy :as nippy])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]
           [java.util Arrays UUID Date Base64]
           [java.nio ByteBuffer BufferOverflowException]
           [java.nio.channels SocketChannel]
           [java.nio.charset StandardCharsets]
           [java.lang String Character]
           [java.net URI]
           [datalevin.io ByteBufferInputStream ByteBufferOutputStream]
           [datalevin.datom Datom]))

(defn dtlv-uri?
  "return true if the given string is a Datalevin connection string"
  [s]
  (when-let [uri (URI. s)]
    (= (.getScheme uri) "dtlv")))

(defn read-nippy-bf
  "Read from a ByteBuffer containing nippy encoded bytes, return a Clojure
  value."
  [^ByteBuffer bf]
  (try
    (nippy/fast-thaw (b/get-bytes bf))
    (catch Exception e
      (u/raise "Unable to read nippy from ByteBuffer:" (ex-message e) {}))))

(def transit-read-handlers
  {"datalevin/Datom" (transit/read-handler d/datom-from-reader)})

(defn read-transit-bf
  "Read from a ByteBuffer containing transit+json encoded bytes,
  return a Clojure value. Consumes the entire buffer"
  [^ByteBuffer bf]
  (try
    (transit/read (transit/reader (ByteBufferInputStream. bf)
                                  :json
                                  {:handlers transit-read-handlers}))
    (catch Exception e
      (u/raise "Unable to read transit from ByteBuffer:" (ex-message e) {}))))

(defn write-nippy-bf
  "Write a Clojure value as nippy encoded bytes into a ByteBuffer"
  [^ByteBuffer bf v]
  (try
    (b/put-bytes bf (nippy/fast-freeze v))
    (catch Exception e
      (u/raise "Unable to write nippy to ByteBuffer:" (ex-message e) {}))))

(def transit-write-handlers
  {Datom (transit/write-handler
           "datalevin/Datom"
           (fn [^Datom d] [(.-e d) (.-a d) (.-v d) (.-tx d)]))})

(defn write-transit-bf
  "Write a Clojure value as transit+json encoded bytes into a ByteBuffer"
  [^ByteBuffer bf v]
  (try
    (transit/write (transit/writer (ByteBufferOutputStream. bf)
                                   :json
                                   {:handlers transit-write-handlers})
                   v)
    (catch Exception e
      (u/raise "Unable to write transit to ByteBuffer:" (ex-message e) {}))))

(defn- write-value-bf
  [bf fmt msg]
  (case (short fmt)
    1 (write-transit-bf bf msg)
    2 (write-nippy-bf bf msg)))

(defn- read-value-bf
  [bf fmt]
  (case (short fmt)
    1 (read-transit-bf bf)
    2 (read-nippy-bf bf)))

(defn write-message-bf
  "Write a message to a ByteBuffer. First byte is format, then four bytes
  length of the whole message (include header), followed by message value"
  ([bf msg]
   (write-message-bf bf msg c/message-format-transit))
  ([^ByteBuffer bf msg fmt]
   (let [start-pos (.position bf)]
     (.position bf (+ c/message-header-size start-pos))
     (write-value-bf bf fmt msg)
     (let [end-pos (.position bf)]
       (.position bf start-pos)
       (.put bf ^byte (unchecked-byte fmt))
       (.putInt bf (- end-pos start-pos))
       (.position bf end-pos)))))

(defn read-transit-bytes
  "Read transit+json encoded bytes into a Clojure value"
  [^bytes bs]
  (try
    (transit/read (transit/reader (ByteArrayInputStream. bs)
                                  :json
                                  {:handlers transit-read-handlers}))
    (catch Exception e
      (u/raise "Unable to read transit:" (ex-message e) {:bytes bs}))))

(defn write-transit-bytes
  "Write a Clojure value as transit+json encoded bytes"
  [v]
  (try
    (let [baos (ByteArrayOutputStream.)]
      (transit/write (transit/writer baos :json
                                     {:handlers transit-write-handlers})
                     v)
      (.toByteArray baos))
    (catch Exception e
      (u/raise "Unable to write transit:" (ex-message e) {:value v}))))

(defn read-value
  [fmt bs]
  (case (short fmt)
    1 (read-transit-bytes bs)
    2 (nippy/fast-thaw bs)))

(defn send-ch
  "Send all data in buffer to channel, will block if channel is busy"
  [^SocketChannel ch ^ByteBuffer bf ]
  (loop []
    (when (.hasRemaining bf)
      (.write ch bf)
      (recur))))

(defn write-message-blocking
  "Write a message in blocking mode"
  [^SocketChannel ch ^ByteBuffer bf msg]
  (.clear bf)
  (write-message-bf bf msg)
  (.flip bf)
  (send-ch ch bf))

(defn receive-one-message
  "Consume one message from the read-bf and return it.
  If there is not enough data for one message, return nil. Prepare the
  buffer for write. If one message is bigger than read-bf, allocate a
  new read-bf. Return `[msg read-bf]`"
  [^ByteBuffer read-bf]
  (let [pos (.position read-bf)]
    (if (> pos c/message-header-size)
      (do (.flip read-bf)
          (let [available (.limit read-bf)
                fmt       (.get read-bf)
                length    ^int (.getInt read-bf)
                read-bf   (if (< (.capacity read-bf) length)
                            (let [^ByteBuffer bf
                                  (ByteBuffer/allocateDirect
                                    (* c/+buffer-grow-factor+ length))]
                              (.rewind read-bf)
                              (b/buffer-transfer read-bf bf)
                              bf)
                            read-bf)]
            (if (< available length)
              (do (doto read-bf
                    (.limit (.capacity read-bf))
                    (.position pos))
                  [nil read-bf])
              (let [ba  (byte-array (- length c/message-header-size))
                    _   (.get read-bf ba)
                    msg (read-value fmt ba)]
                (if (= available length)
                  (.clear read-bf)
                  (doto read-bf
                    (.position length)
                    (.compact)))
                [msg read-bf]))))
      [nil read-bf])))

(defn receive-ch
  "Receive one message from channel and put it in buffer, will block
  until one full message is received. When buffer is too small for a
  message, a new buffer is allocated. Return [msg bf]."
  [^SocketChannel ch ^ByteBuffer bf]
  (loop [^ByteBuffer bf bf]
    (if (> (.position bf) c/message-header-size)
      (let [[msg ^ByteBuffer bf] (receive-one-message bf)]
        (if msg
          [msg bf]
          (let [readn (.read ch bf)]
            (cond
              (> readn 0)  (let [[msg bf] (receive-one-message bf)]
                             (if msg [msg bf] (recur bf)))
              (= readn -1) (do (.close ch) [nil bf])))))
      (let [readn (.read ch bf)]
        (cond
          (> readn 0)  (let [[msg bf] (receive-one-message bf)]
                         (if msg [msg bf] (recur bf)))
          (= readn -1) (do (.close ch) [nil bf]))))))

(defn segment-messages
  "Segment the content of read buffer into messages, and call msg-handler
  on each. The messages are byte arrays. Message parsing will be done in the
  msg-handler. In non-blocking mode, each should be handled by a worker thread,
  so the main event loop is not hindered by slow parsing. Assume each message
  is small enough for the buffer, as big messages are handled by copy-in/out."
  [^ByteBuffer read-bf msg-handler]
  (loop []
    (let [pos (.position read-bf)]
      (when (> pos c/message-header-size)
        (.flip read-bf)
        (let [available (.limit read-bf)
              fmt       (.get read-bf)
              length    (.getInt read-bf)]
          (if (< available length)
            (doto read-bf
              (.limit (.capacity read-bf))
              (.position pos))
            (let [ba (byte-array (- length c/message-header-size))]
              (.get read-bf ba)
              (msg-handler fmt ba)
              (if (= available length)
                (.clear read-bf)
                (do (.compact read-bf)
                    (recur))))))))))
