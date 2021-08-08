(ns datalevin.client
  "Blocking client with a connection pool"
  (:require [datalevin.util :as u]
            [datalevin.constants :as c]
            [clojure.string :as s]
            [datalevin.bits :as b]
            [datalevin.protocol :as p])
  (:import [java.nio.charset StandardCharsets]
           [java.nio ByteBuffer BufferOverflowException]
           [java.nio.channels SocketChannel]
           [java.util ArrayList]
           [java.net InetSocketAddress]))

#_(defn- authenticate
    "Send an authenticate message to server, and wait to receive the response.
  If authentication succeeds,  return a connection id.
  Otherwise, close connection, raise exception"
    [{:keys [reader writer] :as conn} username password database]
    (u/write-message writer {:type     :authentication
                             :username username
                             :password password
                             :database database})
    (let [{:keys [type cid]} (u/read-message reader)]
      (if (= type :authentication-ok)
        (assoc conn :cid cid)
        (do (close conn)
            (u/raise "Authentication failure" {})))))

(defn- send-ch
  [^SocketChannel ch ^ByteBuffer bf ]
  (loop []
    (when (.hasRemaining bf)
      (.write ch bf)
      (recur))))

(defn- receive-ch
  [^SocketChannel ch ^ByteBuffer bf]
  (loop []
    (let [readn (.read ch bf)]
      (cond
        (= readn 0)  (recur)
        (> readn 0)  (or (p/receive-one-message bf)
                         (recur))
        (= readn -1) (.close ch)))))

(defprotocol IConnection
  (request [this msg]
    "Send a message to server and return the response, a blocking call"))

(deftype Connection [^SocketChannel ch ^:volatile-mutable ^ByteBuffer bf]
  IConnection
  (request [this msg]
    (try
      (.clear bf)
      (p/write-message-bf bf msg)
      (.flip bf)
      (send-ch ch bf)
      (.clear bf)
      (receive-ch ch bf)
      (catch BufferOverflowException _
        (let [size (* 10 ^int (.capacity bf))]
          (set! bf (b/allocate-buffer size))
          (request this msg)))
      (catch Exception e
        (u/raise "Error writing message to connection buffer:" (ex-message e)
                 {:msg msg})))))

(defprotocol IConnectionPool
  (get-connection [this] "Get a connection from the pool")
  (release-connection [this connection] "Return the connection back to pool"))

(deftype ConnectionPool [^ArrayList available
                         ^ArrayList used]
  IConnectionPool
  (get-connection [this]
    (locking this
      (let [conn (.remove available (dec (.size available)))]
        (.add used conn)
        conn)))
  (release-connection [this conn]
    (locking this
      (.add available conn)
      (.remove used conn))))

(defn- connect
  [^String host port]
  (try
    (doto (SocketChannel/open)
      (.connect (InetSocketAddress. host ^int port)))
    (catch Exception e
      (u/raise "Unable to connect to server: " (ex-message e)
               {:host host :port port}))))

(defn create
  [host port username password database]
  (let [^ConnectionPool pool (->ConnectionPool (ArrayList.) (ArrayList.))]
    (dotimes [_ c/connection-pool-size]
      (.add ^ArrayList (.-available pool) (connect host port)))
    pool))
