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
           [java.net InetSocketAddress StandardSocketOptions]))

(defn- ^SocketChannel connect
  [^String host port]
  (try
    (doto (SocketChannel/open)
      (.setOption StandardSocketOptions/SO_KEEPALIVE true)
      (.setOption StandardSocketOptions/TCP_NODELAY true)
      (.connect (InetSocketAddress. host ^int port)))
    (catch Exception e
      (u/raise "Unable to connect to server: " (ex-message e)
               {:host host :port port}))))

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
        (> readn 0)  (or (p/receive-one-message bf) (recur))
        (= readn -1) (.close ch)))))

(defprotocol IConnection
  (send-n-receive [this msg]
    "Send a message to server and return the response, a blocking call"))

(deftype Connection [^SocketChannel ch ^:volatile-mutable ^ByteBuffer bf]
  IConnection
  (send-n-receive [this msg]
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
          (send-n-receive this msg)))
      (catch Exception e
        (u/raise "Error writing message to connection buffer:" (ex-message e)
                 {:msg msg})))))

(defprotocol IConnectionPool
  (get-connection [this] "Get a connection from the pool")
  (release-connection [this connection] "Return the connection back to pool"))

(defn- new-connection
  [host port]
  (->Connection
    (connect host port)
    (b/allocate-buffer c/+default-buffer-size+)))

(deftype ConnectionPool [^ArrayList available
                         ^ArrayList used
                         host
                         port]
  IConnectionPool
  (get-connection [this]
    (let [start (System/currentTimeMillis)]
      (loop [size (.size available)]
        (if (> size 0)
          (let [conn ^Connection (.remove available (dec size))]
            (if (.isConnected ^SocketChannel (.-ch conn))
              (do (.add used conn)
                  conn)
              (let [conn (new-connection host port)]
                (.add used conn)
                conn)))
          (if (>= (- (System/currentTimeMillis) start) c/connection-timeout)
            (u/raise "Timeout in obtaining a connection" {})
            (.size available))))))
  (release-connection [this conn]
    (.add available conn)
    (.remove used conn)))

(defn- new-connectionpool
  [host port username password database]
  (let [^ConnectionPool pool (->ConnectionPool (ArrayList.)
                                               (ArrayList.)
                                               host
                                               port)]
    (dotimes [_ c/connection-pool-size]
      (.add ^ArrayList (.-available pool) (new-connection host port)))
    pool))

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

(defprotocol IClient
  (authenticate [this username password])
  (request [this req]))

(deftype Client [^ConnectionPool pool
                 username
                 password
                 ])

(comment

  (def client (create "localhost" 8898 "" "" ""))
  (.size (.-used client))

  (def conn (get-connection client))

  (request conn {:hello "world"})
  )
