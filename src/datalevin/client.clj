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
           [java.util ArrayList UUID]
           [java.net InetSocketAddress StandardSocketOptions URI]))

(defn- send-ch
  "Send all data in buffer to socket"
  [^SocketChannel ch ^ByteBuffer bf ]
  (loop []
    (when (.hasRemaining bf)
      (.write ch bf)
      (recur))))

(defn- receive-ch
  "Receive one message from socket and put it in buffer, will block
  until one full message is received"
  [^SocketChannel ch ^ByteBuffer bf]
  (loop []
    (let [readn (.read ch bf)]
      (cond
        (> readn 0)  (or (p/receive-one-message bf) (recur))
        (= readn -1) (.close ch)))))

(defprotocol IConnection
  (send-n-receive [this msg]
    "Send a message to server and return the response, a blocking call")
  (close [this]))

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
                 {:msg msg}))))
  (close [this]
    (.close ch)))

(defprotocol IConnectionPool
  (get-connection [this] "Get a connection from the pool")
  (release-connection [this connection] "Return the connection back to pool"))

(defn- ^SocketChannel connect-socket
  "connect to server and return the client socket channel"
  [^String host port]
  (try
    (doto (SocketChannel/open)
      (.setOption StandardSocketOptions/SO_KEEPALIVE true)
      (.setOption StandardSocketOptions/TCP_NODELAY true)
      (.connect (InetSocketAddress. host ^int port)))
    (catch Exception e
      (u/raise "Unable to connect to server: " (ex-message e)
               {:host host :port port}))))

(defn- new-connection
  [host port]
  (->Connection
    (connect-socket host port)
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

(defn- authenticate
  "Send an authenticate message to server, and wait to receive the response.
  If authentication succeeds,  return a client id.
  Otherwise, close connection, raise exception"
  [^Connection conn username password]
  (let [{:keys [type client-id message]}
        (send-n-receive conn {:type     :authentication
                              :username username
                              :password password})]
    (if (= type :authentication-ok)
      client-id
      (do (close conn)
          (u/raise "Authentication failure: " message {})))))

(defn- new-connectionpool
  [host port username password]
  (let [conn (new-connection host port)]
    (when-let [client-id (authenticate conn username password)]
      (let [^ConnectionPool pool (->ConnectionPool (ArrayList.)
                                                   (ArrayList.)
                                                   host
                                                   port)
            ^ArrayList available (.-available pool)]
        (.add available conn)
        (dotimes [_ (dec c/connection-pool-size)]
          (let [^Connection c (new-connection host port)]
            (send-n-receive c {:type      :set-client-id
                               :client-id client-id})
            (.add available c)))
        pool))))

(defprotocol IClient
  (startup [this uri-str])
  (request [this req]))

(defn parse-user-info
  [^URI uri]
  (when-let [user-info (.getUserInfo uri)]
    (when-let [[_ username password] (re-find #"(.+):(.+)" user-info)]
      {:username username :password password})))

(defn parse-port
  [^URI uri]
  (let [p (.getPort uri)] (if (= -1 p) c/default-port p)))

(defn parse-db
  [^URI uri]
  (subs (.getPath uri) 1))

(deftype Client [^:volatile-mutable ^ConnectionPool pool
                 ^:volatile-mutable ^UUID id]
  IClient
  (startup [this uri-str]
    (let [uri                         (URI. uri-str)
          {:keys [username password]} (parse-user-info uri)
          host                        (.getHost uri)
          port                        (parse-port uri)
          db                          (parse-db uri)]
      )))

(comment

  (def uri (URI. "dtlv://localhost/sales"))
  (.getUserInfo uri)
  (.getHost uri)
  (.getPort uri)
  (subs (.getPath uri) 1)

  (def client (create "localhost" 8898 "" "" ""))
  (.size (.-used client))

  (def conn (get-connection client))

  (request conn {:hello "world"})
  )
