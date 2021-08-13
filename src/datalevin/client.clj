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
           [java.util ArrayList UUID Collection]
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
  (send-n-receive [conn msg]
    "Send a message to server and return the response, a blocking call")
  (close [conn]))

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
  (release-connection [this connection] "Return the connection back to pool")
  (shutdown-pool [this] "Close all connections in the pool"))

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

(defn- set-client-id
  [conn client-id]
  (let [{:keys [type message]}
        (send-n-receive conn {:type      :set-client-id
                              :client-id client-id})]
    (when-not (= type :set-client-id-ok) (u/raise message {}))))

(deftype ConnectionPool [^ArrayList available
                         ^ArrayList used
                         client-id
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
                (set-client-id conn client-id)
                (.add used conn)
                conn)))
          (if (>= (- (System/currentTimeMillis) start) c/connection-timeout)
            (u/raise "Timeout in obtaining a connection" {})
            (recur (.size available)))))))
  (release-connection [this conn]
    (.add available conn)
    (.remove used conn))
  (shutdown-pool [this]
    (doseq [^Connection conn available]
      (close conn)
      (.remove available conn))
    (doseq [^Connection conn used]
      (close conn)
      (.remove used conn))))

(defn- authenticate
  "Send an authenticate message to server, and wait to receive the response.
  If authentication succeeds,  return a client id.
  Otherwise, close connection, raise exception"
  [host port username password]
  (let [conn (new-connection host port)

        {:keys [type client-id message]}
        (send-n-receive conn {:type     :authentication
                              :username username
                              :password password})]
    (if (= type :authentication-ok)
      client-id
      (do (close conn)
          (u/raise "Authentication failure: " message {})))))

(defn- new-connectionpool
  [host port client-id]
  (let [^ConnectionPool pool (->ConnectionPool (ArrayList.)
                                               (ArrayList.)
                                               client-id
                                               host
                                               port)
        ^ArrayList available (.-available pool)]
    (dotimes [_ c/connection-pool-size]
      (let [conn (new-connection host port)]
        (set-client-id conn client-id)
        (.add available conn)))
    pool))

(defprotocol IClient
  (request [client req] "Send a request, and return response")
  (stop [client] "Stop this client")
  (stopped? [client] "Return true if the client is stopped"))

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
  (let [path (.getPath uri)]
    (when-not (or (s/blank? path) (= path "/"))
      (subs path 1))))

(defn- parse-query
  [^URI uri]
  (when-let [query (.getQuery uri)]
    (->> (s/split query #"&")
         (map #(s/split % #"="))
         (into {}))))

(deftype Client [^URI uri
                 ^ConnectionPool pool
                 ^UUID id]
  IClient
  (request [client req]
    (let [conn (get-connection pool)
          res  (send-n-receive conn req)]
      (release-connection pool conn)
      res))
  (stop [_]
    (shutdown-pool pool))
  (stopped? [_]
    (and (.isEmpty ^ArrayList (.-available pool))
         (.isEmpty ^ArrayList (.-used pool)))))

(defn- init-db
  [client db store schema]
  (let [{:keys [type]}
        (request client (if (= store c/db-store-datalog)
                          (cond-> {:type :open :db-name db}
                            schema (assoc :schema schema))
                          {:type :open-kv :db-name db}))]
    (when (= type :error-response)
      (u/raise "Unable to open database:" db {}))))

(defn new-client
  "Create a new client that maintains a connection pool to a remote
  Datalevin database server. This operation takes at least 0.5 seconds
  in order to perform a secure password hashing that defeats cracking."
  ([uri-str]
   (new-client uri-str nil))
  ([uri-str schema]
   (let [uri                         (URI. uri-str)
         {:keys [username password]} (parse-user-info uri)

         host      (.getHost uri)
         port      (parse-port uri)
         db        (parse-db uri)
         store     (or (get (parse-query uri) "store") c/db-store-datalog)
         client-id (authenticate host port username password)
         pool      (new-connectionpool host port client-id)
         client    (->Client uri pool client-id)]
     (when db (init-db client db store schema))
     client)))

(comment

  (def client (new-client "dtlv://datalevin:datalevin@localhost/testdb"))


  )
