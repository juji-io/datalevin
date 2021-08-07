(ns datalevin.client
  (:require [datalevin.util :as u]
            [datalevin.constants :as c]
            [clojure.string :as s]
            [datalevin.bits :as b]
            [datalevin.protocol :as p])
  (:import [java.nio.charset StandardCharsets]
           [java.nio ByteBuffer]
           [java.nio.channels SocketChannel]
           [java.net InetSocketAddress]))

(defn- connect
  [^String host port]
  (try
    (doto (SocketChannel/open)
      (.connect (InetSocketAddress. host ^int port)))
    (catch Exception e
      (u/raise "Unable to connect to server: " (ex-message e)
               {:host host :port port}))))

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

(defn send
  [^SocketChannel ch ^ByteBuffer bf ]
  (loop []
    (when (.hasRemaining bf)
      (.write ch bf)
      (recur))))

(defn startup
  "Attempt to connect and authenticate to a server, return a connection map"
  [host port username password database]
  (let [bf (ByteBuffer/allocateDirect c/+default-buffer-size+)
        ch ^SocketChannel (connect host port)]
    #_(authenticate conn username password database)
    (p/write-message-bf bf {:hello :world})
    (.flip bf)
    (send ch bf)
    (.close ch)
    ))

(startup "localhost" 8898 "" "" "")

(defprotocol IConnection
  (request [this msg]
    "Send a message to server and return the response, a blocking call"))

(deftype Connection [^SocketChannel ch
                     ^ByteBuffer bf])

(defprotocol IConnectionPool
  (get-connection [this] "Get a connection from the pool")
  (release-connection [this connection] "Return the connection back to pool"))

(defn test-f [])
