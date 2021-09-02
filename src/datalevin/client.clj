(ns datalevin.client
  "Blocking network client with a connection pool"
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

(defprotocol IConnection
  (send-n-receive [conn msg]
    "Send a message to server and return the response, a blocking call")
  (send-only [conn msg] "Send a message without waiting for a response")
  (receive [conn] "Receive a message, a blocking call")
  (close [conn]))

(deftype Connection [^SocketChannel ch
                     ^:volatile-mutable ^ByteBuffer bf]
  IConnection
  (send-n-receive [this msg]
    (try
      (p/write-message-blocking ch bf msg)
      (.clear bf)
      (let [[resp bf'] (p/receive-ch ch bf)]
        (when-not (identical? bf' bf) (set! bf bf'))
        resp)
      (catch BufferOverflowException _
        (let [size (* c/+buffer-grow-factor+ ^int (.capacity bf))]
          (set! bf (b/allocate-buffer size))
          (send-n-receive this msg)))
      (catch Exception e
        (u/raise "Error sending message and receiving response:"
                 (ex-message e) {:msg msg}))))

  (send-only [this msg]
    (try
      (p/write-message-blocking ch bf msg)
      (catch BufferOverflowException _
        (let [size (* c/+buffer-grow-factor+ ^int (.capacity bf))]
          (set! bf (b/allocate-buffer size))
          (send-only this msg)))
      (catch Exception e
        (u/raise "Error sending message:" (ex-message e) {:msg msg}))))

  (receive [this]
    (try
      (let [[resp bf'] (p/receive-ch ch bf)]
        (when-not (identical? bf' bf) (set! bf bf'))
        resp)
      (catch Exception e
        (u/raise "Error receiving data:" (ex-message e) {}))))

  (close [this]
    (.close ch)))

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
  (->Connection (connect-socket host port)
                (b/allocate-buffer c/+default-buffer-size+)))

(defn- set-client-id
  [conn client-id]
  (let [{:keys [type message]}
        (send-n-receive conn {:type      :set-client-id
                              :client-id client-id})]
    (when-not (= type :set-client-id-ok) (u/raise message {}))))

(defprotocol IConnectionPool
  (get-connection [this] "Get a connection from the pool")
  (release-connection [this connection] "Return the connection back to pool")
  (close-pool [this])
  (closed-pool? [this]))

(deftype ConnectionPool [^ArrayList available
                         ^ArrayList used]
  IConnectionPool
  (get-connection [this]
    (let [start (System/currentTimeMillis)]
      (loop [size (.size available)]
        (if (> size 0)
          (locking this
            (let [conn ^Connection (.remove available (dec size))]
              (.add used conn)
              conn))
          (if (>= (- (System/currentTimeMillis) start) c/connection-timeout)
            (u/raise "Timeout in obtaining a connection" {})
            (recur (.size available)))))))

  (release-connection [this conn]
    (locking this
      (.remove used conn)
      (.add available conn)))

  (close-pool [this]
    (dotimes [i (.size used)] (close ^Connection (.get used i)))
    (.clear used)
    (dotimes [i (.size used)] (close ^Connection (.get available i)))
    (.clear available))

  (closed-pool? [this]
    (and (.isEmpty used) (.isEmpty available))))

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
    (close conn)
    (if (= type :authentication-ok)
      client-id
      (u/raise "Authentication failure: " message {}))))

(defn- new-connectionpool
  [host port client-id]
  (let [^ConnectionPool pool (->ConnectionPool (ArrayList.) (ArrayList.))
        ^ArrayList available (.-available pool)]
    (dotimes [_ c/connection-pool-size]
      (let [conn (new-connection host port)]
        (set-client-id conn client-id)
        (.add available conn)))
    pool))

(defprotocol IClient
  (request [client req]
    "Send a request to server and return the response. The response could
     also initiate a copy out")
  (copy-in [client req data batch-size]
    "Copy data to the server. `req` is a request type message,
     `data` is a sequence, `batch-size` decides how to partition the data
      so that each batch fits in buffers along the way")
  (disconnect [client])
  (disconnected? [client]))

(defn parse-user-info
  [^URI uri]
  (when-let [user-info (.getUserInfo uri)]
    (when-let [[_ username password] (re-find #"(.+):(.+)" user-info)]
      {:username username :password password})))

(defn parse-port
  [^URI uri]
  (let [p (.getPort uri)] (if (= -1 p) c/default-port p)))

(defn parse-db
  "Extract the identifier of database from URI. A database is uniquely
  identified by /<creator username>/<database name>. If the creator part
  is missing, current username is assumed. Return a vector
  of [<creator username>, <database name>] "
  [^URI uri]
  (let [path (.getPath uri)]
    (when-not (or (s/blank? path) (= path "/"))
      (let [res (s/split (subs path 1) #"/")
            c   (count res)]
        (if (= c 1)
          [nil (first res)]
          [(first res) (second res)])))))

(defn parse-query
  [^URI uri]
  (when-let [query (.getQuery uri)]
    (->> (s/split query #"&")
         (map #(s/split % #"="))
         (into {}))))

(defn- copy-in*
  [conn req data batch-size ]
  (try
    (doseq [batch (partition batch-size batch-size nil data)]
      (send-only conn batch))
    (send-n-receive conn {:type :copy-done})
    (catch Exception e
      (send-n-receive conn {:type :copy-fail})
      (u/raise "Unable to copy in:" (ex-message e)
               {:req req :count (count data)}))))

(defn- copy-out [conn req]
  (try
    (let [data (transient [])]
      (loop []
        (let [msg (receive conn)]
          (if (map? msg)
            (let [{:keys [type]} msg]
              (if (= type :copy-done)
                {:type :command-complete :result (persistent! data)}
                (u/raise "Server error while copying out data" {:msg msg})))
            (do (doseq [d msg] (conj! data d))
                (recur))))))
    (catch Exception e
      (u/raise "Unable to receive copy:" (ex-message e) {:req req}))))

(deftype Client [^URI uri
                 ^ConnectionPool pool
                 ^UUID id]
  IClient
  (request [client req]
    (let [conn (get-connection pool)]
      (try
        (let [{:keys [type] :as result} (send-n-receive conn req)]
          (if (= type :copy-out-response)
            (copy-out conn req)
            result))
        (catch Exception e (throw e))
        (finally (release-connection pool conn)))))

  (copy-in [client req data batch-size]
    (let [conn (get-connection pool)]
      (try
        (let [{:keys [type]} (send-n-receive conn req)]
          (if (= type :copy-in-response)
            (copy-in* conn req data batch-size)
            (u/raise "Server refuses to accept copy in" {:req req})))
        (catch Exception e (throw e))
        (finally (release-connection pool conn)))))

  (disconnect [client]
    (let [conn (get-connection pool)]
      (send-only conn {:type :disconnect})
      (release-connection pool conn))
    (close-pool pool))

  (disconnected? [client]
    (closed-pool? pool)))

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
  "Create a new client that maintains a pooled connection to a remote
  Datalevin database server. This operation takes at least 0.5 seconds
  in order to perform a secure password hashing that defeats cracking."
  ([uri-str]
   (new-client uri-str nil))
  ([uri-str schema]
   (let [uri                         (URI. uri-str)
         {:keys [username password]} (parse-user-info uri)
         host                        (.getHost uri)
         port                        (parse-port uri)
         [_ db]                      (parse-db uri)
         store                       (or (get (parse-query uri) "store")
                                         c/db-store-datalog)
         client-id                   (authenticate host port username password)
         pool                        (new-connectionpool host port client-id)
         client                      (->Client uri pool client-id)]
     (when db (init-db client db store schema))
     client)))

(defn normal-request
  "Send request to server and returns results. Does not use the
  copy-in protocol. `call` is a keyword, `args` is a vector"
  [client call args]
  (let [{:keys [type message result]}
        (request client {:type call :args args})]
    (if (= type :error-response)
      (u/raise "Request to Datalevin server failed: " message
               {:call call :args args})
      result)))

(defn create-user
  "Create a user that can login. Username will be converted to Kebab case
  (i.e. all lower case and words connected with dashes)."
  [client username password]
  (normal-request client :create-user [username password]))

(defn create-role
  "Create a role. `role-name` is a keyword, `role-desc` is a string."
  [client role-name role-desc]
  (normal-request client :create-role [role-name role-desc]))

(defn assign-role
  "Assign a role to a user. "
  [client role-name username]
  (normal-request client :assign-role [role-name username]))

(defn assign-permission
  "Assign a permission to a role.

  `perm-act` can be one of `:datalevin.server/view`, `:datalevin.server/alter`,
  `:datalevin.server/create`, or `:datalevin.server/control`, with each subsumes
  the former.

  `perm-obj` can be one of `:datalevin.server/database`, `:datalevin.server/role`,
  or `:datalevin.server/server`, where the last one subsumes all the others

  `perm-db` is a database UUID. If it is `nil`, the permission applies to all
  databases."
  [client role-name perm-desc perm-act perm-obj perm-db]
  (normal-request client :assign-permission
                  [role-name perm-desc perm-act perm-obj perm-db]))

(comment

  (def client (new-client "dtlv://datalevin:datalevin@localhost"))

  (create-user client "boyan" "lol")

  (create-role client :test-role "lol")

  (assign-role client :test-role "boyan")

  )
