(ns datalevin.server
  "Non-blocking event driven server"
  (:require [datalevin.util :as u]
            [datalevin.core :as d]
            [datalevin.bits :as b]
            [datalevin.lmdb :as l]
            [datalevin.protocol :as p]
            [datalevin.storage :as st]
            [datalevin.constants :as c]
            [taoensso.timbre :as log])
  (:import [java.nio.charset StandardCharsets]
           [java.nio ByteBuffer BufferOverflowException]
           [java.nio.channels Selector SelectionKey ServerSocketChannel
            SocketChannel]
           [java.net InetSocketAddress]
           [java.security SecureRandom]
           [java.util Iterator UUID]
           [java.util.concurrent Executors Executor ThreadPoolExecutor]
           [datalevin.db DB]
           [org.bouncycastle.crypto.generators Argon2BytesGenerator]
           [org.bouncycastle.crypto.params Argon2Parameters
            Argon2Parameters$Builder]))

(log/refer-timbre)
(log/set-level! :debug)

;; password processing

(defn salt
  "generate a 16 byte salt"
  []
  (let [bs (byte-array 16)]
    (.nextBytes (SecureRandom.) bs)
    bs))

(defn password-hashing
  "hashing password using argon2id algorithm, see
  https://github.com/p-h-c/phc-winner-argon2"
  ([password salt]
   (password-hashing password salt nil))
  ([^String password ^bytes salt
    {:keys [ops-limit mem-limit out-length parallelism]
     ;; these defaults are secure, as it takes about 0.5 second to hash
     :or   {ops-limit   4
            mem-limit   131072
            out-length  32
            parallelism 1}}]
   (let [builder (doto (Argon2Parameters$Builder. Argon2Parameters/ARGON2_id)
                   (.withVersion Argon2Parameters/ARGON2_VERSION_13)
                   (.withIterations ops-limit)
                   (.withMemoryAsKB mem-limit)
                   (.withParallelism parallelism)
                   (.withSalt salt))
         gen     (doto (Argon2BytesGenerator.)
                   (.init (.build builder)))
         out-bs  (byte-array out-length)
         in-bs   (.getBytes password StandardCharsets/UTF_8)]
     (.generateBytes gen in-bs out-bs (int 0) (int out-length))
     (u/encode-base64 out-bs))))

(defn password-matches?
  [in-password password-hash salt]
  (= password-hash (password-hashing in-password salt)))

;; global server resources
;; {root, selector, work-executor, sys-conn, clients}
(def resources (atom {:clients {; client-id -> {user/id, dt-store, kv-store }
                                }}))
(defn- write-to-bf
  "write a message to write buffer, auto grow the buffer"
  [^SelectionKey skey msg]
  (let [state                          (.attachment skey)
        {:keys [^ByteBuffer write-bf]} @state]
    (.clear write-bf)
    (try
      (p/write-message-bf write-bf msg)
      (catch BufferOverflowException _
        (let [size (* 10 ^int (.capacity write-bf))]
          (swap! state assoc :write-bf (b/allocate-buffer size))
          (write-to-bf skey msg))))))

(defn write-message
  "Attempt to write a message immediately, if cannot write all, register
  interest in OP_WRITE event"
  [^SelectionKey skey msg]
  (write-to-bf skey msg)
  (let [{:keys [^ByteBuffer write-bf]} @(.attachment skey)
        ^SocketChannel ch              (.channel skey)]
    (.flip write-bf)
    (.write ch write-bf)
    (when (.hasRemaining write-bf)
      (.interestOpsOr skey SelectionKey/OP_WRITE))))

(defn- handle-write
  "We already tried to write before, now try to write the remaining data.
  Remove interest in OP_WRITE event when done"
  [^SelectionKey skey]
  (let [{:keys [^ByteBuffer write-bf]} @(.attachment skey)
        ^SocketChannel ch              (.channel skey)]
    (.write ch write-bf)
    (when-not (.hasRemaining write-bf)
      (.interestOpsAnd skey (bit-not SelectionKey/OP_WRITE)))))

(defn- pull-user
  [sys-conn username]
  (try
    (d/pull (d/db sys-conn) '[*] [:user/name username])
    (catch Exception _
      nil)))

(defn- authenticate
  [{:keys [username password]}]
  (let [conn (@resources :sys-conn)]
    (when-let [{:keys [user/id user/pw-salt user/pw-hash]}
               (pull-user conn username)]
      (when (password-matches? password pw-hash pw-salt)
        (let [client-id (UUID/randomUUID)]
          (swap! resources assoc-in [:clients client-id :user/id] id)
          client-id)))))

(defn- error-response
  [skey error-msg]
  (write-message skey {:type :error-response :message error-msg}))

(defn- close-port
  []
  (try
    (.close ^ServerSocketChannel (@resources :server-socket))
    (swap! resources dissoc :server-socket)
    (catch Exception e
      (u/raise "Error closing server socket:" (ex-message e) {}))))

(defn- open-port
  [port]
  (try
    (doto (ServerSocketChannel/open)
      (.bind (InetSocketAddress. port))
      (.configureBlocking false))
    (catch Exception e
      (u/raise "Error opening port:" (ex-message e) {}))))

(defn- handle-accept
  [^SelectionKey skey]
  (when-let [client-socket (.accept ^ServerSocketChannel (.channel skey))]
    (doto ^SocketChannel client-socket
      (.configureBlocking false)
      (.register (.selector skey) SelectionKey/OP_READ
                 ;; attach a connection state atom
                 ;; { read-bf, write-bf, client-id }
                 (atom {:read-bf  (ByteBuffer/allocateDirect
                                    c/+default-buffer-size+)
                        :write-bf (ByteBuffer/allocateDirect
                                    c/+default-buffer-size+)})))))

(defn- db-dir
  "translate from user and db-name to server db path"
  [user-id db-name]
  (str (@resources :root) u/+separator+
       user-id u/+separator+
       (b/hexify-string db-name)))

(defmacro wrap-error
  [body]
  `(try
     ~body
     (catch Exception ~'e
       (write-message 'skey {:type    :error-response
                             :message (ex-message ~'e)}))))

;; BEGIN message handlers

(defn- authentication
  [skey message]
  (if-let [client-id (authenticate message)]
    (write-message skey {:type :authentication-ok :client-id client-id})
    (error-response skey "Failed to authenticate")))

(defn- set-client-id
  [^SelectionKey skey message]
  (swap! (.attachment skey) assoc :client-id (message :client-id))
  (write-message skey {:type :set-client-id-ok}))

(defn open
  [^SelectionKey skey {:keys [db-name schema]}]
  (wrap-error
    (let [{:keys [client-id]} @(.attachment skey)
          {:keys [user/id]}   (get-in @resources [:clients client-id])
          dir                 (db-dir id db-name)
          store               (st/open dir schema)]
      (swap! resources assoc-in [:clients client-id :dt-store] store)
      (d/transact! (@resources :sys-conn)
                   [{:database/owner [:user/id id]
                     :database/type  :datalog
                     :database/name  db-name}])
      (write-message skey {:type :command-complete}))))

(defn open-kv
  [^SelectionKey skey {:keys [db-name]}]
  (wrap-error
    (let [{:keys [client-id]} @(.attachment skey)
          {:keys [user/id]}   (get-in @resources [:clients client-id])
          dir                 (db-dir id db-name)
          db                  (l/open-kv dir)]
      (swap! resources assoc-in [:clients client-id :kv-store] db)
      (d/transact! (@resources :sys-conn)
                   [{:database/owner [:user/id id]
                     :database/type  :key-value
                     :database/name  db-name}])
      (write-message skey {:type :command-complete}))))

(defn schema
  [^SelectionKey skey _]
  (wrap-error
    (let [{:keys [client-id]} @(.attachment skey)
          {:keys [dt-store]}  (get-in @resources [:clients client-id])]
      (write-message skey {:type   :command-complete
                           :schema (st/schema dt-store)}))))

;; END message handlers

(def message-handlers
  ['authentication
   'set-client-id
   'open
   'open-kv
   'schema])

(defmacro message-cases
  "Message handler function should have the same name as the incoming message
  type, e.g. '(authentication skey message) for :authentication message type"
  [skey type]
  `(case ~type
     ~@(mapcat
         (fn [sym] [(keyword sym) (list sym 'skey 'message)])
         message-handlers)
     (error-response ~skey (str "Unknown message type " ~type))))

(defn- handle-message
  [^SelectionKey skey fmt msg ]
  (let [{:keys [type] :as message} (p/read-value fmt msg)]
    (log/debug "message received:" message)
    (message-cases skey type)))

(defn- execute
  "Execute a function in a thread from the worker thread pool"
  [f]
  (.execute ^Executor (@resources :work-executor) f))

(defn- handle-read
  [^SelectionKey skey]
  (let [{:keys [^ByteBuffer read-bf]} @(.attachment skey)
        ^SocketChannel ch             (.channel skey)
        readn                         (.read ch read-bf)]
    (cond
      (= readn 0)  :continue
      (> readn 0)  (p/segment-messages
                     read-bf
                     (fn [fmt msg] (execute #(handle-message skey fmt msg))))
      (= readn -1) (.close ch))))

(defn- init-sys-db
  [root]
  (let [sys-conn (d/get-conn (str root u/+separator+ c/system-dir)
                             c/system-schema)]
    (swap! resources assoc :root root :sys-conn sys-conn)
    (when (= 0 (st/datom-count (.-store ^DB (d/db sys-conn)) c/eav))
      (let [s   (salt)
            h   (password-hashing c/default-password s)
            txs [{:db/id        -1
                  :user/name    c/default-username
                  :user/id      0
                  :user/pw-hash h
                  :user/pw-salt s}
                 {:db/id    -2
                  :role/key c/superuser-role}
                 {:user-role/user -1
                  :user-role/role -2}]]
        (d/transact! sys-conn txs)))))

(defn- init-work-executor
  []
  (let [exec (Executors/newWorkStealingPool)]
    (swap! resources assoc :work-executor exec)
    exec))

(defn- close-conn
  "Release resources related to the connection"
  [^SelectionKey skey]
  (let [{:keys [client-id]}         @(.attachment skey)
        {:keys [dt-store kv-store]} (get-in @resources [:clients client-id])
        ^SocketChannel ch           (.channel skey)]
    (when dt-store (st/close dt-store))
    (when kv-store (l/close-kv kv-store))
    (.close ch)))

(defn- shutdown
  []
  (let [{:keys [^Selector selector work-executor sys-conn]} @resources]
    (when selector
      (doseq [skey (.keys selector)] (close-conn skey))
      (when (.isOpen selector)(.close selector)))
    (.shutdown ^ThreadPoolExecutor work-executor)
    (d/close sys-conn))
  (log/info "Bye."))

(defn- open-selector []
  (let [selector (Selector/open)]
    (swap! resources assoc :selector selector)
    selector))

(defn start
  [{:keys [port root]}]
  (try
    (let [server-socket ^ServerSocketChannel (open-port port)
          selector      ^Selector (open-selector)]
      (init-work-executor)
      (init-sys-db root)
      (.register server-socket selector SelectionKey/OP_ACCEPT)
      (log/info "Datalevin server started on port" port)
      (loop []
        (.select selector)
        (loop [^Iterator iter (-> selector (.selectedKeys) (.iterator))]
          (when (.hasNext iter)
            (let [^SelectionKey skey (.next iter)]
              (when (and (.isValid skey) (.isAcceptable skey))
                (handle-accept skey))
              (when (and (.isValid skey) (.isReadable skey))
                (handle-read skey))
              (when (and (.isValid skey) (.isWritable skey))
                (handle-write skey)))
            (.remove iter)
            (recur iter)))
        (when (.isOpen selector) (recur))))
    (catch Exception e
      (u/raise "Error running server:" (ex-message e) {}))
    (finally (shutdown))))

(comment


  )
