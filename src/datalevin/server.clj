(ns datalevin.server
  "Non-blocking event-driven server"
  (:require [datalevin.util :as u]
            [datalevin.core :as d]
            [datalevin.bits :as b]
            [datalevin.lmdb :as l]
            [datalevin.protocol :as p]
            [datalevin.storage :as st]
            [datalevin.constants :as c]
            [taoensso.nippy :as nippy]
            [taoensso.timbre :as log])
  (:import [java.nio.charset StandardCharsets]
           [java.nio ByteBuffer BufferOverflowException]
           [java.nio.channels Selector SelectionKey ServerSocketChannel
            SocketChannel]
           [java.net InetSocketAddress]
           [java.security SecureRandom]
           [java.util Iterator UUID]
           [java.util.concurrent.atomic AtomicBoolean]
           [java.util.concurrent Executors Executor ThreadPoolExecutor
            ConcurrentLinkedQueue]
           [datalevin.db DB]
           [org.bouncycastle.crypto.generators Argon2BytesGenerator]
           [org.bouncycastle.crypto.params Argon2Parameters
            Argon2Parameters$Builder]))

(log/refer-timbre)
(log/set-level! :debug)

(defprotocol IServer
  (start [srv] "Start the server")
  (stop [srv] "Stop the server")
  (get-client [srv client-id] "access client info")
  (add-client [srv client-id user-id] "add an client")
  (remove-client [srv client-id] "remove an client")
  (update-client [srv client-id f] "Update info about a client"))

(defn- close-conn
  "Free resources related to a connection"
  [^SelectionKey skey clients]
  (let [{:keys [client-id]}         @(.attachment skey)
        {:keys [dt-store kv-store]} (clients client-id)
        ^SocketChannel ch           (.channel skey)]
    (when dt-store (st/close dt-store))
    (when kv-store (l/close-kv kv-store))
    (.close ch)))

(declare event-loop)

(deftype Server [^AtomicBoolean running
                 ^int port
                 ^String root
                 ^Selector selector
                 ^ConcurrentLinkedQueue register-queue
                 ^ThreadPoolExecutor work-executor
                 sys-conn
                 ;; client-id -> { user/id, dt-store, kv-store }
                 ^:volatile-mutable clients]
  IServer
  (start [server]
    (.set running true)
    (.start (Thread.
              (fn []
                (log/info "Datalevin server started on port" port)
                (event-loop server)))))

  (stop [server]
    (.set running false)
    (doseq [skey (.keys selector)] (close-conn skey clients))
    (when (.isOpen selector) (.close selector))
    (.shutdown work-executor)
    (d/close sys-conn)
    (log/info "Datalevin server shuts down."))

  (get-client [server client-id]
    (clients client-id))

  (add-client [server client-id user-id]
    (set! clients (assoc clients client-id {:user/id user-id})))

  (remove-client [server client-id]
    (set! clients (dissoc clients client-id)))

  (update-client [server client-id f]
    (set! clients (update clients client-id f))))

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

(defn- pull-user
  [sys-conn username]
  (try
    (d/pull (d/db sys-conn) '[*] [:user/name username])
    (catch Exception _
      nil)))

(defn- authenticate
  [^Server server {:keys [username password]}]
  (when-let [{:keys [user/id user/pw-salt user/pw-hash]}
             (pull-user (.-sys-conn server) username)]
    (when (password-matches? password pw-hash pw-salt)
      (let [client-id (UUID/randomUUID)]
        (add-client server client-id id)
        client-id))))

(defn- write-to-bf
  "write a message to write buffer, auto grow the buffer"
  [^SelectionKey skey msg]
  (let [state                          (.attachment skey)
        {:keys [^ByteBuffer write-bf]} @state]
    (.clear write-bf)
    (try
      (p/write-message-bf write-bf msg)
      (catch BufferOverflowException _
        (let [size (* c/+buffer-grow-factor+ ^int (.capacity write-bf))]
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

(defn- copy-out
  [^SelectionKey skey data batch-size]
  (let [state                             (.attachment skey)
        {:keys [^ByteBuffer write-bf]}    @state
        ^SocketChannel                 ch (.channel skey)]
    (p/write-message-blocking ch write-bf {:type :copy-out-response})
    (doseq [batch (partition batch-size batch-size nil data)]
      (write-to-bf skey batch)
      (let [{:keys [^ByteBuffer write-bf]} @state] ; may have grown
        (.flip write-bf)
        (p/send-ch ch write-bf)))
    (p/write-message-blocking ch write-bf {:type :copy-done})))

(defn- db-dir
  "translate from user and db-name to server db path"
  [^Server server user-id db-name]
  (str (.-root server) u/+separator+
       user-id u/+separator+
       (b/hexify-string db-name)))

(defn- error-response
  [skey error-msg]
  (write-message skey {:type :error-response :message error-msg}))

(defmacro wrap-error
  [body]
  `(try
     ~body
     (catch Exception ~'e
       (log/error ~'e)
       (error-response ~'skey (ex-message ~'e)))))

(defmacro normal-dt-store-handler
  "Handle request to Datalog store that needs no copy-in or copy-out"
  [f]
  `(write-message
     ~'skey
     {:type   :command-complete
      :result (apply
                ~(symbol "datalevin.storage" (str f))
                (:dt-store
                 (get-client ~'server (@(.attachment ~'skey) :client-id)))
                ~'args)}))

;; BEGIN message handlers

(defn- authentication
  [^Server server skey message]
  (if-let [client-id (authenticate server message)]
    (write-message skey {:type :authentication-ok :client-id client-id})
    (error-response skey "Failed to authenticate")))

(defn- disconnect
  [^Server server ^SelectionKey skey _]
  (let [{:keys [client-id]}         @(.attachment skey)
        {:keys [kv-store dt-store]} (get-client server client-id)]
    (when dt-store (st/close dt-store))
    (when kv-store (l/close-kv kv-store))
    (remove-client server client-id)))

(defn- set-client-id
  [^Server server ^SelectionKey skey message]
  (swap! (.attachment skey) assoc :client-id (message :client-id))
  (write-message skey {:type :set-client-id-ok}))

(defn- open
  [^Server server ^SelectionKey skey {:keys [db-name schema]}]
  (wrap-error
    (let [{:keys [client-id]}        @(.attachment skey)
          {:keys [user/id dt-store]} (get-client server client-id)
          dir                        (db-dir server id db-name)]
      (when-not (and dt-store (= dir (st/dir dt-store)))
        (update-client server client-id
                       #(assoc % :dt-store (st/open dir schema)))
        (d/transact! (.-sys-conn server)
                     [{:database/owner [:user/id id]
                       :database/type  :datalog
                       :database/name  db-name}]))
      (write-message skey {:type :command-complete}))))

(defn- close
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error (normal-dt-store-handler close)))

(defn- closed?
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error (normal-dt-store-handler closed?)))

(defn- last-modified
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error (normal-dt-store-handler last-modified)))

(defn- schema
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error (normal-dt-store-handler schema)))

(defn- rschema
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error (normal-dt-store-handler rschema)))

(defn- set-schema
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error (normal-dt-store-handler set-schema)))

(defn- init-max-eid
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error (normal-dt-store-handler init-max-eid)))

(defn- datom-count
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error (normal-dt-store-handler datom-count)))

(defn- load-datoms
  [^Server server ^SelectionKey skey _]
  (wrap-error
    (let [state                                (.attachment skey)
          ^Selector selector                   (.selector skey)
          ^SocketChannel ch                    (.channel skey)
          {:keys [client-id read-bf write-bf]} @state
          {:keys [dt-store]}                   (get-client server client-id)
          datoms                               (transient [])]
      ;; switch this channel to blocking mode for copy-in
      (.cancel skey)
      (.configureBlocking ch true)
      (try
        (p/write-message-blocking ch write-bf {:type :copy-in-response})
        (.clear ^ByteBuffer read-bf)
        (loop [bf read-bf]
          (let [[msg bf'] (p/receive-ch ch bf)]
            (when-not (identical? bf bf') (swap! state assoc :read-bf bf'))
            (if (map? msg)
              (let [{:keys [type]} msg]
                (case type
                  :copy-done (let [txs (persistent! datoms)]
                               (st/load-datoms dt-store txs)
                               (log/debug "Loaded" (count txs) "datoms"))
                  :copy-fail (u/raise "Client error while loading datoms" {})
                  (u/raise "Receive unexpected message while loading datoms"
                           {:msg msg})))
              (do (doseq [datom msg] (conj! datoms datom))
                  (recur bf')))))
        (p/write-message-blocking ch write-bf {:type :command-complete})
        (catch Exception e (throw e))
        (finally
          ;; switch back
          (.configureBlocking ch false)
          (.add ^ConcurrentLinkedQueue (.-register-queue server)
                [ch SelectionKey/OP_READ state])
          (.wakeup selector))))))

(defn- fetch
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error (normal-dt-store-handler fetch)))

(defn- populated?
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error (normal-dt-store-handler populated?)))

(defn- size
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error (normal-dt-store-handler size)))

(defn- head
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error (normal-dt-store-handler head)))

(defn- slice
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (copy-out skey (apply st/slice args) c/+wire-datom-batch-size+)))

(defn- rslice
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (copy-out skey (apply st/rslice args) c/+wire-datom-batch-size+)))

(defn- size-filter
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [[index frozen-pred low-datom high-datom] args

          pred (nippy/thaw frozen-pred)
          args [index pred low-datom high-datom]]
      (normal-dt-store-handler size-filter))))

(defn- head-filter
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [[index frozen-pred low-datom high-datom] args

          pred (nippy/thaw frozen-pred)
          args [index pred low-datom high-datom]]
      (normal-dt-store-handler head-filter))))

(defn- slice-filter
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [[index frozen-pred low-datom high-datom] args

          pred (nippy/thaw frozen-pred)
          args [index pred low-datom high-datom]]
      (copy-out skey (apply st/slice-filter args) c/+wire-datom-batch-size+))))

(defn- rslice-filter
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [[index frozen-pred high-datom low-datom] args

          pred (nippy/thaw frozen-pred)
          args [index pred high-datom low-datom]]
      (copy-out skey (apply st/rslice-filter args) c/+wire-datom-batch-size+))))

(defn- open-kv
  [^Server server ^SelectionKey skey {:keys [db-name]}]
  (wrap-error
    (let [{:keys [client-id]}        @(.attachment skey)
          {:keys [user/id kv-store]} (get-client server client-id)
          dir                        (db-dir id db-name)]
      (when-not (and kv-store (= dir (l/dir kv-store)))
        (update-client server client-id #(assoc % :kv-store (l/open-kv dir)))
        (d/transact! (.-sys-conn server)
                     [{:database/owner [:user/id id]
                       :database/type  :key-value
                       :database/name  db-name}]))
      (write-message skey {:type :command-complete}))))

;; END message handlers

(def message-handlers
  ['authentication
   'disconnect
   'set-client-id
   'open
   'close
   'closed?
   'last-modified
   'schema
   'rschema
   'set-schema
   'init-max-eid
   'datom-count
   'load-datoms
   'fetch
   'populated?
   'size
   'head
   'slice
   'rslice
   'size-filter
   'head-filter
   'slice-filter
   'rslice-filter
   'open-kv
   ])

(defmacro message-cases
  "Message handler function should have the same name as the incoming message
  type, e.g. '(authentication skey message) for :authentication message type"
  [skey type]
  `(case ~type
     ~@(mapcat
         (fn [sym]
           [(keyword sym) (list sym 'server 'skey 'message)])
         message-handlers)
     (error-response ~skey (str "Unknown message type " ~type))))

(defn- handle-message
  [^Server server ^SelectionKey skey fmt msg ]
  (let [{:keys [type] :as message} (p/read-value fmt msg)]
    (log/debug "message received:" (dissoc message :password))
    (message-cases skey type)))

(defn- execute
  "Execute a function in a thread from the worker thread pool"
  [^Server server f]
  (.execute ^Executor (.-work-executor server) f))

(defn segment-messages
  "Segment the content of read buffer into messages, and call msg-handler
  on each. The messages are byte arrays. Message parsing will be done in the
  msg-handler. In non-blocking mode, each should be handled by a worker thread,
  so the main event loop is not hindered by slow parsing. Assume each message is
  small enough for the buffer, as big messages are handled by copy-in/out."
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

(defn- handle-read
  [^Server server ^SelectionKey skey]
  (let [{:keys [^ByteBuffer read-bf]} @(.attachment skey)
        ^SocketChannel ch             (.channel skey)
        readn                         (.read ch read-bf)]
    (cond
      (= readn 0)  :continue
      (> readn 0)  (segment-messages
                     read-bf
                     (fn [fmt msg]
                       (execute server
                                #(handle-message server skey fmt msg))))
      (= readn -1) (.close ch))))

(defn- handle-registration
  [^Server server]
  (let [^Selector selector           (.-selector server)
        ^ConcurrentLinkedQueue queue (.-register-queue server)]
    (loop []
      (when-let [[^SocketChannel ch ops state] (.poll queue)]
        (.register ch selector ops state)
        (log/debug "Registered client" (@state :client-id))
        (recur)))))

(defn- event-loop
  [^Server server ]
  (let [^Selector selector     (.-selector server)
        ^AtomicBoolean running (.-running server)]
    (loop []
      (when (.get running)
        (.select selector)
        (loop [^Iterator iter (-> selector (.selectedKeys) (.iterator))]
          (when (.hasNext iter)
            (let [^SelectionKey skey (.next iter)]
              (when (and (.isValid skey) (.isAcceptable skey))
                (handle-accept skey))
              (when (and (.isValid skey) (.isReadable skey))
                (handle-read server skey))
              (when (and (.isValid skey) (.isWritable skey))
                (handle-write skey)))
            (.remove iter)
            (recur iter)))
        (handle-registration server)
        (recur)))))

(defn- init-sys-db
  [root]
  (let [sys-conn (d/get-conn (str root u/+separator+ c/system-dir)
                             c/system-schema)]
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
        (d/transact! sys-conn txs)))
    sys-conn))

(defn- open-port
  [port]
  (try
    (doto (ServerSocketChannel/open)
      (.bind (InetSocketAddress. port))
      (.configureBlocking false))
    (catch Exception e
      (u/raise "Error opening port:" (ex-message e) {}))))

(defn create
  "Create a Datalevin server"
  [{:keys [port root]}]
  (try
    (let [server-socket ^ServerSocketChannel (open-port port)
          selector      ^Selector (Selector/open)
          running       (AtomicBoolean. false)]
      (.register server-socket selector SelectionKey/OP_ACCEPT)
      (->Server running
                port
                root
                selector
                (ConcurrentLinkedQueue.)
                (Executors/newWorkStealingPool)
                (init-sys-db root)
                {}))
    (catch Exception e
      (u/raise "Error creating server:" (ex-message e) {}))))


(comment


  )
