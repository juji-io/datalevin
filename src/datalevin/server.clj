(ns datalevin.server
  "Non-blocking event-driven database server"
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
           [java.util.concurrent Executors Executor ThreadPoolExecutor
            ConcurrentLinkedQueue]
           [datalevin.db DB]
           [org.bouncycastle.crypto.generators Argon2BytesGenerator]
           [org.bouncycastle.crypto.params Argon2Parameters
            Argon2Parameters$Builder]))

(log/refer-timbre)
(log/set-level! :debug)

;; global server resources:
;; {root, selector, register-queue, work-executor, sys-conn, clients}
(defonce resources
  (atom {:clients        {} ; client-id -> { user/id, dt-store, kv-store }
         :register-queue (ConcurrentLinkedQueue.)}))

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
  [{:keys [username password]}]
  (let [conn (@resources :sys-conn)]
    (when-let [{:keys [user/id user/pw-salt user/pw-hash]}
               (pull-user conn username)]
      (when (password-matches? password pw-hash pw-salt)
        (let [client-id (UUID/randomUUID)]
          (swap! resources assoc-in [:clients client-id :user/id] id)
          client-id)))))

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

(defn- error-response
  [skey error-msg]
  (write-message skey {:type :error-response :message error-msg}))

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
       (log/error ~'e)
       (write-message ~'skey {:type    :error-response
                              :message (ex-message ~'e)}))))

(defmacro normal-dt-store-handler
  "Handle quick request to datalog store that needs no copy-in or out"
  [f]
  `(wrap-error
     (let [dt-store# (get-in @resources [:clients
                                         (@(.attachment ~'skey) :client-id)
                                         :dt-store])]
       (write-message ~'skey
                      {:type   :command-complete
                       :result (apply
                                 ~(symbol "datalevin.storage" (str f))
                                 dt-store#
                                 ~'args)}))))

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

(defn- open
  [^SelectionKey skey {:keys [db-name schema]}]
  (wrap-error
    (let [{:keys [client-id]}        @(.attachment skey)
          {:keys [user/id dt-store]} (get-in @resources [:clients client-id])
          dir                        (db-dir id db-name)]
      (when-not (and dt-store (= dir (st/dir dt-store)))
        (swap! resources assoc-in [:clients client-id :dt-store]
               (st/open dir schema))
        (d/transact! (@resources :sys-conn)
                     [{:database/owner [:user/id id]
                       :database/type  :datalog
                       :database/name  db-name}]))
      (write-message skey {:type :command-complete}))))

(defn- close
  [^SelectionKey skey {:keys [args]}]
  (normal-dt-store-handler close))

(defn- closed?
  [^SelectionKey skey {:keys [args]}]
  (normal-dt-store-handler closed?))

(defn- last-modified
  [^SelectionKey skey {:keys [args]}]
  (normal-dt-store-handler last-modified))

(defn- schema
  [^SelectionKey skey {:keys [args]}]
  (normal-dt-store-handler schema)
  )

(defn- rschema
  [^SelectionKey skey {:keys [args]}]
  (normal-dt-store-handler rschema))

(defn- set-schema
  [^SelectionKey skey {:keys [args]}]
  (normal-dt-store-handler set-schema))

(defn- init-max-eid
  [^SelectionKey skey {:keys [args]}]
  (normal-dt-store-handler init-max-eid))

(defn- datom-count
  [^SelectionKey skey {:keys [args]}]
  (normal-dt-store-handler datom-count))

(defn- load-datoms
  [^SelectionKey skey _]
  (wrap-error
    (let [state                                (.attachment skey)
          ^Selector selector                   (.selector skey)
          ^SocketChannel ch                    (.channel skey)
          {:keys [client-id read-bf write-bf]} @state
          {:keys [dt-store]}                   (get-in @resources
                                                       [:clients client-id])
          datoms                               (transient [])]
      ;; switch this channel to blocking mode
      (.cancel skey)
      (.configureBlocking ch true)
      (try
        (p/write-message-blocking ch write-bf {:type :copy-in-response})
        (.clear ^ByteBuffer read-bf)
        (loop []
          (let [msg (p/receive-ch ch read-bf)]
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
                  (recur)))))
        (p/write-message-blocking ch write-bf {:type :command-complete})
        (catch Exception e (throw e))
        (finally
          ;; switch back
          (.configureBlocking ch false)
          (.add ^ConcurrentLinkedQueue (@resources :register-queue)
                [ch SelectionKey/OP_READ state])
          (.wakeup selector))))))

(defn- fetch
  [^SelectionKey skey {:keys [args]}]
  (normal-dt-store-handler fetch))

(defn- populated?
  [^SelectionKey skey {:keys [args]}]
  (normal-dt-store-handler populated?))

(defn- size
  [^SelectionKey skey {:keys [args]}]
  (normal-dt-store-handler size))

(defn- head
  [^SelectionKey skey {:keys [args]}]
  (normal-dt-store-handler head))

(defn- slice
  [^SelectionKey skey {:keys [args]}]
  )

(defn- rslice
  [^SelectionKey skey {:keys [args]}]
  )

(defn- size-filter
  [^SelectionKey skey {:keys [args]}]
  )

(defn- head-filter
  [^SelectionKey skey {:keys [args]}]
  )

(defn- slice-filter
  [^SelectionKey skey {:keys [args]}]
  )

(defn- rslice-filter
  [^SelectionKey skey {:keys [args]}]
  )

(defn- open-kv
  [^SelectionKey skey {:keys [db-name]}]
  (wrap-error
    (let [{:keys [client-id]}        @(.attachment skey)
          {:keys [user/id kv-store]} (get-in @resources [:clients client-id])
          dir                        (db-dir id db-name)]
      (when-not (and kv-store (= dir (l/dir kv-store)))
        (swap! resources assoc-in [:clients client-id :kv-store]
               (l/open-kv dir))
        (d/transact! (@resources :sys-conn)
                     [{:database/owner [:user/id id]
                       :database/type  :key-value
                       :database/name  db-name}]))
      (write-message skey {:type :command-complete}))))

;; END message handlers

(def message-handlers
  ['authentication
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
         (fn [sym] [(keyword sym) (list sym 'skey 'message)])
         message-handlers)
     (error-response ~skey (str "Unknown message type " ~type))))

(defn- handle-message
  [^SelectionKey skey fmt msg ]
  (let [{:keys [type] :as message} (p/read-value fmt msg)]
    (log/debug "message received:" (dissoc message :password))
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

(defn- handle-registration
  [^Selector selector]
  (let [^ConcurrentLinkedQueue queue (@resources :register-queue)]
    (loop []
      (when-let [[^SocketChannel ch ops state] (.poll queue)]
        (.register ch selector ops state)
        (log/debug "Registered client" (@state :client-id))
        (recur)))))

(defn- event-loop
  [^Selector selector]
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
    (handle-registration selector)
    (when (.isOpen selector) (recur))))

(defn start
  [{:keys [port root]}]
  (try
    (let [server-socket ^ServerSocketChannel (open-port port)
          selector      ^Selector (open-selector)]
      (init-work-executor)
      (init-sys-db root)
      (.register server-socket selector SelectionKey/OP_ACCEPT)
      (log/info "Datalevin server started on port" port)
      (event-loop selector))
    (catch Exception e
      (u/raise "Error running server:" (ex-message e) {}))
    (finally (shutdown))))

(comment


  )
