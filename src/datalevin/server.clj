(ns datalevin.server
  "Non-blocking event-driven server"
  (:require [datalevin.util :as u]
            [datalevin.core :as d]
            [datalevin.bits :as b]
            [datalevin.query :as q]
            [datalevin.db :as db]
            [datalevin.lmdb :as l]
            [datalevin.protocol :as p]
            [datalevin.storage :as st]
            [datalevin.constants :as c]
            [taoensso.nippy :as nippy]
            [taoensso.timbre :as log]
            [clojure.string :as s])
  (:import [java.nio.charset StandardCharsets]
           [java.nio ByteBuffer BufferOverflowException]
           [java.nio.file Files Paths]
           [java.nio.channels Selector SelectionKey ServerSocketChannel
            SocketChannel]
           [java.net InetSocketAddress]
           [java.security SecureRandom]
           [java.util Iterator UUID]
           [java.util.concurrent.atomic AtomicBoolean]
           [java.util.concurrent Executors Executor ExecutorService
            ConcurrentLinkedQueue]
           [datalevin.db DB]
           [datalevin.datom Datom]
           [org.bouncycastle.crypto.generators Argon2BytesGenerator]
           [org.bouncycastle.crypto.params Argon2Parameters
            Argon2Parameters$Builder]))

(log/refer-timbre)
(log/set-level! :debug)

;; permission acts
(derive ::alter ::view)
(derive ::create ::alter)
(derive ::control ::create)

;; permission objects
(derive ::server ::database)
(derive ::server ::user)
(derive ::server ::role)

(defn- user-permissions
  [sys-conn username]
  (map first
       (d/q '[:find (pull ?p [:permission/act :permission/obj
                              {:permission/db [:database/name]}])
              :in $ ?uname
              :where
              [?u :user/name ?uname]
              [?ur :user-role/user ?u]
              [?ur :user-role/role ?r]
              [?rp :role-perm/role ?r]
              [?rp :role-perm/perm ?p]]
            @sys-conn username)))

(defprotocol IServer
  (start [srv] "Start the server")
  (stop [srv] "Stop the server")
  (get-client [srv client-id] "access client info")
  (add-client [srv client-id username] "add an client")
  (remove-client [srv client-id] "remove an client")
  (update-client [srv client-id f] "Update info about a client")
  (get-store [srv dir] "access an open store")
  (add-store [srv dir store] "add a store")
  (remove-store [srv dir] "remove a store"))

(defn- close-conn
  [^SelectionKey skey]
  (.close ^SocketChannel (.channel skey)))

(declare event-loop)

(deftype Server [^AtomicBoolean running
                 ^int port
                 ^String root
                 ^ServerSocketChannel server-socket
                 ^Selector selector
                 ^ConcurrentLinkedQueue register-queue
                 ^ExecutorService work-executor
                 sys-conn
                 ;; client-id -> { username, permissions,
                 ;;                dt-store, dt-db, kv-store }
                 ^:volatile-mutable clients
                 ;; dir -> store
                 ^:volatile-mutable stores]
  IServer
  (start [server]
    (.set running true)
    (.start (Thread.
              (fn []
                (log/info "Datalevin server started on port" port)
                (event-loop server)))))

  (stop [server]
    (.set running false)
    (.wakeup selector)
    (doseq [skey (.keys selector)] (close-conn skey))
    (.close server-socket)
    (when (.isOpen selector) (.close selector))
    (.shutdown work-executor)
    (doseq [dir (keys stores)] (remove-store server dir))
    (d/close sys-conn)
    (log/info "Datalevin server shuts down."))

  (get-client [_ client-id]
    (clients client-id))

  (add-client [_ client-id username]
    (let [perms (user-permissions sys-conn username)]
      (log/debug "Added client for user" username
                 "with permissions:" (pr-str perms))
      (set! clients (assoc clients client-id
                           {:username username :permissions perms}))))

  (remove-client [_ client-id]
    (set! clients (dissoc clients client-id)))

  (update-client [_ client-id f]
    (set! clients (update clients client-id f)))

  (get-store [_ dir]
    (when-let [store (stores dir)]
      (cond
        (instance? datalevin.storage.IStore store)
        (when-not (st/closed? store) store)
        (instance? datalevin.lmdb.ILMDB store)
        (when-not (l/closed-kv? store) store)
        :else (u/raise "Unknown store" {:dir dir}))))

  (add-store [_ dir store]
    (set! stores (assoc stores dir store)))

  (remove-store [server dir]
    (when-let [store (get-store server dir)]
      (cond
        (instance? datalevin.storage.IStore store) (st/close store)
        (instance? datalevin.lmdb.ILMDB store)     (l/close-kv store)
        :else
        (u/raise "Unknown store" {:dir dir})))
    (set! stores (dissoc stores dir))))

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

;; db management

(defn- pull-user
  [sys-conn username]
  (try
    (d/pull (d/db sys-conn) '[*] [:user/name username])
    (catch Exception _
      nil)))

(defn- pull-role
  [sys-conn role-key]
  (try
    (d/pull (d/db sys-conn) '[*] [:role/key role-key])
    (catch Exception _
      nil)))

;; each user is a role, similar to postgres
(defn- user-role-key [username] (keyword "datalevin.role" username))

(defn- transact-new-user
  [sys-conn username password]
  (let [s (salt)
        h (password-hashing password s)]
    (d/transact! sys-conn [{:db/id        -1
                            :user/name    username
                            :user/pw-hash h
                            :user/pw-salt s}
                           {:db/id    -2
                            :role/key (user-role-key username)}
                           {:db/id          -3
                            :user-role/user -1
                            :user-role/role -2}])))

(defn- transact-new-role
  [sys-conn role-key]
  (d/transact! sys-conn [{:role/key role-key}]))

(defn- transact-user-role
  [sys-conn role-key username]
  (d/transact! sys-conn [{:user-role/user [:user/name username]
                          :user-role/role [:role/key role-key]}]))

(defn- permission-eid
  [sys-conn perm-act perm-obj perm-db]
  (d/q '[:find ?p .
         :in $ ?act ?obj ?db
         :where
         [?p :permission/act ?act]
         [?p :permission/obj ?obj]
         [?p :permission/db [:database/name ?db]]]
       @sys-conn perm-act perm-obj perm-db))

(defn- transact-role-permission
  [sys-conn role-key perm-act perm-obj perm-db]
  (let [perm-eid (permission-eid sys-conn perm-act perm-obj perm-db)]
    (d/transact! sys-conn [{:role-perm/perm perm-eid
                            :role-perm/role [:role/key role-key]}])))

#_(defn- permit-db?
    [sys-conn username req-act req-db-name db-eid]
    (d/q '[:find ?eid .
           :in $ ?uid ?act ?dname ?eid
           :where
           [?eid :database/name ?dname]
           [?u :user/name ?uid]
           [?ur :user-role/user ?u]
           [?ur :user-role/role ?r]
           [?rp :role-perm/role ?r]
           [?rp :role-perm/perm ?p]
           [?p :permission/db ?eid]
           ]
         @sys-conn username req-act req-db-name db-eid))

(defn- transact-new-db
  [sys-conn username db-type db-name]
  (d/transact! sys-conn
               [{:db/id         -1
                 :database/type db-type
                 :database/name db-name}
                {:db/id          -2
                 :permission/act ::create
                 :permission/obj ::database
                 :permission/db  -1}
                {:db/id          -3
                 :role-perm/perm -2
                 :role-perm/role [:role/key (user-role-key username)]}]))

(defn- authenticate
  [^Server server {:keys [username password]}]
  (when-let [{:keys [user/pw-salt user/pw-hash]}
             (pull-user (.-sys-conn server) username)]
    (when (password-matches? password pw-hash pw-salt)
      (let [client-id (UUID/randomUUID)]
        (add-client server client-id username)
        client-id))))

(defn- has-permission?
  [req-act req-obj req-db-name user-permissions]
  (some (fn [{:keys [permission/act permission/obj permission/db] :as p}]
          (log/debug p)
          (and (isa? act req-act)
               (isa? obj req-obj)
               (if req-db-name
                 (if db
                   (= req-db-name (db :database/name))
                   true)
                 true)))
        user-permissions))

(defmacro ^:no-doc wrap-permission
  [req-act req-obj req-db-name message & body]
  `(let [{:keys [~'client-id]}   @(~'.attachment ~'skey)
         {:keys [~'permissions]} (get-client ~'server ~'client-id)]
     (if (has-permission? ~req-act ~req-obj ~req-db-name ~'permissions)
       (do ~@body)
       (u/raise ~message {}))))

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

(defn- write-message
  "write a message to channel"
  [^SelectionKey skey msg]
  (write-to-bf skey msg)
  (let [{:keys [^ByteBuffer write-bf]}    @(.attachment skey)
        ^SocketChannel                 ch (.channel skey)]
    (.flip write-bf)
    (p/send-ch ch write-bf)))

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

(defn- copy-in
  "Continuously read batched data from the client"
  [^Server server ^SelectionKey skey]
  (let [state                      (.attachment skey)
        {:keys [read-bf write-bf]} @state
        ^Selector selector         (.selector skey)
        ^SocketChannel ch          (.channel skey)
        data                       (transient [])]
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
                :copy-done :break
                :copy-fail (u/raise "Client error while loading data" {})
                (u/raise "Receive unexpected message while loading data"
                         {:msg msg})))
            (do (doseq [d msg] (conj! data d))
                (recur bf')))))
      (let [txs (persistent! data)]
        (log/debug "Copied in" (count txs) "data items")
        txs)
      (catch Exception e (throw e))
      (finally
        ;; switch back
        (.configureBlocking ch false)
        (.add ^ConcurrentLinkedQueue (.-register-queue server)
              [ch SelectionKey/OP_READ state])
        (.wakeup selector)))))

(defn- copy-out
  "Continiously write data out to client in batches"
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
    (p/write-message-blocking ch write-bf {:type :copy-done})
    (log/debug "Copied out" (count data) "data items")))

(defn- db-dir
  "translate from db-name to server db path"
  [^Server server db-name]
  (str (.-root server) u/+separator+ (b/hexify-string db-name)))

(defn- db-exists?
  [^Server server db-name]
  (u/file-exists (str (db-dir server db-name) u/+separator+ "data.mdb")))

(defn- error-response
  [^SelectionKey skey error-msg]
  (let [{:keys [^ByteBuffer write-bf]}    @(.attachment skey)
        ^SocketChannel                 ch (.channel skey)]
    (p/write-message-blocking ch write-bf
                              {:type :error-response :message error-msg})))

(defmacro ^:no-doc wrap-error
  [& body]
  `(try
     ~@body
     (catch Exception ~'e
       (log/error ~'e)
       (error-response ~'skey (ex-message ~'e)))))

(defn- dt-store
  [^Server server ^SelectionKey skey]
  (:dt-store (get-client server (@(.attachment skey) :client-id))))

(defmacro ^:no-doc normal-dt-store-handler
  "Handle request to Datalog store that needs no copy-in or copy-out"
  [f]
  `(write-message
     ~'skey
     {:type   :command-complete
      :result (apply
                ~(symbol "datalevin.storage" (str f))
                (dt-store ~'server ~'skey)
                ~'args)}))

(defn- kv-store
  [^Server server ^SelectionKey skey]
  (:kv-store (get-client server (@(.attachment skey) :client-id))))

(defmacro ^:no-doc normal-kv-store-handler
  "Handle request to key-value store that needs no copy-in or copy-out"
  [f]
  `(write-message
     ~'skey
     {:type   :command-complete
      :result (apply
                ~(symbol "datalevin.lmdb" (str f))
                (kv-store ~'server ~'skey)
                ~'args)}))

;; BEGIN message handlers

(defn- authentication
  [^Server server skey message]
  (wrap-error
    (if-let [client-id (authenticate server message)]
      (write-message skey {:type :authentication-ok :client-id client-id})
      (u/raise "Failed to authenticate" {}))))

(defn- disconnect
  [^Server server ^SelectionKey skey _]
  (let [{:keys [client-id]} @(.attachment skey)
        selector            (.selector skey)]
    (when (.isOpen selector)
      (doseq [^SelectionKey k (.keys selector)
              :let            [state (.attachment k)]
              :when           state]
        (when (= client-id (@state :client-id))
          (close-conn k))))
    (remove-client server client-id)))

(defn- set-client-id
  [^Server server ^SelectionKey skey message]
  (swap! (.attachment skey) assoc :client-id (message :client-id))
  (write-message skey {:type :set-client-id-ok}))

(defn- create-user
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [sys-conn            (.-sys-conn server)
          [username password] args
          username            (u/lisp-case username)]
      (wrap-permission
        ::control ::server nil
        "Don't have permission to create user"
        (if (pull-user sys-conn username)
          (u/raise "User already exits" {:username username})
          (if (s/blank? password)
            (u/raise "Password is required when creating user" {})
            (do (transact-new-user sys-conn username password)
                (write-message skey {:type     :command-complete
                                     :username username}))))))))

(defn- create-role
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [sys-conn   (.-sys-conn server)
          [role-key] args]
      (wrap-permission
        ::control ::server nil
        "Don't have permission to create role"
        (if (pull-role sys-conn role-key)
          (u/raise "Role already exits" {:role-key role-key})
          (do (transact-new-role sys-conn role-key)
              (write-message skey {:type :command-complete})))))))

(defn- assign-role
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [sys-conn            (.-sys-conn server)
          [role-key username] args]
      (wrap-permission
        ::control ::server nil
        "Don't have permission to assign role"
        (do (transact-user-role sys-conn role-key username)
            (write-message skey {:type :command-complete}))))))

(defn- assign-permission
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [sys-conn                             (.-sys-conn server)
          [role-key perm-act perm-obj perm-db] args]
      (wrap-permission
        ::control ::server nil
        "Don't have permission to assign role"
        (do (transact-role-permission sys-conn role-key perm-act perm-obj perm-db)
            (write-message skey {:type :command-complete}))))))

(defn- open
  "Open a store. NB. stores are left open"
  [^Server server ^SelectionKey skey {:keys [db-name schema]}]
  (wrap-error
    (let [{:keys [client-id]}         @(.attachment skey)
          {:keys [username dt-store]} (get-client server client-id)
          existing-db?                (db-exists? server db-name)
          sys-conn                    (.-sys-conn server)]
      (wrap-permission
        (if existing-db? ::view ::create)
        ::database
        (when existing-db? db-name)
        "Don't have permission to open database"
        (let [dir   (db-dir server db-name)
              store (or (get-store server dir)
                        (st/open dir schema))]
          (add-store server dir store)
          (when-not (and dt-store (= dt-store store))
            (update-client server client-id
                           #(assoc % :dt-store store :dt-db (db/new-db store))))
          (when-not existing-db?
            (transact-new-db sys-conn username c/dt-type db-name)
            (update-client server client-id
                           #(assoc % :permissions
                                   (user-permissions sys-conn username))))
          (write-message skey {:type :command-complete}))))))

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

(defn- swap-attr
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [[attr frozen-f x y] args
          f                   (nippy/fast-thaw frozen-f)
          args                [attr f x y]]
      (normal-dt-store-handler swap-attr))))

(defn- datom-count
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error (normal-dt-store-handler datom-count)))

(defn- load-datoms
  [^Server server ^SelectionKey skey {:keys [mode args]}]
  (wrap-error
    (let [{:keys [client-id]} @(.attachment skey)
          {:keys [dt-store]}  (get-client server client-id)]
      (case mode
        :copy-in (do (st/load-datoms dt-store (copy-in server skey))
                     (write-message skey {:type :command-complete}))
        :request (normal-dt-store-handler load-datoms)
        (u/raise "Missing :mode when loading datoms" {})))))

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

(defn- tail
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error (normal-dt-store-handler tail)))

(defn- slice
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [datoms (apply st/slice (dt-store server skey) args)]
      (if (< (count datoms) c/+wire-datom-batch-size+)
        (write-message skey {:type :command-complete :result datoms})
        (copy-out skey datoms c/+wire-datom-batch-size+)))))

(defn- rslice
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [datoms (apply st/rslice (dt-store server skey) args)]
      (if (< (count datoms) c/+wire-datom-batch-size+)
        (write-message skey {:type :command-complete :result datoms})
        (copy-out skey datoms c/+wire-datom-batch-size+)))))

(defn- size-filter
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [[index frozen-pred low-datom high-datom] args

          pred (nippy/fast-thaw frozen-pred)
          args [index pred low-datom high-datom]]
      (normal-dt-store-handler size-filter))))

(defn- head-filter
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [[index frozen-pred low-datom high-datom] args

          pred (nippy/fast-thaw frozen-pred)
          args [index pred low-datom high-datom]]
      (normal-dt-store-handler head-filter))))

(defn- tail-filter
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [[index frozen-pred high-datom low-datom] args

          pred (nippy/fast-thaw frozen-pred)
          args [index pred high-datom low-datom]]
      (normal-dt-store-handler tail-filter))))

(defn- slice-filter
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [[index frozen-pred low-datom high-datom] args

          pred   (nippy/fast-thaw frozen-pred)
          args   [index pred low-datom high-datom]
          datoms (apply st/slice-filter (dt-store server skey) args)]
      (if (< (count datoms) c/+wire-datom-batch-size+)
        (write-message skey {:type :command-complete :result datoms})
        (copy-out skey datoms c/+wire-datom-batch-size+)))))

(defn- rslice-filter
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [[index frozen-pred high-datom low-datom] args

          pred   (nippy/fast-thaw frozen-pred)
          args   [index pred high-datom low-datom]
          datoms (apply st/rslice-filter (dt-store server skey) args)]
      (if (< (count datoms) c/+wire-datom-batch-size+)
        (write-message skey {:type :command-complete :result datoms})
        (copy-out skey datoms c/+wire-datom-batch-size+)))))

(defn- open-kv
  [^Server server ^SelectionKey skey {:keys [db-name]}]
  (wrap-error
    (let [{:keys [client-id]}         @(.attachment skey)
          {:keys [username kv-store]} (get-client server client-id)
          dir                         (db-dir server db-name)
          existing-db?                (db-exists? server db-name)
          store                       (or (get-store server dir)
                                          (l/open-kv dir))]
      (add-store server dir store)
      (when-not (and kv-store (= dir (l/dir kv-store)))
        (update-client server client-id #(assoc % :kv-store store)))
      (when-not existing-db?
        (transact-new-db (.-sys-conn server) username c/kv-type db-name))
      (write-message skey {:type :command-complete}))))

(defn- close-kv
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error (normal-kv-store-handler close-kv)))

(defn- closed-kv?
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error (normal-kv-store-handler closed-kv?)))

(defn- open-dbi
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (do (apply l/open-dbi (kv-store server skey) args)
        (write-message skey {:type :command-complete}))))

(defn- clear-dbi
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error (normal-kv-store-handler clear-dbi)))

(defn- drop-dbi
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error (normal-kv-store-handler drop-dbi)))

(defn- list-dbis
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error (normal-kv-store-handler list-dbis)))

;; TODO use LMDB copyfd to write to socket directly
;; However, LMDBJava does not wrap copyfd
(defn- copy
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [[compact?] args
          tf         (u/tmp-dir (str "copy-" (UUID/randomUUID)))
          path       (Paths/get (str tf u/+separator+ "data.mdb")
                                (into-array String []))]
      (l/copy (kv-store server skey) tf compact?)
      (copy-out skey
                (u/encode-base64 (Files/readAllBytes path))
                8192))))

(defn- stat
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error (normal-kv-store-handler stat)))

(defn- entries
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error (normal-kv-store-handler entries)))

(defn- transact-kv
  [^Server server ^SelectionKey skey {:keys [mode args]}]
  (wrap-error
    (let [{:keys [client-id]} @(.attachment skey)
          {:keys [kv-store]}  (get-client server client-id)]
      (case mode
        :copy-in (do (l/transact-kv kv-store (copy-in server skey))
                     (write-message skey {:type :command-complete}))
        :request (normal-kv-store-handler transact-kv)
        (u/raise "Missing :mode when transacting kv" {})))))

(defn- get-value
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error (normal-kv-store-handler get-value)))

(defn- get-first
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error (normal-kv-store-handler get-first)))

(defn- get-range
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [data (apply l/get-range (kv-store server skey) args)]
      (if (< (count data) c/+wire-datom-batch-size+)
        (write-message skey {:type :command-complete :result data})
        (copy-out skey data c/+wire-datom-batch-size+)))))

(defn- range-count
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error (normal-kv-store-handler range-count)))

(defn- get-some
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [ [dbi-name frozen-pred k-range k-type v-type ignore-key?] args

          pred (nippy/fast-thaw frozen-pred)
          args [dbi-name pred k-range k-type v-type ignore-key?]]
      (normal-kv-store-handler get-some))))

(defn- range-filter
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [[dbi-name frozen-pred k-range k-type v-type ignore-key?] args

          pred (nippy/fast-thaw frozen-pred)
          args [dbi-name pred k-range k-type v-type ignore-key?]
          data (apply l/range-filter (kv-store server skey) args)]
      (if (< (count data) c/+wire-datom-batch-size+)
        (write-message skey {:type :command-complete :result data})
        (copy-out skey data c/+wire-datom-batch-size+)))))

(defn- range-filter-count
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [[dbi-name frozen-pred k-range k-type] args

          pred (nippy/fast-thaw frozen-pred)
          args [dbi-name pred k-range k-type]]
      (normal-kv-store-handler range-filter-count))))

(defn- q
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [db             (:dt-db (get-client server
                                             (@(.attachment skey) :client-id)))
          [query inputs] args
          inputs         (replace {:remote-db-placeholder db} inputs)
          data           (apply q/q query inputs)]
      (if (< (count data) c/+wire-datom-batch-size+)
        (write-message skey {:type :command-complete :result data})
        (copy-out skey data c/+wire-datom-batch-size+)))))

;; END message handlers

(def message-handlers
  ['authentication
   'disconnect
   'set-client-id
   'create-user
   'create-role
   'assign-role
   'assign-permission
   'open
   'close
   'closed?
   'last-modified
   'schema
   'rschema
   'set-schema
   'init-max-eid
   'swap-attr
   'datom-count
   'load-datoms
   'fetch
   'populated?
   'size
   'head
   'tail
   'slice
   'rslice
   'size-filter
   'head-filter
   'tail-filter
   'slice-filter
   'rslice-filter
   'open-kv
   'close-kv
   'closed-kv?
   'open-dbi
   'clear-dbi
   'drop-dbi
   'list-dbis
   'copy
   'stat
   'entries
   'transact-kv
   'get-value
   'get-first
   'get-range
   'range-count
   'get-some
   'range-filter
   'range-filter-count
   'q])

(defmacro ^:no-doc message-cases
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
    (log/debug "Message received:" (dissoc message :password))
    (message-cases skey type)))

(defn- execute
  "Execute a function in a thread from the worker thread pool"
  [^Server server f]
  (.execute ^Executor (.-work-executor server) f))

(defn- handle-read
  [^Server server ^SelectionKey skey]
  (let [state                         (.attachment skey)
        {:keys [^ByteBuffer read-bf]} @state
        capacity                      (.capacity read-bf)
        ^SocketChannel ch             (.channel skey)
        readn                         (.read ch read-bf)]
    (cond
      (= readn 0)  :continue
      (> readn 0)  (if (= (.position read-bf) capacity)
                     (let [size (* c/+buffer-grow-factor+ capacity)
                           bf   (b/allocate-buffer size)]
                       (.flip read-bf)
                       (b/buffer-transfer read-bf bf)
                       (swap! state assoc :read-bf bf))
                     (p/extract-message
                       read-bf
                       (fn [fmt msg]
                         (execute server
                                  #(handle-message server skey fmt msg)))))
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
        (handle-registration server)
        (.select selector)
        (when (.get running)
          (loop [^Iterator iter (-> selector (.selectedKeys) (.iterator))]
            (when (.hasNext iter)
              (let [^SelectionKey skey (.next iter)]
                (when (and (.isValid skey) (.isAcceptable skey))
                  (handle-accept skey))
                (when (and (.isValid skey) (.isReadable skey))
                  (handle-read server skey)))
              (.remove iter)
              (recur iter))))
        (recur)))))

(defn- init-sys-db
  [root]
  (let [sys-conn (d/get-conn (str root u/+separator+ c/system-dir)
                             c/server-schema)]
    (when (= 0 (st/datom-count (.-store ^DB (d/db sys-conn)) c/eav))
      (let [s   (salt)
            h   (password-hashing c/default-password s)
            txs [{:db/id        -1
                  :user/name    c/default-username
                  :user/pw-hash h
                  :user/pw-salt s}
                 {:db/id    -2
                  :role/key (user-role-key c/default-username)}
                 {:db/id          -3
                  :user-role/user -1
                  :user-role/role -2}
                 {:db/id          -4
                  :permission/act ::control
                  :permission/obj ::server}
                 {:db/id          -5
                  :role-perm/perm -4
                  :role-perm/role -2}]]
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
                server-socket
                selector
                (ConcurrentLinkedQueue.)
                (Executors/newWorkStealingPool)
                (init-sys-db root)
                {}
                {}))
    (catch Exception e
      (u/raise "Error creating server:" (ex-message e) {}))))


(comment

  (require '[clj-memory-meter.core :as mm])

  (def server (create {:port c/default-port
                       :root (u/tmp-dir
                               (str "remote-test-" (UUID/randomUUID)))}))

  (def conn (.-sys-conn server))

  (start server)

  (pull-role conn :test-role)


  (user-permissions conn "datalevin")

  (stop server)

  )
