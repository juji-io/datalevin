(ns ^:no-doc datalevin.server
  "Non-blocking event-driven database server with role based access control"
  (:require
   [datalevin.util :as u]
   [datalevin.core :as d]
   [datalevin.bits :as b]
   [datalevin.query :as q]
   [datalevin.db :as db]
   [datalevin.lmdb :as l]
   [datalevin.protocol :as p]
   [datalevin.storage :as st]
   [datalevin.search :as sc]
   [datalevin.built-ins :as dbq]
   [datalevin.constants :as c]
   [taoensso.timbre :as log]
   [clojure.stacktrace :as stt]
   [clojure.string :as s])
  (:import
   [java.nio.charset StandardCharsets]
   [java.nio ByteBuffer BufferOverflowException]
   [java.nio.file Files Paths]
   [java.nio.channels Selector SelectionKey ServerSocketChannel SocketChannel]
   [java.net InetSocketAddress]
   [java.security SecureRandom]
   [java.util Iterator UUID Map]
   [java.util.concurrent.atomic AtomicBoolean]
   [java.util.concurrent Executors Executor ExecutorService
    ConcurrentLinkedQueue ConcurrentHashMap Semaphore]
   [datalevin.db DB]
   [datalevin.storage IStore Store]
   [datalevin.lmdb ILMDB]
   [org.bouncycastle.crypto.generators Argon2BytesGenerator]
   [org.bouncycastle.crypto.params Argon2Parameters Argon2Parameters$Builder]))

(defprotocol IServer
  (start [srv] "Start the server")
  (stop [srv] "Stop the server"))

;; system db management

(def server-schema
  (merge c/implicit-schema
         c/entity-time-schema
         {:user/name    {:db/doc       "User name, must be unique"
                         :db/unique    :db.unique/identity
                         :db/valueType :db.type/string}
          :user/pw-hash {:db/doc       "Hash of password"
                         :db/valueType :db.type/string}
          :user/pw-salt {:db/doc       "Salt of password"
                         :db/valueType :db.type/bytes}

          :database/name {:db/doc       "Database name, must be unique"
                          :db/unique    :db.unique/identity
                          :db/valueType :db.type/string}
          :database/type {:db/doc       "Database type, :datalog or :key-value"
                          :db/valueType :db.type/keyword}

          :role/key {:db/doc       "Role name, a keyword, must be unique"
                     :db/valueType :db.type/keyword
                     :db/unique    :db.unique/identity}

          :permission/act {:db/doc       "Securable action: ::view, ::alter,
                                          ::create, or ::control"
                           :db/valueType :db.type/keyword}
          :permission/obj {:db/doc       "Securable object type: ::database,
                                          ::user, ::role, or ::server"
                           :db/valueType :db.type/keyword}
          :permission/tgt {:db/doc       "Securable target, an entity id"
                           :db/valueType :db.type/ref}

          :user-role/user {:db/doc       "User part of a user role assignment"
                           :db/valueType :db.type/ref}
          :user-role/role {:db/doc       "Role part of a user role assignment"
                           :db/valueType :db.type/ref}

          :role-perm/role {:db/doc       "Role part of a role permission grant"
                           :db/valueType :db.type/ref}
          :role-perm/perm {:db/doc       "Permission part of a permission grant"
                           :db/valueType :db.type/ref}}))

;; permission securable actions
(derive ::alter ::view)
(derive ::create ::alter)
(derive ::control ::create)

(def permission-actions #{::view ::alter ::create ::control})

;; permission securable object types
(derive ::server ::database)
(derive ::server ::user)
(derive ::server ::role)

(def permission-objects #{::database ::user ::role ::server})

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
     (b/encode-base64 out-bs))))

(defn password-matches?
  [in-password password-hash salt]
  (= password-hash (password-hashing in-password salt)))

(defn- pull-user
  [sys-conn username]
  {:pre [(d/conn? sys-conn)]}
  (try
    (d/pull @sys-conn '[*] [:user/name username])
    (catch Exception _
      nil)))

(defn- query-user
  [sys-conn username]
  {:pre [(d/conn? sys-conn)]}
  (d/q '[:find ?u .
         :in $ ?uname
         :where
         [?u :user/name ?uname]]
       @sys-conn username))

(defn- pull-db
  [sys-conn db-name]
  {:pre [(d/conn? sys-conn)]}
  (try
    (d/pull @sys-conn '[*] [:database/name db-name])
    (catch Exception _
      nil)))

(defn- query-role
  [sys-conn role-key]
  {:pre [(d/conn? sys-conn)]}
  (d/q '[:find ?r .
         :in $ ?rk
         :where
         [?r :role/key ?rk]]
       @sys-conn role-key))

(defn- user-eid [sys-conn username] (query-user sys-conn username))

(defn- db-eid [sys-conn db-name] (:db/id (pull-db sys-conn db-name)))

(defn- role-eid [sys-conn role-key] (query-role sys-conn role-key))

(defn- eid->username
  [sys-conn eid]
  (:user/name (d/pull @sys-conn [:user/name] eid)))

(defn- eid->db-name
  [sys-conn eid]
  (:database/name (d/pull @sys-conn [:database/name] eid)))

(defn- eid->role-key
  [sys-conn eid]
  (:user/name (d/pull @sys-conn [:user/anme] eid)))

(defn- query-users [sys-conn]
  (d/q '[:find [?uname ...]
         :where
         [?u :user/name ?uname]]
       @sys-conn))

(defn- user-roles
  [sys-conn username]
  (d/q '[:find [?rk ...]
         :in $ ?uname
         :where
         [?u :user/name ?uname]
         [?ur :user-role/user ?u]
         [?ur :user-role/role ?r]
         [?r :role/key ?rk]]
       @sys-conn username))

(defn- query-roles [sys-conn]
  (d/q '[:find [?rk ...]
         :where
         [?r :role/key ?rk]]
       @sys-conn))

(defn- perm-tgt-eid
  [sys-conn perm-obj perm-tgt]
  (case perm-obj
    ::database (db-eid sys-conn perm-tgt)
    ::user     (user-eid sys-conn perm-tgt)
    ::role     (role-eid sys-conn perm-tgt)
    ::server   nil))

(defn- perm-tgt-name
  [sys-conn perm-obj perm-tgt]
  (case perm-obj
    ::database (eid->db-name sys-conn perm-tgt)
    ::user     (eid->username sys-conn perm-tgt)
    ::role     (eid->role-key sys-conn perm-tgt)
    ::server   nil))

(defn- user-permissions
  ([sys-conn username ]
   (mapv first
         (d/q '[:find (pull ?p [:permission/act :permission/obj :permission/tgt])
                :in $ ?uname
                :where
                [?u :user/name ?uname]
                [?ur :user-role/user ?u]
                [?ur :user-role/role ?r]
                [?rp :role-perm/role ?r]
                [?rp :role-perm/perm ?p]]
              @sys-conn username))))

(defn- role-permissions
  [sys-conn role-key]
  (mapv first
        (d/q '[:find (pull ?p [:permission/act :permission/obj :permission/tgt])
               :in $ ?rk
               :where
               [?r :role/key ?rk]
               [?ur :user-role/role ?r]
               [?rp :role-perm/role ?r]
               [?rp :role-perm/perm ?p]]
             @sys-conn role-key)))

(defn- user-role-eid
  [sys-conn uid rid]
  (when (and uid rid)
    (d/q '[:find ?ur .
           :in $ ?u ?r
           :where
           [?ur :user-role/user ?u]
           [?ur :user-role/role ?r]]
         @sys-conn uid rid)))

(defn- permission-eid
  ([sys-conn perm-tgt]
   (when perm-tgt
     (d/q '[:find [?p ...]
            :in $ ?tgt
            :where
            [?p :permission/tgt ?tgt]]
          @sys-conn perm-tgt)))
  ([sys-conn perm-act perm-obj perm-tgt]
   (if perm-tgt
     (d/q '[:find ?p .
            :in $ ?act ?obj ?tgt
            :where
            [?p :permission/act ?act]
            [?p :permission/obj ?obj]
            [?p :permission/tgt ?tgt]]
          @sys-conn perm-act perm-obj perm-tgt)
     (d/q '[:find ?p .
            :in $ ?act ?obj
            :where
            [?p :permission/act ?act]
            [?p :permission/obj ?obj]
            (not [?p :permission/tgt ?tgt])]
          @sys-conn perm-act perm-obj))))

(defn- role-permission-eid
  [sys-conn rid pid]
  (when (and rid pid)
    (d/q '[:find ?rp .
           :in $ ?r ?p
           :where
           [?rp :role-perm/role ?r]
           [?rp :role-perm/perm ?p]]
         @sys-conn rid pid)))

(defn- query-databases
  [sys-conn]
  (d/q '[:find [?dname ...]
         :where
         [?d :database/name ?dname]]
       @sys-conn))

;; each user is a role, similar to postgres
(defn- user-role-key [username] (keyword "datalevin.role" username))

(defn- user-role-key?
  ([sys-conn role-key]
   (user-role-key? sys-conn role-key nil))
  ([sys-conn role-key username]
   (let [ns (namespace role-key)
         n  (name role-key)]
     (and (= ns "datalevin.role")  (query-user sys-conn n)
          (if username (= n username) true)))))

(defn- transact-new-user
  [sys-conn username password]
  (if (query-user sys-conn username)
    (u/raise "User already exits" {:username username})
    (let [s (salt)]
      (d/transact! sys-conn [{:db/id        -1
                              :user/name    username
                              :user/pw-hash (password-hashing password s)
                              :user/pw-salt s}
                             {:db/id    -2
                              :role/key (user-role-key username)}
                             {:db/id          -3
                              :user-role/user -1
                              :user-role/role -2}
                             {:db/id          -4
                              :permission/act ::alter
                              :permission/obj ::user
                              :permission/tgt -1}
                             {:db/id          -5
                              :role-perm/perm -4
                              :role-perm/role -2}
                             {:db/id          -6
                              :permission/act ::view
                              :permission/obj ::role
                              :permission/tgt -2}
                             {:db/id          -7
                              :role-perm/perm -6
                              :role-perm/role -2}]))))

(defn- transact-new-password
  [sys-conn username password]
  (let [s (salt)]
    (d/transact! sys-conn [{:user/name    username
                            :user/pw-hash (password-hashing password s)
                            :user/pw-salt s}])))

(defn- transact-drop-user
  [sys-conn uid username]
  (let [rid    (role-eid sys-conn (user-role-key username))
        urid   (user-role-eid sys-conn uid rid)
        pids   (permission-eid sys-conn uid)
        p-txs  (mapv (fn [pid] [:db/retractEntity pid]) pids)
        rpids  (mapv (partial role-permission-eid sys-conn rid) pids)
        rp-txs (mapv (fn [rpid] [:db/retractEntity rpid]) rpids)]
    (d/transact! sys-conn (concat rp-txs p-txs
                                  [[:db/retractEntity urid]
                                   [:db/retractEntity rid]
                                   [:db/retractEntity uid]]))))

(defn- transact-new-role
  [sys-conn role-key]
  (if (query-role sys-conn role-key)
    (u/raise "Role already exits" {:role-key role-key})
    (d/transact! sys-conn [{:db/id    -1
                            :role/key role-key}
                           {:db/id          -2
                            :permission/act ::view
                            :permission/obj ::role
                            :permission/tgt -1}
                           {:db/id          -3
                            :role-perm/perm -2
                            :role-perm/role -1}])))

(defn- transact-drop-role
  [sys-conn rid]
  (let [ur-txs (mapv (fn [urid] [:db/retractEntity urid])
                     (d/q '[:find [?ur ...]
                            :in $ ?rid
                            :where
                            [?ur :user-role/role ?rid]]
                          @sys-conn rid))
        pids   (permission-eid sys-conn rid)
        p-txs  (mapv (fn [pid] [:db/retractEntity pid]) pids)
        rpids  (mapv (partial role-permission-eid sys-conn rid) pids)
        rp-txs (mapv (fn [rpid] [:db/retractEntity rpid]) rpids)]
    (d/transact! sys-conn (concat rp-txs p-txs ur-txs
                                  [[:db/retractEntity rid]]))))

(defn- transact-user-role
  [sys-conn rid username]
  (if-let [uid (user-eid sys-conn username)]
    (d/transact! sys-conn [{:user-role/user uid :user-role/role rid}])
    (u/raise "User does not exist." {:username username})))

(defn- transact-withdraw-role
  [sys-conn rid username]
  (if-let [uid (user-eid sys-conn username)]
    (when-let [urid (user-role-eid sys-conn uid rid)]
      (d/transact! sys-conn [[:db/retractEntity urid]]))
    (u/raise "User does not exist." {:username username})))

(defn- transact-role-permission
  [sys-conn rid perm-act perm-obj perm-tgt]
  (if-let [pid (permission-eid sys-conn perm-act perm-obj perm-tgt)]
    (d/transact! sys-conn [{:role-perm/perm pid :role-perm/role rid}])
    (if perm-tgt
      (if-let [tid (perm-tgt-eid sys-conn perm-obj perm-tgt)]
        (d/transact! sys-conn [{:db/id          -1
                                :permission/act perm-act
                                :permission/obj perm-obj
                                :permission/tgt tid}
                               {:db/id          -2
                                :role-perm/perm -1
                                :role-perm/role rid}])
        (u/raise "Permission target does not exist." {}))
      (d/transact! sys-conn [{:db/id          -1
                              :permission/act perm-act
                              :permission/obj perm-obj}
                             {:db/id          -2
                              :role-perm/perm -1
                              :role-perm/role rid}]))))

(defn- transact-revoke-permission
  [sys-conn rid perm-act perm-obj perm-tgt]
  (if-let [pid (permission-eid sys-conn perm-act perm-obj perm-tgt)]
    (when-let [rpid (role-permission-eid sys-conn rid pid)]
      (d/transact! sys-conn [[:db/retractEntity rpid]]))
    (u/raise "Permission does not exist." {})))

(defn- transact-new-db
  [sys-conn username db-type db-name]
  (d/transact! sys-conn
               [{:db/id         -1
                 :database/type db-type
                 :database/name db-name}
                {:db/id          -2
                 :permission/act ::create
                 :permission/obj ::database
                 :permission/tgt -1}
                {:db/id          -3
                 :role-perm/perm -2
                 :role-perm/role [:role/key (user-role-key username)]}]))

(defn- transact-drop-db
  [sys-conn did]
  (let [pids     (d/q '[:find [?p ...]
                        :in $ ?did
                        :where
                        [?p :permission/tgt ?did]]
                      @sys-conn did)
        pids-txs (mapv (fn [pid] [:db/retractEntity pid]) pids)
        rpids    (mapcat (fn [pid]
                           (d/q '[:find [?rp ...]
                                  :in $ ?pid
                                  :where
                                  [?rp :role-perm/perm ?pid]]
                                @sys-conn pid))
                         pids)
        rp-txs   (mapv (fn [rpid] [:db/retractEntity rpid]) rpids)]
    (d/transact! sys-conn (concat rp-txs pids-txs [[:db/retractEntity did]]))))

(defn- close-store
  [store]
  (cond
    (instance? datalevin.storage.IStore store) (st/close store)
    (instance? datalevin.lmdb.ILMDB store)     (l/close-kv store)
    :else                                      (u/raise "Unknown store" {})))

(defn- has-permission?
  [req-act req-obj req-tgt user-permissions]
  (some (fn [{:keys [permission/act permission/obj permission/tgt] :as p}]
          (and (isa? act req-act)
               (isa? obj req-obj)
               (if req-tgt
                 (if tgt (= req-tgt (tgt :db/id)) true)
                 (if tgt false true))))
        user-permissions))

(defmacro wrap-permission
  [req-act req-obj req-tgt message & body]
  `(let [{:keys [~'client-id ~'write-bf]} @(~'.attachment ~'skey)
         ~'ch                             (~'.channel ~'skey)
         {:keys [~'permissions]}          (get-client ~'server ~'client-id)]
     (if ~'permissions
       (if (has-permission? ~req-act ~req-obj ~req-tgt ~'permissions)
         (do ~@body)
         (u/raise ~message {}))
       (do
         (remove-client ~'server ~'client-id)
         (p/write-message-blocking ~'ch ~'write-bf {:type :reconnect})))))

(declare event-loop close-conn store->db-name session-lmdb remove-store)

(def session-dbi "datalevin-server/sessions")

(deftype Server [^AtomicBoolean running
                 ^int port
                 ^String root
                 ^long idle-timeout
                 ^ServerSocketChannel server-socket
                 ^Selector selector
                 ^ConcurrentLinkedQueue register-queue
                 ^ExecutorService work-executor
                 sys-conn
                 ;; client session data, a map of
                 ;; client-id -> { ip, uid, username, roles, permissions,
                 ;;                last-active,
                 ;;                stores -> { db-name -> {datalog?
                 ;;                                        dbis -> #{dbi-name}}}
                 ;;                engines -> #{ db-name }
                 ;;                dt-dbs -> #{ db-name } }
                 ^ConcurrentHashMap clients
                 ;; db state data, a map of
                 ;; db-name -> { store, search engine
                 ;;              datalog db, lock, write txn runner,
                 ;;              and writing variants of stores }
                 dbs]
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
    (doseq [db-name (keys dbs)] (remove-store server db-name))
    (d/close sys-conn)
    (log/info "Datalevin server shuts down.")))

(defn- get-client [^Server server client-id]
  (get (.-clients server) client-id))

(defn- add-client
  [^Server server ip client-id username]
  (let [sys-conn (.-sys-conn server)
        roles    (user-roles sys-conn username)
        perms    (user-permissions sys-conn username)
        session  {:ip          ip
                  :uid         (user-eid sys-conn username)
                  :username    username
                  :last-active (System/currentTimeMillis)
                  :stores      {}
                  :engines     #{}
                  :dt-dbs      #{}
                  :roles       roles
                  :permissions perms}]
    (d/transact-kv (session-lmdb sys-conn)
                   [[:put session-dbi client-id session :uuid :data]])
    (.put ^Map (.-clients server) client-id session)
    (log/info "Added client " client-id
              "from:" ip
              "for user:" username)))

(defn- remove-client
  [^Server server client-id]
  (d/transact-kv (session-lmdb (.-sys-conn server))
                 [[:del session-dbi client-id :uuid]])
  (.remove ^Map (.-clients server) client-id)
  (log/info "Removed client:" client-id))

(defn- update-client
  [^Server server client-id f]
  (let [session (f (get-client server client-id))]
    (d/transact-kv (session-lmdb (.-sys-conn server))
                   [[:put session-dbi client-id session :uuid :data]])
    (.put ^Map (.-clients server) client-id session)))

(defn- get-stores
  [^Server server]
  (into {} (map (fn [[db-name m]] [db-name (:store m)]) (.-dbs server))))

(defn- get-store
  ([^Server server db-name writing?]
   (get-in (.-dbs server) [db-name (if writing? :wstore :store)]))
  ([server db-name]
   (get-store server db-name false)))

(defn- update-db
  [^Server server db-name f]
  (let [^Map dbs (.-dbs server)]
    (.put dbs db-name (f (get dbs db-name {})))))

(defn- add-store
  [^Server server db-name store]
  (update-db server db-name
             #(cond-> (assoc % :store store)
                (instance? IStore store) (assoc :dt-db (db/new-db store)))))

(defn- get-db
  ([server db-name]
   (get-db server db-name false))
  ([^Server server db-name writing?]
   (let [m (get (.-dbs server) db-name)]
     (if writing? (:wdt-db m) (:dt-db m)))))

(defn- remove-store
  [^Server server db-name]
  (when-let [store (get-store server db-name)]
    (if-let [db (get-db server db-name)]
      (db/close-db db)
      (close-store store)))
  (.remove ^Map (.-dbs server) db-name))

(defn- update-cached-role
  [^Server server target-username]
  (let [sys-conn    (.-sys-conn server)
        roles       (user-roles sys-conn target-username)
        permissions (user-permissions sys-conn target-username)]
    (doseq [cid (keep (fn [[client-id {:keys [username]}]]
                        (when (= target-username username) client-id))
                      (.-clients server))]
      (update-client server cid
                     #(assoc % :roles roles :permissions permissions)))))

(defn- disconnect-client*
  [^Server server client-id]
  (remove-client server client-id)
  (let [^Selector selector (.-selector server)]
    (when (.isOpen selector)
      (doseq [^SelectionKey k (.keys selector)
              :let            [state (.attachment k)]
              :when           state]
        (when (= client-id (@state :client-id))
          (close-conn k))))))

(defn- disconnect-user
  [^Server server tgt-username]
  (doseq [[client-id {:keys [username]}] (.-clients server)
          :when                          (= tgt-username username)]
    (disconnect-client* server client-id)))

(defn- update-cached-permission
  [^Server server target-role]
  (let [sys-conn (.-sys-conn server)]
    (doseq [[cid uname] (keep (fn [[client-id {:keys [username roles]}]]
                                (when (some #(= % target-role) roles)
                                  [client-id username]))
                              (.-clients server))]
      (update-client server cid
                     #(assoc % :permissions
                             (user-permissions sys-conn uname))))))

;; networking

(defn- write-message
  "write a message to channel, auto grow the buffer"
  [^SelectionKey skey msg]
  (let [state                          (.attachment skey)
        {:keys [^ByteBuffer write-bf]} @state
        ^SocketChannel  ch             (.channel skey)]
    (try
      (p/write-message-blocking ch write-bf msg)
      (catch BufferOverflowException _
        (let [size (* ^long c/+buffer-grow-factor+ ^int (.capacity write-bf))]
          (vswap! state assoc :write-bf (b/allocate-buffer size))
          (write-message skey msg))))))

(defn- handle-accept
  [^SelectionKey skey]
  (when-let [client-socket (.accept ^ServerSocketChannel (.channel skey))]
    (doto ^SocketChannel client-socket
      (.configureBlocking false)
      (.register (.selector skey) SelectionKey/OP_READ
                 ;; attach a connection state
                 ;; { read-bf, write-bf, client-id }
                 (volatile! {:read-bf  (ByteBuffer/allocateDirect
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
          (when-not (identical? bf bf') (vswap! state assoc :read-bf bf'))
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
    (locking write-bf
      (p/write-message-blocking ch write-bf {:type :copy-out-response})
      (doseq [batch (partition batch-size batch-size nil data)]
        (write-message skey batch))
      (p/write-message-blocking ch write-bf {:type :copy-done}))
    (log/debug "Copied out" (count data) "data items")))

(defn- open-port
  [port]
  (try
    (doto (ServerSocketChannel/open)
      (.bind (InetSocketAddress. port))
      (.configureBlocking false))
    (catch Exception e
      (u/raise "Error opening port:" (ex-message e) {}))))

(defn- get-ip [^SelectionKey skey]
  (let [ch ^SocketChannel (.channel skey)]
    (.toString (.getAddress ^InetSocketAddress (.getRemoteAddress ch)))))

(defn- close-conn
  [^SelectionKey skey]
  (.close ^SocketChannel (.channel skey)))

(defn- error-response
  [^SelectionKey skey error-msg error-data]
  (let [{:keys [^ByteBuffer write-bf]}    @(.attachment skey)
        ^SocketChannel                 ch (.channel skey)]
    (p/write-message-blocking ch write-bf
                              {:type     :error-response
                               :message  error-msg
                               :err-data error-data})))

(defmacro wrap-error
  [& body]
  `(try
     ~@body
     (catch Exception ~'e
       (log/error ~'e)
       (error-response ~'skey (ex-message ~'e) (ex-data ~'e)))))

;; db

(defn- db-dir
  "translate from db-name to server db path"
  [root db-name]
  (str root u/+separator+ (b/hexify-string db-name)))

(defn- db-exists?
  [^Server server db-name]
  (u/file-exists
    (str (db-dir (.-root server) db-name) u/+separator+ "data.mdb")))

(defn- dir->db-name
  [^Server server dir]
  (b/unhexify-string
    (s/replace-first dir (str (.-root server) u/+separator+) "")))

(defn- store->db-name
  [server store]
  (dir->db-name
    server
    (cond
      (instance? IStore store) (st/dir store)
      (instance? ILMDB store)  (l/dir store)
      :else                    (u/raise "Unknown store type" {}))))

(defn- db-store
  [^Server server ^SelectionKey skey db-name]
  (when (((get-client server (@(.attachment skey) :client-id)) :stores)
         db-name)
    (get-store server db-name)))

(defn- writing-lmdb
  [^Server server db-name]
  (get-in (.-dbs server) [db-name :wlmdb]))

(defn- writing-store
  [^Server server db-name]
  (get-in (.-dbs server) [db-name :wstore]))

(defn- store
  [^Server server ^SelectionKey skey db-name writing?]
  (if writing?
    (writing-store server db-name)
    (db-store server skey db-name)))

(defn- lmdb
  [^Server server ^SelectionKey skey db-name writing?]
  (if writing?
    (writing-lmdb server db-name)
    (db-store server skey db-name)))

(defn- store-closed?
  [store]
  (cond
    (instance? IStore store) (st/closed? store)
    (instance? ILMDB store)  (l/closed-kv? store)
    :else                    (u/raise "Unknown store type" {})))

(defn- store-in-use? [[db-name store]] (when-not (store-closed? store) db-name))

(defn- db-in-use?
  [server db-name]
  (when-let [store (get-store server db-name)]
    (not (store-closed? store))))

(defn- in-use-dbs [server] (keep store-in-use? (get-stores server)))

(defmacro normal-dt-store-handler
  "Handle request to Datalog store that needs no copy-in or copy-out"
  [f]
  `(write-message
     ~'skey
     {:type   :command-complete
      :result (apply
                ~(symbol "datalevin.storage" (str f))
                (store ~'server ~'skey (nth ~'args 0) ~'writing?)
                (rest ~'args))}))

(defmacro normal-kv-store-handler
  "Handle request to key-value store that needs no copy-in or copy-out"
  [f]
  `(write-message
     ~'skey
     {:type   :command-complete
      :result (apply
                ~(symbol "datalevin.lmdb" (str f))
                (lmdb ~'server ~'skey (nth ~'args 0) ~'writing?)
                (rest ~'args))}))

(defn- search-engine
  [^Server server ^SelectionKey skey db-name]
  (when (((get-client server (@(.attachment skey) :client-id)) :engines)
         db-name)
    (get-in (.-dbs server) [db-name :engine])))

(defn- new-search-engine
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [[db-name opts]      args
          store               (get-store server db-name)
          {:keys [client-id]} @(.attachment skey)
          engine              (or (search-engine server skey db-name)
                                  (sc/new-search-engine store opts))]
      (update-client server client-id #(update % :engines conj db-name))
      (update-db server db-name #(assoc % :engine engine))
      (write-message skey {:type :command-complete}))))

(defmacro search-handler
  "Handle request to search engine"
  [f]
  `(write-message
     ~'skey
     {:type   :command-complete
      :result (apply
                ~(symbol "datalevin.search" (str f))
                (search-engine ~'server ~'skey (nth ~'args 0))
                (rest ~'args))}))

(defn- open-store
  [root db-name dbis datalog?]
  (let [dir (db-dir root db-name)]
    (if datalog?
      (st/open dir)
      (let [lmdb (l/open-kv dir)]
        (doseq [dbi dbis] (l/open-dbi lmdb dbi))
        lmdb))))

(defn- open-server-store
  "Open a store. NB. stores are left open"
  [^Server server ^SelectionKey skey {:keys [db-name schema opts]} db-type]
  (wrap-error
    (let [{:keys [client-id]} @(.attachment skey)
          {:keys [username]}  (get-client server client-id)
          db-name             (u/lisp-case db-name)
          existing-db?        (db-exists? server db-name)
          sys-conn            (.-sys-conn server)]
      (log/debug "open" db-name "that exist?" existing-db?)
      (wrap-permission
        (if existing-db? ::view ::create)
        ::database
        (when existing-db? (db-eid sys-conn db-name))
        "Don't have permission to open database"
        (let [dir      (db-dir (.-root server) db-name)
              store    (or (when-let [ds (get-store server db-name)]
                             (if (instance? Store ds)
                               (when-not (st/closed? ds)
                                 (when schema
                                   (st/set-schema ds schema))
                                 ds)
                               (when-not (l/closed-kv? ds)
                                 ds)))
                           (case db-type
                             :datalog   (st/open dir schema opts)
                             :key-value (l/open-kv dir opts)))
              datalog? (instance? Store store)]
          (add-store server db-name store)
          (update-client server client-id
                         #(cond-> %
                            true     (update :stores assoc db-name
                                             {:datalog? datalog?
                                              :dbis     #{}})
                            datalog? (update :dt-dbs conj db-name)))
          (when-not existing-db?
            (transact-new-db sys-conn username db-type db-name)
            (update-client server client-id
                           #(assoc % :permissions
                                   (user-permissions sys-conn username))))
          (write-message skey {:type :command-complete}))))))

(defn- session-lmdb [sys-conn] (.-lmdb ^Store (.-store ^DB (d/db sys-conn))))

(defn- init-sys-db
  [root]
  (let [sys-conn (d/get-conn (str root u/+separator+ c/system-dir)
                             server-schema)]
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

(defn- load-sessions
  [sys-conn]
  (let [lmdb (session-lmdb sys-conn)]
    (d/open-dbi lmdb session-dbi)
    (ConcurrentHashMap.
      ^Map (into {} (d/get-range lmdb session-dbi [:all] :uuid :data)))))

(defn- reopen-dbs
  [root clients ^ConcurrentHashMap dbs]
  (doseq [[_ {:keys [stores engines dt-dbs]}] clients]
    (doseq [[db-name {:keys [datalog? dbis]}]
            stores
            :when (not (get-in dbs [db-name :store]))
            :let  [m (get dbs db-name {})]]
      (.put dbs db-name
            (assoc m :store (open-store root db-name dbis datalog?))))
    (doseq [db-name engines
            :when   (not (get-in dbs [db-name :engine]))
            :let    [m (get dbs db-name {})]]
      (.put dbs db-name
            (assoc m :engine
                   (d/new-search-engine (get-in dbs [db-name :store])))))
    (doseq [db-name dt-dbs
            :when   (not (get-in dbs [db-name :dt-db]))
            :let    [m (get dbs db-name {})]]
      (.put dbs db-name
            (assoc m :dt-db
                   (db/new-db (get-in dbs [db-name :store])))))))

(defn- authenticate
  [^Server server ^SelectionKey skey {:keys [username password]}]
  (when-let [{:keys [user/pw-salt user/pw-hash]}
             (pull-user (.-sys-conn server) username)]
    (when (password-matches? password pw-hash pw-salt)
      (let [client-id (UUID/randomUUID)
            ip        (get-ip skey)]
        (add-client server ip client-id username)
        client-id))))

(defn- client-display
  [^Server server [client-id m]]
  (let [sys-conn (.-sys-conn server)]
    [client-id
     (-> m
         (update :permissions
                 #(mapv
                    (fn [{:keys [permission/act permission/obj
                                permission/tgt]}]
                      (if-let [{:keys [db/id]} tgt]
                        [act obj (perm-tgt-name sys-conn obj id)]
                        [act obj]))
                    %))
         (assoc :open-dbs (:stores m))
         (select-keys [:ip :username :roles :permissions :open-dbs]))]))

;; BEGIN message handlers

(def message-handlers
  ['authentication
   'disconnect
   'set-client-id
   'create-user
   'reset-password
   'drop-user
   'list-users
   'create-role
   'drop-role
   'list-roles
   'create-database
   'close-database
   'drop-database
   'list-databases
   'list-databases-in-use
   'assign-role
   'withdraw-role
   'list-user-roles
   'grant-permission
   'revoke-permission
   'list-role-permissions
   'list-user-permissions
   'query-system
   'show-clients
   'disconnect-client
   'open
   'close
   'closed?
   'opts
   'assoc-opt
   'last-modified
   'schema
   'rschema
   'set-schema
   'init-max-eid
   'max-tx
   'swap-attr
   'del-attr
   'rename-attr
   'datom-count
   'load-datoms
   'tx-data
   'open-transact
   'close-transact
   'abort-transact
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
   'open-transact-kv
   'close-transact-kv
   'abort-transact-kv
   'transact-kv
   'get-value
   'get-first
   'get-range
   'range-count
   'get-some
   'range-filter
   'range-filter-count
   'visit
   'q
   'fulltext-datoms
   'new-search-engine
   'add-doc
   'remove-doc
   'clear-docs
   'doc-indexed?
   'doc-count
   'search
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

(defn- authentication
  [^Server server skey message]
  (wrap-error
    (if-let [client-id (authenticate server skey message)]
      (write-message skey {:type :authentication-ok :client-id client-id})
      (u/raise "Failed to authenticate" {}))))

(defn- disconnect
  [server ^SelectionKey skey _]
  (let [{:keys [client-id]} @(.attachment skey)]
    (disconnect-client* server client-id)))

(defn- set-client-id
  [^Server server ^SelectionKey skey message]
  (vswap! (.attachment skey) assoc :client-id (message :client-id))
  (write-message skey {:type :set-client-id-ok}))

(defn- create-user
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [sys-conn            (.-sys-conn server)
          [username password] args
          username            (u/lisp-case username)]
      (wrap-permission
        ::create ::user nil
        "Don't have permission to create user"
        (if (s/blank? password)
          (u/raise "Password is required when creating user." {})
          (do (transact-new-user sys-conn username password)
              (write-message skey {:type     :command-complete
                                   :username username})))))))

(defn- reset-password
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [sys-conn            (.-sys-conn server)
          [username password] args
          uid                 (user-eid sys-conn username)]
      (if uid
        (wrap-permission
          ::alter ::user uid
          (str "Don't have permission to reset password of " username)
          (if (s/blank? password)
            (u/raise "New password is required when resetting password" {})
            (do (transact-new-password sys-conn username password)
                (write-message skey {:type :command-complete}))))
        (u/raise "User does not exist" {:username username})))))

(defn- drop-user
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [sys-conn   (.-sys-conn server)
          [username] args
          uid        (user-eid sys-conn username)]
      (if (= username c/default-username)
        (u/raise "Default user cannot be dropped." {})
        (if uid
          (wrap-permission
            ::create ::user uid
            "Don't have permission to drop the user"
            (disconnect-user server username)
            (transact-drop-user sys-conn uid username)
            (write-message skey {:type :command-complete}))
          (u/raise "User does not exist." {:user username}))))))

(defn- list-users
  [^Server server ^SelectionKey skey _]
  (wrap-error
    (wrap-permission
      ::view ::user nil
      "Don't have permission to list users"
      (write-message skey {:type   :command-complete
                           :result (query-users (.-sys-conn server))}))))

(defn- create-role
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [[role-key] args]
      (wrap-permission
        ::create ::role nil
        "Don't have permission to create role"
        (transact-new-role (.-sys-conn server) role-key)
        (write-message skey {:type :command-complete})))))

(defn- drop-role
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [sys-conn   (.-sys-conn server)
          [role-key] args
          rid        (role-eid sys-conn role-key)]
      (if rid
        (if (user-role-key? sys-conn role-key)
          (u/raise "Cannot drop default role of an active user" {})
          (wrap-permission
            ::create ::role rid
            "Don't have permission to drop the role"
            (transact-drop-role sys-conn rid)
            (update-cached-permission server role-key)
            (write-message skey {:type :command-complete})))
        (u/raise "Role does not exist." {:role role-key})))))

(defn- list-roles
  [^Server server ^SelectionKey skey _]
  (wrap-error
    (wrap-permission
      ::view ::role nil
      "Don't have permission to list roles"
      (write-message skey {:type   :command-complete
                           :result (query-roles (.-sys-conn server))}))))

(defn- create-database
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [sys-conn            (.-sys-conn server)
          {:keys [client-id]} @(.attachment skey)
          {:keys [username]}  (get-client server client-id)
          [db-name db-type]   args
          db-name             (u/lisp-case db-name)]
      (wrap-permission
        ::create ::database nil
        "Don't have permission to create database"
        (if (db-exists? server db-name)
          (u/raise "Database already exists." {:db db-name})
          (do
            (open-server-store server skey
                               {:db-name db-name} db-type)
            (transact-new-db sys-conn username db-type db-name)
            (update-client server client-id
                           #(assoc % :permissions
                                   (user-permissions sys-conn username)))))
        (write-message skey {:type :command-complete})))))

(defn- close-database
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [sys-conn            (.-sys-conn server)
          [db-name]           args
          {:keys [client-id]} @(.attachment skey)
          did                 (db-eid sys-conn db-name)]
      (if did
        (if (get-store server db-name)
          (wrap-permission
            ::create ::database did
            "Don't have permission to close the database"
            (doseq [[cid {:keys [stores]}] (.-clients server)
                    :when                  (stores db-name)]
              (when (not= client-id cid)
                (disconnect-client* server cid)))
            (remove-store server db-name)
            (write-message skey {:type :command-complete}))
          (u/raise "Database is closed already." {}))
        (u/raise "Database doe snot exist." {})))))

(defn- drop-database
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [sys-conn  (.-sys-conn server)
          [db-name] args
          did       (db-eid sys-conn db-name)]
      (if did
        (wrap-permission
          ::create ::database did
          "Don't have permission to drop the database"
          (if (db-in-use? server db-name)
            (u/raise "Cannot drop a database currently in use." {})
            (do (transact-drop-db sys-conn did)
                (u/delete-files (db-dir (.-root server) db-name))
                (write-message skey {:type :command-complete}))))
        (u/raise "Database does not exist." {})))))

(defn- list-databases
  [^Server server ^SelectionKey skey _]
  (wrap-error
    (wrap-permission
      ::create ::database nil
      "Don't have permission to list databases"
      (write-message skey {:type   :command-complete
                           :result (query-databases (.-sys-conn server))}))))

(defn- list-databases-in-use
  [^Server server ^SelectionKey skey _]
  (wrap-error
    (wrap-permission
      ::create ::database nil
      "Don't have permission to list databases in use"
      (write-message skey {:type   :command-complete
                           :result (in-use-dbs server)}))))

(defn- assign-role
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [sys-conn            (.-sys-conn server)
          [role-key username] args
          rid                 (role-eid sys-conn role-key)]
      (if rid
        (wrap-permission
          ::alter ::role rid
          "Don't have permission to assign the role to user"
          (transact-user-role sys-conn rid username)
          (update-cached-role server username)
          (write-message skey {:type :command-complete}))
        (u/raise "Role does not exist." {})))))

(defn- withdraw-role
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [sys-conn            (.-sys-conn server)
          [role-key username] args
          rid                 (role-eid sys-conn role-key)]
      (if rid
        (if (user-role-key? sys-conn role-key username)
          (u/raise "Cannot withdraw the default role of a user" {})
          (wrap-permission
            ::alter ::role rid
            "Don't have permission to withdraw the role from user"
            (transact-withdraw-role sys-conn rid username)
            (update-cached-role server username)
            (write-message skey {:type :command-complete})))
        (u/raise "Role does not exist." {})))))

(defn- list-user-roles
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [sys-conn   (.-sys-conn server)
          [username] args
          uid        (user-eid sys-conn username)]
      (if uid
        (wrap-permission
          ::view ::user uid
          "Don't have permission to view the user's roles"
          (write-message skey {:type   :command-complete
                               :result (user-roles sys-conn username)}))
        (u/raise "User does not exist." {})))))

(defn- grant-permission
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [sys-conn                              (.-sys-conn server)
          [role-key perm-act perm-obj perm-tgt] args
          rid                                   (role-eid sys-conn role-key)]
      (if rid
        (wrap-permission
          ::alter ::role rid
          "Don't have permission to grant permission to the role"
          (if (and (permission-actions perm-act) (permission-objects perm-obj))
            (transact-role-permission sys-conn rid perm-act perm-obj perm-tgt)
            (u/raise "Unknown permission action or object." {}))
          (update-cached-permission server role-key)
          (write-message skey {:type :command-complete}))
        (u/raise "Role does not exist." {})))))

(defn- revoke-permission
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [sys-conn                              (.-sys-conn server)
          [role-key perm-act perm-obj perm-tgt] args
          rid                                   (role-eid sys-conn role-key)]
      (if rid
        (wrap-permission
          ::alter ::role rid
          "Don't have permission to revoke permission from the role"
          (transact-revoke-permission sys-conn rid perm-act perm-obj perm-tgt)
          (update-cached-permission server role-key)
          (write-message skey {:type :command-complete}))
        (u/raise "Role does not exist." {})))))

(defn- list-role-permissions
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [sys-conn   (.-sys-conn server)
          [role-key] args
          rid        (role-eid sys-conn role-key)]
      (if rid
        (wrap-permission
          ::view ::role rid
          "Don't have permission to list permissions of the role"
          (write-message skey {:type   :command-complete
                               :result (role-permissions sys-conn role-key)}))
        (u/raise "Role does not exist." {})))))

(defn- list-user-permissions
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [sys-conn   (.-sys-conn server)
          [username] args
          uid        (user-eid sys-conn username)]
      (if uid
        (wrap-permission
          ::view ::user uid
          "Don't have permission to list permission of the user"
          (write-message skey {:type   :command-complete
                               :result (user-permissions sys-conn username)}))
        (u/raise "User does not exist." {})))))

(defn- query-system
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [[query arguments] args]
      (wrap-permission
        ::view ::server nil
        "Don't have permission to query system."
        (write-message skey {:type   :command-complete
                             :result (apply d/q query
                                            @(.-sys-conn server)
                                            arguments)})))))
(defn- show-clients
  [^Server server ^SelectionKey skey _]
  (wrap-error
    (wrap-permission
      ::view ::server nil
      "Don't have permission to show clients."
      (write-message skey
                     {:type   :command-complete
                      :result (->> (.-clients server)
                                   (map (partial client-display server))
                                   (into {}))}))))

(defn- disconnect-client
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [[cid] args]
      (wrap-permission
        ::control ::server nil
        "Don't have permission to disconnect a client"
        (disconnect-client* server cid)
        (write-message skey {:type :command-complete})))))

(defn- open
  "Open a datalog store."
  [^Server server ^SelectionKey skey message]
  (open-server-store server skey message c/dl-type))

(defn- close
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-dt-store-handler close)))

(defn- closed?
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [db-name (nth args 0)
          res     (if-let [s (store server skey db-name writing?)]
                    (st/closed? s)
                    true)]
      (write-message skey {:type :command-complete :result res}))))

(defn- opts
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-dt-store-handler opts)))

(defn- assoc-opt
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-dt-store-handler assoc-opt)))

(defn- last-modified
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-dt-store-handler last-modified)))

(defn- schema
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-dt-store-handler schema)))

(defn- rschema
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-dt-store-handler rschema)))

(defn- set-schema
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (wrap-permission
      ::alter ::database (db-eid (.-sys-conn server)
                                 (store->db-name
                                   server
                                   (db-store server skey (nth args 0))))
      "Don't have permission to alter the database"
      (normal-dt-store-handler set-schema))))

(defn- init-max-eid
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-dt-store-handler init-max-eid)))

(defn- max-tx
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-dt-store-handler max-tx)))

(defn- swap-attr
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [frozen (nth args 2)
          args   (replace {frozen (b/deserialize frozen)} args)]
      (normal-dt-store-handler swap-attr))))

(defn- del-attr
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-dt-store-handler del-attr)))

(defn- rename-attr
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-dt-store-handler rename-attr)))

(defn- datom-count
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-dt-store-handler datom-count)))

(defn- load-datoms
  [^Server server ^SelectionKey skey {:keys [mode args writing?]}]
  (wrap-error
    (let [db-name  (nth args 0)
          sys-conn (.-sys-conn server)]
      (wrap-permission
        ::alter ::database (db-eid sys-conn db-name)
        "Don't have permission to alter the database"
        (case mode
          :copy-in (let [dt-store (store server skey db-name writing?)]
                     (st/load-datoms dt-store (copy-in server skey))
                     (write-message skey {:type :command-complete}))
          :request (normal-dt-store-handler load-datoms)
          (u/raise "Missing :mode when loading datoms" {}))))))

(defn- transact*
  [db txs s? server db-name writing?]
  (try
    (d/with db txs {} s?)
    (catch Exception e
      (when (:resized (ex-data e))
        (let [^DB new-db (db/new-db (get-store server db-name writing?))]
          (update-db server db-name
                     #(assoc % (if writing? :wdt-db :dt-db)
                             new-db))))
      (throw e))))

(defn- tx-data
  [^Server server ^SelectionKey skey {:keys [mode args writing?]}]
  (wrap-error
    (let [db-name  (nth args 0)
          sys-conn (.-sys-conn server)]
      (wrap-permission
        ::alter ::database (db-eid sys-conn db-name)
        "Don't have permission to alter the database"
        (let [txs (case mode
                    :copy-in (copy-in server skey)
                    :request (nth args 1)
                    (u/raise "Missing :mode when transact data" {}))
              db  (get-db server db-name writing?)
              s?  (last args)
              rp  (transact* db txs s? server db-name writing?)
              db  (:db-after rp)
              _   (update-db server db-name
                             #(assoc % (if writing? :wdt-db :dt-db) db))
              rp  (assoc-in rp [:tempids :max-eid] (:max-eid db))
              ct  (+ (count (:tx-data rp)) (count (:tempids rp)))
              res (select-keys rp [:tx-data :tempids])]
          (if (< ct ^long c/+wire-datom-batch-size+)
            (write-message skey {:type :command-complete :result res})
            (let [{:keys [tx-data tempids]} res]
              (copy-out skey (into tx-data tempids)
                        c/+wire-datom-batch-size+))))))))

(defn- fetch
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-dt-store-handler fetch)))

(defn- populated?
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-dt-store-handler populated?)))

(defn- size
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-dt-store-handler size)))

(defn- head
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-dt-store-handler head)))

(defn- tail
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-dt-store-handler tail)))

(defn- slice
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [db-name (nth args 0)
          datoms  (apply st/slice
                         (store server skey db-name writing?)
                         (rest args))]
      (if (< (count datoms) ^long c/+wire-datom-batch-size+)
        (write-message skey {:type :command-complete :result datoms})
        (copy-out skey datoms c/+wire-datom-batch-size+)))))

(defn- rslice
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [db-name (nth args 0)
          datoms  (apply st/rslice
                         (store server skey db-name writing?)
                         (rest args))]
      (if (< (count datoms) ^long c/+wire-datom-batch-size+)
        (write-message skey {:type :command-complete :result datoms})
        (copy-out skey datoms c/+wire-datom-batch-size+)))))

(defn- size-filter
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [frozen (nth args 2)
          args   (replace {frozen (b/deserialize frozen)} args)]
      (normal-dt-store-handler size-filter))))

(defn- head-filter
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [frozen (nth args 2)
          args   (replace {frozen (b/deserialize frozen)} args)]
      (normal-dt-store-handler head-filter))))

(defn- tail-filter
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [frozen (nth args 2)
          args   (replace {frozen (b/deserialize frozen)} args)]
      (normal-dt-store-handler tail-filter))))

(defn- slice-filter
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [frozen (nth args 2)
          args   (replace {frozen (b/deserialize frozen)} args)
          datoms (apply st/slice-filter
                        (store server skey (nth args 0) writing?)
                        (rest args))]
      (if (< (count datoms) ^long c/+wire-datom-batch-size+)
        (write-message skey {:type :command-complete :result datoms})
        (copy-out skey datoms c/+wire-datom-batch-size+)))))

(defn- rslice-filter
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [frozen (nth args 2)
          args   (replace {frozen (b/deserialize frozen)} args)
          datoms (apply st/rslice-filter
                        (store server skey (nth args 0) writing?)
                        (rest args))]
      (if (< (count datoms) ^long c/+wire-datom-batch-size+)
        (write-message skey {:type :command-complete :result datoms})
        (copy-out skey datoms c/+wire-datom-batch-size+)))))

(defn- open-kv
  [^Server server ^SelectionKey skey message]
  (open-server-store server skey message c/kv-type))

(defn- close-kv
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (update-db server (nth args 0) #(dissoc % :wlmdb :lock))
    (normal-kv-store-handler close-kv)))

(defn- closed-kv?
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-kv-store-handler closed-kv?)))

(defn- open-dbi
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [{:keys [client-id]} @(.attachment skey)
          db-name             (nth args 0)
          kv                  (lmdb server skey db-name writing?)
          args                (rest args)
          dbi-name            (first args)]
      (apply l/open-dbi kv args)
      (update-client server client-id
                     #(update-in % [:stores db-name :dbis] conj dbi-name)))
    (write-message skey {:type :command-complete})))

(defn- clear-dbi
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-kv-store-handler clear-dbi)))

(defn- drop-dbi
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [{:keys [client-id]} @(.attachment skey)
          db-name             (nth args 0)
          kv                  (lmdb server skey db-name writing?)
          args                (rest args)
          dbi-name            (first args)]
      (l/drop-dbi kv dbi-name)
      (update-client server client-id
                     #(update-in % [:stores db-name :dbis] disj dbi-name)))
    (write-message skey {:type :command-complete})))

(defn- list-dbis
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-kv-store-handler list-dbis)))

;; TODO use LMDB copyfd to write to socket directly
;; However, LMDBJava does not wrap copyfd
(defn- copy
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [[db-name compact?] args
          tf                 (u/tmp-dir (str "copy-" (UUID/randomUUID)))
          path               (Paths/get (str tf u/+separator+ "data.mdb")
                                        (into-array String []))]
      (try
        (l/copy (lmdb server skey db-name writing?) tf compact?)
        (copy-out skey
                  (b/encode-base64 (Files/readAllBytes path))
                  8192)
        (finally (u/delete-files tf))))))

(defn- stat
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-kv-store-handler stat)))

(defn- entries
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-kv-store-handler entries)))

(defn- get-lock
  [^Server server db-name]
  (let [dbs (.-dbs server)]
    (locking dbs
      (or (get-in dbs [db-name :lock])
          (let [lock (Semaphore. 1)]
            (update-db server db-name #(assoc % :lock lock))
            lock)))))

(defn- get-kv-store
  [server db-name]
  (let [s (get-store server db-name)]
    (if (instance? ILMDB s) s (.-lmdb ^Store s))))

(declare write-txn-runner run-calls halt-run)

(defn- open-transact-kv
  [^Server server ^SelectionKey skey {:keys [args] :as message}]
  (wrap-error
    (let [db-name  (nth args 0)
          sys-conn (.-sys-conn server)]
      (wrap-permission
        ::alter ::database (db-eid sys-conn db-name)
        "Don't have permission to alter the database"
        (.acquire ^Semaphore (get-lock server db-name))
        (let [kv-store (get-kv-store server db-name)]
          (locking kv-store
            (update-db server db-name
                       #(assoc % :wlmdb (l/open-transact-kv kv-store)))
            (let [runner (write-txn-runner server db-name kv-store)]
              (write-message skey {:type :command-complete})
              (run-calls runner))))))))

(defn- close-transact-kv
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [db-name  (nth args 0)
          kv-store (get-kv-store server db-name)
          sys-conn (.-sys-conn server)
          dbs      (.-dbs server)]
      (wrap-permission
        ::alter ::database (db-eid sys-conn db-name)
        "Don't have permission to alter the database"
        (l/close-transact-kv kv-store)
        (halt-run (get-in dbs [db-name :runner]))
        (update-db server db-name #(dissoc % :runner :wlmdb))
        (write-message skey {:type :command-complete})
        (.release ^Semaphore (get-in dbs [db-name :lock]))))))

(defn- abort-transact-kv
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [db-name  (nth args 0)
          sys-conn (.-sys-conn server)]
      (wrap-permission
        ::alter ::database (db-eid sys-conn db-name)
        "Don't have permission to alter the database"
        (normal-kv-store-handler abort-transact-kv)))))

(defn- open-transact
  [^Server server ^SelectionKey skey {:keys [args] :as message}]
  (wrap-error
    (let [db-name  (nth args 0)
          sys-conn (.-sys-conn server)]
      (wrap-permission
        ::alter ::database (db-eid sys-conn db-name)
        "Don't have permission to alter the database"
        (.acquire ^Semaphore (get-lock server db-name))
        (let [kv-store (get-kv-store server db-name)]
          (locking kv-store
            (let [wlmdb  (l/open-transact-kv kv-store)
                  ostore (get-store server db-name)
                  wstore (st/transfer ostore wlmdb)
                  runner (write-txn-runner server db-name kv-store)]
              (update-db
                server db-name #(assoc %
                                       :wlmdb wlmdb
                                       :wstore wstore
                                       :wdt-db (db/new-db wstore)))
              (write-message skey {:type :command-complete})
              (run-calls runner))))))))

(defn- close-transact
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [db-name  (nth args 0)
          kv-store (get-kv-store server db-name)
          sys-conn (.-sys-conn server)
          dbs      (.-dbs server)]
      (wrap-permission
        ::alter ::database (db-eid sys-conn db-name)
        "Don't have permission to alter the database"
        (l/close-transact-kv kv-store)
        (halt-run (get-in dbs [db-name :runner]))
        (add-store
          server db-name
          (st/transfer (get-in dbs [db-name :wstore]) kv-store))
        (update-db
          server db-name
          #(dissoc % :wlmdb :wstore :wdt-db :runner))
        (write-message skey {:type :command-complete})
        (.release ^Semaphore (get-in dbs [db-name :lock]))))))

(defn- abort-transact
  [^Server server ^SelectionKey skey {:keys [args]}]
  (wrap-error
    (let [db-name  (nth args 0)
          kv-store (get-kv-store server db-name)
          sys-conn (.-sys-conn server)]
      (wrap-permission
        ::alter ::database (db-eid sys-conn db-name)
        "Don't have permission to alter the database"
        (l/abort-transact-kv kv-store)
        (write-message skey {:type :command-complete})))))

(defn- transact-kv
  [^Server server ^SelectionKey skey {:keys [mode args writing?]}]
  (wrap-error
    (let [db-name  (nth args 0)
          kv-store (get-store server db-name)
          sys-conn (.-sys-conn server)]
      (wrap-permission
        ::alter ::database (db-eid sys-conn db-name)
        "Don't have permission to alter the database"
        (case mode
          :copy-in (do (l/transact-kv kv-store (copy-in server skey))
                       (write-message skey {:type :command-complete}))
          :request (normal-kv-store-handler transact-kv)
          (u/raise "Missing :mode when transacting kv" {}))))))

(defn- get-value
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-kv-store-handler get-value)))

(defn- get-first
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-kv-store-handler get-first)))

(defn- get-range
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [db-name (nth args 0)
          data    (apply l/get-range
                         (lmdb server skey db-name writing?)
                         (rest args))]
      (if (< (count data) ^long c/+wire-datom-batch-size+)
        (write-message skey {:type :command-complete :result data})
        (copy-out skey data c/+wire-datom-batch-size+)))))

(defn- range-count
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (normal-kv-store-handler range-count)))

(defn- get-some
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [frozen (nth args 2)
          args   (replace {frozen (b/deserialize frozen)} args)]
      (normal-kv-store-handler get-some))))

(defn- range-filter
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [frozen  (nth args 2)
          args    (replace {frozen (b/deserialize frozen)} args)
          db-name (nth args 0)
          data    (apply l/range-filter
                         (lmdb server skey db-name writing?)
                         (rest args))]
      (if (< (count data) ^long c/+wire-datom-batch-size+)
        (write-message skey {:type :command-complete :result data})
        (copy-out skey data c/+wire-datom-batch-size+)))))

(defn- range-filter-count
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [frozen (nth args 2)
          args   (replace {frozen (b/deserialize frozen)} args)]
      (normal-kv-store-handler range-filter-count))))

(defn- visit
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [frozen (nth args 2)
          args   (replace {frozen (b/deserialize frozen)} args)]
      (normal-kv-store-handler visit))))

(defn- q
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [[db-name query inputs] args
          db                     (get-db server db-name writing?)
          inputs                 (replace {:remote-db-placeholder db} inputs)
          data                   (apply q/q query inputs)]
      (if (coll? data)
        (if (< (count data) ^long c/+wire-datom-batch-size+)
          (write-message skey {:type :command-complete :result data})
          (copy-out skey data c/+wire-datom-batch-size+))
        (write-message skey {:type :command-complete :result data})))))

(defn- fulltext-datoms
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error
    (let [[db-name query opts] args
          db                   (get-db server db-name writing?)
          data                 (dbq/fulltext db query opts)]
      (if (< (count data) ^long c/+wire-datom-batch-size+)
        (write-message skey {:type :command-complete :result data})
        (copy-out skey data c/+wire-datom-batch-size+)))))

(defn- add-doc
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (search-handler add-doc)))

(defn- remove-doc
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (search-handler remove-doc)))

(defn- clear-docs
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (search-handler clear-docs)))

(defn- doc-indexed?
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (search-handler doc-indexed?)))

(defn- doc-count
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (search-handler doc-count)))

(defn- search
  [^Server server ^SelectionKey skey {:keys [args writing?]}]
  (wrap-error (search-handler search)))

;; END message handlers

(defprotocol IRunner
  "Ensure calls within `with-transaction-kv` run in the same thread that
  runs `open-transact-kv`, otherwise LMDB will deadlock"
  (new-message [this skey message])
  (run-calls [this])
  (halt-run [this]))

(deftype Runner [server kv-store sk msg running?]
  IRunner
  (new-message [_ skey message]
    (vreset! sk skey)
    (vreset! msg message))

  (halt-run [_] (vreset! running? false))

  (run-calls [_]
    (locking kv-store
      (loop []
        (.wait ^Object kv-store)
        (let [{:keys [type] :as message} @msg
              skey                       @sk]
          (message-cases skey type))
        (when @running? (recur))))))

(defn- write-txn-runner
  [^Server server db-name kv-store]
  (let [runner (->Runner server kv-store (volatile! nil)
                         (volatile! nil) (volatile! true))]
    (update-db server db-name #(assoc % :runner runner))
    runner))

(defn- execute
  "Execute a function in a thread from the worker thread pool"
  [^Server server f]
  (.execute ^Executor (.-work-executor server) f))

(defn- handle-writing
  [^Server server ^SelectionKey skey {:keys [args] :as message}]
  (try
    (let [db-name  (nth args 0)
          kv-store (get-kv-store server db-name)
          runner   (get-in (.-dbs server) [db-name :runner])]
      (new-message runner skey message)
      (locking kv-store (.notify kv-store)))
    (catch Exception e
      ;; (stt/print-stack-trace e)
      (error-response skey (str "Error Handling with-transaction message:"
                                (ex-message e)) {}))))

(defn- set-last-active
  [^Server server ^SelectionKey skey]
  (let [{:keys [client-id]} @(.attachment skey)]
    (when client-id
      (update-client server client-id
                     #(assoc % :last-active (System/currentTimeMillis))))))

(defn- handle-message
  [^Server server ^SelectionKey skey fmt msg ]
  (try
    (let [{:keys [type writing?] :as message} (p/read-value fmt msg)]
      (log/debug "Message received:" (dissoc message :password :args))
      (set-last-active server skey)
      (if writing?
        (handle-writing server skey message)
        (message-cases skey type)))
    (catch Exception e
      ;; (stt/print-stack-trace e)
      (log/error "Error Handling message:" e))))

(defn- handle-read
  [^Server server ^SelectionKey skey]
  (try
    (let [state                         (.attachment skey)
          {:keys [^ByteBuffer read-bf]} @state
          capacity                      (.capacity read-bf)
          ^SocketChannel ch             (.channel skey)
          ^int readn                    (p/read-ch ch read-bf)]
      (cond
        (> readn 0)  (if (= (.position read-bf) capacity)
                       (let [size (* ^long c/+buffer-grow-factor+ capacity)
                             bf   (b/allocate-buffer size)]
                         (.flip read-bf)
                         (b/buffer-transfer read-bf bf)
                         (vswap! state assoc :read-bf bf))
                       (p/extract-message
                         read-bf
                         (fn [fmt msg]
                           (execute
                             server #(handle-message server skey fmt msg)))))
        (= readn 0)  :continue
        (= readn -1) (.close ch)))
    (catch java.io.IOException e
      (if (s/includes? (ex-message e) "Connection reset by peer")
        (.close (.channel skey))
        (log/error "Read IOException:" (ex-message e))))
    (catch Exception e
      ;; (stt/print-stack-trace e)
      (log/error "Read error:" (ex-message e)))))

(defn- handle-registration
  [^Server server]
  (let [^Selector selector           (.-selector server)
        ^ConcurrentLinkedQueue queue (.-register-queue server)]
    (loop []
      (when-let [[^SocketChannel ch ops state] (.poll queue)]
        (.register ch selector ops state)
        (log/debug "Registered client" (@state :client-id))
        (recur)))))

(defn- remove-idle-sessions
  [^Server server]
  (let [timeout (.-idle-timeout server)
        clients (.-clients server)]
    (doseq [[client-id session] clients
            :let                [{:keys [last-active]} session]
            :when               last-active]
      (when (< ^long timeout
               (- (System/currentTimeMillis) ^long last-active))
        (disconnect-client* server client-id)))))

(defn- event-loop
  [^Server server]
  (let [^Selector selector     (.-selector server)
        ^AtomicBoolean running (.-running server)]
    (loop []
      (when (.get running)
        (remove-idle-sessions server)
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

(defn create
  "Create a Datalevin server. Initially not running, call `start` to run."
  [{:keys [port root idle-timeout verbose]
    :or   {port         8898
           root         "/var/lib/datalevin"
           idle-timeout 86400000  ;; 24 hours
           verbose      false}}]
  {:pre [(int? port) (not (s/blank? root))]}
  (try
    (log/set-min-level! (if verbose :debug :info))
    (let [^ServerSocketChannel server-socket (open-port port)
          ^Selector selector                 (Selector/open)
          running                            (AtomicBoolean. false)
          sys-conn                           (init-sys-db root)
          clients                            (load-sessions sys-conn)
          dbs                                (ConcurrentHashMap.)]
      (reopen-dbs root clients dbs)
      (.register server-socket selector SelectionKey/OP_ACCEPT)
      (->Server running
                port
                root
                idle-timeout
                server-socket
                selector
                (ConcurrentLinkedQueue.)
                (Executors/newCachedThreadPool) ; with-txn may be many
                sys-conn
                clients
                dbs))
    (catch Exception e
      (u/raise "Error creating server:" (ex-message e) {}))))
