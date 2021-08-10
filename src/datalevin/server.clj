(ns datalevin.server
  "Non-blocking event driven server"
  (:require [datalevin.util :as u]
            [datalevin.core :as d]
            [datalevin.protocol :as p]
            [datalevin.storage :as st]
            [datalevin.constants :as c])
  (:import [java.nio.charset StandardCharsets]
           [java.nio ByteBuffer]
           [java.nio.channels Selector SelectionKey ServerSocketChannel
            SocketChannel]
           [java.net InetSocketAddress]
           [java.security SecureRandom]
           [java.util Iterator]
           [java.util.concurrent Executors Executor ThreadPoolExecutor]
           [datalevin.db DB]
           [org.bouncycastle.crypto.generators Argon2BytesGenerator]
           [org.bouncycastle.crypto.params Argon2Parameters
            Argon2Parameters$Builder]))

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
;; { selector, work-executor, sys-conn}
(def resources (atom {}))

(defn- execute
  [f]
  (.execute ^Executor (@resources :work-executor) f))

(defn- authenticate
  [{:keys [username password database]}]
  )

(defn- prepare-db [msg]
  )

#_(defn- fatal-error-response
    [writer error-msg]
    (u/write-message writer {:type    :error-response
                             :message error-msg})
    false)

#_(defn- success-response
   [writer response]
   (u/write-message writer response)
   true)

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
                 ;; { read-bf, write-bf, client-id, db-name, conn, ... }
                 (atom {:read-bf  (ByteBuffer/allocateDirect
                                    c/+default-buffer-size+)
                        :write-bf (ByteBuffer/allocateDirect
                                    c/+default-buffer-size+)})))))

(defn- handle-message
  [skey type msg ]
  (println "message received:" (p/read-value type msg))
  #_(case type
      :authentication
      (if-let [cid (authenticate msg)]
        (do (u/write-message writer {:type :authentication-ok
                                     :cid  cid})
            (if (prepare-db msg)
              (success-response writer {:type :ready-for-query})
              (fatal-error-response writer "Unable to prepare the database")))
        (fatal-error-response writer "Failed to authenticate"))))

(defn- process-read
  [^SelectionKey skey]
  (let [{:keys [^ByteBuffer read-bf]} @(.attachment skey)]
    (p/segment-messages read-bf
                        (fn [type msg]
                          (execute #(handle-message skey type msg))))))

(defn- handle-read
  [^SelectionKey skey]
  (let [{:keys [^ByteBuffer read-bf]} @(.attachment skey)
        ch                            ^SocketChannel (.channel skey)
        readn                         (.read ch read-bf)]
    (cond
      (= readn 0)  :continue
      (> readn 0)  (process-read skey)
      (= readn -1) (.close ch))))

(defn- handle-write
  [^SelectionKey skey]
  )

(defn- init-sys-db
  [root]
  (let [sys-conn (d/get-conn (str root u/+separator+ c/system-dir)
                             c/system-schema)]
    (swap! resources assoc :sys-conn sys-conn)
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

(defn- shutdown
  []
  (let [{:keys [^Selector selector work-executor sys-conn]} @resources]
    (when selector
      (doseq [^SelectionKey skey (.keys selector)]
        (.close ^SocketChannel (.channel skey)))
      (when (.isOpen selector)(.close selector)))
    (.shutdown ^ThreadPoolExecutor work-executor)
    (d/close sys-conn))
  (println "Bye."))

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
      (println "Datalevin server started on port" port)
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
