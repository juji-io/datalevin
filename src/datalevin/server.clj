(ns datalevin.server
  (:require [datalevin.util :as u]
            [datalevin.bits :as b]
            [datalevin.db :as d]
            [datalevin.constants :as c]
            [datalevin.interpret :as i]
            [clojure.string :as s])
  (:import [java.io BufferedReader PushbackReader PrintWriter InputStreamReader]
           [java.nio.charset StandardCharsets]
           [java.net ServerSocket Socket]
           [java.security SecureRandom]
           [java.util.concurrent Executors ThreadPoolExecutor]
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
     :or   {ops-limit   3 ; takes about 200ms to hash w/ these settings
            mem-limit   66536
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

(defn- authenticate [msg]
  )

(defn- prepare-db [msg]
  )

(defn- fatal-error-response
  [writer error-msg]
  (u/write-message writer {:type    :error-response
                           :message error-msg})
  false)

(defn- success-response
  [writer response]
  (u/write-message writer response)
  true)

(defn- handle-message
  "handle an incoming message, return true if connection is to be kept, otherwise
  return false"
  [writer {:keys [type] :as msg}]
  (case type
    :authentication
    (if-let [cid (authenticate msg)]
      (do (u/write-message writer {:type :authentication-ok
                                   :cid  cid})
          (if (prepare-db msg)
            (success-response writer {:type :ready-for-query})
            (fatal-error-response writer "Unable to prepare the database")))
      (fatal-error-response writer "Failed to authenticate"))))

(defn handle-connection
  [server-db ^Socket socket]
  (try
    (with-open [reader (BufferedReader.
                         (InputStreamReader. (.getInputStream socket))
                         c/+message-buffer-size+)
                writer (PrintWriter. (.getOutputStream socket) true)]
      (loop []
        (when-let [msg (u/read-message reader)]
          (when (handle-message writer msg)
            (recur)))))
    (catch Exception e
      (u/raise "Error handling connection:" (ex-message e) {}))
    (finally (.close socket))))

(defn- open-server-db
  [port root])

(defn start
  [{:keys [port root]}]
  (let [server-db     (open-server-db port root)
        cores         (.availableProcessors (Runtime/getRuntime))
        executor      (Executors/newFixedThreadPool cores)
        server-socket (ServerSocket. port)]
    (println "Datalevin server started on port" port)
    (loop []
      (let [client-socket (.accept server-socket)]
        (.execute executor #(handle-connection server-db client-socket))
        (recur)))))
