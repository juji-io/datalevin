(ns datalevin.server
  (:require [datalevin.util :as u]
            [datalevin.bits :as b]
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

(defn handle-connection
  [^Socket socket]
  (with-open [out (PrintWriter. (.getOutputStream socket) true)
              in  (InputStreamReader. (.getInputStream socket))]
    (let [code (s/join (doall (line-seq (BufferedReader. in))))
          _    (println "got code: " code)
          res  (with-out-str (i/exec-code code))
          _    (println "got res: " res)
          ]
      (.println out res))))

(defn start
  [{:keys [port]}]
  (let [server-socket (ServerSocket. port)
        cores         (.availableProcessors (Runtime/getRuntime))
        executor      (Executors/newFixedThreadPool cores)]
    (loop []
      (let [client-socket (.accept server-socket)]
        (.execute executor #(handle-connection client-socket))
        (recur)))))
