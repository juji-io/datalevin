(ns datalevin.client
  (:require [datalevin.util :as u]
            [clojure.string :as s])
  (:import [java.io BufferedReader PushbackReader PrintWriter InputStreamReader
            OutputStreamWriter]
           [java.nio.charset StandardCharsets]
           [java.net Socket]))

(defn- open-socket
  [^String host port]
  (try
    (Socket. host (int port))
    (catch Exception e
      (u/raise "Unable to connect to server: " (ex-message e)
               {:host host :port port}))))

(defn connect
  "connect to server"
  [host port]
  (with-open [^Socket socket (open-socket host port)
              in             (InputStreamReader. (.getInputStream socket))
              out            (PrintWriter. (.getOutputStream socket) true)]
    (println "opened socket")
    (.println out "(+ 1 2 3)")
    (println "sent code")
    (println (s/join (doall (line-seq (BufferedReader. in)))))))

(connect "localhost" 8898)
