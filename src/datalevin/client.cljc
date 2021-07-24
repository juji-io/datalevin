(ns datalevin.client
  (:require [datalevin.util :as u])
  (:import [java.io BufferedReader PushbackReader IOException InputStreamReader
            OutputStreamWriter]
           [java.nio.charset StandardCharsets]
           [java.net Socket]))

(defn connect
  "connect to server"
  [^String host port]
  (try
    (let [socket (Socket. host (int port))])
    (catch Exception e
      (u/raise "Unable to connect to server: " (ex-message e)))))
