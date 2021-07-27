(ns datalevin.client
  (:require [datalevin.util :as u]
            [datalevin.constants :as c]
            [clojure.string :as s])
  (:import [java.io BufferedReader PrintWriter InputStreamReader]
           [java.nio.charset StandardCharsets]
           [java.net Socket]))

(defn- open-socket
  [^String host port]
  (try
    (Socket. host (int port))
    (catch Exception e
      (u/raise "Unable to connect to server: " (ex-message e)
               {:host host :port port}))))

(defn- connect
  "connect to server, return a connection map"
  [host port]
  (when-let  [^Socket socket (open-socket host port)]
    {:socket socket
     :reader (BufferedReader.
               (InputStreamReader. (.getInputStream socket))
               c/+message-buffer-size+)
     :writer (PrintWriter. (.getOutputStream socket) true)}))

(defn close
  "close the connection"
  [{:keys [socket]}]
  (.close ^Socket socket))

(defn- authenticate
  "Send an authenticate message to server, and wait to receive the response.
  If authentication succeeds,  return a connection id.
  Otherwise, close connection, raise exception"
  [{:keys [reader writer] :as conn} username password database]
  (u/write-message writer {:type     :authentication
                           :username username
                           :password password
                           :database database})
  (let [{:keys [type cid]} (u/read-message reader)]
    (if (= type :authentication-ok)
      (assoc conn :cid cid)
      (do (close conn)
          (u/raise "Authentication failure" {})))))

(defn startup
  "Attempt to connect and authenticate to a server, return a connection map
  if successful, otherwise raise exception"
  [host port username password database]
  (when-let [conn (connect host port)]
    (authenticate conn username password database)))
