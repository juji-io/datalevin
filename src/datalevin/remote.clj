(ns datalevin.remote
  "Remote storage on a server"
  (:require [datalevin.util :as u]
            [datalevin.constants :as c]
            [datalevin.client :as cl]
            [datalevin.bits :as b]
            [datalevin.protocol :as p])
  (:import [datalevin.client Client]
           [datalevin.storage IStore]
           [java.nio.charset StandardCharsets]
           [java.nio ByteBuffer BufferOverflowException]
           [java.nio.channels SocketChannel]
           [java.util ArrayList UUID]
           [java.net InetSocketAddress StandardSocketOptions URI]))

(deftype RemoteStore [^String uri
                      ^Client client]
  IStore
  (dir [_]
    uri)
  (close [_]
    (cl/stop client))
  (closed? [_]
    (cl/stopped? client))
  (schema [_]
    (let [{:keys [type schema]} (cl/request client {:type :schema})]
      (if (= type :error-response)
        (u/raise "Unable to get schema of remote db:" uri {})
        schema))))

(defn open
  "Open a remote store"
  ([uri-str]
   (open uri-str nil))
  ([uri-str schema]
   (let [uri (URI. uri-str)]
     (assert (cl/parse-db uri) "URI should contain a database name")
     (->RemoteStore uri-str (cl/new-client uri-str schema)))))

(comment

  (open "dtlv://datalevin:datalevin@localhost/remote")

  )
