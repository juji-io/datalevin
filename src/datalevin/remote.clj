(ns datalevin.remote
  "Proxy for the remote storage"
  (:require [datalevin.util :as u]
            [datalevin.constants :as c]
            [datalevin.client :as cl]
            [datalevin.storage :as s]
            [datalevin.bits :as b]
            [datalevin.datom :as d]
            [datalevin.protocol :as p]
            [clojure.string :as str])
  (:import [datalevin.client Client]
           [datalevin.storage IStore]
           [java.nio.charset StandardCharsets]
           [java.nio ByteBuffer BufferOverflowException]
           [java.nio.channels SocketChannel]
           [java.util ArrayList UUID]
           [java.net InetSocketAddress StandardSocketOptions URI]))



(deftype DatalogStore [^String uri
                       ^Client client]
  IStore
  (dir [_]
    uri)
  (close [_]
    (let [{:keys [type message]} (cl/request client {:type :close})]
      (when (= type :error-response)
        (u/raise "Unable to close remote db:" message {:uri uri}))))
  (closed? [_]
    (let [{:keys [type message closed?]} (cl/request client {:type :closed?})]
      (if (= type :error-response)
        (u/raise "Unable to check remote db:" message {:uri uri})
        closed?)))
  (schema [_]
    (let [{:keys [type message schema]} (cl/request client {:type :schema})]
      (if (= type :error-response)
        (u/raise "Unable to get schema of remote db:" message {:uri uri})
        schema)))
  (set-schema [_ new-schema]
    (let [{:keys [type message schema]}
          (cl/request client {:type :set-schema :new-schema new-schema})]
      (if (= type :error-response)
        (u/raise "Unable to set schema of remote db:" message {:uri uri})
        schema)))
  (init-max-eid [_]
    (let [{:keys [type message max-eid]}
          (cl/request client {:type :init-max-eid})]
      (if (= type :error-response)
        (u/raise "Unable to get max eid of remote db:" message {:uri uri})
        max-eid)))
  (datom-count [_ index]
    (let [{:keys [type message datom-count]}
          (cl/request client {:type :datom-count :index index})]
      (if (= type :error-response)
        (u/raise "Unable to get datom-count of remote db:" message {:uri uri})
        datom-count)))
  (load-datoms [_ datoms]
    (let [{:keys [type message]}
          (cl/copy-in client {:type :load-datoms}
                      datoms c/+wire-datom-batch-size+)]
      (when (= type :error-response)
        (u/raise "Error loading datoms to server:" message {:uri uri}))))
  )

(defn- redact-uri
  [s]
  (if (p/dtlv-uri? s)
    (str/replace-first s #"(dtlv://.+):(.+)@" "$1:***@")
    s))

(defn open
  "Open a remote Datalog store"
  ([uri-str]
   (open uri-str nil))
  ([uri-str schema]
   (let [uri (URI. uri-str)]
     (assert (cl/parse-db uri) "URI should contain a database name")
     (->DatalogStore (redact-uri uri-str)
                     (cl/new-client uri-str schema)))))

(comment

  (def store (open "dtlv://datalevin:datalevin@localhost/remote1"))

  (s/load-datoms store [(d/datom 9 :name "Boyan" 223)
                        (d/datom 10 :name "Huahai" 223)])

  (s/datom-count store :eavt)

  (s/close store)

  (instance? IStore store)

  (s/closed? store)

  (s/schema store)

  (s/set-schema store {:aka  {:db/cardinality :db.cardinality/many}
                       :name {:db/valueType :db.type/string
                              :db/unique    :db.unique/identity}})

  )
