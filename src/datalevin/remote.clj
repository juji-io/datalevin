(ns datalevin.remote
  "Proxy for remote storage"
  (:require [datalevin.util :as u]
            [datalevin.constants :as c]
            [datalevin.client :as cl]
            [datalevin.storage :as s]
            [datalevin.datom :as d]
            [datalevin.protocol :as p]
            [taoensso.nippy :as nippy]
            [com.rpl.nippy-serializable-fn]
            [clojure.string :as str])
  (:import [datalevin.client Client]
           [datalevin.storage IStore]
           [datalevin.datom Datom]
           [java.nio.charset StandardCharsets]
           [java.nio ByteBuffer BufferOverflowException]
           [java.nio.channels SocketChannel]
           [java.util ArrayList UUID]
           [java.net InetSocketAddress StandardSocketOptions URI]))

(defmacro normal-dt-store-request
  "Request to Datalog store and returns results. Does not use the
  copy-in protocol"
  [call args]
  `(let [{:keys [~'type ~'message ~'result]}
         (cl/request ~'client {:type ~call :args ~args})]
     (if (= ~'type :error-response)
       (u/raise "Unable to access remote db:" ~'message {:uri ~'uri})
       ~'result)))

(deftype DatalogStore [^String uri ^Client client]
  IStore
  (dir [_] uri)

  (close [_] (normal-dt-store-request :close nil))

  (closed? [_] (normal-dt-store-request :closed? nil))

  (last-modified [_] (normal-dt-store-request :last-modified nil))

  (schema [_] (normal-dt-store-request :schema nil))

  (rschema [_] (normal-dt-store-request :rschema nil))

  (set-schema [_ new-schema] (normal-dt-store-request :set-schema [new-schema]))

  (init-max-eid [_] (normal-dt-store-request :init-max-eid nil))

  (datom-count [_ index] (normal-dt-store-request :datom-count [index]))

  (load-datoms [_ datoms]
    (let [{:keys [type message]}
          (cl/copy-in client {:type :load-datoms}
                      datoms c/+wire-datom-batch-size+)]
      (when (= type :error-response)
        (u/raise "Error loading datoms to server:" message {:uri uri}))))

  (fetch [_ datom] (normal-dt-store-request :fetch [datom]))

  (populated? [_ index low-datom high-datom]
    (normal-dt-store-request :populated? [index low-datom high-datom]))

  (size [_ index low-datom high-datom]
    (normal-dt-store-request :size [index low-datom high-datom]))

  (head [_ index low-datom high-datom]
    (normal-dt-store-request :head [index low-datom high-datom]))

  (slice [_ index low-datom high-datom]
    (normal-dt-store-request :slice [index low-datom high-datom]))

  (rslice [_ index high-datom low-datom]
    (normal-dt-store-request :rslice [index high-datom low-datom]))

  (size-filter [_ index pred low-datom high-datom]
    (let [frozen-pred (nippy/freeze pred)]
      (normal-dt-store-request :size-filter
                               [index frozen-pred low-datom high-datom])))

  (head-filter [_ index pred low-datom high-datom]
    (let [frozen-pred (nippy/freeze pred)]
      (normal-dt-store-request :size-filter
                               [index frozen-pred low-datom high-datom])))

  (slice-filter [_ index pred low-datom high-datom]
    (let [frozen-pred (nippy/freeze pred)]
      (normal-dt-store-request :size-filter
                               [index frozen-pred low-datom high-datom])))

  (rslice-filter [_ index pred high-datom low-datom]
    (let [frozen-pred (nippy/freeze pred)]
      (normal-dt-store-request :size-filter
                               [index frozen-pred high-datom low-datom]))))

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

  (def store (open "dtlv://datalevin:datalevin@localhost/remote"))

  (s/load-datoms store [(d/datom 5 :name "Ola" 223)
                        (d/datom 6 :name "Jimmy" 223)])

  (s/fetch store (d/datom 3 :name "Yunyao"))

  (s/last-modified store)

  (s/datom-count store :eavt)

  (s/closed? store)

  (s/populated? store :eavt (d/datom 1 :name "Boyan") (d/datom 1 :name "Boyan"))

  (s/close store)

  (s/size store :eavt (d/datom 1 :name "Boyan") (d/datom 1 :name "Boyan"))

  (s/schema store)

  (s/rschema store)

  (s/set-schema store {:aka  {:db/cardinality :db.cardinality/many}
                       :name {:db/valueType :db.type/string
                              :db/unique    :db.unique/identity}})

  )
