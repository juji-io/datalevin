(ns datalevin.remote
  "Proxy for remote stores"
  (:require [datalevin.util :as u]
            [datalevin.constants :as c]
            [datalevin.client :as cl]
            [datalevin.storage :as s]
            [datalevin.lmdb :as l]
            [datalevin.datom :as d]
            [datalevin.protocol :as p]
            [taoensso.nippy :as nippy]
            [com.rpl.nippy-serializable-fn]
            [clojure.string :as str])
  (:import [datalevin.client Client]
           [datalevin.storage IStore]
           [datalevin.lmdb ILMDB]
           [datalevin.datom Datom]
           [java.nio.charset StandardCharsets]
           [java.nio ByteBuffer BufferOverflowException]
           [java.nio.channels SocketChannel]
           [java.util ArrayList UUID]
           [java.net InetSocketAddress StandardSocketOptions URI]))

(defmacro normal-request
  "Request to remote store and returns results. Does not use the
  copy-in protocol"
  [call args]
  `(let [{:keys [~'type ~'message ~'result]}
         (cl/request ~'client {:type ~call :args ~args})]
     (if (= ~'type :error-response)
       (u/raise "Unable to access remote db:" ~'message {:uri ~'uri})
       ~'result)))

;; remote datalog store

(deftype DatalogStore [^String uri ^Client client]
  IStore
  (dir [_] uri)

  (close [_] (normal-request :close nil))

  (closed? [_] (normal-request :closed? nil))

  (last-modified [_] (normal-request :last-modified nil))

  (schema [_] (normal-request :schema nil))

  (rschema [_] (normal-request :rschema nil))

  (set-schema [_ new-schema] (normal-request :set-schema [new-schema]))

  (init-max-eid [_] (normal-request :init-max-eid nil))

  (swap-attr [this attr f]
    (s/swap-attr this attr f nil nil))
  (swap-attr [this attr f x]
    (s/swap-attr this attr f x nil))
  (swap-attr [_ attr f x y]
    (let [frozen-f (nippy/freeze f)]
      (normal-request :swap-attr [attr frozen-f x y])))

  (datom-count [_ index] (normal-request :datom-count [index]))

  (load-datoms [_ datoms]
    (let [{:keys [type message]}
          (if (< (count datoms) c/+wire-datom-batch-size+)
            (cl/request client {:type :load-datoms :mode :request
                                :args [datoms]})
            (cl/copy-in client {:type :load-datoms :mode :copy-in}
                        datoms c/+wire-datom-batch-size+))]
      (when (= type :error-response)
        (u/raise "Error loading datoms to server:" message {:uri uri}))))

  (fetch [_ datom] (normal-request :fetch [datom]))

  (populated? [_ index low-datom high-datom]
    (normal-request :populated? [index low-datom high-datom]))

  (size [_ index low-datom high-datom]
    (normal-request :size [index low-datom high-datom]))

  (head [_ index low-datom high-datom]
    (normal-request :head [index low-datom high-datom]))

  (slice [_ index low-datom high-datom]
    (normal-request :slice [index low-datom high-datom]))

  (rslice [_ index high-datom low-datom]
    (normal-request :rslice [index high-datom low-datom]))

  (size-filter [_ index pred low-datom high-datom]
    (let [frozen-pred (nippy/freeze pred)]
      (normal-request :size-filter
                      [index frozen-pred low-datom high-datom])))

  (head-filter [_ index pred low-datom high-datom]
    (let [frozen-pred (nippy/freeze pred)]
      (normal-request :head-filter
                      [index frozen-pred low-datom high-datom])))

  (slice-filter [_ index pred low-datom high-datom]
    (let [frozen-pred (nippy/freeze pred)]
      (normal-request :slice-filter
                      [index frozen-pred low-datom high-datom])))

  (rslice-filter [_ index pred high-datom low-datom]
    (let [frozen-pred (nippy/freeze pred)]
      (normal-request :rslice-filter
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

;; kv store

(deftype KVStore [^String uri ^Client client]
  ILMDB

  (dir [_] uri)

  (close-kv [_] (normal-request :close-kv nil))

  (closed-kv? [_] (normal-request :closed-kv? nil))

  (open-dbi [db dbi-name]
    (l/open-dbi db dbi-name c/+max-key-size+ c/+default-val-size+))
  (open-dbi [db dbi-name key-size]
    (l/open-dbi db dbi-name key-size c/+default-val-size+))
  (open-dbi [_ dbi-name key-size val-size]
    (normal-request :open-dbi [dbi-name key-size val-size]))

  ;; (clear-dbi [db dbi-name]
  ;;   "Clear data in the DBI (i.e sub-db), but leave it open")
  ;; (drop-dbi [db dbi-name]
  ;;   "Clear data in the DBI (i.e. sub-db), then delete it")
  ;; (list-dbis [db] "List the names of the sub-databases")
  ;; (copy
  ;;   [db dest]
  ;;   [db dest compact?]
  ;;   "Copy the database to a destination directory path, optionally compact
  ;;    while copying, default not compact. ")
  ;; (stat
  ;;   [db]
  ;;   [db dbi-name]
  ;;   "Return the statitics of the unnamed top level database or a named DBI
  ;;    (i.e. sub-database) as a map")
  ;; (entries [db dbi-name]
  ;;   "Get the number of data entries in a DBI (i.e. sub-db)")
  ;; (transact-kv [db txs]
  ;;   "Update DB, insert or delete key value pairs.")
  ;; (get-value
  ;;   [db dbi-name k]
  ;;   [db dbi-name k k-type]
  ;;   [db dbi-name k k-type v-type]
  ;;   [db dbi-name k k-type v-type ignore-key?]
  ;;   "Get kv pair of the specified key `k`. ")
  ;; (get-first
  ;;   [db dbi-name k-range]
  ;;   [db dbi-name k-range k-type]
  ;;   [db dbi-name k-range k-type v-type]
  ;;   [db dbi-name k-range k-type v-type ignore-key?]
  ;;   "Return the first kv pair in the specified key range;")
  ;; (get-range
  ;;   [db dbi-name k-range]
  ;;   [db dbi-name k-range k-type]
  ;;   [db dbi-name k-range k-type v-type]
  ;;   [db dbi-name k-range k-type v-type ignore-key?]
  ;;   "Return a seq of kv pairs in the specified key range;")
  ;; (range-count
  ;;   [db dbi-name k-range]
  ;;   [db dbi-name k-range k-type]
  ;;   "Return the number of kv pairs in the specified key range, does not process
  ;;    the kv pairs.")
  ;; (get-some
  ;;   [db dbi-name pred k-range]
  ;;   [db dbi-name pred k-range k-type]
  ;;   [db dbi-name pred k-range k-type v-type]
  ;;   [db dbi-name pred k-range k-type v-type ignore-key?]
  ;;   "Return the first kv pair that has logical true value of `(pred x)`")
  ;; (range-filter
  ;;   [db dbi-name pred k-range]
  ;;   [db dbi-name pred k-range k-type]
  ;;   [db dbi-name pred k-range k-type v-type]
  ;;   [db dbi-name pred k-range k-type v-type ignore-key?]
  ;;   "Return a seq of kv pair in the specified key range, for only those
  ;;    return true value for `(pred x)`.")
  ;; (range-filter-count
  ;;   [db dbi-name pred k-range]
  ;;   [db dbi-name pred k-range k-type]
  ;;   "Return the number of kv pairs in the specified key range, for only those
  ;;    return true value for `(pred x)`")
  )

(defn open-kv
  "Open a remote kv store."
  [uri-str]
  (let [uri     (URI. uri-str)
        uri-str (str uri-str
                     (if (cl/parse-query uri) "&" "?")
                     "store=" c/db-store-kv)]
    (assert (cl/parse-db uri) "URI should contain a database name")
    (->KVStore (redact-uri uri-str) (cl/new-client uri-str))))

(comment

  (require '[clj-memory-meter.core :as mm])

  (def store (open-kv "dtlv://datalevin:datalevin@localhost/remote"))

  (mm/measure store)

  (l/closed-kv? store)

  (l/close-kv store)

  (l/open-dbi store "a")

  )
