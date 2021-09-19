(ns datalevin.remote
  "Proxy for remote stores"
  (:require [datalevin.util :as u]
            [datalevin.constants :as c]
            [datalevin.client :as cl]
            [datalevin.storage :as s]
            [datalevin.lmdb :as l]
            [taoensso.nippy :as nippy]
            [clojure.string :as str])
  (:import [datalevin.client Client]
           [datalevin.storage IStore]
           [datalevin.lmdb ILMDB]
           [java.nio.file Files Paths StandardOpenOption LinkOption]
           [java.nio.file.attribute PosixFilePermissions FileAttribute]
           [java.net URI]))

(defn dtlv-uri?
  "return true if the given string is a Datalevin connection string"
  [s]
  (when s (str/starts-with? s "dtlv://")))

(defn redact-uri
  [s]
  (if (dtlv-uri? s)
    (str/replace-first s #"(dtlv://.+):(.+)@" "$1:***@")
    s))

;; remote datalog store

(defprotocol IRemoteQuery
  (q [store query inputs]
    "For special case of queries with a single remote store as source"))

(deftype DatalogStore [^String uri
                       ^String db-name
                       ^Client client]
  IStore
  (dir [_] uri)

  (close [_]
    (when-not (cl/disconnected? client)
      (cl/normal-request client :close [db-name])
      (cl/disconnect client)))

  (closed? [_]
    (if (cl/disconnected? client)
      true
      (cl/normal-request client :closed? [db-name])))

  (last-modified [_]
    (cl/normal-request client :last-modified [db-name]))

  (schema [_] (cl/normal-request client :schema [db-name]))

  (rschema [_] (cl/normal-request client :rschema [db-name]))

  (set-schema [_ new-schema]
    (cl/normal-request client :set-schema [db-name new-schema]))

  (init-max-eid [_]
    (cl/normal-request client :init-max-eid [db-name]))

  (swap-attr [this attr f]
    (s/swap-attr this attr f nil nil))
  (swap-attr [this attr f x]
    (s/swap-attr this attr f x nil))
  (swap-attr [_ attr f x y]
    (let [frozen-f (nippy/fast-freeze f)]
      (cl/normal-request client :swap-attr [db-name attr frozen-f x y])))

  (datom-count [_ index]
    (cl/normal-request client :datom-count [db-name index]))

  (load-datoms [_ datoms]
    (let [{:keys [type message]}
          (if (< (count datoms) c/+wire-datom-batch-size+)
            (cl/request client {:type :load-datoms
                                :mode :request
                                :args [db-name datoms]})
            (cl/copy-in client {:type :load-datoms
                                :mode :copy-in
                                :args [db-name]}
                        datoms c/+wire-datom-batch-size+))]
      (when (= type :error-response)
        (u/raise "Error loading datoms to server:" message {:uri uri}))))

  (fetch [_ datom] (cl/normal-request client :fetch [db-name datom]))

  (populated? [_ index low-datom high-datom]
    (cl/normal-request client :populated? [db-name index low-datom high-datom]))

  (size [_ index low-datom high-datom]
    (cl/normal-request client :size [db-name index low-datom high-datom]))

  (head [_ index low-datom high-datom]
    (cl/normal-request client :head [db-name index low-datom high-datom]))

  (tail [_ index high-datom low-datom]
    (cl/normal-request client :tail [db-name index high-datom low-datom]))

  (slice [_ index low-datom high-datom]
    (cl/normal-request client :slice [db-name index low-datom high-datom]))

  (rslice [_ index high-datom low-datom]
    (cl/normal-request client :rslice [db-name index high-datom low-datom]))

  (size-filter [_ index pred low-datom high-datom]
    (let [frozen-pred (nippy/fast-freeze pred)]
      (cl/normal-request client :size-filter
                         [db-name index frozen-pred low-datom high-datom])))

  (head-filter [_ index pred low-datom high-datom]
    (let [frozen-pred (nippy/fast-freeze pred)]
      (cl/normal-request client :head-filter
                         [db-name index frozen-pred low-datom high-datom])))

  (tail-filter [_ index pred high-datom low-datom]
    (let [frozen-pred (nippy/fast-freeze pred)]
      (cl/normal-request client :tail-filter
                         [db-name index frozen-pred high-datom low-datom])))

  (slice-filter [_ index pred low-datom high-datom]
    (let [frozen-pred (nippy/fast-freeze pred)]
      (cl/normal-request client :slice-filter
                         [db-name index frozen-pred low-datom high-datom])))

  (rslice-filter [_ index pred high-datom low-datom]
    (let [frozen-pred (nippy/fast-freeze pred)]
      (cl/normal-request client :rslice-filter
                         [db-name index frozen-pred high-datom low-datom])))

  IRemoteQuery
  (q [_ query inputs]
    (cl/normal-request client :q [db-name query inputs])))

(defn open
  "Open a remote Datalog store"
  ([uri-str]
   (open uri-str nil))
  ([uri-str schema]
   (let [uri (URI. uri-str)]
     (if-let [db-name (cl/parse-db uri)]
       (let [client (cl/new-client uri-str)
             store  (or (get (cl/parse-query uri) "store")
                        c/db-store-datalog)]
         (cl/open-database client db-name store schema)
         (->DatalogStore uri-str db-name client))
       (u/raise "URI should contain a database name" {})))))

;; remote kv store

(deftype KVStore [^String uri
                  ^String db-name
                  ^Client client]
  ILMDB
  (dir [_] uri)

  (close-kv [_]
    (cl/normal-request client :close-kv [db-name])
    (cl/disconnect client))

  (closed-kv? [_]
    (if (cl/disconnected? client)
      true
      (cl/normal-request client :closed-kv? [db-name])))

  (open-dbi [db dbi-name]
    (l/open-dbi db dbi-name c/+max-key-size+ c/+default-val-size+))
  (open-dbi [db dbi-name key-size]
    (l/open-dbi db dbi-name key-size c/+default-val-size+))
  (open-dbi [_ dbi-name key-size val-size]
    (cl/normal-request client :open-dbi [db-name dbi-name key-size val-size]))

  (clear-dbi [db dbi-name]
    (cl/normal-request client :clear-dbi [db-name dbi-name]))

  (drop-dbi [db dbi-name]
    (cl/normal-request client :drop-dbi [db-name dbi-name]))

  (list-dbis [db] (cl/normal-request client :list-dbis [db-name]))

  (copy [db dest] (l/copy db dest false))
  (copy [db dest compact?]
    (let [bs   (->> (cl/normal-request client :copy [db-name compact?])
                    (apply str)
                    u/decode-base64)
          dir  (Paths/get dest (into-array String []))
          file (Paths/get (str dest u/+separator+ "data.mdb")
                          (into-array String []))]
      (when-not (Files/exists dir (into-array LinkOption []))
        (u/create-dirs dest))
      (Files/write file ^bytes bs
                   ^"[Ljava.nio.file.StandardOpenOption;"
                   (into-array StandardOpenOption []))))

  (stat [db] (l/stat db nil))
  (stat [db dbi-name] (cl/normal-request client :stat [db-name dbi-name]))

  (entries [db dbi-name] (cl/normal-request client :entries [db-name dbi-name]))

  (transact-kv [db txs]
    (let [{:keys [type message]}
          (if (< (count txs) c/+wire-datom-batch-size+)
            (cl/request client {:type :transact-kv
                                :mode :request
                                :args [db-name txs]})
            (cl/copy-in client {:type :transact-kv
                                :mode :copy-in
                                :args [db-name]}
                        txs c/+wire-datom-batch-size+))]
      (when (= type :error-response)
        (u/raise "Error transacting kv to server:" message {:uri uri}))))

  (get-value [db dbi-name k]
    (l/get-value db dbi-name k :data :data true))
  (get-value [db dbi-name k k-type]
    (l/get-value db dbi-name k k-type :data true))
  (get-value [db dbi-name k k-type v-type]
    (l/get-value db dbi-name k k-type v-type true))
  (get-value [db dbi-name k k-type v-type ignore-key?]
    (cl/normal-request client :get-value
                       [db-name dbi-name k k-type v-type ignore-key?]))

  (get-first [db dbi-name k-range]
    (l/get-first db dbi-name k-range :data :data false))
  (get-first [db dbi-name k-range k-type]
    (l/get-first db dbi-name k-range k-type :data false))
  (get-first [db dbi-name k-range k-type v-type]
    (l/get-first db dbi-name k-range k-type v-type false))
  (get-first [db dbi-name k-range k-type v-type ignore-key?]
    (cl/normal-request client :get-first
                       [db-name dbi-name k-range k-type v-type ignore-key?]))

  (get-range [db dbi-name k-range]
    (l/get-range db dbi-name k-range :data :data false))
  (get-range [db dbi-name k-range k-type]
    (l/get-range db dbi-name k-range k-type :data false))
  (get-range [db dbi-name k-range k-type v-type]
    (l/get-range db dbi-name k-range k-type v-type false))
  (get-range [db dbi-name k-range k-type v-type ignore-key?]
    (cl/normal-request client :get-range
                       [db-name dbi-name k-range k-type v-type ignore-key?]))

  (range-count [db dbi-name k-range]
    (l/range-count db dbi-name k-range :data))
  (range-count [db dbi-name k-range k-type]
    (cl/normal-request client :range-count [db-name dbi-name k-range k-type]))

  (get-some [db dbi-name pred k-range]
    (l/get-some db dbi-name pred k-range :data :data false))
  (get-some [db dbi-name pred k-range k-type]
    (l/get-some db dbi-name pred k-range k-type :data false))
  (get-some [db dbi-name pred k-range k-type v-type]
    (l/get-some db dbi-name pred k-range k-type v-type false))
  (get-some [db dbi-name pred k-range k-type v-type ignore-key?]
    (let [frozen-pred (nippy/fast-freeze pred)]
      (cl/normal-request client :get-some
                         [db-name dbi-name frozen-pred k-range k-type v-type
                          ignore-key?])))

  (range-filter [db dbi-name pred k-range]
    (l/range-filter db dbi-name pred k-range :data :data false))
  (range-filter [db dbi-name pred k-range k-type]
    (l/range-filter db dbi-name pred k-range k-type :data false))
  (range-filter [db dbi-name pred k-range k-type v-type]
    (l/range-filter db dbi-name pred k-range k-type v-type false))
  (range-filter [db dbi-name pred k-range k-type v-type ignore-key?]
    (let [frozen-pred (nippy/fast-freeze pred)]
      (cl/normal-request client :range-filter
                         [db-name dbi-name frozen-pred k-range k-type v-type
                          ignore-key?])))

  (range-filter-count [db dbi-name pred k-range]
    (l/range-filter-count db dbi-name pred k-range :data))
  (range-filter-count [db dbi-name pred k-range k-type]
    (let [frozen-pred (nippy/fast-freeze pred)]
      (cl/normal-request client :range-filter-count
                         [db-name dbi-name frozen-pred k-range k-type]))))

(defn open-kv
  "Open a remote kv store."
  [uri-str]
  (let [uri     (URI. uri-str)
        uri-str (str uri-str
                     (if (cl/parse-query uri) "&" "?")
                     "store=" c/db-store-kv)]
    (if-let [db-name (cl/parse-db uri)]
      (let [client (cl/new-client uri-str)]
        (cl/open-database client db-name c/db-store-kv)
        (->KVStore uri-str db-name client))
      (u/raise "URI should contain a database name"))))
