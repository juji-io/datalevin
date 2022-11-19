(ns ^:no-doc datalevin.remote
  "Proxy for remote stores"
  (:require [datalevin.util :as u]
            [datalevin.constants :as c]
            [datalevin.client :as cl]
            [datalevin.storage :as s]
            [datalevin.bits :as b]
            [datalevin.search :as sc]
            [datalevin.lmdb :as l :refer [IWritingLMDB]]
            [clojure.string :as str])
  (:import [datalevin.client Client]
           [datalevin.storage IStore]
           [datalevin.lmdb ILMDB]
           [datalevin.search ISearchEngine IIndexWriter]
           [java.nio.file Files Paths StandardOpenOption LinkOption]
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

(defn- load-datoms*
  [client db-name datoms datom-type simulated?]
  (let [t (if (= datom-type :txs)
            :tx-data
            :load-datoms)
        {:keys [type message result]}
        (if (< (count datoms) ^long c/+wire-datom-batch-size+)
          (cl/request client {:type t
                              :mode :request
                              :args (if (= datom-type :txs)
                                      [db-name datoms simulated?]
                                      [db-name datoms])})
          (cl/copy-in client {:type t
                              :mode :copy-in
                              :args (if (= datom-type :txs)
                                      [db-name simulated?]
                                      [db-name])}
                      datoms c/+wire-datom-batch-size+))]
    (when (= type :error-response)
      (u/raise "Error loading datoms to server:" message {}))
    result))

;; remote datalog db

(defprotocol IRemoteDB
  (q [store query inputs]
    "For special case of queries with a single remote store as source,
     send the query and inputs over to remote server")
  (fulltext-datoms [store query opts])
  (tx-data [store data simulated?]
    "Send to remote server the data from call to `db/transact-tx-data`"))

(deftype DatalogStore [^String uri
                       ^String db-name
                       opts
                       ^Client client]
  IStore

  (opts [_] opts)

  (db-name [_] db-name)

  (dir [_] uri)

  (close [_]
    (when-not (cl/disconnected? client)
      (cl/normal-request client :close [db-name])))

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

  (max-tx [_]
    (cl/normal-request client :max-tx [db-name]))

  (swap-attr [this attr f]
    (s/swap-attr this attr f nil nil))
  (swap-attr [this attr f x]
    (s/swap-attr this attr f x nil))
  (swap-attr [_ attr f x y]
    (let [frozen-f (b/serialize f)]
      (cl/normal-request client :swap-attr [db-name attr frozen-f x y])))

  (del-attr [_ attr]
    (cl/normal-request client :del-attr [db-name attr]))

  (rename-attr [_ attr new-attr]
    (cl/normal-request client :rename-attr [db-name attr new-attr]))

  (datom-count [_ index]
    (cl/normal-request client :datom-count [db-name index]))

  (load-datoms [_ datoms]
    (load-datoms* client db-name datoms :raw false))

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
    (let [frozen-pred (b/serialize pred)]
      (cl/normal-request client :size-filter
                         [db-name index frozen-pred low-datom high-datom])))

  (head-filter [_ index pred low-datom high-datom]
    (let [frozen-pred (b/serialize pred)]
      (cl/normal-request client :head-filter
                         [db-name index frozen-pred low-datom high-datom])))

  (tail-filter [_ index pred high-datom low-datom]
    (let [frozen-pred (b/serialize pred)]
      (cl/normal-request client :tail-filter
                         [db-name index frozen-pred high-datom low-datom])))

  (slice-filter [_ index pred low-datom high-datom]
    (let [frozen-pred (b/serialize pred)]
      (cl/normal-request client :slice-filter
                         [db-name index frozen-pred low-datom high-datom])))

  (rslice-filter [_ index pred high-datom low-datom]
    (let [frozen-pred (b/serialize pred)]
      (cl/normal-request client :rslice-filter
                         [db-name index frozen-pred high-datom low-datom])))

  IRemoteDB
  (q [_ query inputs]
    (cl/normal-request client :q [db-name query inputs]))
  (fulltext-datoms [_ query opts]
    (cl/normal-request client :fulltext-datoms [db-name query opts]))
  (tx-data [store data simulated?]
    (load-datoms* client db-name data :txs simulated?)))

(defn open
  "Open a remote Datalog store"
  ([uri-str]
   (open uri-str nil))
  ([uri-str schema]
   (open (cl/new-client uri-str) uri-str schema nil))
  ([uri-str schema opts]
   (open (cl/new-client uri-str) uri-str schema opts))
  ([client uri-str schema opts]
   (let [uri (URI. uri-str)]
     (if-let [db-name (cl/parse-db uri)]
       (let [store (or (get (cl/parse-query uri) "store")
                       c/db-store-datalog)]
         (cl/open-database client db-name store schema opts)
         (->DatalogStore uri-str db-name opts client))
       (u/raise "URI should contain a database name" {})))))


;; remote kv store

(declare ->WritingKVStore)

(deftype KVStore [^String uri
                  ^String db-name
                  ^Client client]
  IWritingLMDB
  (writing? [_] false)

  ILMDB
  (close-kv [_]
    (when-not (cl/disconnected? client)
      (cl/normal-request client :close-kv [db-name])))

  (closed-kv? [_]
    (if (cl/disconnected? client)
      true
      (cl/normal-request client :closed-kv? [db-name])))

  (dir [_] uri)

  (open-dbi [db dbi-name]
    (l/open-dbi db dbi-name nil))
  (open-dbi [_ dbi-name opts]
    (cl/normal-request client :open-dbi [db-name dbi-name opts]))

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

  (open-transact-kv [db]
    (cl/normal-request client :open-transact-kv [db-name])
    (->WritingKVStore db))

  (close-transact-kv [db] (cl/normal-request client :close-transact-kv [db-name]))

  (transact-kv [db txs]
    (let [{:keys [type message]}
          (if (< (count txs) ^long c/+wire-datom-batch-size+)
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
    (let [frozen-pred (b/serialize pred)]
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
    (let [frozen-pred (b/serialize pred)]
      (cl/normal-request client :range-filter
                         [db-name dbi-name frozen-pred k-range k-type v-type
                          ignore-key?])))

  (range-filter-count [db dbi-name pred k-range]
    (l/range-filter-count db dbi-name pred k-range :data))
  (range-filter-count [db dbi-name pred k-range k-type]
    (let [frozen-pred (b/serialize pred)]
      (cl/normal-request client :range-filter-count
                         [db-name dbi-name frozen-pred k-range k-type])))

  (visit [db dbi-name visitor k-range]
    (l/visit db dbi-name visitor k-range :data))
  (visit [db dbi-name visitor k-range k-type]
    (let [frozen-visitor (b/serialize visitor)]
      (cl/normal-request client :visit
                         [db-name dbi-name frozen-visitor k-range k-type]))))

(deftype WritingKVStore [^KVStore db]
  IWritingLMDB
  (writing? [_] true)

  ILMDB
  (close-kv [_] (l/close-kv db))

  (closed-kv? [_] (l/closed-kv? db))

  (dir [_] (l/dir db))

  (open-dbi [this dbi-name]
    (l/open-dbi this dbi-name nil))
  (open-dbi [_ dbi-name opts]
    (l/open-dbi db dbi-name opts))

  (clear-dbi [_ dbi-name] (l/clear-dbi db dbi-name))

  (drop-dbi [_ dbi-name] (l/drop-dbi db dbi-name))

  (list-dbis [_] (l/list-dbis db))

  (copy [this dest] (l/copy this dest false))
  (copy [_ dest compact?] (l/copy db dest compact?))

  (stat [this] (l/stat this nil))
  (stat [_ dbi-name] (l/stat db dbi-name))

  (entries [_ dbi-name] (l/entries db dbi-name))

  (transact-kv [_ txs] (l/transact-kv db txs))

  (get-value [this dbi-name k]
    (l/get-value this dbi-name k :data :data true))
  (get-value [this dbi-name k k-type]
    (l/get-value this dbi-name k k-type :data true))
  (get-value [this dbi-name k k-type v-type]
    (l/get-value this dbi-name k k-type v-type true))
  (get-value [_ dbi-name k k-type v-type ignore-key?]
    (cl/normal-request
      (.-client db) :get-value
      [(.-db-name db) dbi-name k k-type v-type ignore-key?]
      true))

  (get-first [this dbi-name k-range]
    (l/get-first this dbi-name k-range :data :data false))
  (get-first [this dbi-name k-range k-type]
    (l/get-first this dbi-name k-range k-type :data false))
  (get-first [this dbi-name k-range k-type v-type]
    (l/get-first this dbi-name k-range k-type v-type false))
  (get-first [_ dbi-name k-range k-type v-type ignore-key?]
    (cl/normal-request
      (.-client db) :get-first
      [(.-db-name db) dbi-name k-range k-type v-type ignore-key?]
      true))

  (get-range [this dbi-name k-range]
    (l/get-range this dbi-name k-range :data :data false))
  (get-range [this dbi-name k-range k-type]
    (l/get-range this dbi-name k-range k-type :data false))
  (get-range [this dbi-name k-range k-type v-type]
    (l/get-range this dbi-name k-range k-type v-type false))
  (get-range [_ dbi-name k-range k-type v-type ignore-key?]
    (cl/normal-request
      (.-client db) :get-range
      [(.-db-name db) dbi-name k-range k-type v-type ignore-key?]
      true))

  (range-count [this dbi-name k-range]
    (l/range-count this dbi-name k-range :data))
  (range-count [_ dbi-name k-range k-type]
    (cl/normal-request
      (.-client db) :range-count
      [(.-db-name db) dbi-name k-range k-type]
      true))

  (get-some [this dbi-name pred k-range]
    (l/get-some this dbi-name pred k-range :data :data false))
  (get-some [this dbi-name pred k-range k-type]
    (l/get-some this dbi-name pred k-range k-type :data false))
  (get-some [this dbi-name pred k-range k-type v-type]
    (l/get-some this dbi-name pred k-range k-type v-type false))
  (get-some [_ dbi-name pred k-range k-type v-type ignore-key?]
    (let [frozen-pred (b/serialize pred)]
      (cl/normal-request
        (.-client db) :get-some
        [(.-db-name db) dbi-name frozen-pred k-range k-type v-type
         ignore-key?]
        true)))

  (range-filter [this dbi-name pred k-range]
    (l/range-filter this dbi-name pred k-range :data :data false))
  (range-filter [this dbi-name pred k-range k-type]
    (l/range-filter this dbi-name pred k-range k-type :data false))
  (range-filter [this dbi-name pred k-range k-type v-type]
    (l/range-filter this dbi-name pred k-range k-type v-type false))
  (range-filter [_ dbi-name pred k-range k-type v-type ignore-key?]
    (let [frozen-pred (b/serialize pred)]
      (cl/normal-request
        (.-client db) :range-filter
        [(.-db-name db) dbi-name frozen-pred k-range k-type v-type
         ignore-key?]
        true)))

  (range-filter-count [this dbi-name pred k-range]
    (l/range-filter-count this dbi-name pred k-range :data))
  (range-filter-count [_ dbi-name pred k-range k-type]
    (let [frozen-pred (b/serialize pred)]
      (cl/normal-request
        (.-client db) :range-filter-count
        [(.-db-name db) dbi-name frozen-pred k-range k-type]
        true)))

  (visit [this dbi-name visitor k-range]
    (l/visit this dbi-name visitor k-range :data))
  (visit [_ dbi-name visitor k-range k-type]
    (let [frozen-visitor (b/serialize visitor)]
      (cl/normal-request
        (.-client db) :visit
        [(.-db-name db) dbi-name frozen-visitor k-range k-type]
        true))))

(defn open-kv
  "Open a remote kv store."
  ([uri-str]
   (open-kv uri-str nil))
  ([uri-str opts]
   (open-kv (cl/new-client uri-str) uri-str opts))
  ([client uri-str opts]
   (let [uri     (URI. uri-str)
         uri-str (str uri-str
                      (if (cl/parse-query uri) "&" "?")
                      "store=" c/db-store-kv)]
     (if-let [db-name (cl/parse-db uri)]
       (do (cl/open-database client db-name c/db-store-kv opts)
           (->KVStore uri-str db-name client))
       (u/raise "URI should contain a database name" {})))))

;; remote search

(deftype SearchEngine [^KVStore store]
  ISearchEngine
  (add-doc [this doc-ref doc-text]
    (cl/normal-request (.-client store) :add-doc
                       [(.-db-name store) doc-ref doc-text]))

  (remove-doc [this doc-ref]
    (cl/normal-request (.-client store) :remove-doc
                       [(.-db-name store) doc-ref]))

  (clear-docs [this]
    (cl/normal-request (.-client store) :clear-docs [(.-db-name store)]))

  (doc-indexed? [this doc-ref]
    (cl/normal-request (.-client store) :doc-indexed?
                       [(.-db-name store) doc-ref]))

  (doc-count [this]
    (cl/normal-request (.-client store) :doc-count [(.-db-name store)]))

  (doc-refs [this]
    (cl/normal-request (.-client store) :doc-refs [(.-db-name store)]))

  (search [this query]
    (sc/search this query {}))
  (search [this query opts]
    (cl/normal-request (.-client store) :search
                       [(.-db-name store) query opts])))

(defn new-search-engine
  ([store]
   (new-search-engine store nil))
  ([^KVStore store opts]
   (cl/normal-request (.-client store) :new-search-engine
                      [(.-db-name store) opts])
   (->SearchEngine store)))

(deftype IndexWriter [^KVStore store]
  IIndexWriter
  (write [this doc-ref doc-text]
    (cl/normal-request (.-client store) :write
                       [(.-db-name store) doc-ref doc-text]))

  (commit [this]
    (cl/normal-request (.-client store) :commit [(.-db-name store)])))

(defn search-index-writer
  ([store]
   (search-index-writer store nil))
  ([^KVStore store opts]
   (cl/normal-request (.-client store) :search-index-writer
                      [(.-db-name store) opts])
   (->IndexWriter store)))
