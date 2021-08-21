(ns datalevin.remote
  "Proxy for remote stores"
  (:require [datalevin.util :as u]
            [datalevin.constants :as c]
            [datalevin.client :as cl]
            [datalevin.storage :as s]
            [datalevin.bits :as b]
            [datalevin.lmdb :as l]
            [datalevin.protocol :as p]
            [taoensso.nippy :as nippy]
            [com.rpl.nippy-serializable-fn]
            [clojure.string :as str])
  (:import [datalevin.client Client]
           [datalevin.storage IStore]
           [datalevin.lmdb ILMDB]
           [java.net URI]))

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
    (let [frozen-f (nippy/fast-freeze f)]
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
    (let [frozen-pred (nippy/fast-freeze pred)]
      (normal-request :size-filter
                      [index frozen-pred low-datom high-datom])))

  (head-filter [_ index pred low-datom high-datom]
    (let [frozen-pred (nippy/fast-freeze pred)]
      (normal-request :head-filter
                      [index frozen-pred low-datom high-datom])))

  (slice-filter [_ index pred low-datom high-datom]
    (let [frozen-pred (nippy/fast-freeze pred)]
      (normal-request :slice-filter
                      [index frozen-pred low-datom high-datom])))

  (rslice-filter [_ index pred high-datom low-datom]
    (let [frozen-pred (nippy/fast-freeze pred)]
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

  (clear-dbi [db dbi-name] (normal-request :clear-dbi [dbi-name]))

  (drop-dbi [db dbi-name] (normal-request :drop-dbi [dbi-name]))

  (list-dbis [db] (normal-request :list-dbis nil))

  (copy [db dest] (l/copy db dest false))
  (copy [db dest compact?] (normal-request :copy [dest compact?]))

  (stat [db] (l/stat db nil))
  (stat [db dbi-name] (normal-request :stat [dbi-name]))

  (entries [db dbi-name] (normal-request :entries [dbi-name]))

  (transact-kv [db txs]
    (let [{:keys [type message]}
          (if (< (count txs) c/+wire-datom-batch-size+)
            (cl/request client {:type :transact-kv :mode :request
                                :args [txs]})
            (cl/copy-in client {:type :transact-kv :mode :copy-in}
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
    (normal-request :get-value [dbi-name k k-type v-type ignore-key?]))

  (get-first [db dbi-name k]
    (l/get-first db dbi-name k :data :data false))
  (get-first [db dbi-name k k-type]
    (l/get-first db dbi-name k k-type :data false))
  (get-first [db dbi-name k k-type v-type]
    (l/get-first db dbi-name k k-type v-type false))
  (get-first [db dbi-name k k-type v-type ignore-key?]
    (normal-request :get-first [dbi-name k k-type v-type ignore-key?]))

  (get-range [db dbi-name k]
    (l/get-range db dbi-name k :data :data false))
  (get-range [db dbi-name k k-type]
    (l/get-range db dbi-name k k-type :data false))
  (get-range [db dbi-name k k-type v-type]
    (l/get-range db dbi-name k k-type v-type false))
  (get-range [db dbi-name k k-type v-type ignore-key?]
    (normal-request :get-range [dbi-name k k-type v-type ignore-key?]))

  (range-count [db dbi-name k-range]
    (l/range-count db dbi-name k-range :data))
  (range-count [db dbi-name k-range k-type]
    (normal-request :range-count [dbi-name k-range k-type]))

  (get-some [db dbi-name pred k-range]
    (l/get-some db dbi-name pred k-range :data :data false))
  (get-some [db dbi-name pred k-range k-type]
    (l/get-some db dbi-name pred k-range k-type :data false))
  (get-some [db dbi-name pred k-range k-type v-type]
    (l/get-some db dbi-name pred k-range k-type v-type false))
  (get-some [db dbi-name pred k-range k-type v-type ignore-key?]
    (let [frozen-pred (nippy/fast-freeze pred)]
      (normal-request :get-some
                      [dbi-name frozen-pred k-range k-type v-type
                       ignore-key?])))

  (range-filter [db dbi-name pred k-range]
    (l/range-filter db dbi-name pred k-range :data :data false))
  (range-filter [db dbi-name pred k-range k-type]
    (l/range-filter db dbi-name pred k-range k-type :data false))
  (range-filter [db dbi-name pred k-range k-type v-type]
    (l/range-filter db dbi-name pred k-range k-type v-type false))
  (range-filter [db dbi-name pred k-range k-type v-type ignore-key?]
    (let [frozen-pred (nippy/fast-freeze pred)]
      (normal-request :range-filter
                      [dbi-name frozen-pred k-range k-type v-type
                       ignore-key?])))

  (range-filter-count [db dbi-name pred k-range]
    (l/range-filter-count db dbi-name pred k-range :data))
  (range-filter-count [db dbi-name pred k-range k-type]
    (let [frozen-pred (nippy/fast-freeze pred)]
      (normal-request :range-filter-count
                      [dbi-name frozen-pred k-range k-type])))
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

  (open-kv (l/dir store))

  (mm/measure store)

  (l/closed-kv? store)

  (l/close-kv store)

  (l/open-dbi store "z")

  (let [ks  (shuffle (range 0 10000))
        vs  (map inc ks)
        txs (map (fn [k v] [:put "z" k v :long :long]) ks vs)]
    (l/transact-kv store txs))

  (def pred (fn [kv]
              (let [^long k (b/read-buffer (l/k kv) :long)]
                (< 10 k 20))))

  (l/range-filter-count store "z" pred [:all] :long)

  (l/range-filter store "z" pred [:all] :long :long)

  (l/range-filter store "z" pred [:all] :long :long true)

  (l/clear-dbi store "a")

  )
