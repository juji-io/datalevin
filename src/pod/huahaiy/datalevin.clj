(ns ^:no-doc pod.huahaiy.datalevin
  (:refer-clojure :exclude [read read-string])
  (:require [bencode.core :as bencode]
            [sci.core :as sci]
            [datalevin.core :as d]
            [datalevin.lmdb :as l]
            [datalevin.interpret :as i]
            [datalevin.util :as u]
            [datalevin.datom :as dd]
            [datalevin.db :as db]
            [datalevin.storage :as st]
            [clojure.java.io :as io]
            [clojure.walk :as w])
  (:import [java.io PushbackInputStream]
           [java.nio.charset StandardCharsets]
           [datalevin.datom Datom]
           [datalevin.db DB TxReport]
           [datalevin.storage Store]
           [datalevin.entity Entity]
           [java.util UUID])
  (:gen-class))

(def pod-ns "pod.huahaiy.datalevin")

(def debug? true)

(defn debug [& args]
  (when debug?
    (binding [*out* (io/writer "/tmp/datalevin-pod-debug.log" :append true)]
      (apply println args))))

(def stdin (PushbackInputStream. System/in))

(defn- write [v]
  (bencode/write-bencode System/out v)
  (.flush System/out))

(defn- read-string [^bytes v]
  (String. v StandardCharsets/UTF_8))

(defn- read []
  (bencode/read-bencode stdin))


;; dbs

;; uuid -> conn
(defonce ^:private dl-conns (atom {}))

;; uuid -> writing wconn
(defonce ^:private wdl-conns (atom {}))

;; uuid -> dl db
(defonce ^:private dl-dbs (atom {}))

;; uuid -> writing dl db
(defonce ^:private wdl-dbs (atom {}))

;; uuid -> kv db
(defonce ^:private kv-dbs (atom {}))

;; uuid -> writing kv db
(defonce ^:private wkv-dbs (atom {}))

;; exposed functions

(defn pod-fn [fn-name args & body]
  (intern 'pod.huahaiy.datalevin (symbol fn-name)
          (sci/eval-form i/ctx (apply list 'fn args body)))
  {::inter-fn fn-name})

(defn- get-cn [{:keys [::conn writing?]}]
  (if writing? (get @wdl-conns conn) (get @dl-conns conn)))

(defn- get-db [{:keys [::db writing?]}]
  (if writing? (get @wdl-dbs db) (get @dl-dbs db)))

(defn- get-kv [{:keys [::kv-db writing?]}]
  (if writing? (get @wkv-dbs kv-db) (get @kv-dbs kv-db)))

(defn entid [dl eid] (when-let [d (get-db dl)] (d/entid d eid)))

(defn entity [{:keys [::db] :as dl} eid]
  (when-let [^DB d (get-db dl)]
    (let [^Entity e (d/entity d eid)]
      (assoc @(.-cache e) :db/id (.-eid e) :db-name db))))

(defn touch [{:keys [db-name db/id]}]
  (when-let [d (get @dl-dbs db-name)]
    (let [^Entity e (d/touch (d/entity d id))]
      (assoc @(.-cache e) :db/id id :db-name db-name))))

(defn pull [dl selector eid]
  (when-let [d (get-db dl)]
    (d/pull d selector eid)))

(defn pull-many [dl selector eids]
  (when-let [d (get-db dl)]
    (d/pull-many d selector eids)))

(defn q [q & inputs]
  (apply d/q q (w/postwalk #(if (::db %) (get-db %) %) inputs)))

(defn empty-db
  ([] (empty-db nil nil))
  ([dir] (empty-db dir nil))
  ([dir schema]
   (let [id (UUID/randomUUID)
         db (d/empty-db dir schema {:db-name id})]
     (swap! dl-dbs assoc id db)
     {::db id})))

(defn db? [dl] (when-let [d (get-db dl)] (d/db? d)))

(defn init-db
  ([datoms]
   (init-db datoms nil nil))
  ([datoms dir]
   (init-db datoms dir nil))
  ([datoms dir schema]
   (let [db (d/init-db (map #(apply dd/datom %) datoms) dir schema)
         id (UUID/randomUUID)]
     (swap! dl-dbs assoc id db)
     {::db id})))

(defn close-db [dl] (when-let [d (get-db dl)] (d/close-db d)))

(defn datoms
  ([dl index]
   (when-let [d (get-db dl)] (map dd/datom-eav (d/datoms d index))))
  ([dl index c1]
   (when-let [d (get-db dl)] (map dd/datom-eav (d/datoms d index c1))))
  ([dl index c1 c2]
   (when-let [d (get-db dl)] (map dd/datom-eav (d/datoms d index c1 c2))))
  ([dl index c1 c2 c3]
   (when-let [d (get-db dl)]
     (map dd/datom-eav (d/datoms d index c1 c2 c3))))
  ([dl index c1 c2 c3 c4]
   (when-let [d (get-db dl)]
     (map dd/datom-eav (d/datoms d index c1 c2 c3 c4)))))

(defn seek-datoms
  ([dl index]
   (when-let [d (get-db dl)] (map dd/datom-eav (d/seek-datoms d index))))
  ([dl index c1]
   (when-let [d (get-db dl)]
     (map dd/datom-eav (d/seek-datoms d index c1))))
  ([dl index c1 c2]
   (when-let [d (get-db dl)]
     (map dd/datom-eav (d/seek-datoms d index c1 c2))))
  ([dl index c1 c2 c3]
   (when-let [d (get-db dl)]
     (map dd/datom-eav (d/seek-datoms d index c1 c2 c3))))
  ([dl index c1 c2 c3 c4]
   (when-let [d (get-db dl)]
     (map dd/datom-eav (d/seek-datoms d index c1 c2 c3 c4)))))

(defn fulltext-datoms
  ([dl query]
   (when-let [d (get-db dl)]
     (map dd/datom-eav (d/fulltext-datoms d query))))
  ([dl query opts]
   (when-let [d (get-db dl)]
     (map dd/datom-eav (d/fulltext-datoms d query opts)))))

(defn rseek-datoms
  ([dl index]
   (when-let [d (get-db dl)] (map dd/datom-eav (d/rseek-datoms d index))))
  ([dl index c1]
   (when-let [d (get-db dl)]
     (map dd/datom-eav (d/rseek-datoms d index c1))))
  ([dl index c1 c2]
   (when-let [d (get-db dl)]
     (map dd/datom-eav (d/rseek-datoms d index c1 c2))))
  ([dl index c1 c2 c3]
   (when-let [d (get-db dl)]
     (map dd/datom-eav (d/rseek-datoms d index c1 c2 c3))))
  ([dl index c1 c2 c3 c4]
   (when-let [d (get-db dl)]
     (map dd/datom-eav (d/rseek-datoms d index c1 c2 c3 c4)))))

(defn index-range [dl attr start end]
  (when-let [d (get-db dl)]
    (map dd/datom-eav (d/index-range d attr start end))))

(defn conn? [cn] (when-let [c (get-cn cn)] (d/conn? c)))

(defn conn-from-db [{:keys [::db] :as dl}]
  (when-let [d (get-db dl)]
    (let [conn (d/conn-from-db d)]
      (swap! dl-conns assoc db conn)
      {::conn db})))

(defn create-conn
  ([] (conn-from-db (empty-db)))
  ([dir] (conn-from-db (empty-db dir)))
  ([dir schema] (conn-from-db (empty-db dir schema))))

(defn close [{:keys [::conn]}]
  (let [[old _] (swap-vals! dl-conns dissoc conn)]
    (when-let [c (get old conn)]
      (d/close c))))

(defn closed? [cn] (when-let [c (get-cn cn)] (d/closed? c)))

(defn transact!
  ([cn tx-data]
   (transact! cn tx-data nil))
  ([cn tx-data tx-meta]
   (when-let [c (get-cn cn)]
     (let [rp (d/transact! c tx-data tx-meta)]
       {:datoms-transacted (count (:tx-data rp))}))))

(defn db [{:keys [::conn] :as cn}]
  (when-let [c (get-cn cn)]
    (let [db (d/db c)]
      (if-let [i (some (fn [[i d]] (when (= db d) i)) @dl-dbs)]
        {::db i}
        (do (swap! dl-dbs assoc conn db)
            {::db conn})))))

(defn schema [cn] (when-let [c (get-cn cn)] (d/schema c)))

(defn update-schema
  [cn schema-update]
  (when-let [c (get-cn cn)] (d/update-schema c schema-update)))

(defn get-conn
  ([dir]
   (get-conn dir nil))
  ([dir schema]
   (let [conn (d/get-conn dir schema)]
     (if-let [id (some (fn [[id c]] (when (= conn c) id)) @dl-conns)]
       {::conn id}
       (let [id (UUID/randomUUID)]
         (swap! dl-dbs assoc id @conn)
         (swap! dl-conns assoc id conn)
         {::conn id})))))

(defn open-kv [dir]
  (let [db (d/open-kv dir)
        id (UUID/randomUUID)]
    (swap! kv-dbs assoc id db)
    {::kv-db id}))

(defn close-kv [db] (when-let [d (get-kv db)] (d/close-kv d)))

(defn closed-kv? [db] (when-let [d (get-kv db)] (d/closed-kv? d)))

(defn dir [db] (when-let [d (get-kv db)] (d/dir d)))

(defn open-dbi
  ([db dbi-name]
   (when-let [d (get-kv db)] (d/open-dbi d dbi-name) nil))
  ([db dbi-name opts]
   (when-let [d (get-kv db)] (d/open-dbi d dbi-name opts) nil)))

(defn clear-dbi [db] (when-let [d (get-kv db)] (d/clear-dbi d)))

(defn drop-dbi [db] (when-let [d (get-kv db)] (d/drop-dbi d)))

(defn list-dbis [db] (when-let [d (get-kv db)] (d/list-dbis d)))

(defn copy
  ([db dest]
   (copy db dest false))
  ([db dest compact?]
   (when-let [d (get-kv db)] (d/copy d dest compact?))))

(defn stat
  ([db]
   (when-let [d (get-kv db)] (d/stat d)))
  ([db dbi-name]
   (when-let [d (get-kv db)] (d/stat d dbi-name))))

(defn entries [db dbi-name] (when-let [d (get-kv db)] (d/entries d dbi-name)))

(defn open-transact-kv [{:keys [::kv-db] :as db}]
  (when-let [d (get @kv-dbs kv-db)]
    (let [wdb (l/open-transact-kv d)]
      (swap! wkv-dbs assoc kv-db wdb)
      (assoc db :writing? true))))

(defn close-transact-kv [{:keys [::kv-db]}]
  (when-let [d (get @kv-dbs kv-db)]
    (swap! wkv-dbs dissoc kv-db)
    (l/close-transact-kv d)))

(defn abort-transact-kv [{:keys [::kv-db]}]
  (when-let [d (get @wkv-dbs kv-db)]
    (d/abort-transact-kv d)))

(defn open-transact [{:keys [::conn] :as cn}]
  (when-let [c (get @dl-conns conn)]
    (let [s  (.-store ^DB @c)
          l  (.-lmdb ^Store s)
          wl (l/open-transact-kv l)
          ws (st/transfer s wl)
          wd (db/new-db ws)]
      (swap! wdl-dbs assoc conn wd)
      (swap! wdl-conns assoc conn (atom wd :meta (meta c)))
      (assoc cn :writing? true))))

(defn close-transact [{:keys [::conn]}]
  (when-let [c (get @dl-conns conn)]
    (let [wc (get @wdl-conns conn)
          ws (.-store ^DB @wc)
          l  (.-lmdb ^Store (.-store ^DB @c))]
      (reset! c (db/new-db (st/transfer ws l)))
      (swap! wdl-dbs dissoc conn)
      (swap! wdl-conns dissoc conn)
      (l/close-transact-kv l))))

(defn abort-transact [{:keys [::conn]}]
  (when-let [c (get @dl-conns conn)]
    (let [wc (get @wdl-conns conn)
          ws (.-store ^DB @wc)
          wl (.-lmdb ^Store ws)]
      (d/abort-transact-kv wl))))

(defn transact-kv [db txs] (when-let [d (get-kv db)] (d/transact-kv d txs)))

(defn get-value
  ([db dbi-name k]
   (when-let [d (get-kv db)] (d/get-value d dbi-name k)))
  ([db dbi-name k k-type]
   (when-let [d (get-kv db)] (d/get-value d dbi-name k k-type)))
  ([db dbi-name k k-type v-type]
   (when-let [d (get-kv db)]
     (d/get-value d dbi-name k k-type v-type)))
  ([db dbi-name k k-type v-type ignore-key?]
   (when-let [d (get-kv db)]
     (d/get-value d dbi-name k k-type v-type ignore-key?))))

(defn get-first
  ([db dbi-name k-range]
   (when-let [d (get-kv db)] (d/get-first d dbi-name k-range)))
  ([db dbi-name k-range k-type]
   (when-let [d (get-kv db)] (d/get-first d dbi-name k-range k-type)))
  ([db dbi-name k-range k-type v-type]
   (when-let [d (get-kv db)]
     (d/get-first d dbi-name k-range k-type v-type)))
  ([db dbi-name k-range k-type v-type ignore-key?]
   (when-let [d (get-kv db)]
     (d/get-first d dbi-name k-range k-type v-type ignore-key?))))

(defn get-range
  ([db dbi-name k-range]
   (when-let [d (get-kv db)] (d/get-range d dbi-name k-range)))
  ([db dbi-name k-range k-type]
   (when-let [d (get-kv db)] (d/get-range d dbi-name k-range k-type)))
  ([db dbi-name k-range k-type v-type]
   (when-let [d (get-kv db)]
     (d/get-range d dbi-name k-range k-type v-type)))
  ([db dbi-name k-range k-type v-type ignore-key?]
   (when-let [d (get-kv db)]
     (d/get-range d dbi-name k-range k-type v-type ignore-key?))))

(defn range-count
  ([db dbi-name k-range]
   (when-let [d (get-kv db)] (d/range-count d dbi-name k-range)))
  ([db dbi-name k-range k-type]
   (when-let [d (get-kv db)] (d/range-count d dbi-name k-range k-type))))

(defn get-some
  ([db dbi-name pred k-range]
   (when-let [d (get-kv db)] (d/get-some d dbi-name pred k-range)))
  ([db dbi-name pred k-range k-type]
   (when-let [d (get-kv db)]
     (d/get-some d dbi-name pred k-range k-type)))
  ([db dbi-name pred k-range k-type v-type]
   (when-let [d (get-kv db)]
     (d/get-some d dbi-name pred k-range k-type v-type)))
  ([db dbi-name pred k-range k-type v-type ignore-key?]
   (when-let [d (get-kv db)]
     (d/get-some d dbi-name pred k-range k-type v-type ignore-key?))))

(defn range-filter
  ([db dbi-name pred k-range]
   (when-let [d (get-kv db)] (d/range-filter d dbi-name pred k-range)))
  ([db dbi-name pred k-range k-type]
   (when-let [d (get-kv db)]
     (d/range-filter d dbi-name pred k-range k-type)))
  ([db dbi-name pred k-range k-type v-type]
   (when-let [d (get-kv db)]
     (d/range-filter d dbi-name pred k-range k-type v-type)))
  ([db dbi-name pred k-range k-type v-type ignore-key?]
   (when-let [d (get-kv db)]
     (d/range-filter d dbi-name pred k-range k-type v-type ignore-key?))))

(defn range-filter-count
  ([db dbi-name pred k-range]
   (when-let [d (get-kv db)] (d/range-count d dbi-name pred k-range)))
  ([db dbi-name pred k-range k-type]
   (when-let [d (get-kv db)]
     (d/range-count d dbi-name pred k-range k-type))))

(defn visit
  ([db dbi-name pred k-range]
   (when-let [d (get-kv db)] (d/visit d dbi-name pred k-range)))
  ([db dbi-name pred k-range k-type]
   (when-let [d (get-kv db)]
     (d/visit d dbi-name pred k-range k-type))))

;; pods

(def ^:private exposed-vars
  {'pod-fn             pod-fn
   'entid              entid
   'entity             entity
   'touch              touch
   'pull               pull
   'pull-many          pull-many
   'empty-db           empty-db
   'db?                db?
   'init-db            init-db
   'close-db           close-db
   'datoms             datoms
   'seek-datoms        seek-datoms
   'fulltext-datoms    fulltext-datoms
   'rseek-datoms       rseek-datoms
   'index-range        index-range
   'conn?              conn?
   'conn-from-db       conn-from-db
   'create-conn        create-conn
   'close              close
   'closed?            closed?
   'transact!          transact!
   'db                 db
   'schema             schema
   'update-schema      update-schema
   'get-conn           get-conn
   'q                  q
   'open-kv            open-kv
   'close-kv           close-kv
   'closed-kv?         closed-kv?
   'dir                dir
   'open-dbi           open-dbi
   'clear-dbi          clear-dbi
   'drop-dbi           drop-dbi
   'list-dbis          list-dbis
   'copy               copy
   'stat               stat
   'entries            entries
   'open-transact-kv   open-transact-kv
   'close-transact-kv  close-transact-kv
   'abort-transact-kv  abort-transact-kv
   'open-transact      open-transact
   'close-transact     close-transact
   'abort-transact     abort-transact
   'transact-kv        transact-kv
   'get-value          get-value
   'get-first          get-first
   'get-range          get-range
   'range-count        range-count
   'get-some           get-some
   'range-filter       range-filter
   'range-filter-count range-filter-count
   'visit              visit
   })

(defmacro defpodfn
  [fn-name args & body]
  `(pod-fn '~fn-name '~args '~@body))

(defmacro with-transaction-kv
  [binding & body]
  `(let [db# ~(second binding)]
     (try
       (let [~(first binding) (open-transact-kv db#)]
         ~@body)
       (finally (close-transact-kv db#)))))

(defmacro with-transaction
  [binding & body]
  `(let [conn# ~(second binding)]
     (try
       (let [~(first binding) (open-transact conn#)]
         ~@body)
       (finally (close-transact conn#)))))

(def ^:private lookup
  (zipmap (map (fn [sym] (symbol pod-ns (name sym))) (keys exposed-vars))
          (vals exposed-vars)))

(defn- all-vars []
  (concat (mapv (fn [k] {"name" (name k)})
                (keys exposed-vars))
          [{"name" "defpodfn"
            "code"
            "(defmacro defpodfn
              [fn-name args & body]
              `(pod-fn '~fn-name
                      '~args
                      '~@body))"}
           {"name" "with-transaction-kv"
            "code"
            "(defmacro with-transaction-kv
              [binding & body]
              `(let [db# ~(second binding)]
                (try
                  (let [~(first binding) (open-transact-kv db#)]
                    ~@body)
                  (finally
                    (close-transact-kv db#)))))"}
           {"name" "with-transaction"
            "code"
            "(defmacro with-transaction
               [binding & body]
               `(let [conn# ~(second binding)]
                  (try
                    (let [~(first binding) (open-transact conn#)]
                      ~@body)
                    (finally (close-transact conn#)))))"}]))

(defn run []
  (loop []
    (let [message (try (read)
                       (catch java.io.EOFException _
                         ::EOF))]
      (when-not (identical? ::EOF message)
        (let [op (-> message (get "op") read-string keyword)
              id (or (some-> message (get "id") read-string) "unknown")]
          (case op
            :describe
            (do (write {"format"     "transit+json"
                        "namespaces" [{"name" "pod.huahaiy.datalevin"
                                       "vars" (all-vars)}]
                        "id"         id
                        "ops"        {"shutdown" {}}})
                (recur))
            :invoke
            (do (try
                  (let [var  (-> (get message "var")
                                 read-string
                                 symbol)
                        args (-> (get message "args")
                                 read-string
                                 u/read-transit-string)]
                    ;; (debug "var" var "args" args)
                    (if-let [f (lookup var)]
                      (let [value (u/write-transit-string
                                    (apply f args))
                            reply {"value"  value
                                   "id"     id
                                   "status" ["done"]}]
                        (write reply))
                      (throw (ex-info (str "Var not found: " var) {}))))
                  (catch Throwable e
                    (binding [*out* *err*]
                      (println e))
                    (let [reply {"ex-message" (.getMessage e)
                                 "ex-data"    (u/write-transit-string
                                                (assoc (ex-data e)
                                                       :type
                                                       (str (class e))))
                                 "id"         id
                                 "status"     ["done" "error"]}]
                      (write reply))))
                (recur))
            :shutdown
            (do (doseq [conn (vals @dl-conns)] (d/close conn))
                (doseq [db (vals @kv-dbs)] (d/close-kv db))
                (System/exit 0))
            (do
              (write {"err" (str "unknown op:" (name op))})
              (recur))))))))

(defn -main [& _]
  (run))
