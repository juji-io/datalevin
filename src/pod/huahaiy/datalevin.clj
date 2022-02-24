(ns pod.huahaiy.datalevin
  (:refer-clojure :exclude [read read-string])
  (:require [bencode.core :as bencode]
            [datalevin.core :as d]
            [datalevin.interpret :as i]
            [datalevin.query :as q]
            [datalevin.util :as u]
            [datalevin.datom :as dd]
            [clojure.java.io :as io]
            [clojure.walk :as w])
  (:import [java.io PushbackInputStream]
           [java.nio.charset StandardCharsets]
           [datalevin.datom Datom]
           [datalevin.db TxReport]
           [java.util UUID])
  (:gen-class))

(def pod-ns "pod.huahaiy.datalevin")

(def debug? true)

(defn debug [& args]
  (when debug?
    (binding [*out* (io/writer "/tmp/pod-debug.log" :append true)]
      (apply println args))))

(def stdin (PushbackInputStream. System/in))

(defn- write [v]
  (bencode/write-bencode System/out v)
  (.flush System/out))

(defn- read-string [^bytes v]
  (String. v StandardCharsets/UTF_8))

(defn- read []
  (bencode/read-bencode stdin))

;; uuid -> conn
(defonce ^:private dl-conns (atom {}))

;; uuid -> db
(defonce ^:private dl-dbs (atom {}))

;; uuid -> db
(defonce ^:private kv-dbs (atom {}))

;; exposed functions

(defn definterfn [fn-name args body]
  (i/definterfn fn-name args body)
  {::inter-fn fn-name})

(defn entid [{:keys [::db]} eid]
  (when-let [d (get @dl-dbs db)]
    (d/entid d eid)))

(defn pull [{:keys [::db]} selector eid]
  (when-let [d (get @dl-dbs db)]
    (d/pull d selector eid)))

(defn pull-many [{:keys [::db]} selector eids]
  (when-let [d (get @dl-dbs db)]
    (d/pull-many d selector eids)))

(defn q [q & inputs]
  (apply d/q q (w/postwalk #(if-let [db (::db %)] (get @dl-dbs db) %)
                           inputs)))

(defn empty-db
  ([] (empty-db nil nil))
  ([dir] (empty-db dir nil))
  ([dir schema]
   (let [db (d/empty-db dir schema)
         id (UUID/randomUUID)]
     (swap! dl-dbs assoc id db)
     {::db id})))

(defn db? [{:keys [::db]}]
  (when-let [d (get @dl-dbs db)]
    (d/db? d)))

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

(defn close-db [{:keys [::db]}]
  (when-let [d (get @dl-dbs db)]
    (d/close-db d)))

(defn datoms
  ([{:keys [::db]} index]
   (when-let [d (get @dl-dbs db)] (map dd/datom-eav (d/datoms d index))))
  ([{:keys [::db]} index c1]
   (when-let [d (get @dl-dbs db)] (map dd/datom-eav (d/datoms d index c1))))
  ([{:keys [::db]} index c1 c2]
   (when-let [d (get @dl-dbs db)] (map dd/datom-eav (d/datoms d index c1 c2))))
  ([{:keys [::db]} index c1 c2 c3]
   (when-let [d (get @dl-dbs db)]
     (map dd/datom-eav (d/datoms d index c1 c2 c3))))
  ([{:keys [::db]} index c1 c2 c3 c4]
   (when-let [d (get @dl-dbs db)]
     (map dd/datom-eav (d/datoms d index c1 c2 c3 c4)))))

(defn seek-datoms
  ([{:keys [::db]} index]
   (when-let [d (get @dl-dbs db)] (map dd/datom-eav (d/seek-datoms d index))))
  ([{:keys [::db]} index c1]
   (when-let [d (get @dl-dbs db)]
     (map dd/datom-eav (d/seek-datoms d index c1))))
  ([{:keys [::db]} index c1 c2]
   (when-let [d (get @dl-dbs db)]
     (map dd/datom-eav (d/seek-datoms d index c1 c2))))
  ([{:keys [::db]} index c1 c2 c3]
   (when-let [d (get @dl-dbs db)]
     (map dd/datom-eav (d/seek-datoms d index c1 c2 c3))))
  ([{:keys [::db]} index c1 c2 c3 c4]
   (when-let [d (get @dl-dbs db)]
     (map dd/datom-eav (d/seek-datoms d index c1 c2 c3 c4)))))

(defn rseek-datoms
  ([{:keys [::db]} index]
   (when-let [d (get @dl-dbs db)] (map dd/datom-eav (d/rseek-datoms d index))))
  ([{:keys [::db]} index c1]
   (when-let [d (get @dl-dbs db)]
     (map dd/datom-eav (d/rseek-datoms d index c1))))
  ([{:keys [::db]} index c1 c2]
   (when-let [d (get @dl-dbs db)]
     (map dd/datom-eav (d/rseek-datoms d index c1 c2))))
  ([{:keys [::db]} index c1 c2 c3]
   (when-let [d (get @dl-dbs db)]
     (map dd/datom-eav (d/rseek-datoms d index c1 c2 c3))))
  ([{:keys [::db]} index c1 c2 c3 c4]
   (when-let [d (get @dl-dbs db)]
     (map dd/datom-eav (d/rseek-datoms d index c1 c2 c3 c4)))))

(defn index-range [{:keys [::db]} attr start end]
  (when-let [d (get @dl-dbs db)]
    (map dd/datom-eav (d/index-range d attr start end))))

(defn conn? [{:keys [::conn]}]
  (when-let [c (get @dl-conns conn)] (d/conn? c)))

(defn conn-from-db [{:keys [::db]}]
  (when-let [d (get @dl-dbs db)]
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

(defn closed? [{:keys [::conn]}]
  (when-let [c (get @dl-conns conn)]
    (d/closed? c)))

(defn transact!
  ([conn tx-data]
   (transact! conn tx-data nil))
  ([{:keys [::conn]} tx-data tx-meta]
   (when-let [c (get @dl-conns conn)]
     (let [rp (d/transact! c tx-data tx-meta)]
       {:datoms-transacted (count (:tx-data rp))}))))

(defn db [{:keys [::conn]}]
  (when-let [c (get @dl-conns conn)]
    (let [db (d/db c)]
      (if-let [i (some (fn [[i d]] (when (= db d) i)) @dl-dbs)]
        {::db i}
        (do (swap! dl-dbs assoc conn db)
            {::db conn})))))

(defn schema [{:keys [::conn]}]
  (when-let [c (get @dl-conns conn)]
    (d/schema c)))

(defn update-schema [{:keys [::conn]} schema-update]
  (when-let [c (get @dl-conns conn)]
    (d/update-schema c schema-update)))

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

(defn close-kv [{:keys [::kv-db]}]
  (when-let [d (get @kv-dbs kv-db)]
    (d/close-kv d)))

(defn closed-kv? [{:keys [::kv-db]}]
  (when-let [d (get @kv-dbs kv-db)]
    (d/closed-kv? d)))

(defn dir [{:keys [::kv-db]}]
  (when-let [d (get @kv-dbs kv-db)]
    (d/dir d)))

(defn open-dbi
  ([{:keys [::kv-db]}] (when-let [d (get @kv-dbs kv-db)] (d/open-dbi d) nil))
  ([{:keys [::kv-db]} dbi-name]
   (when-let [d (get @kv-dbs kv-db)] (d/open-dbi d dbi-name) nil))
  ([{:keys [::kv-db]} dbi-name key-size]
   (when-let [d (get @kv-dbs kv-db)] (d/open-dbi d dbi-name key-size) nil))
  ([{:keys [::kv-db]} dbi-name key-size val-size]
   (when-let [d (get @kv-dbs kv-db)]
     (d/open-dbi d dbi-name key-size val-size) nil))
  ([{:keys [::kv-db]} dbi-name key-size val-size flags]
   (when-let [d (get @kv-dbs kv-db)]
     (d/open-dbi d dbi-name key-size val-size flags) nil)))

(defn clear-dbi [{:keys [::kv-db]}]
  (when-let [d (get @kv-dbs kv-db)]
    (d/clear-dbi d)))

(defn drop-dbi [{:keys [::kv-db]}]
  (when-let [d (get @kv-dbs kv-db)]
    (d/drop-dbi d)))

(defn list-dbis [{:keys [::kv-db]}]
  (when-let [d (get @kv-dbs kv-db)]
    (d/list-dbis d)))

(defn copy
  ([db dest]
   (copy db dest false))
  ([{:keys [::kv-db]} dest compact?]
   (when-let [d (get @kv-dbs kv-db)]
     (d/copy d dest compact?))))

(defn stat
  ([{:keys [::kv-db]}]
   (when-let [d (get @kv-dbs kv-db)]
     (d/stat d)))
  ([{:keys [::kv-db]} dbi-name]
   (when-let [d (get @kv-dbs kv-db)]
     (d/stat d dbi-name))))

(defn entries [{:keys [::kv-db]} dbi-name]
  (when-let [d (get @kv-dbs kv-db)]
    (d/entries d dbi-name)))

(defn transact-kv [{:keys [::kv-db]} txs]
  (when-let [d (get @kv-dbs kv-db)]
    (d/transact-kv d txs)))

(defn get-value
  ([{:keys [::kv-db]} dbi-name k]
   (when-let [d (get @kv-dbs kv-db)] (d/get-value d dbi-name k)))
  ([{:keys [::kv-db]} dbi-name k k-type]
   (when-let [d (get @kv-dbs kv-db)] (d/get-value d dbi-name k k-type)))
  ([{:keys [::kv-db]} dbi-name k k-type v-type]
   (when-let [d (get @kv-dbs kv-db)]
     (d/get-value d dbi-name k k-type v-type)))
  ([{:keys [::kv-db]} dbi-name k k-type v-type ignore-key?]
   (when-let [d (get @kv-dbs kv-db)]
     (d/get-value d dbi-name k k-type v-type ignore-key?))))

(defn get-first
  ([{:keys [::kv-db]} dbi-name k-range]
   (when-let [d (get @kv-dbs kv-db)] (d/get-first d dbi-name k-range)))
  ([{:keys [::kv-db]} dbi-name k-range k-type]
   (when-let [d (get @kv-dbs kv-db)] (d/get-first d dbi-name k-range k-type)))
  ([{:keys [::kv-db]} dbi-name k-range k-type v-type]
   (when-let [d (get @kv-dbs kv-db)]
     (d/get-first d dbi-name k-range k-type v-type)))
  ([{:keys [::kv-db]} dbi-name k-range k-type v-type ignore-key?]
   (when-let [d (get @kv-dbs kv-db)]
     (d/get-first d dbi-name k-range k-type v-type ignore-key?))))

(defn get-range
  ([{:keys [::kv-db]} dbi-name k-range]
   (when-let [d (get @kv-dbs kv-db)] (d/get-range d dbi-name k-range)))
  ([{:keys [::kv-db]} dbi-name k-range k-type]
   (when-let [d (get @kv-dbs kv-db)] (d/get-range d dbi-name k-range k-type)))
  ([{:keys [::kv-db]} dbi-name k-range k-type v-type]
   (when-let [d (get @kv-dbs kv-db)]
     (d/get-range d dbi-name k-range k-type v-type)))
  ([{:keys [::kv-db]} dbi-name k-range k-type v-type ignore-key?]
   (when-let [d (get @kv-dbs kv-db)]
     (d/get-range d dbi-name k-range k-type v-type ignore-key?))))

(defn range-count
  ([{:keys [::kv-db]} dbi-name k-range]
   (when-let [d (get @kv-dbs kv-db)] (d/range-count d dbi-name k-range)))
  ([{:keys [::kv-db]} dbi-name k-range k-type]
   (when-let [d (get @kv-dbs kv-db)] (d/range-count d dbi-name k-range k-type))))

(defn get-some
  ([{:keys [::kv-db]} dbi-name pred k-range]
   (when-let [d (get @kv-dbs kv-db)] (d/get-some d dbi-name pred k-range)))
  ([{:keys [::kv-db]} dbi-name pred k-range k-type]
   (when-let [d (get @kv-dbs kv-db)]
     (d/get-some d dbi-name pred k-range k-type)))
  ([{:keys [::kv-db]} dbi-name pred k-range k-type v-type]
   (when-let [d (get @kv-dbs kv-db)]
     (d/get-some d dbi-name pred k-range k-type v-type)))
  ([{:keys [::kv-db]} dbi-name pred k-range k-type v-type ignore-key?]
   (when-let [d (get @kv-dbs kv-db)]
     (d/get-some d dbi-name pred k-range k-type v-type ignore-key?))))

(defn range-filter
  ([{:keys [::kv-db]} dbi-name pred k-range]
   (when-let [d (get @kv-dbs kv-db)] (d/range-filter d dbi-name pred k-range)))
  ([{:keys [::kv-db]} dbi-name pred k-range k-type]
   (when-let [d (get @kv-dbs kv-db)]
     (d/range-filter d dbi-name pred k-range k-type)))
  ([{:keys [::kv-db]} dbi-name pred k-range k-type v-type]
   (when-let [d (get @kv-dbs kv-db)]
     (d/range-filter d dbi-name pred k-range k-type v-type)))
  ([{:keys [::kv-db]} dbi-name pred k-range k-type v-type ignore-key?]
   (when-let [d (get @kv-dbs kv-db)]
     (d/range-filter d dbi-name pred k-range k-type v-type ignore-key?))))

(defn range-filter-count
  ([{:keys [::kv-db]} dbi-name pred k-range]
   (when-let [d (get @kv-dbs kv-db)] (d/range-count d dbi-name pred k-range)))
  ([{:keys [::kv-db]} dbi-name pred k-range k-type]
   (when-let [d (get @kv-dbs kv-db)]
     (d/range-count d dbi-name pred k-range k-type))))

;; pods

(def ^:private exposed-vars
  {'definterfn         definterfn
   'entid              entid
   'pull               pull
   'pull-many          pull-many
   'empty-db           empty-db
   'db?                db?
   'init-db            init-db
   'close-db           close-db
   'datoms             datoms
   'seek-datoms        seek-datoms
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
   'transact-kv        transact-kv
   'get-value          get-value
   'get-first          get-first
   'get-range          get-range
   'range-count        range-count
   'get-some           get-some
   'range-filter       range-filter
   'range-filter-count range-filter-count
   })

(def ^:private lookup
  (zipmap (map (fn [sym] (symbol pod-ns (name sym))) (keys exposed-vars))
          (vals exposed-vars)))

(defn run []
  (loop []
    (let [message (try (read)
                       (catch java.io.EOFException _
                         ::EOF))]
      (when-not (identical? ::EOF message)
        (let [op (-> message (get "op") read-string keyword)
              id (or (some-> message (get "id") read-string) "unknown")]
          (case op
            :describe (do (write {"format"     "transit+json"
                                  "namespaces" [{"name" "pod.huahaiy.datalevin"
                                                 "vars"
                                                 (mapv (fn [k] {"name" (name k)})
                                                       (keys exposed-vars))}]
                                  "id"         id
                                  "ops"        {"shutdown" {}}})
                          (recur))
            :invoke   (do
                        (try
                          (let [var  (-> (get message "var")
                                         read-string
                                         symbol)
                                args (-> (get message "args")
                                         read-string
                                         u/read-transit-string)]
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
            :shutdown (do (doseq [conn (vals @dl-conns)] (d/close conn))
                          (doseq [db (vals @kv-dbs)] (d/close-kv db))
                          (System/exit 0))
            (do
              (write {"err" (str "unknown op:" (name op))})
              (recur))))))))

(defn -main [& _]
  (run))
