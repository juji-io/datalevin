(ns datalevin-bench.core
  (:require
   [datalevin.core :as d]
   [datalevin.constants :as c]
   [clojure.string :as s]
   [next.jdbc :as jdbc]
   [next.jdbc.sql :as sql])
  (:import
   [java.util.concurrent Semaphore]
   [org.eclipse.collections.impl.list.mutable FastList]))

;; max write throughput/latency benchmark

(def max-write-dbi "test")

;; limit the number of threads in flight
(def in-flight 1000)

;; total number of writes
(def total 10000000)

(def keyspace (* 2 total))

;; print numbers every this number of writes
(def report 100000)

(defn max-write-bench
  [batch-size tx-fn add-fn]
  (println
    "Time (seconds),Throughput (writes/second),Write Latency (milliseconds),Commit Latency (milliseconds)")
  (let [sem        (Semaphore. (* in-flight batch-size))
        target     (long (/ report batch-size))
        write-time (volatile! 0)
        sync-count (volatile! 0)
        inserted   (volatile! 0)
        start-time (System/currentTimeMillis)
        prev-time  (volatile! start-time)
        sync-time  (volatile! start-time)
        measure    (fn [_]
                     (.release sem batch-size)
                     (vreset! sync-time (System/currentTimeMillis))
                     (vswap! sync-count inc)
                     (vswap! inserted + batch-size))]
    (loop [counter 0]
      (when (<= (* counter batch-size) total)
        (.acquire sem batch-size)
        (when (and (= 0 (mod counter target))
                   (not= 0 counter) (not= 0 @sync-count))
          (let [duration (- @sync-time start-time)]
            (println (str
                       (long (/ duration 1000))
                       ","
                       (format "%.1f" (double (* (/ @inserted duration) 1000)))
                       ","
                       (format "%.4f" (double (/ @write-time @sync-count)))
                       ","
                       (format "%.4f" (double (/ (- @sync-time @prev-time)
                                                 @sync-count))))))
          (vreset! write-time 0)
          (vreset! prev-time @sync-time)
          (vreset! sync-count 0))
        (let [txs    (when add-fn
                       (reduce
                         (fn [^FastList txs _]
                           (add-fn txs)
                           txs)
                         (FastList. batch-size)
                         (range 0 batch-size)))
              before (System/currentTimeMillis)]
          (tx-fn txs measure)
          (vswap! write-time + (- (System/currentTimeMillis) before)))
        (recur (inc counter))))))

(def id (volatile! 0))

(defn write
  [{:keys [base-dir batch f]}]
  (let [kv?      (s/starts-with? (name f) "kv")
        dl?      (s/starts-with? (name f) "dl")
        sql?     (s/starts-with? (name f) "sql")
        kvdb     (when kv?
                   (doto (d/open-kv (str base-dir "/max-write-db-" f "-" batch)
                                    {:mapsize 60000
                                     :flags   (-> c/default-env-flags
                                                  ;; (conj :writemap)
                                                  ;; (conj :mapasync)
                                                  ;; (conj :nosync)
                                                  ;; (conj :nometasync)
                                                  )
                                     })
                     (d/open-dbi max-write-dbi)))
        kv-async (fn [txs measure]
                   (d/transact-kv-async kvdb max-write-dbi txs
                                        :id :string measure))
        kv-sync  (fn [txs measure]
                   (measure (d/transact-kv kvdb max-write-dbi txs
                                           :id :string)))
        kv-add   (fn [^FastList txs]
                   (.add txs [:put (vswap! id + 2) (str (random-uuid))]))
        conn     (when dl?
                   (d/get-conn (str base-dir "/max-write-db-" f "-" batch)
                               {:k {:db/valueType :db.type/long}
                                :v {:db/valueType :db.type/string}}
                               {:kv-opts {:mapsize 60000}}))
        dl-async (fn [txs measure] (d/transact-async conn txs nil measure))
        dl-sync  (fn [txs measure] (measure (d/transact! conn txs nil)))
        dl-add   (fn [^FastList txs]
                   (.add txs {:k (vswap! id + 2) :v (str (random-uuid))}))
        sql-conn (when sql?
                   (let [conn (jdbc/get-connection {:dbtype "sqlite"
                                                    :dbname (str base-dir "/sqlite-" batch)})]
                     (jdbc/execute! conn
                                    ["CREATE TABLE IF NOT EXISTS my_table (
                     k INTEGER PRIMARY KEY, v TEXT)"])
                     conn))
        sql-tx   (fn [txs measure]
                   (measure (sql/insert-multi! sql-conn :my_table txs)))
        sql-add  (fn [^FastList txs]
                   (.add txs {:k (vswap! id + 2) :v (str (random-uuid))}))
        tx-fn    (case f
                   kv-async kv-async
                   kv-sync  kv-sync
                   dl-async dl-async
                   dl-sync  dl-sync
                   sql-tx   sql-tx)
        add-fn   (cond
                   kv?  kv-add
                   dl?  dl-add
                   sql? sql-add)]
    (max-write-bench batch tx-fn add-fn)
    (when kvdb (d/close-kv kvdb))
    (when conn (d/close conn))
    (when sql-conn (.close sql-conn))))

(defn mixed
  [{:keys [dir f]}]
  (let [kv?      (s/starts-with? (name f) "kv")
        dl?      (s/starts-with? (name f) "dl")
        sql?     (s/starts-with? (name f) "sql")
        kvdb     (when kv?
                   (doto (d/open-kv dir
                                    {:mapsize 60000
                                     :flags   (-> c/default-env-flags
                                                  ;; (conj :writemap)
                                                  ;; (conj :mapasync)
                                                  ;; (conj :nosync)
                                                  ;; (conj :nometasync)
                                                  )
                                     })
                     (d/open-dbi max-write-dbi)))
        kv-async (fn [txs measure]
                   (d/get-value kvdb max-write-dbi (rand-int keyspace) :id)
                   (d/transact-kv-async kvdb max-write-dbi txs
                                        :id :string measure))
        kv-sync  (fn [txs measure]
                   (d/get-value kvdb max-write-dbi (rand-int keyspace) :id)
                   (measure (d/transact-kv kvdb max-write-dbi txs
                                           :id :string)))
        kv-add   (fn [^FastList txs]
                   (.add txs [:put (rand-int keyspace) (str (random-uuid))]))
        conn     (when dl?
                   (d/get-conn dir {:k {:db/valueType :db.type/long}
                                    :v {:db/valueType :db.type/string}}
                               {:kv-opts {:mapsize 60000}}))
        query    '[:find ?v
                   :in $ ?k
                   :where [?e :k ?k] [?e :v ?v]]
        dl-async (fn [txs measure]
                   (d/q query (d/db conn) (rand-int keyspace))
                   (d/transact-async conn txs nil measure))
        dl-sync  (fn [txs measure]
                   (d/q query (d/db conn) (rand-int keyspace))
                   (measure (d/transact! conn txs nil)))
        dl-add   (fn [^FastList txs]
                   (.add txs {:k (rand-int keyspace) :v (str (random-uuid))}))
        sql-conn (when sql?
                   (let [conn (jdbc/get-connection {:dbtype "sqlite"
                                                    :dbname dir})]
                     (jdbc/execute! conn
                                    ["CREATE TABLE IF NOT EXISTS my_table (
                     k INTEGER PRIMARY KEY, v TEXT)"])
                     conn))
        tx       "INSERT OR REPLACE INTO my_table (k, v) values (?, ?)"
        sql-tx   (fn [txs measure]
                   (jdbc/execute-one! sql-conn
                                      ["SELECT v FROM my_table WHERE k = ?"
                                       (rand-int keyspace)])
                   (measure (jdbc/execute! sql-conn [tx (first txs)])))
        sql-add  (fn [^FastList txs]
                   (.add txs [(rand-int keyspace) (str (random-uuid))]))
        tx-fn    (case f
                   kv-async kv-async
                   kv-sync  kv-sync
                   dl-async dl-async
                   dl-sync  dl-sync
                   sql-tx   sql-tx)
        add-fn   (cond
                   kv?  kv-add
                   dl?  dl-add
                   sql? sql-add)]
    (max-write-bench 1 tx-fn add-fn)
    (when kvdb (d/close-kv kvdb))
    (when conn (d/close conn))
    (when sql-conn (.close sql-conn))))
