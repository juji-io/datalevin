(ns datalevin-bench.core
  "Max write throughput benchmark"
  (:require
   [datalevin.core :as d]
   [datalevin.constants :as c]
   [clojure.string :as s]
   [next.jdbc :as jdbc]
   [next.jdbc.sql :as sql])
  (:import
   [java.util Random]
   [java.util.concurrent Semaphore]
   [org.eclipse.collections.impl.list.mutable FastList]))

;; for kv
(def max-write-dbi "test")

;; limit the number of threads in flight
(def in-flight 1000)

;; total number of writes for a task
(def total 1000000)

;; integer key range
(def keyspace (* 2 total))

;; measure every this number of writes, also when to deref futures for async
(def report 10000)

(defn print-header []
  (println
    "Number of Writes,Time (seconds),Throughput (writes/second),Write Latency (milliseconds),Commit Latency (milliseconds)"))

(defn print-row
  [written inserted write-time sync-count sync-time prev-time start-time]
  (let [duration (- @sync-time start-time)]
    (println
      (str
        written
        ","
        (format "%.2f" (double (/ duration 1000)))
        ","
        (format "%.1f" (double (* (/ @inserted duration) 1000)))
        ","
        (format "%.1f" (double (/ @write-time @sync-count)))
        ","
        (format "%.1f" (double (/ (- @sync-time @prev-time)
                                  @sync-count)))))))

(defn max-write-bench
  [batch-size tx-fn add-fn async?]
  (print-header)
  (let [sem        (Semaphore. (* in-flight batch-size))
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
    (loop [counter 0
           fut     nil]
      (let [written (* counter batch-size)]
        (if (< written total)
          (do
            (.acquire sem batch-size)
            (when (and (= 0 (mod written report))
                       (not= 0 counter)
                       (not= 0 @sync-count))
              (when async? @fut)
              (print-row (* counter batch-size) inserted write-time sync-count
                         sync-time prev-time start-time)
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
                  before (System/currentTimeMillis)
                  fut    (tx-fn txs measure)]
              (vswap! write-time + (- (System/currentTimeMillis) before))
              (recur (inc counter) fut)))
          (do
            (when async? @fut)
            (print-row written inserted write-time sync-count
                       sync-time prev-time start-time)))))))

(def id (volatile! 0))

(defn write
  [{:keys [batch f]}]
  (let [nf       (name f)
        kv?      (s/starts-with? nf "kv")
        dl?      (s/starts-with? nf "dl")
        sql?     (s/starts-with? nf "sql")
        async?   (s/ends-with? nf "async")
        kvdb     (when kv?
                   (doto (d/open-kv (str f "-" batch)
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
                   (d/get-conn (str f "-" batch)
                               {:k {:db/valueType :db.type/long}
                                :v {:db/valueType :db.type/string}}
                               {:kv-opts {:mapsize 60000}}))
        dl-async (fn [txs measure] (d/transact-async conn txs nil measure))
        dl-sync  (fn [txs measure] (measure (d/transact! conn txs nil)))
        dl-add   (fn [^FastList txs]
                   (.add txs {:k (vswap! id + 2) :v (str (random-uuid))}))
        sql-conn (when sql?
                   (let [conn (jdbc/get-connection
                                {:dbtype "sqlite"
                                 :dbname (str "sqlite-" batch)})]
                     (jdbc/execute! conn ["PRAGMA journal_mode=WAL;"])
                     (jdbc/execute! conn ["PRAGMA synchronous=FULL;"])
                     (jdbc/execute! conn ["CREATE TABLE IF NOT EXISTS my_table (
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
    (max-write-bench batch tx-fn add-fn async?)
    (when kvdb
      (let [written (d/entries kvdb max-write-dbi)]
        (when-not (= written total) (println "Write only" written)))
      (d/close-kv kvdb))
    (when conn
      (let [datoms (d/count-datoms (d/db conn) nil nil nil)]
        (when-not (= datoms (* 2 total)) (println "Write only" datoms)))
      (d/close conn))
    (when sql-conn
      (let [written (-> (jdbc/execute! sql-conn ["SELECT count(1) FROM my_table"])
                        ffirst
                        val)]
        (when-not (= written total) (println "Write only" written)))
      (.close sql-conn))))

(def random (Random.))

(defn random-int [] (.nextInt random keyspace))

(defn mixed
  [{:keys [dir f]}]
  (let [nf       (name f)
        kv?      (s/starts-with? nf "kv")
        dl?      (s/starts-with? nf "dl")
        sql?     (s/starts-with? nf "sql")
        kvdb     (when kv?
                   (doto (d/open-kv dir
                                    {:mapsize 60000
                                     :flags   c/default-env-flags})
                     (d/open-dbi max-write-dbi)))
        kv-async (fn [txs measure]
                   (d/get-value kvdb max-write-dbi (random-int) :id)
                   (d/transact-kv-async kvdb max-write-dbi txs
                                        :id :string measure))
        kv-sync  (fn [txs measure]
                   (d/get-value kvdb max-write-dbi (random-int) :id)
                   (measure (d/transact-kv kvdb max-write-dbi txs
                                           :id :string)))
        kv-add   (fn [^FastList txs]
                   (.add txs [:put (random-int) (str (random-uuid))]))
        conn     (when dl?
                   (d/get-conn dir {:k {:db/valueType :db.type/long}
                                    :v {:db/valueType :db.type/string}}
                               {:kv-opts {:mapsize 60000}}))
        query    '[:find (pull ?e [:v])
                   :in $ ?k
                   :where [?e :k ?k]]
        dl-async (fn [txs measure]
                   (d/q query (d/db conn) (random-int))
                   (d/transact-async conn txs nil measure))
        dl-sync  (fn [txs measure]
                   (d/q query (d/db conn) (random-int))
                   (measure (d/transact! conn txs nil)))
        dl-add   (fn [^FastList txs]
                   (.add txs {:k (random-int) :v (str (random-uuid))}))
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
                                       (random-int)])
                   (let [vs (first txs)]
                     (measure (jdbc/execute! sql-conn [tx (first vs) (peek vs)]))))
        sql-add  (fn [^FastList txs]
                   (.add txs [(random-int) (str (random-uuid))]))
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
    (max-write-bench 1 tx-fn add-fn false)
    (when kvdb (d/close-kv kvdb))
    (when conn (d/close conn))
    (when sql-conn (.close sql-conn))))
