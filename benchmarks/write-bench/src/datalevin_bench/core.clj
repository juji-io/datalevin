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
    "Number of Writes,Time (seconds),Throughput (writes/second),Call Latency (milliseconds),Commit Latency (milliseconds)"))

(defn print-row
  [written inserted write-time sync-count sync-time prev-time start-time]
  (let [duration (- @sync-time start-time)]
    (println
      (str
        written
        ","
        (format "%.2f" (double (/ duration 1000)))
        ","
        (format "%.2f" (double (* (/ @inserted duration) 1000)))
        ","
        (format "%.2f" (double (/ @write-time @sync-count)))
        ","
        (format "%.2f" (double (/ (- @sync-time @prev-time) @sync-count)))))))

(def prepare-stage-order
  [:normalize
   :resolve-ids
   :apply-op-semantics
   :delta-plan
   :side-index-overlay-build
   :finalize
   :execute])

(defn- empty-prepare-summary
  []
  {:tx-count 0
   :stages   {}})

(defn- add-measure
  [stage-summary value total-k sample-k]
  (if (number? value)
    (-> stage-summary
        (update total-k (fnil + 0) value)
        (update sample-k (fnil inc 0)))
    stage-summary))

(defn- accumulate-stage
  [stage-summary stage-stats]
  (-> (or stage-summary {})
      (add-measure (:elapsed-ns stage-stats) :elapsed-ns :elapsed-samples)
      (add-measure (:input-count stage-stats) :input-count :input-samples)
      (add-measure (:output-count stage-stats) :output-count :output-samples)
      (add-measure (:tx-datoms stage-stats) :tx-datoms :tx-datoms-samples)
      (add-measure (:tempids stage-stats) :tempids :tempids-samples)))

(defn- accumulate-prepare-summary
  [summary prepare-stats]
  (reduce-kv
    (fn [s stage stage-stats]
      (update-in s [:stages stage] accumulate-stage stage-stats))
    (update summary :tx-count inc)
    prepare-stats))

(defn- avg
  [total samples]
  (when (and (number? samples) (pos? samples))
    (double (/ total samples))))

(defn- print-prepare-summary
  [summary]
  (binding [*out* *err*]
    (println "# Prepare stage summary")
    (println "# stage,avg-ms,total-ms,samples,avg-input,avg-output,avg-tx-datoms,avg-tempids")
    (doseq [stage prepare-stage-order
            :let [s (get-in summary [:stages stage])]
            :when s]
      (let [elapsed-ms     (double (/ (or (:elapsed-ns s) 0) 1000000.0))
            elapsed-samples (or (:elapsed-samples s) 0)
            avg-ms         (or (avg elapsed-ms elapsed-samples) 0.0)
            avg-input      (avg (:input-count s) (:input-samples s))
            avg-output     (avg (:output-count s) (:output-samples s))
            avg-tx-datoms  (avg (:tx-datoms s) (:tx-datoms-samples s))
            avg-tempids    (avg (:tempids s) (:tempids-samples s))]
        (println
          (str stage
               ","
               (format "%.6f" avg-ms)
               ","
               (format "%.3f" elapsed-ms)
               ","
               elapsed-samples
               ","
               (if avg-input (format "%.2f" avg-input) "")
               ","
               (if avg-output (format "%.2f" avg-output) "")
               ","
               (if avg-tx-datoms (format "%.2f" avg-tx-datoms) "")
               ","
               (if avg-tempids (format "%.2f" avg-tempids) "")))))))

(defn max-write-bench
  ([batch-size tx-fn add-fn async?]
   (max-write-bench batch-size tx-fn add-fn async? nil))
  ([batch-size tx-fn add-fn async? on-report]
   (print-header)
   (let [sem        (Semaphore. (* in-flight batch-size))
         write-time (volatile! 0)
         sync-count (volatile! 0)
         inserted   (volatile! 0)
         start-time (System/currentTimeMillis)
         prev-time  (volatile! start-time)
         sync-time  (volatile! start-time)
         measure    (fn [res]
                      (when on-report (on-report res))
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
                        sync-time prev-time start-time))))))))

(def id (volatile! 0))

(defn- path-under
  [base leaf]
  (if (and (string? base) (not (s/blank? base)))
    (str (java.io.File. base leaf))
    leaf))

(defn- resolve-collect-prepare-stats?
  [use-prepare-path? collect-prepare-stats?]
  (if (some? collect-prepare-stats?)
    (boolean collect-prepare-stats?)
    (boolean use-prepare-path?)))

(defn write
  [{:keys [base-dir batch f use-prepare-path? collect-prepare-stats?]}]
  (let [nf       (name f)
        kv?      (s/starts-with? nf "kv")
        dl?      (s/starts-with? nf "dl")
        sql?     (s/starts-with? nf "sql")
        async?   (s/ends-with? nf "async")
        collect-prepare-stats? (resolve-collect-prepare-stats?
                                 use-prepare-path?
                                 collect-prepare-stats?)
        kv-dir   (path-under base-dir (str f "-" batch))
        sql-dir  (path-under base-dir (str "sqlite-" batch))
        kvdb     (when kv?
                   (doto (d/open-kv kv-dir
                                    {:mapsize 60000
                                     :flags   (-> c/default-env-flags
                                                  ;; (conj :writemap)
                                                  ;; (conj :mapasync)
                                                  (conj :nosync)
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
                   (d/get-conn kv-dir
                               {:k {:db/valueType :db.type/long}
                                :v {:db/valueType :db.type/string}}
                               {:kv-opts {:mapsize 60000
                                          :flags   (-> c/default-env-flags
                                                       ;; (conj :writemap)
                                                       ;; (conj :mapasync)
                                                       ;; (conj :nosync)
                                                       ;; (conj :nometasync)
                                                       )
                                          }}))
        dl-async (fn [txs measure] (d/transact-async conn txs nil measure))
        dl-sync  (fn [txs measure] (measure (d/transact! conn txs nil)))
        dl-add   (fn [^FastList txs]
                   (.add txs {:k (vswap! id + 2) :v (str (random-uuid))}))
        sql-conn (when sql?
                   (let [conn (jdbc/get-connection
                                {:dbtype "sqlite"
                                 :dbname sql-dir})]
                     (jdbc/execute! conn ["PRAGMA journal_mode=WAL;"])
                     (jdbc/execute! conn ["PRAGMA synchronous=FULL;"])
                     (jdbc/execute! conn ["PRAGMA synchronous=NORMAL;"])
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
                   sql? sql-add)
        prepare-summary (when (and dl? collect-prepare-stats?)
                          (atom (empty-prepare-summary)))
        on-report (when (and dl? collect-prepare-stats?)
                    (fn [res]
                      (when-let [ps (and (map? res) (:prepare-stats res))]
                        (swap! prepare-summary
                               accumulate-prepare-summary ps))))]
    (binding [c/*use-prepare-path*      (boolean use-prepare-path?)
              c/*collect-prepare-stats* collect-prepare-stats?]
      (max-write-bench batch tx-fn add-fn async? on-report))
    (when (and prepare-summary (pos? (:tx-count @prepare-summary)))
      (print-prepare-summary @prepare-summary))
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
  [{:keys [dir f use-prepare-path? collect-prepare-stats?]}]
  (let [nf       (name f)
        kv?      (s/starts-with? nf "kv")
        dl?      (s/starts-with? nf "dl")
        sql?     (s/starts-with? nf "sql")
        collect-prepare-stats? (resolve-collect-prepare-stats?
                                 use-prepare-path?
                                 collect-prepare-stats?)
        kvdb     (when kv?
                   (doto (d/open-kv dir
                                    {:mapsize 60000
                                     :flags   (-> c/default-env-flags
                                                  ;; (conj :writemap)
                                                  ;; (conj :mapasync)
                                                  ;; (conj :nosync)
                                                  ;; (conj :nometasync)
                                                  )})
                     (d/open-dbi max-write-dbi)))
        kv-async (fn [txs measure]
                   (d/get-value kvdb max-write-dbi (random-int) :id :string)
                   (d/transact-kv-async kvdb max-write-dbi txs
                                        :id :string measure))
        kv-sync  (fn [txs measure]
                   (d/get-value kvdb max-write-dbi (random-int) :id :string)
                   (measure (d/transact-kv kvdb max-write-dbi txs
                                           :id :string)))
        kv-add   (fn [^FastList txs]
                   (.add txs [:put (random-int) (str (random-uuid))]))
        conn     (when dl?
                   (d/get-conn dir {:k {:db/valueType :db.type/long}
                                    :v {:db/valueType :db.type/string}}
                               {:kv-opts {:mapsize 60000
                                          :flags   (-> c/default-env-flags
                                                       ;; (conj :writemap)
                                                       ;; (conj :mapasync)
                                                       ;; (conj :nosync)
                                                       ;; (conj :nometasync)
                                                       )}}))
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
                   sql? sql-add)
        prepare-summary (when (and dl? collect-prepare-stats?)
                          (atom (empty-prepare-summary)))
        on-report (when (and dl? collect-prepare-stats?)
                    (fn [res]
                      (when-let [ps (and (map? res) (:prepare-stats res))]
                        (swap! prepare-summary
                               accumulate-prepare-summary ps))))]
    (binding [c/*use-prepare-path*      (boolean use-prepare-path?)
              c/*collect-prepare-stats* collect-prepare-stats?]
      (max-write-bench 1 tx-fn add-fn false on-report))
    (when (and prepare-summary (pos? (:tx-count @prepare-summary)))
      (print-prepare-summary @prepare-summary))
    (when kvdb (d/close-kv kvdb))
    (when conn (d/close conn))
    (when sql-conn (.close sql-conn))))

(defn dl-init
  [{:keys [dir]}]
  (let [es      (range 1 (inc total))
        datoms1 (mapv (fn [e k] (d/datom e :k k))
                      es (repeatedly total random-int))
        datoms2 (mapv (fn [e v] (d/datom e :v v))
                      es (repeatedly total #(str (random-uuid))))
        start   (System/currentTimeMillis)
        db      (-> (d/init-db datoms1 dir {:k {:db/valueType :db.type/long}
                                            :v {:db/valueType :db.type/string}}
                               {:kv-opts {:mapsize 60000}})
                    (d/fill-db datoms2))]
    (println "took" (- (System/currentTimeMillis) start) "milliseconds")
    (d/close-db db)))
