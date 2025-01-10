(ns datalevin-bench.core
  (:require
   [datalevin.core :as d]
   [clojure.string :as s])
  (:import
   [java.util.concurrent Semaphore]
   [org.eclipse.collections.impl.list.mutable FastList]))

;; max write throughput/latency benchmark

(def max-write-dbi "test")

;; limit the number of threads in flight
(def in-flight 1000)

;; total number of writes
(def total 100000000)

;; print numbers every this number of writes
(def report 1000000)

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
        (let [txs    (reduce
                       (fn [^FastList txs _]
                         (add-fn txs)
                         txs)
                       (FastList. batch-size)
                       (range 0 batch-size))
              before (System/currentTimeMillis)]
          (tx-fn txs measure)
          (vswap! write-time + (- (System/currentTimeMillis) before)))
        (recur (inc counter))))))

(defn write
  [{:keys [batch f]}]
  (let [kv?      (s/starts-with? (name f) "kv")
        kvdb     (when kv?
                   (doto (d/open-kv (str "max-write-db-" f "-" batch)
                                    {:mapsize 60000})
                     (d/open-dbi max-write-dbi)))
        kv-async (fn [txs measure]
                   (d/transact-kv-async kvdb max-write-dbi txs
                                        :uuid :uuid measure))
        kv-sync  (fn [txs measure]
                   (measure (d/transact-kv kvdb max-write-dbi txs
                                           :uuid :uuid)))
        kv-add   (fn [^FastList txs]
                   (.add txs [:put (random-uuid) (random-uuid)]))
        conn     (when (not kv?)
                   (d/get-conn (str "max-write-db-" f "-" batch)
                               {:k {:db/valueType :db.type/uuid}
                                :v {:db/valueType :db.type/uuid}}
                               {:kv-opts {:mapsize 200000}}))
        dl-async (fn [txs measure] (d/transact-async conn txs nil measure))
        dl-sync  (fn [txs measure] (measure (d/transact! conn (seq txs) nil)))
        dl-add   (fn [^FastList txs]
                   (.add txs {:k (random-uuid) :v (random-uuid)}))
        tx-fn    (case f
                   kv-async kv-async
                   kv-sync  kv-sync
                   dl-async dl-async
                   dl-sync  dl-sync)
        add-fn   (if kv? kv-add dl-add)]
    (max-write-bench batch tx-fn add-fn)
    (when kvdb (d/close-kv kvdb))
    (when conn (d/close conn))))
