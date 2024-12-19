(ns datalevin-bench.core
  (:require
   [datalevin.core :as d])
  (:import
   [java.util.concurrent Semaphore]
   [org.eclipse.collections.impl.list.mutable FastList]))

;; max write throughput/latency benchmark

(def max-write-dbi "test")

(defn gen-uuid [] (str (random-uuid)))

;; limit the number of threads in flight
(def in-flight 1000)

(defn max-write-bench
  [batch-size tx-fn]
  (let [sem        (Semaphore. (* in-flight batch-size))
        target     (long (/ 100000 batch-size))
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
    (println
      "Time (seconds),Throughput (writes/second),Write Latency (milliseconds),Commit Latency (milliseconds)")
    (loop [counter 0]
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
                       (.add txs [:put [(gen-uuid) (gen-uuid)] (gen-uuid)])
                       txs)
                     (FastList. batch-size)
                     (range 0 batch-size))
            before (System/currentTimeMillis)]
        (tx-fn txs measure)
        (vswap! write-time + (- (System/currentTimeMillis) before)))
      (recur (inc counter)))))

(defn write
  [{:keys [batch f]}]
  (let [db       (doto (d/open-kv (str "max-write-db-" f "-" batch)
                                  {:mapsize 60000})
                   (d/open-dbi max-write-dbi))
        kv-async (fn [txs measure]
                   (d/transact-kv-async db max-write-dbi txs
                                        [:string :string] :string measure))
        kv-sync  (fn [txs measure]
                   (measure (d/transact-kv db max-write-dbi txs
                                           [:string :string] :string)))]
    (max-write-bench batch (condp = f
                             'kv-async kv-async
                             'kv-sync  kv-sync))
    (d/close-kv db)))
