(ns datalevin-bench.core
  (:require
   [datalevin.core :as d])
  (:import
   [org.eclipse.collections.impl.list.mutable FastList]))

;; max write throughput/latency benchmark

(def max-write-dbi "test")
(def max-write-async-kv-db
  (doto (d/open-kv "max-write-async-kv-db")
    (d/open-dbi max-write-dbi)))

(defn gen-uuid [] (str (random-uuid)))

(defn max-write-bench
  [batch-size tx-fn]
  (let [target     (long (/ 100000 batch-size))
        start-time (System/currentTimeMillis)
        prev-time  (volatile! start-time)
        sync-time  (volatile! start-time)
        sync-count (volatile! 0)
        inserted   (volatile! 0)
        measure    (fn [_]
                     (vreset! sync-time (System/currentTimeMillis))
                     (vswap! sync-count inc)
                     (vswap! inserted + batch-size))]
    (println "Time (seconds), Throughput (writes/second), Latency (milliseconds)")
    (loop [counter 0]
      (when (and (= 0 (mod counter target)) (not= 0 counter))
        (println (str
                   (long (/ (- @sync-time start-time) 1000))
                   ", "
                   (format "%.1f"
                           (double (* (/ @inserted (- @sync-time start-time))
                                      1000)))
                   ", "
                   (format "%.1f" (double (/ (- @sync-time @prev-time)
                                             @sync-count)))))
        (vreset! prev-time @sync-time)
        (vreset! sync-count 0))
      (let [txs (reduce
                  (fn [^FastList txs _]
                    (.add txs [:put [(gen-uuid) (gen-uuid)] (gen-uuid)])
                    txs)
                  (FastList. batch-size)
                  (range 0 batch-size))]
        (tx-fn txs measure))
      (recur (inc counter)))))

(defn kv-async
  [txs measure]
  (d/transact-kv-async max-write-async-kv-db max-write-dbi txs
                       [:string :string] :string measure))

(defn -main [&opts]
  (max-write-bench 100 kv-async)
  (d/close-kv max-write-async-kv-db))
