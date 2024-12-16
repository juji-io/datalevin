(ns datalevin-bench.core
  (:require
   [datalevin.core :as d]
   [datalevin.constants :as c])
  (:import
   [org.eclipse.collections.impl.list.mutable FastList]
   [java.util.concurrent Semaphore]))

;; max write throughput benchmark

(def max-write-dbi "test")
(def max-write-async-kv-db
  (doto (d/open-kv "max-write-async-kv-db")
    (d/open-dbi max-write-dbi)))

(defn gen-uuid [] (str (random-uuid)))

(defn max-write-async-kv [num-pending batch-size]
  (println "batch-limit" c/*transact-kv-async-batch-limit*
           "batch-size" batch-size)
  (let [sem        (Semaphore. num-pending)
        target     (long (/ 100000 batch-size))
        start-time (System/currentTimeMillis)
        inserted   (atom 0)]
    (loop [counter 0]
      (.acquire sem batch-size)
      (when (= 0 (mod counter target))
        (println "INSERTED" @inserted "in"
                 (- (System/currentTimeMillis) start-time) "millis"))
      (let [txs (reduce
                  (fn [^FastList txs _]
                    (.add txs [:put [(gen-uuid) (gen-uuid)] (gen-uuid)])
                    txs)
                  (FastList. batch-size)
                  (range 0 batch-size))]
        (d/transact-kv-async max-write-async-kv-db max-write-dbi txs
                             [:string :string] :string
                             (fn [_]
                               (.release sem batch-size)
                               (swap! inserted + batch-size))))
      (recur (inc counter)))))

(defn -main [&opts]
  (max-write-async-kv 100000 500)
  (d/close-kv max-write-async-kv-db))
