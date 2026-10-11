(ns datalevin.tx-group-batch-collection-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.batch.charge :as charge])
  (:import [java.util Iterator]
           [java.util.concurrent ConcurrentLinkedQueue]
           [java.util.concurrent.atomic AtomicInteger]))

(defn- echo-values [b]
  (into-array Object
              (map #(batch/data (batch/batch-at b %))
                   (range (batch/batch-count b)))))

(defn- queued-descriptor
  (^datalevin.tx_group.batch.Descriptor [collector value deadline]
   (queued-descriptor collector value deadline nil))
  (^datalevin.tx_group.batch.Descriptor [collector value deadline context]
   (#'batch/admit! collector 1024 deadline)
   (batch/->Descriptor nil (volatile! value) context 1024 deadline 1024
                       (AtomicInteger. 0) (volatile! nil) (Thread/currentThread))))

(defn- counted-collector
  ^datalevin.tx_group.batch.Collector [opts visits]
  (let [^datalevin.tx_group.batch.Collector c (batch/create echo-values opts)
        queue (proxy [ConcurrentLinkedQueue] []
                (iterator []
                  (let [^ConcurrentLinkedQueue this this
                        ^Iterator it (proxy-super iterator)]
                    (reify Iterator
                      (hasNext [_] (.hasNext it))
                      (next [_] (swap! visits inc) (.next it))
                      (remove [_] (.remove it))))))]
    (batch/->Collector
      (.lock c) (.progress c) queue (.queued c) (.queued-deadlines c)
      (.active c) (.serving c) (.active-batch c) (.failure c) (.waiters c)
      (.budget c) (.max-requests c) (.batch-limit c) (.batch-max-bytes c)
      (.shared-reserved c) (.preparation-timeout-ms c) (.collection-delay-nanos c)
      (.rmw-allowance c) (.published c) (.next-id c) (.executor c)
      (.check-prepared! c) (.on-failure! c))))

(deftest selection-does-not-visit-the-unbounded-tail-after-a-boundary
  (doseq [boundary [:count :bytes :owner-isolated :tail-isolated]
          timed-owner? [false true]]
    (let [visits (atom 0)
          collector (counted-collector
                      {:limits (charge/resolve-limits
                                 {:write-batch-size (if (= boundary :count) 1 256)
                                  :write-batch-max-bytes (if (= boundary :bytes) 1024 8388608)
                                  :wal-rmw-max-bytes 1024})}
                      visits)
          deadline (if timed-owner? (+ (System/nanoTime) 60000000000) 0)
          ds (mapv #(queued-descriptor collector % (if (zero? %) deadline 0)
                                      {:isolated? (or (and (= boundary :owner-isolated) (zero? %))
                                                      (and (= boundary :tail-isolated) (= % 1)))})
                   (range 128))]
      (doseq [d ds] (#'batch/publish-and-elect! collector d))
      (let [selected (#'batch/claim-next-batch! collector (first ds))]
        (is (= 1 (batch/batch-count selected)))
        (is (= (if (= boundary :tail-isolated) 2 1) @visits))
        (is (= deadline (batch/batch-cutoff selected)))
        (is (zero? (.get ^AtomicInteger (.queued-deadlines collector))))
        (reset! visits 0)
        (is (= 0 (batch/collect-ready! selected)))
        (is (= (if (= boundary :tail-isolated) 1 0) @visits))
        (#'batch/run-sealed-batch! collector selected))
      (batch/close! collector)
      (doseq [d ds] (#'batch/release-descriptor! collector d))
      (is (zero? (:requests (batch/usage collector)))))))

(deftest queue-deadline-count-tracks-all-removal-paths
  (doseq [removal [:unlink :release :close :fence :select :collect]]
    (let [^datalevin.tx_group.batch.Collector collector (batch/create echo-values)
          owner (queued-descriptor collector :owner 0)
          timed (queued-descriptor collector :timed (+ (System/nanoTime) 60000000000))]
      (#'batch/publish-and-elect! collector owner)
      (let [selected (when (= removal :collect)
                       (#'batch/claim-next-batch! collector owner))]
        (#'batch/publish-and-elect! collector timed)
        (is (= 1 (.get ^AtomicInteger (.queued-deadlines collector))))
        (case removal
          :unlink (#'batch/unlink! collector timed)
          :release (#'batch/release-descriptor! collector timed)
          :close (batch/close! collector)
          :fence (batch/fence! collector (ex-info "test fence" {}))
          :select (#'batch/run-sealed-batch!
                    collector (#'batch/claim-next-batch! collector owner))
          :collect (do (is (= 1 (batch/collect-ready! selected)))
                       (#'batch/run-sealed-batch! collector selected)))
        (is (zero? (.get ^AtomicInteger (.queued-deadlines collector)))))
      (batch/close! collector)
      (doseq [d [owner timed]] (#'batch/release-descriptor! collector d))
      (is (zero? (:requests (batch/usage collector)))))))

(deftest blocked-selection-stops-after-the-last-queued-deadline
  (doseq [collect? [false true] expired? [false true]]
    (let [visits (atom 0)
          collector (counted-collector nil visits)
          owner (queued-descriptor collector :owner 0 {:isolated? true})
          deadline (if expired? 1 (+ (System/nanoTime) 60000000000))
          tail (mapv #(queued-descriptor collector % (if (= % 3) deadline 0)) (range 128))]
      (#'batch/publish-and-elect! collector owner)
      (let [selected (when collect? (#'batch/claim-next-batch! collector owner))]
        (doseq [d tail] (#'batch/publish-and-elect! collector d))
        (reset! visits 0)
        (let [selected (or selected (#'batch/claim-next-batch! collector owner))]
          (when collect? (is (= 0 (batch/collect-ready! selected))))
          (is (= (if collect? 4 5) @visits))
          (is (= (if expired? 0 1) (.get ^AtomicInteger (.queued-deadlines collector))))
          (is (= expired? (some? @(.result ^datalevin.tx_group.batch.Descriptor (tail 3)))))
          (#'batch/run-sealed-batch! collector selected)))
      (batch/close! collector)
      (is (zero? (.get ^AtomicInteger (.queued-deadlines collector))))
      (doseq [d (cons owner tail)] (#'batch/release-descriptor! collector d))
      (is (zero? (:requests (batch/usage collector)))))))

(deftest full-collection-leaves-tail-expiry-to-next-sealing
  (doseq [limit [1 2]]
    (let [collector (batch/create echo-values
                                  {:limits (charge/resolve-limits
                                             {:write-batch-size limit})})
          owner (queued-descriptor collector :owner 0)
          expired-prefix (when (= limit 2)
                           (queued-descriptor collector :expired-prefix 1))
          added (when (= limit 2) (queued-descriptor collector :added 0))
          tail (queued-descriptor collector :tail 0)
          expired-tail (queued-descriptor collector :expired-tail 1)]
      (#'batch/publish-and-elect! collector owner)
      (let [selected (#'batch/claim-next-batch! collector owner)]
        (doseq [d (concat (when (= limit 2) [expired-prefix added])
                         [tail expired-tail])]
          (#'batch/publish-and-elect! collector d))
        (is (= (dec limit) (batch/collect-ready! selected)))
        (is (= limit (batch/batch-count selected)))
        (when (= limit 2)
          (is (= :txlog/write-deadline-exceeded
                 (:error (ex-data (second @(.result expired-prefix)))))))
        (is (nil? @(.result expired-tail)))
        (is (= 0 (batch/collect-ready! selected)))
        (is (nil? @(.result expired-tail)))
        (is (false? (batch/selected? tail)))
        (#'batch/run-sealed-batch! collector selected))
      (is (true? (#'batch/try-lead! collector tail)))
      (#'batch/lead! collector tail)
      (is (= [true :tail] @(.result tail)))
      (is (= :txlog/write-deadline-exceeded
             (:error (ex-data (second @(.result expired-tail))))))
      (doseq [d (remove nil? [owner expired-prefix added tail expired-tail])]
        (#'batch/release-descriptor! collector d))
      (is (batch/serving? collector))
      (is (zero? (:requests (batch/usage collector))))
      (is (zero? (:bytes (batch/usage collector)))))))
