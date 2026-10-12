(ns ycsb-bench.lifecycle-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.server :as server]
            [datalevin.client :as client]
            [datalevin.util :as u]
            [ycsb-bench.server :as owned-server]
            [ycsb-bench.core-test :as shared]
            [ycsb-bench.runner :as runner]
            [ycsb-bench.store :as store]
            [ycsb-bench.support :as support])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]
           [java.util.concurrent CountDownLatch ExecutionException TimeUnit]
           [java.util.concurrent.atomic AtomicInteger]))

(defn- check-cancellation! [reason]
  (let [entered (CountDownLatch. 1)
        cancelled (CountDownLatch. 1)
        release (CountDownLatch. 1)
        completed (CountDownLatch. 1)
        calls (AtomicInteger.)
        active (AtomicInteger.)
        closed (atom nil)
        used-after-close? (atom false)
        result (promise)
        db (reify store/Records
             (put-records! [_ _] nil)
             (record-count [_] 1)
             (read-record [_ _]
               (.incrementAndGet active)
               (try
                 (let [call (.incrementAndGet calls)]
                   (cond
                     (= call 1)
                     (do
                       (.countDown entered)
                       ;; Model a native operation that consumes interruptions
                       ;; but continues using its store until explicitly released.
                       (loop []
                         (when-not (try (.await release) true
                                        (catch InterruptedException _
                                          (.countDown cancelled)
                                          false))
                           (recur))))
                     (and (= reason :worker-failure) (= call 2))
                     (do (.await entered)
                         (throw (ex-info "Injected worker failure" {})))))
                 (when @closed (reset! used-after-close? true))
                 ["a"]
                 (finally (.decrementAndGet active))))
             (close-store! [_] (reset! closed {:active (.get active)})))
        coordinator
        (Thread.
          ^Runnable
          (fn []
            (try
              (runner/run-case!
                {:api :kv :mode :embedded :workload :c :distribution :uniform
                 :records 1 :ops 3 :warmup 0 :field-count 1 :field-length 1
                 :threads (if (= reason :worker-failure) 2 1)
                 :timeout-ms 30 :phase-timeout-ms (if (= reason :timeout) 500 10000)}
                {:with-stores (support/with-stores (fn [_] db))})
              (deliver result {:unexpected-success? true})
              (catch Throwable t
                (deliver result {:error t :interrupted? (.isInterrupted (Thread/currentThread))}))
              (finally (.countDown completed)))))]
    (.start coordinator)
    (try
      (is (.await entered 5 TimeUnit/SECONDS))
      (when (= reason :interruption) (.interrupt coordinator))
      (is (.await cancelled 5 TimeUnit/SECONDS))
      ;; Exceed the old shutdown limit while the native operation is active.
      (is (not (.await completed 150 TimeUnit/MILLISECONDS)))
      (is (nil? @closed))
      (.interrupt coordinator)
      (is (not (.await completed 150 TimeUnit/MILLISECONDS)))
      (is (nil? @closed))
      (finally
        (.countDown release)
        (.join coordinator 5000)))
    (is (not (.isAlive coordinator)))
    (is (= {:active 0} @closed))
    (is (false? @used-after-close?))
    (is (= (if (= reason :worker-failure) 2 1) (.get calls))
        "Clearing an interrupt must not allow another operation after cancellation")
    (let [{:keys [error interrupted?]} (deref result 1000 {})]
      (is (true? interrupted?))
      (case reason
        :timeout (is (= "Benchmark phase timed out" (some-> error ex-message)))
        :worker-failure
        (do (is (instance? ExecutionException error))
            (is (= "Injected worker failure" (some-> error ex-cause ex-message))))
        :interruption (is (instance? InterruptedException error))))))

(deftest cancellation-waits-before-store-cleanup-test
  (doseq [reason [:timeout :worker-failure :interruption]]
    (testing (name reason) (check-cancellation! reason))))

(deftest remote-password-uri-roundtrip-test
  ;; Set the actual admin password, then exercise the benchmark's URI builder
  ;; through authentication for both APIs, including URI delimiters and Unicode.
  (let [root (str (Files/createTempDirectory "datalevin-ycsb-password-"
                                            (make-array FileAttribute 0)))
        opts shared/small-options
        password "test pass +:/?@#% 雪"
        srv (owned-server/start! root opts)
        port (get-in srv [:info :port])]
    (try
      (let [admin (client/new-client (store/remote-uri port "" (server/get-default-password)))]
        (try
          (client/reset-password admin "datalevin" password)
          (finally (client/disconnect admin))))
      (doseq [api [:kv :datalog]]
        (testing (name api)
          (let [db (#'store/open-store! api
                                       (store/remote-uri port (str "password-" (name api)) password)
                                       (assoc opts :api api :mode :remote))]
            (try
              (shared/check-adapter! db)
              (finally (store/close-store! db))))))
      (finally
        ((:close srv))
        (u/delete-files root)))))
