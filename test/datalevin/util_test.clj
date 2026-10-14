(ns datalevin.util-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.util :as u])
  (:import [clojure.lang Atom]
           [java.lang Thread$State]
           [java.util.concurrent Callable ConcurrentLinkedQueue CountDownLatch
            ExecutorService Future TimeUnit]))

(defn- await-getters [threads]
  (let [deadline (+ (System/nanoTime) 10000000000)]
    (loop []
      (cond
        (every? #(contains? #{Thread$State/BLOCKED Thread$State/TIMED_WAITING}
                            (.getState ^Thread %)) threads)
        nil
        (< (System/nanoTime) deadline)
        (do (Thread/sleep 1) (recur))
        :else (throw (AssertionError. "Executor getters did not reach publication"))))))

(defn- check-concurrent-executor [^Atom pool-atom get-pool shutdown]
  (let [initial @pool-atom
        validator (.getValidator pool-atom)
        created (ConcurrentLinkedQueue.)
        start (CountDownLatch. 1)
        calling (CountDownLatch. 8)
        publishing (CountDownLatch. 1)
        publish (CountDownLatch. 1)
        results (vec (repeatedly 8 promise))
        threads (mapv (fn [result]
                        (Thread.
                          ^Runnable
                          (fn []
                            (deliver result
                                     (try
                                       (.await start)
                                       (.countDown calling)
                                       (get-pool)
                                       (catch Throwable e e))))))
                      results)]
    ;; Pause publication through the atom's real validator, without redefining
    ;; Vars. Every caller must reach either publication or the creation lock
    ;; before proceeding, so the old read-then-reset race is deterministic.
    (.setValidator pool-atom
                   (fn [pool]
                     (when-not (identical? initial pool)
                       (.add created pool)
                       (.countDown publishing)
                       (assert (.await publish 10 TimeUnit/SECONDS)
                               "Executor publication timed out"))
                     true))
    (try
      (doseq [^Thread thread threads] (.start thread))
      (.countDown start)
      (assert (.await calling 10 TimeUnit/SECONDS) "Executor getters did not start")
      (assert (.await publishing 10 TimeUnit/SECONDS) "Executor was not created")
      (await-getters threads)
      (.countDown publish)
      (doseq [^Thread thread threads]
        (.join thread 10000)
        (assert (not (.isAlive thread)) "Executor getter did not finish"))
      (let [pools (mapv deref results)
            ^ExecutorService pool (first pools)]
        (doseq [result pools]
          (when (instance? Throwable result) (throw result)))
        (is (identical? pool @pool-atom))
        (doseq [^ExecutorService result pools]
          (is (identical? pool result))
          ;; Start real worker threads so shutdown also covers the non-daemon
          ;; threads retained by any orphaned executors on the unfixed path.
          (is (= :ran (.get ^Future (.submit result ^Callable (fn [] :ran))
                           10 TimeUnit/SECONDS))))
        (shutdown)
        (doseq [^ExecutorService result pools] (is (.isShutdown result))))
      (finally
        (.countDown start)
        (.countDown publish)
        (doseq [^Thread thread threads] (.join thread 10000))
        (.setValidator pool-atom validator)
        ;; Explicitly free every candidate, including any lost by the old race.
        (doseq [^ExecutorService pool (seq created)]
          (.shutdownNow pool)
          (.awaitTermination pool 10 TimeUnit/SECONDS))))))

(deftest lazy-executors-create-one-pool-for-concurrent-callers
  (doseq [[label pool-atom get-pool shutdown]
          [["workers" u/worker-thread-pool-atom u/get-worker-thread-pool
            u/shutdown-worker-thread-pool]
           ["scheduler" u/scheduler-atom u/get-scheduler u/shutdown-scheduler]]]
    (shutdown)
    (locking pool-atom (reset! pool-atom nil))
    (testing (str label " first creation")
      (check-concurrent-executor pool-atom get-pool shutdown))
    (testing (str label " replacement after shutdown")
      (check-concurrent-executor pool-atom get-pool shutdown))))

(deftest lisp-case-preserves-name-normalization-test
  (doseq [[input expected]
          [["db" "db"] ["db123" "db123"] ["123-db" "123-db"]
           ["a-b-c" "a-b-c"] ["SimpleName" "simple-name"]
           ["HTTPServer" "http-server"] ["simple_name" "simple-name"]
           ["--simple--name--" "simple-name"] ["aB" "a-b"]
           ["User42" "user42"] ["UPPER_CASE" "upper-case"]
           ["résumé" "r-sum"] ["" ""] [" -_ / " ""]]]
    (is (= expected (u/lisp-case input)) input)))
