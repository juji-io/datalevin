(ns datalevin.server.password-hashing-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.client :as client]
            [datalevin.server :as server]
            [datalevin.server.auth :as auth]
            [datalevin.test.core :refer [allocate-port db-fixture]]
            [datalevin.util :as u])
  (:import [java.util.concurrent CountDownLatch Semaphore TimeUnit]))

(use-fixtures :once db-fixture)

(def ^:private ^:const mib 1048576)
(def ^:private ^:const default-memory-kib 131072)

(defn- outcome [f] (try (f) (catch Throwable t t)))

(defn- admitted [capacity memory-kib f]
  (#'auth/with-password-hashing-capacity capacity memory-kib f))

(defn- busy? [error]
  (= {:error :server/busy :retryable? true :reason :password-hashing-capacity}
     (ex-data error)))

(defn- with-running-hashes [capacity memory-kib n f]
  (let [entered (CountDownLatch. n)
        release (CountDownLatch. 1)
        tasks (vec (repeatedly n
                     #(future
                        (outcome
                          (fn []
                            (admitted capacity memory-kib
                                      (fn []
                                        (.countDown entered)
                                        (when-not (.await release 5 TimeUnit/SECONDS)
                                          (throw (ex-info "Hash barrier timed out" {})))
                                        :finished)))))))]
    (try
      (is (.await entered 5 TimeUnit/SECONDS))
      (f)
      (finally
        (.countDown release)
        (doseq [task tasks] (is (= :finished (deref task 5000 ::timeout))))))))

(deftest hashing-capacity-scales-with-heap-and-has-a-separate-concurrency-cap
  (doseq [[heap-mib allowed] [[256 0] [512 1] [1024 2] [2048 4] [8192 4]]]
    (testing (str heap-mib " MiB heap")
      (let [heap (* mib (long heap-mib))
            capacity (#'auth/password-hashing-capacity (constantly heap))
            invoked (atom false)]
        (with-running-hashes
          capacity default-memory-kib allowed
          #(let [attempt (future (outcome (fn []
                                           (admitted capacity default-memory-kib
                                                     (fn [] (reset! invoked true))))))]
             (is (= {:error :server/busy :retryable? (pos? (long allowed))
                     :reason :password-hashing-capacity}
                    (ex-data (deref attempt 1000 ::timeout))))
             (is (false? @invoked) "rejection precedes the hashing callback")))
        (when (pos? (long allowed))
          (is (= :available (admitted capacity default-memory-kib (constantly :available)))))))))

(deftest larger-hashes-consume-proportionally-more-memory-capacity
  (let [heap (* 1024 mib)
        capacity (#'auth/password-hashing-capacity (constantly heap))]
    (with-running-hashes
      capacity (* 2 default-memory-kib) 1
      #(is (busy? (outcome (fn []
                            (admitted capacity default-memory-kib
                                      (fn [] (throw (AssertionError. "Hash must not run")))))))))))

(deftest concurrent-hashes-share-one-memory-budget
  (let [capacity (#'auth/password-hashing-capacity (constantly (* 1024 mib)))]
    (with-running-hashes
      capacity (* 3 (quot default-memory-kib 2)) 1
      (fn []
        ;; A 192 MiB hash reserves 384 MiB of this 512 MiB budget. A further
        ;; default hash cannot fit, but a 64 MiB hash can still use the rest.
        (is (busy? (outcome #(admitted capacity default-memory-kib (constantly :unexpected)))))
        (is (= :available (admitted capacity (quot default-memory-kib 2) (constantly :available))))))
    (is (= :available (admitted capacity default-memory-kib (constantly :available))))))

(deftest hashing-failures-return-both-memory-and-concurrency-capacity
  (let [heap (* 512 mib)
        capacity (#'auth/password-hashing-capacity (constantly heap))]
    (doseq [failure [(ex-info "Hash failed" {}) (AssertionError. "Hash failed")]]
      (is (identical? failure
                      (outcome #(admitted capacity default-memory-kib (fn [] (throw failure))))))
      (is (= :available (admitted capacity default-memory-kib (constantly :available)))))))

(deftest cancellation-cannot-return-capacity-while-hashing-still-runs
  (let [heap (* 512 mib)
        capacity (#'auth/password-hashing-capacity (constantly heap))
        entered (promise)
        interrupted (promise)
        release (CountDownLatch. 1)
        result (promise)
        thread (Thread.
                 ^Runnable
                 #(deliver result
                           (outcome
                             (fn []
                               (admitted
                                 capacity default-memory-kib
                                 (fn []
                                   (deliver entered true)
                                   (loop []
                                     (when-not (try (.await release 5 TimeUnit/SECONDS)
                                                    (catch InterruptedException _
                                                      (deliver interrupted true)
                                                      false))
                                       (recur)))
                                   :finished))))))]
    (try
      (.start thread)
      (is (true? (deref entered 5000 false)))
      (.interrupt thread)
      (is (true? (deref interrupted 5000 false)))
      (is (busy? (outcome #(admitted capacity default-memory-kib (constantly :unexpected)))))
      (.countDown release)
      (.join thread 5000)
      (is (not (.isAlive thread)))
      (is (= :finished (deref result 1000 ::timeout)))
      (is (= :available (admitted capacity default-memory-kib (constantly :available))))
      (finally (.countDown release) (.join thread 5000)))))

(deftest password-hashing-entry-points-share-the-process-limit
  (let [^Semaphore slots (:slots @#'auth/hashing-capacity)
        held (.drainPermits slots)]
    (try
      (is (busy? (outcome #(auth/password-hashing "password" (auth/salt)))))
      (is (busy? (outcome #(auth/password-matches? "incorrect" "stored-hash" (auth/salt)))))
      (is (busy? (outcome #(auth/transact-new-password nil "known-user" "new-password"))))
      (finally (.release slots held)))))

(deftest authentication-overload-is-retryable-and-does-not-create-a-session
  (let [root (u/tmp-dir (str "password-hashing-admission-" (random-uuid)))
        port (allocate-port)
        srv (server/create {:root root :port port})
        ^Semaphore slots (:slots @#'auth/hashing-capacity)]
    (try
      (server/start srv)
      (let [held (.drainPermits slots)]
        (try
          (is (busy? (outcome #(client/new-client
                                (str "dtlv://datalevin:incorrect@127.0.0.1:" port)
                                {:pool-size 1 :time-out 1000}))))
          (is (empty? (.-clients ^datalevin.server.Server srv)))
          (finally (.release slots held))))
      (let [c (client/new-client (str "dtlv://datalevin:datalevin@127.0.0.1:" port)
                                  {:pool-size 1 :time-out 3000})]
        (try (is (= [] (client/list-databases c)))
             (finally (client/disconnect c))))
      (finally (server/stop srv) (u/delete-files root)))))
