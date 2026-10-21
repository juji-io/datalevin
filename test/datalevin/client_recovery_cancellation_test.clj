(ns datalevin.client-recovery-cancellation-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.client :as client]
            [datalevin.client.request :as cr]
            [datalevin.client-recovery-chaos :as chaos]
            [datalevin.server :as server]
            [datalevin.server.session :as session]
            [datalevin.test.core :refer [allocate-port db-fixture]]
            [datalevin.util :as u])
  (:import [datalevin.client ClientState IClientInternals]
           [datalevin.server Server]
           [java.io IOException]
           [java.util.concurrent ConcurrentHashMap CountDownLatch TimeUnit]
           [java.util.concurrent.atomic AtomicReference]))

(use-fixtures :once db-fixture)

(defn- await! [pred]
  (let [deadline (+ (System/nanoTime) 5000000000)]
    (loop []
      (when-not (pred)
        (when (>= (System/nanoTime) deadline)
          (throw (ex-info "Recovery cancellation barrier timed out" {})))
        (Thread/sleep 5)
        (recur)))))

(defn- worker [f]
  (let [result (promise)
        thread (Thread.
                 ^Runnable
                 (fn []
                   (let [before (.isInterrupted (Thread/currentThread))
                         value (try (f) (catch Throwable error error))]
                     (deliver result {:value value :before before
                                      :after (.isInterrupted (Thread/currentThread))}))))]
    (.start thread)
    {:thread thread :result result}))

(defn- await-waiter! [{:keys [^Thread thread]}]
  ;; Observe the actual shared wait instead of relying on scheduling sleeps.
  (await! #(some (fn [^StackTraceElement frame]
                   (.contains (.getClassName frame) "$wait_for_recovery_BANG_"))
                 (.getStackTrace thread))))

(defn- result! [task]
  (let [result (deref (:result task) 5000 ::timeout)]
    (when (= ::timeout result) (throw (ex-info "Recovery caller did not finish" {})))
    result))

(defn- assert-success! [task expected]
  (let [{:keys [value before after]} (result! task)]
    (is (= expected value))
    (is (false? before))
    (is (false? after))))

(defn- assert-interrupted! [task]
  (let [{:keys [value before after]} (result! task)]
    (is (instance? Throwable value))
    (is (false? before))
    (is (true? after))))

(defn- stop-workers! [tasks]
  (doseq [{:keys [^Thread thread]} tasks]
    (when (.isAlive thread) (.interrupt thread)))
  (doseq [{:keys [^Thread thread]} tasks]
    (.join thread 5000)
    (is (not (.isAlive thread)))))

(defn- ha-recovery [failure-mode]
  (let [base (Object.)
        entered (CountDownLatch. 1)
        release (CountDownLatch. 1)
        attempts (atom 0)
        created (Object.)
        failure (ex-info "Authentication rejected" {:error :authentication/failed})
        req {:type :list-databases}
        context {:client base :time-out 5000}
        new-client (fn [_ _ _]
                     (when (= 1 (swap! attempts inc))
                       (.countDown entered)
                       (try
                         (when-not (.await release 5 TimeUnit/SECONDS)
                           (throw (ex-info "HA creation barrier timed out" {})))
                         (when (= failure-mode :shared-error) (throw failure))
                         (catch InterruptedException error
                           (case failure-mode
                             :wrapped (throw (ex-info "Owner cancelled" {} error))
                             :flag-only (do (.interrupt (Thread/currentThread))
                                            (throw (IOException. "Owner transport cancelled")))
                             (throw error)))))
                     created)
        recover (fn [timeout-ms]
                  (cr/with-budget (cr/budget timeout-ms 0 nil req)
                    (#'client/attempt-ha-endpoint-request
                      req context "127.0.0.1" 1
                      (fn [_ _] {:type :command-complete :result :ok})
                      (fn [_]) new-client)))
        ^ClientState state (#'client/client-state base)]
    {:entered entered :release release :attempts attempts :recover recover :failure failure
     :pending (.-creating state)
     :cleanup #(#'client/evict-retry-client! base "127.0.0.1:1" (fn [_]))}))

(deftest cancelling-ha-owner-does-not-interrupt-live-waiters
  (doseq [mode [:bare :wrapped :flag-only]]
    (testing (name mode)
      (let [{:keys [^CountDownLatch entered ^CountDownLatch release attempts recover
                    ^ConcurrentHashMap pending cleanup]} (ha-recovery mode)
            tasks (atom [])]
        (try
          (let [owner (worker #(recover 5000))]
            (swap! tasks conj owner)
            (is (.await entered 5 TimeUnit/SECONDS))
            (let [waiters (vec (repeatedly 3 #(worker (fn [] (recover 5000)))))]
              (swap! tasks into waiters)
              (doseq [waiter waiters] (await-waiter! waiter))
              (.interrupt ^Thread (:thread owner))
              (assert-interrupted! owner)
              (doseq [waiter waiters] (assert-success! waiter {:kind :success :result :ok}))
              (is (= 2 @attempts) "live waiters share one replacement attempt")
              (is (.isEmpty pending))))
          (finally (.countDown release) (stop-workers! @tasks) (cleanup)))))))

(deftest ordinary-ha-owner-failures-remain-shared-without-interrupting-callers
  (let [{:keys [^CountDownLatch entered ^CountDownLatch release attempts recover failure
                ^ConcurrentHashMap pending cleanup]} (ha-recovery :shared-error)
        tasks (atom [])]
    (try
      (let [owner (worker #(recover 5000))]
        (swap! tasks conj owner)
        (is (.await entered 5 TimeUnit/SECONDS))
        (let [waiter (worker #(recover 5000))]
          (swap! tasks conj waiter)
          (await-waiter! waiter)
          (.countDown release)
          (doseq [task [owner waiter]]
            (assert-success! task {:kind :exception :exception failure}))
          (is (= 1 @attempts))
          (is (.isEmpty pending))))
      (finally (.countDown release) (stop-workers! @tasks) (cleanup)))))

(deftest cancelling-or-expiring-ha-waiter-does-not-cancel-the-owner
  (doseq [mode [:interrupt :deadline]]
    (testing (name mode)
      (let [{:keys [^CountDownLatch entered ^CountDownLatch release attempts recover
                    ^ConcurrentHashMap pending cleanup]} (ha-recovery :bare)
            tasks (atom [])]
        (try
          (let [owner (worker #(recover 5000))]
            (swap! tasks conj owner)
            (is (.await entered 5 TimeUnit/SECONDS))
            (let [waiter (worker #(recover (if (= mode :deadline) 300 5000)))
                  untouched (worker #(recover 5000))]
              (swap! tasks into [waiter untouched])
              (await-waiter! waiter)
              (await-waiter! untouched)
              (if (= mode :interrupt)
                (do (.interrupt ^Thread (:thread waiter))
                    (assert-interrupted! waiter))
                (let [{:keys [value after]} (result! waiter)]
                  (is (= :exception (:kind value)))
                  (is (= :socket/timeout (:error (ex-data (:exception value)))))
                  (is (false? after))))
              (is (not (realized? (:result owner))))
              (is (= 1 @attempts))
              (is (= 1 (.size pending)))
              (.countDown release)
              (assert-success! owner {:kind :success :result :ok})
              (assert-success! untouched {:kind :success :result :ok})
              (is (= 1 @attempts))
              (is (.isEmpty pending))))
          (finally (.countDown release) (stop-workers! @tasks) (cleanup)))))))

(defn- with-renewal [f]
  (let [root (u/tmp-dir (str "recovery-cancellation-" (random-uuid)))
        port (allocate-port)
        ^Server srv (server/create {:root root :port port})
        proxy (#'chaos/start-proxy! port)
        c (atom nil)
        tasks (atom [])
        entered (CountDownLatch. 1)
        release (CountDownLatch. 1)
        authentications (atom 0)]
    (try
      (server/start srv)
      (reset! c (client/new-client (str "dtlv://datalevin:datalevin@127.0.0.1:" (:port proxy))
                                   {:pool-size 1 :time-out 10000}))
      (session/remove-client {:clients-fn (fn [_] (.-clients srv))
                              :sys-conn-fn (fn [_] (.-sys-conn srv))}
                             srv (client/get-id @c))
      (#'chaos/policy!
        proxy {:mode :pass
               :observe (fn [direction type _]
                          (when (and (= direction :reply) (= type :authentication)
                                     (= 1 (swap! authentications inc)))
                            (.countDown entered)
                            (when-not (.await release 10 TimeUnit/SECONDS)
                              (throw (ex-info "Renewal reply barrier timed out" {})))))})
      (f {:client @c :entered entered :release release :authentications authentications
          :start #(let [task (worker (fn [] (client/list-databases @c)))]
                    (swap! tasks conj task) task)})
      (finally
        (.countDown release)
        (#'chaos/policy! proxy {:mode :pass})
        (#'chaos/cut-flows! proxy)
        (stop-workers! @tasks)
        (when @c (.disposeClient ^IClientInternals @c))
        (server/stop srv)
        (#'chaos/stop-proxy! proxy)
        (u/delete-files root)))))

(deftest cancelling-renewal-owner-allows-live-callers-to-renew-and-finish
  (with-renewal
    (fn [{:keys [client ^CountDownLatch entered authentications start]}]
      (let [pool (client/get-pool client)
            owner (start)]
        (is (.await entered 5 TimeUnit/SECONDS))
        (let [waiters (vec (repeatedly 3 start))]
          (doseq [waiter waiters] (await-waiter! waiter))
          (.interrupt ^Thread (:thread owner))
          (assert-interrupted! owner)
          (doseq [waiter waiters] (assert-success! waiter []))
          (is (= 2 @authentications))
          (is (not (identical? pool (client/get-pool client))))
          (is (nil? (.get ^AtomicReference (.-renewal (.clientState ^IClientInternals client)))))
          (is (not (client/disconnected? client))))))))

(deftest cancelling-renewal-waiter-keeps-the-owner-and-other-callers-live
  (with-renewal
    (fn [{:keys [client ^CountDownLatch entered ^CountDownLatch release authentications start]}]
      (let [owner (start)]
        (is (.await entered 5 TimeUnit/SECONDS))
        (let [waiter (start)
              untouched (start)]
          (await-waiter! waiter)
          (await-waiter! untouched)
          (.interrupt ^Thread (:thread waiter))
          (assert-interrupted! waiter)
          (is (not (realized? (:result owner))))
          (is (= 1 @authentications))
          (.countDown release)
          (assert-success! owner [])
          (assert-success! untouched [])
          (is (= 1 @authentications))
          (is (nil? (.get ^AtomicReference (.-renewal (.clientState ^IClientInternals client)))))
          (is (not (client/disconnected? client))))))))
