(ns datalevin.client-retry-lifecycle-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.client :as client])
  (:import
   [datalevin.client Connection ConnectionPool]
   [java.nio ByteBuffer]
   [java.nio.channels SocketChannel]
   [java.nio.channels.spi SelectorProvider]))

(defn- local-client
  []
  (let [channel (proxy [SocketChannel] [(SelectorProvider/provider)]
                  (read [_] 0)
                  (write [^ByteBuffer src]
                    (let [n (.remaining src)]
                      (.position src (.limit src))
                      n))
                  (implConfigureBlocking [_])
                  (implCloseSelectableChannel []))
        conn (client/->Connection channel 1000 (ByteBuffer/allocate 65536))
        ^ConnectionPool pool (#'client/connection-pool "localhost" 19000 nil 1 1000)]
    (#'client/install-pool-connection! pool (aget ^objects (.-slots pool) 0) conn)
    (client/->Client "user" "password" "localhost" 19000 1 1000 nil pool)))

(defn- retry-client
  ([closed? writes] (retry-client closed? writes (fn [])))
  ([closed? writes on-open]
   (reify client/IClient
     (request [_ req]
       (when @closed? (throw (ex-info "Retry client is closed" {})))
       (when (= :open-kv (:type req)) (on-open))
       (when (= :transact-kv (:type req)) (swap! writes conj req))
       {:type :command-complete :result :transacted})
     (disconnect [_] (reset! closed? true))
     (disconnected? [_] @closed?))))

(deftest retry-client-created-during-disconnect-cannot-resurrect-parent
  (doseq [pause-at [:creation :opening]]
    (testing (name pause-at)
      (let [base (local-client)
            endpoint "localhost:19001"
            req {:type :transact-kv :args ["db" [[:put "data" :key :value]]]}
            closed? (atom false)
            writes (atom [])
            preparing (promise)
            release (promise)
            pause! (fn []
                     (deliver preparing true)
                     (when-not (deref release 5000 false)
                       (throw (ex-info "Timed out waiting to prepare retry client" {}))))
            target (retry-client closed? writes #(when (= :opening pause-at) (pause!)))
            result (future
                     (try
                       (#'client/retry-ha-write-request*
                        req "leader changed"
                        {:error :ha/write-rejected :retryable? true
                         :ha-retry-endpoints [endpoint]}
                        {:client base :host "localhost" :port 19000 :time-out 1000
                         :ha-write-retry-timeout-ms 1000 :ha-write-retry-delay-ms 0}
                        client/request client/disconnect
                        (fn [_ _ _]
                          (when (= :creation pause-at) (pause!))
                          target)
                        #(#'client/set-preferred-ha-endpoint! base %))
                       (catch Exception e e)))]
        (try
          (is (true? (deref preparing 5000 false)))
          ;; Disconnect must finish while authentication or opening is pending.
          (client/disconnect base)
          (is (client/disconnected? base))
          (deliver release true)
          (let [outcome (deref result 5000 ::timeout)]
            (is (instance? clojure.lang.ExceptionInfo outcome))
            (is (re-find #"This client is closed" (str outcome))))
          (is (client/disconnected? target))
          (is (nil? (#'client/cached-retry-client base endpoint)))
          (is (empty? @writes))
          (is (thrown-with-msg? clojure.lang.ExceptionInfo #"This client is closed"
                                (client/normal-request base :transact-kv (:args req))))
          (is (empty? @writes))
          (finally
            (deliver release true)
            (future-cancel result)
            (#'client/disconnect-retry-clients! base client/disconnect)
            (client/disconnect target)
            (client/close-pool (client/get-pool base))))))))

(deftest disconnect-closes-published-retry-and-prevents-further-creation
  (let [base (local-client)
        endpoint "localhost:19001"
        closed? (atom false)
        writes (atom [])
        target (retry-client closed? writes)
        created (atom 0)
        prepare! #(#'client/prepare-retry-client
                    {:type :transact-kv :args ["db"]} {:client base}
                    "localhost" 19001 client/disconnect
                    (fn [_ _ _] (swap! created inc) target))]
    (try
      (is (identical? target (:client (prepare!))))
      (is (identical? target (:client (prepare!))))
      (is (= 1 @created))
      (client/disconnect base)
      (is (client/disconnected? target))
      (is (nil? (#'client/cached-retry-client base endpoint)))
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"This client is closed"
                            (prepare!)))
      (is (= 1 @created))
      (finally
        (#'client/disconnect-retry-clients! base client/disconnect)
        (client/disconnect target)
        (client/close-pool (client/get-pool base))))))

(deftest failed-database-open-closes-new-retry-clients
  (doseq [mode [:uncached :cached :replacement]
          failure-kind [:server-error :exception :error]]
    (testing (str (name mode) " / " (name failure-kind))
      (let [base (when-not (= :uncached mode) (local-client))
            previous (when (= :replacement mode) (local-client))
            endpoint "localhost:19001"
            req {:type :transact-kv :args ["db"]}
            failure (case failure-kind
                      :server-error nil
                      :exception (ex-info "Database open failed" {})
                      :error (AssertionError. "Database open failed"))]
        (try
          (when previous
            ;; Its timeout differs from the requested 2000 ms.
            (#'client/cache-retry-client! base endpoint previous))
          (dotimes [_ 3]
            (let [transport (local-client)
                  pool (client/get-pool transport)
                  ^Connection conn (client/get-connection pool)
                  ^SocketChannel channel (.-ch conn)
                  requests (atom [])
                  disconnects (atom 0)
                  target (reify client/IClient
                           (request [_ request]
                             (swap! requests conj request)
                             (if failure
                               (throw failure)
                               {:type :error-response :message "Database open failed"}))
                           (disconnect [_]
                             (swap! disconnects inc)
                             (client/disconnect transport))
                           (disconnected? [_] (client/disconnected? transport)))]
              (client/release-connection pool conn)
              (try
                (let [outcome (try
                                (#'client/prepare-retry-client
                                 req {:client base :time-out 2000}
                                 "localhost" 19001 client/disconnect
                                 (fn [_ _ _] target))
                                (catch Throwable t t))]
                  (if failure
                    (is (identical? failure outcome))
                    (is (and (instance? clojure.lang.ExceptionInfo outcome)
                             (re-find #"Database open failed" (ex-message outcome))))))
                (is (= [:open-kv] (mapv :type @requests)))
                (is (= 1 @disconnects))
                (is (client/closed-pool? pool))
                (is (not (.isOpen channel)))
                (is (nil? (.get ^java.util.Map @#'client/ha-retry-open-targets target)))
                (when base
                  (is (nil? (#'client/cached-retry-client base endpoint)))
                  (is (not (client/disconnected? base))))
                (when previous
                  (is (client/disconnected? previous)))
                (finally (client/close-pool pool)))))
          (finally
            (when base (client/disconnect base))
            (when previous (client/close-pool (client/get-pool previous)))))))))

(deftest failed-database-open-preserves-error-when-disconnect-fails
  (doseq [cleanup-error [(ex-info "Disconnect failed" {})
                        (AssertionError. "Disconnect failed")]]
    (let [failure (ex-info "Database open failed" {})
          closed? (atom false)
          target (retry-client closed? (atom []) #(throw failure))
          outcome (try
                    (#'client/prepare-retry-client
                     {:type :transact-kv :args ["db"]} {} "localhost" 19001
                     (fn [retry-client]
                       (client/disconnect retry-client)
                       (throw cleanup-error))
                     (fn [_ _ _] target))
                    (catch Throwable t t))]
      (is (identical? failure outcome))
      (is (client/disconnected? target))
      (is (nil? (.get ^java.util.Map @#'client/ha-retry-open-targets target))))))
