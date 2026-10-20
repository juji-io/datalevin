(ns datalevin.server.connection-admission-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.main :as main]
            [datalevin.server :as server]
            [datalevin.server.connection :as connection]
            [datalevin.server.copy :as copy]
            [datalevin.server.handlers :as handlers])
  (:import [java.net InetSocketAddress StandardSocketOptions]
           [java.nio.channels SelectionKey Selector ServerSocketChannel SocketChannel]
           [java.nio.file Files OpenOption]
           [java.nio.file.attribute FileAttribute]
           [java.util.concurrent ConcurrentHashMap CountDownLatch Semaphore TimeUnit]
           [java.util.concurrent.atomic AtomicLong]
           [java.util.concurrent.locks ReentrantReadWriteLock]))

(defn- await! [pred]
  (let [deadline (+ (System/nanoTime) 5000000000)]
    (loop []
      (when-not (pred)
        (when (>= (System/nanoTime) deadline)
          (throw (ex-info "Connection admission test timed out" {})))
        (Thread/sleep 5)
        (recur)))))

(defn- with-listener [f]
  (with-open [listener (doto (ServerSocketChannel/open)
                         (.bind (InetSocketAddress. "127.0.0.1" 0))
                         (.configureBlocking false))
              selector (Selector/open)]
    (let [keys (ConcurrentHashMap.)
          threads (ConcurrentHashMap.)
          connections (ConcurrentHashMap/newKeySet)
          key (.register listener selector SelectionKey/OP_ACCEPT)
          port (.getPort ^InetSocketAddress (.getLocalAddress listener))]
      (try (f key port connections keys threads)
           (finally
             (doseq [key (.values keys)] (connection/close! key))
             (doseq [^Thread thread (.values threads)] (.join thread 5000)))))))

(defn- connect ^SocketChannel [port]
  (SocketChannel/open (InetSocketAddress. "127.0.0.1" (int port))))

(deftest connection-limits-must-be-positive-and-finite
  (doseq [option [:max-connections :max-unauthenticated-connections
                  :authentication-timeout-ms :read-timeout-ms :write-timeout-ms]
          invalid [0 -1 nil 1.5 "10" Long/MAX_VALUE]]
    (let [error (try (connection/limits {option invalid}) nil
                     (catch clojure.lang.ExceptionInfo e (ex-data e)))]
      (is (= :server/invalid-connection-limit (:error error)))
      (is (= option (:option error))))))

(deftest unauthenticated-connection-limit-is-capped-by-the-total
  (doseq [[opts expected] [[{:max-connections 1} 1]
                          [{:max-connections 2
                            :max-unauthenticated-connections 4} 2]
                          [{:max-connections 2
                            :max-unauthenticated-connections 2} 2]
                          [{:max-connections 4
                            :max-unauthenticated-connections 2} 2]]]
    (let [limits (connection/limits opts)]
      (is (= expected (:max-unauthenticated-connections limits)))
      (is (= expected (.availablePermits ^Semaphore (:unauthenticated-slots limits))))
      (is (= (:max-connections opts) (:max-connections limits)
             (.availablePermits ^Semaphore (:slots limits)))))))

(deftest write-deadline-is-configurable-through-the-cli
  (is (= 1500 (get-in (main/validate-args ["serv" "--write-timeout-ms" "1500"])
                      [:options :write-timeout-ms])))
  (doseq [invalid ["0" "-1" "2147483648"]]
    (is (:exit-message (main/validate-args ["serv" "--write-timeout-ms" invalid])))))

(deftest non-reading-peers-release-the-owner-admission-and-read-lock
  (let [payload (byte-array 4194304)
        path (Files/createTempFile "datalevin-write-deadline-" ".bin"
                                   (make-array FileAttribute 0))]
    (try
      (Files/write path payload (make-array OpenOption 0))
      (doseq [kind [:response :copy-out :copy-file]]
        (testing (name kind)
          (with-listener
            (fn [listener port connections keys threads]
              (let [limits (connection/limits {:max-connections 1
                                               :max-unauthenticated-connections 1
                                               :read-timeout-ms 1500
                                               :write-timeout-ms 300})
                    runtime-lock (ReentrantReadWriteLock.)
                    read-lock (.readLock runtime-lock)
                    entered (promise)
                    result (promise)
                    serve! (fn [^SelectionKey key]
                             (.lock read-lock)
                             (try
                               (connection/authenticated! key)
                               (connection/begin-read! key)
                               (connection/end-read! key)
                               (.setOption ^SocketChannel (.channel key)
                                           StandardSocketOptions/SO_SNDBUF (Integer/valueOf 1024))
                               (deliver entered [key (Thread/currentThread)])
                               (let [deps {:write-message-fn #'server/write-message}]
                                 (case kind
                                   :response (#'server/write-message
                                               key {:type :command-complete :result payload})
                                   :copy-out (copy/copy-out deps key [payload] 1)
                                   :copy-file (copy/copy-file-out deps key path nil)))
                               (deliver result :unexpected-success)
                               (catch Exception e (deliver result (ex-data e)))
                               (finally (.unlock read-lock))))]
                (with-open [_peer (doto (SocketChannel/open)
                                   (.setOption StandardSocketOptions/SO_RCVBUF (Integer/valueOf 1024))
                                   (.connect (InetSocketAddress. "127.0.0.1" (int port))))]
                  (let [start (System/nanoTime)]
                    (connection/accept! listener connections keys threads limits serve! (constantly nil))
                    (let [[key ^Thread owner] (deref entered 5000 nil)
                          lease (:connection-lease @(.attachment ^SelectionKey key))]
                      (await! #(pos? (.get ^AtomicLong (:write-deadline lease))))
                      (is (zero? (.get ^AtomicLong (:read-deadline lease))))
                      (is (= 1 (.getReadLockCount runtime-lock)))
                      (is (zero? (.availablePermits ^Semaphore (:slots limits))))
                      (await! #(do (connection/expire! keys) (empty? threads)))
                      (.join owner 1000)
                      (is (not (.isAlive owner)))
                      (is (< (- (System/nanoTime) start) 1500000000))
                      (is (= {:error :server/connection-timeout :phase :write}
                             (deref result 1000 ::timeout)))
                      (is (empty? connections))
                      (is (empty? keys))
                      (is (zero? (.getReadLockCount runtime-lock)))
                      (is (= 1 (.availablePermits ^Semaphore (:slots limits))))
                      (is (= 1 (.availablePermits ^Semaphore (:unauthenticated-slots limits))))))))))))
      (finally (Files/deleteIfExists path)))))

(deftest copy-out-progress-cannot-renew-the-response-deadline
  (with-listener
    (fn [listener port connections keys threads]
      (let [limits (connection/limits {:write-timeout-ms 150})
            result (promise)
            batches (atom 0)
            serve! (fn [key]
                     (connection/authenticated! key)
                     (deliver result
                              (try
                                (copy/copy-out
                                  {:write-message-fn
                                   (fn [key batch]
                                     (swap! batches inc)
                                     (Thread/sleep 100)
                                     (#'server/write-message key batch))}
                                  key (range 10) 1)
                                :unexpected-success
                                (catch Exception e (ex-data e)))))]
        (with-open [_peer (connect port)]
          (connection/accept! listener connections keys threads limits serve! (constantly nil))
          ;; No watchdog is needed for progress between frames: the next write
          ;; must observe the transfer's original deadline, before :copy-done.
          (is (= {:error :server/connection-timeout :phase :write}
                 (deref result 2000 ::timeout)))
          (is (<= 1 @batches 2))
          (await! #(empty? threads))
          (is (= (:max-connections limits) (.availablePermits ^Semaphore (:slots limits)))))))))

(deftest failed-connection-initialization-returns-all-capacity
  (with-listener
    (fn [key port connections keys threads]
      (let [limits (connection/limits {:max-connections 1 :max-unauthenticated-connections 1})
            failure (ex-info "Context initialization failed" {})
            events (atom [])
            codec-context (reify java.io.Closeable
                            (close [_] (swap! events conj :close-codec)))
            create-codec (fn []
                           (swap! events conj :create-codec)
                           codec-context)]
        (with-open [_socket (connect port)]
          (is (identical? failure
                          (try
                            (connection/accept! key connections keys threads limits
                                                (constantly nil)
                                                (fn [_] (throw failure)) create-codec)
                            nil
                            (catch Throwable t t)))))
        (is (empty? @events) "context initialization failure must not allocate a codec")
        (is (empty? connections))
        (is (empty? keys))
        (is (empty? threads))
        (is (= 1 (.availablePermits ^Semaphore (:slots limits))))
        (is (= 1 (.availablePermits ^Semaphore (:unauthenticated-slots limits))))
        (with-open [_socket (connect port)]
          (let [served (promise)]
            (connection/accept! key connections keys threads limits
                                (fn [^SelectionKey key]
                                  (deliver served [(Thread/currentThread)
                                                   @(.attachment key)]))
                                (constantly ::context) create-codec)
            (let [[^Thread owner attachment] (deref served 5000 nil)]
              (is (some? owner) "released capacity admits the next connection")
              (when owner
                (.join owner 5000)
                (is (not (.isAlive owner))))
              (is (= ::context (:context attachment)))
              (is (identical? codec-context (:codec-context attachment)))))
          (is (= [:create-codec :close-codec] @events)
              "the accepted connection owns and closes its codec exactly once")
          (is (empty? connections))
          (is (empty? keys))
          (is (empty? threads))
          (is (= 1 (.availablePermits ^Semaphore (:slots limits))))
          (is (= 1 (.availablePermits ^Semaphore (:unauthenticated-slots limits)))))))))

(deftest cancelled-owners-retain-capacity-until-they-finish
  (with-listener
    (fn [key port connections keys threads]
      (let [limits (connection/limits {:max-connections 1 :max-unauthenticated-connections 1})
            entered (promise)
            release (CountDownLatch. 1)
            contexts (atom 0)
            context! (fn [_] (swap! contexts inc))
            serve! (fn [owned-key]
                     (deliver entered [owned-key (Thread/currentThread)])
                     (loop []
                       (when-not (try (.await release 10 TimeUnit/MILLISECONDS)
                                      (catch InterruptedException _ false))
                         (recur))))]
        (try
          (with-open [_first (connect port)]
            (connection/accept! key connections keys threads limits serve! context!)
            (let [[owned-key ^Thread owner] (deref entered 5000 [nil nil])]
              (is (some? owner))
              (connection/close! owned-key)
              (is (empty? connections))
              (is (= 1 (count threads)))
              (with-open [_rejected (connect port)]
                (connection/accept! key connections keys threads limits serve! context!)
                (is (= 1 @contexts) "rejected sockets do not allocate connection contexts")
                (is (= 1 (count threads))))
              (is (zero? (.availablePermits ^Semaphore (:slots limits))))
              (is (zero? (.availablePermits ^Semaphore (:unauthenticated-slots limits))))
              (.countDown release)
              (.join owner 5000)
              (is (not (.isAlive owner)))
              (is (empty? threads))
              (is (= 1 (.availablePermits ^Semaphore (:slots limits))))
              (is (= 1 (.availablePermits ^Semaphore (:unauthenticated-slots limits))))))
          (with-open [_next (connect port)]
            (connection/accept! key connections keys threads limits (constantly nil) context!)
            (await! #(empty? threads))
            (is (= 2 @contexts)))
          (finally (.countDown release)))))))

(deftest authentication-cannot-publish-after-its-deadline
  (with-listener
    (fn [key port connections keys threads]
      (let [limits (connection/limits {:authentication-timeout-ms 10})
            entered (promise)
            release (CountDownLatch. 1)
            result (promise)
            removed (atom [])
            replies (atom [])
            id (random-uuid)
            deps {:authenticate
                  (fn [& _]
                    (deliver entered true)
                    ;; Simulate synchronous credential work that ignores
                    ;; transport cancellation until its dependency returns.
                    (loop []
                      (when-not (try (.await release 10 TimeUnit/MILLISECONDS)
                                     (catch InterruptedException _ false))
                        (recur)))
                    id)
                  :remove-client (fn [_ client-id] (swap! removed conj client-id))
                  :write-message (fn [_ reply] (swap! replies conj reply))}
            serve! (fn [key]
                     (deliver result
                              (try (handlers/authentication deps nil key {})
                                   (catch clojure.lang.ExceptionInfo e (ex-data e)))))]
        (try
          (with-open [_socket (connect port)]
            (connection/accept! key connections keys threads limits serve! (constantly nil))
            (is (true? (deref entered 5000 false)))
            (Thread/sleep 20)
            (connection/expire! keys)
            (is (empty? connections))
            (is (= 1 (count threads)))
            (.countDown release)
            (is (= :server/connection-timeout (:error (deref result 5000 {}))))
            (await! #(empty? threads))
            (is (= [id] @removed))
            (is (empty? @replies)))
          (finally (.countDown release)))))))
