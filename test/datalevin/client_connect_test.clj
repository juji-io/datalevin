(ns datalevin.client-connect-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.client :as client]
            [datalevin.client.request :as cr])
  (:import [java.net InetSocketAddress]
           [java.nio.channels SelectionKey Selector ServerSocketChannel SocketChannel]
           [java.nio.channels.spi AbstractSelector SelectorProvider]
           [java.util HashSet]
           [java.util.concurrent ConcurrentHashMap]
           [java.util.concurrent.atomic AtomicBoolean]))

(defn- transport-owner []
  {:lock (Object.) :closed (AtomicBoolean. false)
   :resources (ConcurrentHashMap/newKeySet)})

(defn- pending-connect [ready-on-interrupt?]
  (let [entered (promise)
        selects (atom 0)
        connects (atom 0)
        finishes (atom 0)
        ^Selector delegate (Selector/open)
        channel (proxy [SocketChannel] [(SelectorProvider/provider)]
                  (setOption [_ _] nil)
                  (connect [_] (swap! connects inc) false)
                  (finishConnect [] (swap! finishes inc) true)
                  (implConfigureBlocking [_])
                  (implCloseSelectableChannel []))
        selector (proxy [AbstractSelector] [(SelectorProvider/provider)]
                   (register [ch _ _]
                     (proxy [SelectionKey] []
                       (channel [] ch)
                       (cancel [])
                       (isValid [] (.isOpen ^SocketChannel ch))))
                   (select [timeout]
                     (swap! selects inc)
                     (deliver entered true)
                     (let [ready (.select delegate (long timeout))]
                       (if (and ready-on-interrupt?
                                (.isInterrupted (Thread/currentThread)))
                         1
                         ready)))
                   (selectedKeys [] (HashSet.))
                   (implCloseSelector [] (.close delegate)))]
    {:channel channel :selector selector :delegate delegate :entered entered
     :selects selects :connects connects :finishes finishes
     :factories {:open-channel (constantly channel)
                 :open-selector (constantly selector)}}))

(defn- connect-worker [owner budget factories interrupted?]
  (let [result (promise)
        worker (binding [cr/*request* budget cr/*transport-owners* [owner]]
                 (bound-fn []
                   (when interrupted? (.interrupt (Thread/currentThread)))
                   (let [error (try
                                 (#'client/connect-socket "127.0.0.1" 1 5000 factories)
                                 nil
                                 (catch Throwable t t))]
                     (deliver result {:error error
                                      :interrupted? (.isInterrupted (Thread/currentThread))}))))
        thread (Thread. ^Runnable worker)]
    (.start thread)
    {:thread thread :result result}))

(deftest interrupted-pending-connect-unwinds-instead-of-spinning
  (doseq [budget? [false true]
          ready? [false true]]
    (testing (str "request budget=" budget? ", readiness on interrupt=" ready?)
      (let [{:keys [channel selector delegate entered selects finishes factories]}
            (pending-connect ready?)
            owner (transport-owner)
            budget (when budget? (cr/budget 5000 0 nil nil))
            {:keys [^Thread thread result]} (connect-worker owner budget factories false)]
        (try
          (is (true? (deref entered 1000 false)))
          (is (= ::pending (deref result 25 ::pending)))
          (.interrupt thread)
          (.join thread 1000)
          (is (not (.isAlive thread)) "cancellation must finish before the 5-second deadline")
          (let [{:keys [error interrupted?]} (deref result 1000 {})]
            (is (or (instance? InterruptedException error)
                    (instance? InterruptedException (ex-cause error))))
            (is (true? interrupted?)))
          (is (= 1 @selects) "an interrupted select is never retried")
          (is (zero? @finishes) "readiness cannot publish an interrupted connection")
          (is (not (.isOpen ^SocketChannel channel)))
          (is (not (.isOpen ^Selector selector)))
          (is (not (.isOpen ^Selector delegate)))
          (is (empty? (:resources owner)))
          (finally
            (.close ^SocketChannel channel)
            (.close ^Selector selector)
            (.join thread 1000)))))))

(deftest already-interrupted-connect-does-not-start-network-work
  (let [{:keys [channel selector connects factories]} (pending-connect false)
        owner (transport-owner)
        {:keys [^Thread thread result]} (connect-worker owner nil factories true)]
    (try
      (.join thread 1000)
      (is (not (.isAlive thread)))
      (let [{:keys [error interrupted?]} (deref result 1000 {})]
        (is (instance? InterruptedException (ex-cause error)))
        (is (true? interrupted?)))
      (is (zero? @connects))
      (is (not (.isOpen ^SocketChannel channel)))
      (is (empty? (:resources owner)))
      (finally
        (.close ^SocketChannel channel)
        (.close ^Selector selector)
        (.join thread 1000)))))

(deftest pending-connect-timeouts-still-release-transports
  (doseq [budget? [false true]]
    (let [{:keys [channel selector delegate factories]} (pending-connect false)
          owner (transport-owner)
          budget (when budget? (cr/budget 200 0 nil nil))]
      (try
        (let [error (binding [cr/*request* budget cr/*transport-owners* [owner]]
                      (try (#'client/connect-socket "127.0.0.1" 1
                                                    (if budget? 5000 200) factories)
                           nil
                           (catch Exception e e)))]
          (is (= :socket/timeout (:error (ex-data error))))
          (when budget?
            (is (= :deadline-exceeded (:termination-reason (ex-data error)))))
          (is (not (.isInterrupted (Thread/currentThread))))
          (is (not (.isOpen ^SocketChannel channel)))
          (is (not (.isOpen ^Selector selector)))
          (is (not (.isOpen ^Selector delegate)))
          (is (empty? (:resources owner))))
        (finally
          (.close ^SocketChannel channel)
          (.close ^Selector selector))))))

(deftest successful-connect-releases-its-temporary-selector
  (with-open [listener (doto (ServerSocketChannel/open)
                        (.bind (InetSocketAddress. "127.0.0.1" 0)))]
    (let [port (.getPort ^InetSocketAddress (.getLocalAddress listener))
          owner (transport-owner)]
      (binding [cr/*transport-owners* [owner]]
        (with-open [^SocketChannel channel (#'client/connect-socket "127.0.0.1" port 5000)]
          (is (.isConnected channel))
          (is (.isBlocking channel))
          (is (= #{channel} (set (:resources owner))))
          (cr/untrack-transport! channel))))))
