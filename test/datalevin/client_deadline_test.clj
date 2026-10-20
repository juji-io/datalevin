(ns datalevin.client-deadline-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.client :as client]
            [datalevin.client.request :as cr]
            [datalevin.constants :as c]
            [datalevin.protocol :as p]
            [taoensso.nippy :as nippy])
  (:import [datalevin.client ClientState Connection ConnectionPool IClientInternals]
           [java.net InetSocketAddress StandardSocketOptions]
           [java.nio ByteBuffer]
           [java.nio.channels SocketChannel ServerSocketChannel]
           [java.nio.channels.spi SelectorProvider]
           [java.util.concurrent ConcurrentHashMap]
           [java.util.concurrent.atomic AtomicBoolean AtomicReference]))

(deftype PausedValue [entered release value])
(deftype WireValue [value])
(def ^:dynamic *decode-pause* nil)

(nippy/extend-freeze PausedValue :datalevin.test/deadline-paused-value
                     [value out]
                     (deliver (.-entered ^PausedValue value) true)
                     (when-not (deref (.-release ^PausedValue value) 5000 false)
                       (throw (ex-info "Serializer barrier timed out" {})))
                     (.writeLong out (long (.-value ^PausedValue value))))

(nippy/extend-thaw :datalevin.test/deadline-paused-value [in] (.readLong in))

(nippy/extend-freeze WireValue :datalevin.test/deadline-wire-value
                     [value out] (.writeLong out (long (.-value ^WireValue value))))

(nippy/extend-thaw :datalevin.test/deadline-wire-value
                   [in]
                   (when-let [[entered release] *decode-pause*]
                     (deliver entered true)
                     (when-not (deref release 5000 false)
                       (throw (ex-info "Decoder barrier timed out" {}))))
                   (.readLong in))

(defn- scripted-client [timeout response]
  (let [writes (atom 0)
        frame (ByteBuffer/allocate 65536)
        _ (p/write-message-bf frame response c/message-format-nippy)
        _ (.flip frame)
        channel (proxy [SocketChannel] [(SelectorProvider/provider)]
                  (write [^ByteBuffer src]
                    (swap! writes inc)
                    (let [n (.remaining src)] (.position src (.limit src)) n))
                  (read [^ByteBuffer dst]
                    (let [n (.remaining frame)]
                      (cond (zero? @writes) 0
                            (pos? n) (do (.put dst frame) n)
                            :else -1)))
                  (implConfigureBlocking [_])
                  (implCloseSelectableChannel []))
        conn (client/->Connection channel timeout (ByteBuffer/allocate 65536))
        ^ConnectionPool pool (#'client/connection-pool "127.0.0.1" 1 nil 1 timeout)]
    (#'client/install-pool-connection! pool (aget ^objects (.-slots pool) 0) conn)
    {:client (client/->Client "user" "password" "127.0.0.1" 1 1 timeout nil pool)
     :connection conn :channel channel :writes writes}))

(defn- outcome [f] (try (f) (catch Throwable t t)))
(defn- elapsed-ms [start] (quot (- (System/nanoTime) start) 1000000))
(defn- dispose! [c] (.disposeClient ^IClientInternals c))

(deftest encoding-past-the-deadline-never-sends-a-mutation
  (let [{:keys [client channel writes]} (scripted-client 100 {:type :command-complete})
        entered (promise)
        release (promise)
        task (future
               (outcome #(client/request client
                                         {:type :transact-kv
                                          :args ["db" [(PausedValue. entered release 7)]]})))]
    (try
      (is (true? (deref entered 5000 false)))
      (Thread/sleep 140)
      (deliver release true)
      (let [error (deref task 2000 ::timeout)]
        (is (= :deadline-exceeded (:termination-reason (ex-data error))))
        (is (not (:indeterminate? (ex-data error))))
        (is (zero? @writes)))
      (is (not (.isOpen ^SocketChannel channel)))
      (is (not (client/disconnected? client)))
      (finally (deliver release true) (dispose! client)))))

(deftest disconnect-does-not-wait-for-a-decoder-and-preserves-its-result
  (let [{:keys [client channel connection]} (scripted-client
                                             5000 {:type :command-complete
                                                   :result (WireValue. 42)})
        entered (promise)
        release (promise)
        task (future
               (binding [*decode-pause* [entered release]]
                 (outcome #(client/request client {:type :list-databases :args []}))))]
    (try
      (is (true? (deref entered 5000 false)))
      (let [closing (future (outcome #(client/disconnect client)))]
        (is (not= ::timeout (deref closing 500 ::timeout))))
      (is (client/disconnected? client))
      (is (not (.isOpen ^SocketChannel channel)))
      (is (not (.get ^AtomicBoolean (.-context-closed ^Connection connection))))
      (deliver release true)
      (is (= 42 (:result (deref task 2000 nil))))
      (is (.get ^AtomicBoolean (.-context-closed ^Connection connection)))
      (finally (deliver release true) (dispose! client)))))

(deftest disconnect-never-checks-out-or-replaces-a-connection
  (doseq [mode [:borrowed :broken]]
    (testing (name mode)
      (let [{:keys [client writes connection]} (scripted-client 5000 nil)
            pool (client/get-pool client)
            resolved (atom 0)]
        (if (= :borrowed mode)
          (#'client/claim-connection-slot pool)
          (client/close connection))
        (try
          (binding [client/*resolve-address* (fn [& _]
                                               (swap! resolved inc)
                                               (throw (ex-info "Unexpected resolver" {})))]
            (let [start (System/nanoTime)]
              (client/disconnect client)
              (is (< (elapsed-ms start) 400))))
          (is (zero? @writes))
          (is (zero? @resolved))
          (is (client/disconnected? client))
          (finally (dispose! client)))))))

(deftest a-stalled-resolver-keeps-one-renewal-claim-and-cannot-connect-late
  (doseq [kind [:renewal :ha] stop [:deadline :disconnect]]
    (testing (str (name kind) " / " (name stop))
      (with-open [listener (doto (ServerSocketChannel/open)
                             (.bind (InetSocketAddress. "127.0.0.1" 0))
                             (.configureBlocking false))]
        (let [{:keys [client]} (scripted-client 5000 nil)
              pool (client/get-pool client)
              entered (promise)
              release (promise)
              resolutions (atom 0)
              resolve! (fn [_ _]
                         (swap! resolutions inc)
                         (deliver entered true)
                         (when-not (deref release 5000 false)
                           (throw (ex-info "Resolver barrier timed out" {})))
                         (.getLocalAddress listener))
              renew! (fn [ms]
                       (binding [client/*resolve-address* resolve!]
                         (binding [cr/*request* (cr/budget ms 0
                                                           (.-closed ^ClientState (.clientState ^IClientInternals client))
                                                           {:type :get-value :args ["db"]})]
                           (outcome
                            #(if (= kind :renewal)
                               (.renewSession ^IClientInternals client pool (cr/deadline))
                               (#'client/prepare-retry-client
                                {:type :get-value :args ["db"]}
                                (#'client/client-routing-context client)
                                "127.0.0.1" 1 client/disconnect
                                #'client/new-client-for-endpoint))))))
              owner (future (renew! 100))]
          (try
            (is (true? (deref entered 5000 false)))
            (let [waiter (future (renew! 60))]
              (is (= :deadline-exceeded
                     (:termination-reason (ex-data (deref waiter 500 ::timeout))))))
            (is (= 1 @resolutions))
            (is (not (realized? owner)))
            (when (= :disconnect stop)
              (let [closing (future (outcome #(client/disconnect client)))]
                (is (not= ::timeout (deref closing 500 ::timeout)))))
            (Thread/sleep 70)
            (deliver release true)
            (let [error (deref owner 2000 ::timeout)]
              (is (= (if (= :disconnect stop) :client-disconnected :deadline-exceeded)
                     (:termination-reason (ex-data error)))))
            (is (nil? (.accept listener)) "expired/closed resolution must not connect")
            (let [^ClientState state (.clientState ^IClientInternals client)]
              (is (nil? (.get ^AtomicReference (.-renewal state))))
              (is (.isEmpty ^ConcurrentHashMap (.-creating state))))
            (is (= (= :disconnect stop) (client/disconnected? client)))
            (finally (deliver release true) (dispose! client))))))))

(deftest failed-endpoint-opening-disposes-locally-after-the-deadline
  (let [{base :client} (scripted-client 100 nil)
        {candidate :client channel :channel writes :writes}
        (scripted-client 5000 {:type :error-response :message "Database open failed"
                               :extra (WireValue. 1)})
        entered (promise)
        release (promise)
        resolver-calls (atom 0)
        task (future
               (binding [*decode-pause* [entered release]
                         client/*resolve-address* (fn [& _]
                                                    (swap! resolver-calls inc)
                                                    (throw (ex-info "Cleanup tried to reconnect" {})))]
                 (outcome
                  #(#'client/prepare-retry-client
                    {:type :transact-kv :args ["db"]}
                    {:client base :time-out 100} "127.0.0.1" 1
                    client/disconnect (fn [& _] candidate)))))]
    (try
      (is (true? (deref entered 5000 false)))
      ;; A normal disconnect would try to repair this unusable slot. Deadline
      ;; exhaustion and disposal must instead close its already-owned resources.
      (.close ^SocketChannel channel)
      (Thread/sleep 140)
      (deliver release true)
      (let [error (deref task 2000 ::timeout)]
        (is (re-find #"Database open failed" (str error))))
      (is (= 1 @writes))
      (is (zero? @resolver-calls))
      (is (client/disconnected? candidate))
      (is (nil? (#'client/cached-retry-client base "127.0.0.1:1")))
      (is (.isEmpty ^ConcurrentHashMap
           (.-creating ^ClientState (.clientState ^IClientInternals base))))
      (is (not (client/disconnected? base)))
      (finally (deliver release true) (dispose! base) (dispose! candidate)))))

(defn- congested-client []
  (with-open [listener (doto (ServerSocketChannel/open)
                         (.bind (InetSocketAddress. "127.0.0.1" 0)))]
    (let [ch (doto (SocketChannel/open)
               (.setOption StandardSocketOptions/SO_SNDBUF (Integer/valueOf 1024))
               (.connect (.getLocalAddress listener))
               (.configureBlocking false))
          peer (.accept listener)
          data (ByteBuffer/allocate 1048576)
          until (+ (System/nanoTime) 5000000000)]
      (.setOption peer StandardSocketOptions/SO_RCVBUF (Integer/valueOf 1024))
      (loop []
        (when (>= (System/nanoTime) until)
          (.close ch) (.close peer)
          (throw (ex-info "Failed to fill socket send buffer" {})))
        (when-not (.hasRemaining data) (.clear data))
        (if (pos? (.write ch data))
          (recur)
          (do
            ;; Let any initial TCP window drain, then fill it again. This must
            ;; be persistent backpressure when notification begins.
            (Thread/sleep 20)
            (when (pos? (.write ch data)) (recur)))))
      (let [conn (client/->Connection ch 5000 (ByteBuffer/allocate 65536))
            ^ConnectionPool pool (#'client/connection-pool "127.0.0.1" 1 nil 1 5000)]
        (#'client/install-pool-connection! pool (aget ^objects (.-slots pool) 0) conn)
        {:client (client/->Client "u" "p" "127.0.0.1" 1 1 5000 nil pool)
         :channel ch :peer peer}))))

(deftest all-owned-endpoint-notifications-share-one-disconnect-budget
  (let [{base :client} (scripted-client 5000 nil)
        children (vec (repeatedly 4 congested-client))]
    (try
      (doseq [[i child] (map-indexed vector children)]
        (#'client/cache-retry-client! base (str "endpoint:" i) (:client child)))
      (binding [client/*resolve-address* (fn [& _] (throw (ex-info "Unexpected DNS" {})))]
        (let [start (System/nanoTime)]
          (client/disconnect base)
          (is (<= 80 (elapsed-ms start)) "at least one notification hit backpressure")
          (is (< (elapsed-ms start) 280))))
      (is (client/disconnected? base))
      (doseq [{:keys [client channel]} children]
        (is (client/disconnected? client))
        (is (not (.isOpen ^SocketChannel channel))))
      (finally
        (dispose! base)
        (doseq [{:keys [client peer]} children]
          (dispose! client)
          (.close ^SocketChannel peer))))))

