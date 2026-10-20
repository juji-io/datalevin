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

(defn- await! [pred]
  (let [until (+ (System/nanoTime) 5000000000)]
    (loop []
      (when-not (pred)
        (when (>= (System/nanoTime) until)
          (throw (ex-info "Client lifecycle barrier timed out" {})))
        (Thread/sleep 5)
        (recur)))))

(deftest request-scopes-respect-nested-bindings-and-release-after-failure
  (let [outer (cr/budget 5000 0 nil {:type :get-value :args ["outer"]})
        expired (cr/budget 0 0 nil {:type :get-value :args ["expired"]})
        failure (ex-info "Scope failed" {})]
    (is (nil? (cr/current)))
    (cr/with-budget outer
      (is (identical? outer (cr/current)))
      (binding [cr/*request* expired]
        (is (identical? expired (cr/current)))
        (is (= :deadline-exceeded
               (:termination-reason (ex-data (outcome #(cr/check! :nested)))))))
      (is (identical? outer (cr/current)))
      (binding [cr/*request* nil] (is (nil? (cr/current))))
      (is (identical? outer (cr/current)))
      (is (identical? failure
                      (outcome #(cr/with-budget outer (throw failure)))))
      (is (identical? outer (cr/current))))
    (is (nil? (cr/current)))
    (is (identical? failure (outcome #(cr/with-budget outer (throw failure)))))
    (is (nil? (cr/current)))
    (cr/with-budget expired
      (is (= :deadline-exceeded
             (:termination-reason (ex-data (outcome #(cr/check! :next-request)))))))
    (is (nil? (cr/current)))))

(deftest local-only-budgets-respect-nested-scopes-and-explicit-bindings
  (let [outer (cr/budget 5000 0 nil {:type :get-value :args ["outer"]})
        expired (cr/budget 0 0 nil {:type :get-value :args ["expired"]})]
    (is (nil? (cr/current)))
    (cr/with-local-budget outer
      (is (nil? cr/*request*))
      (is (identical? outer (cr/current)))
      (cr/with-budget (throw (ex-info "Must inherit the local budget" {}))
        (is (identical? outer (cr/current)))
        (is (identical? outer (deref (future (cr/current)) 1000 ::timeout))))
      (cr/with-local-budget expired
        (is (identical? expired (cr/current)))
        (is (= :deadline-exceeded
               (:termination-reason (ex-data (outcome #(cr/check! :local-only)))))))
      (is (identical? outer (cr/current)))
      (binding [cr/*request* expired]
        (is (identical? expired (cr/current))))
      (binding [cr/*request* nil]
        (is (nil? (cr/current))))
      (is (identical? outer (cr/current))))
    (is (nil? (cr/current)))))

(deftest disconnect-respects-an-expired-local-only-budget
  (let [{:keys [client channel writes]} (scripted-client 5000 nil)
        budget (cr/budget 0 0 nil nil)]
    (try
      (cr/with-local-budget budget
        (client/disconnect client)
        (is (identical? budget (cr/current))))
      (is (zero? @writes) "expired cleanup must not start a disconnect notification")
      (is (client/disconnected? client))
      (is (not (.isOpen ^SocketChannel channel)))
      (is (nil? (cr/current)))
      (finally (dispose! client)))))

(deftest local-only-budgets-preserve-write-outcome-uncertainty
  (let [request {:type :transact-kv :args ["db"]}
        budget (cr/budget 5000 0 nil request)
        failure (ex-info "Transport failed" {})
        reject! #(#'client/reject-unsafe-transport-replay! request "localhost:8898" failure)]
    (cr/with-local-budget budget
      (is (identical? failure (outcome reject!))
          "a failed exchange before sending preserves the original failure")
      (cr/failed-exchange! request)
      (let [error (outcome reject!)]
        (is (= :ha/write-indeterminate (get-in (ex-data error) [:err-data :error])))
        (is (identical? failure (ex-cause error)))))))

(deftest conveyed-callers-keep-their-budget-after-the-parent-scope-exits
  (let [closed (AtomicBoolean. false)
        budget (cr/budget 5000 0 closed {:type :get-value :args ["parent"]})
        entered (promise)
        release (promise)
        child (cr/with-budget budget
                (future
                  (cr/with-budget (throw (ex-info "Budget must be conveyed" {}))
                    (deliver entered (identical? budget (cr/current)))
                    (when-not (deref release 5000 false)
                      (throw (ex-info "Parent did not release child" {})))
                    [(identical? budget (cr/current))
                     (:termination-reason (ex-data (outcome #(cr/check! :child))))])))]
    (try
      (is (true? (deref entered 5000 false)))
      (is (nil? (cr/current)))
      (cr/with-budget (cr/budget 5000 0 nil {:type :get-value :args ["next"]})
        (.set closed true)
        (deliver release true)
        (is (= [true :client-disconnected] (deref child 5000 ::timeout)))
        (is (nil? (cr/check! :independent-request))))
      (is (nil? (cr/current)))
      (finally (deliver release true)))))

(deftest nested-wire-scopes-preserve-mutation-outcomes
  (let [request {:type :transact-kv :args ["db"] :client-op-id "original-op"}
        failure (ex-info "Transport failed" {})]
    (cr/with-budget (cr/budget 5000 0 nil request)
      (cr/track-exchange! request)
      (cr/with-wire-request request
        (cr/with-wire-request {:type :get-value :args ["db"]}
          (cr/sending!))
        (cr/failed-attempt!)
        (is (identical? failure (cr/failure failure))
            "a nested read's bytes cannot make the outer write indeterminate")
        (is (identical? failure
                        (outcome #(cr/with-wire-request {:type :get-value :args ["db"]}
                                    (throw failure)))))
        (cr/sending!)
        (cr/failed-attempt!)
        (let [error (cr/failure failure)]
          (is (= :ha/write-indeterminate (:error (ex-data error))))
          (is (= "original-op" (:client-op-id (ex-data error)))))))
    (is (nil? (cr/wire-request)))
    (is (nil? (cr/current)))))

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

(deftest child-renewal-cannot-publish-after-disconnect-under-a-parent-budget
  (with-open [listener (doto (ServerSocketChannel/open)
                         (.bind (InetSocketAddress. "127.0.0.1" 0)))]
    (let [{parent :client} (scripted-client 10000 nil)
          {original :client} (scripted-client 10000 nil)
          pool (client/get-pool original)
          child (client/->Client "user" "password" "127.0.0.1"
                                 (.getPort ^InetSocketAddress (.getLocalAddress listener))
                                 1 10000 (client/get-id original) pool)
          ^ClientState state (.clientState ^IClientInternals child)
          entered (promise)
          release (promise)
          peer (future
                 (outcome
                  #(doseq [response [{:type :authentication-ok :client-id (random-uuid)}
                                    {:type :set-client-id-ok :extra (WireValue. 1)}]]
                     (let [conn (client/->Connection (.accept listener) 10000
                                                    (ByteBuffer/allocate 65536))]
                       (try
                         (client/receive conn)
                         (client/send-only conn response)
                         (finally (client/close conn)))))))
          renewal (future
                    (binding [*decode-pause* [entered release]
                              cr/*request* (client/request-budget parent nil)]
                      (outcome #(.renewSession ^IClientInternals child pool (cr/deadline)))))]
      (try
        (is (true? (deref entered 5000 false)))
        (let [attempt (.get ^AtomicReference (.-renewal state))
              channels (filterv #(instance? SocketChannel %)
                                (get-in attempt [:transport-owner :resources]))
              closing
              ;; Keep the captured pool open after disconnect marks the child
              ;; closed, while allowing renewal to finish decoding registration.
              (locking pool
                (let [closing (future (outcome #(client/disconnect child)))]
                  (await! #(client/disconnected? child))
                  (is (not (client/closed-pool? pool)))
                  (is (not (client/disconnected? parent)))
                  (deliver release true)
                  (await! #(or (realized? renewal)
                               (not (identical? pool (client/get-pool child)))))
                  (is (identical? pool (client/get-pool child)))
                  (is (= (client/get-id original) (client/get-id child)))
                  closing))]
          (let [error (deref renewal 5000 ::timeout)]
            (is (= :client/closed (:error (ex-data error))))
            (is (= :client-disconnected (:termination-reason (ex-data error)))))
          (is (nil? (deref closing 5000 ::timeout)))
          (is (seq channels))
          (doseq [^SocketChannel channel channels]
            (is (not (.isOpen channel)))
            ;; Disconnect closes provisional channels directly. Removal from
            ;; this registry proves the rejected candidate's Connection closed.
            (is (not (.containsKey ^ConcurrentHashMap @#'client/connection-wire-opts channel))))
          (is (nil? (.get ^AtomicReference (.-renewal state))))
          (is (client/disconnected? child))
          (is (client/closed-pool? (client/get-pool child)))
          (is (not (client/disconnected? parent))))
        (is (nil? (deref peer 5000 ::timeout)))
        (finally
          (deliver release true)
          (.close listener)
          (deref renewal 5000 nil)
          (deref peer 5000 nil)
          (dispose! child)
          (dispose! original)
          (dispose! parent))))))

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

