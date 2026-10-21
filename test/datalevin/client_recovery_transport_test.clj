(ns datalevin.client-recovery-transport-test
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
           [java.nio.channels Channel Selector]
           [java.util.concurrent ConcurrentHashMap]
           [java.util.concurrent.atomic AtomicBoolean AtomicReference]))

(use-fixtures :once db-fixture)

(defn- outcome [f] (try (f) (catch Throwable t t)))

(defn- await! [pred]
  (let [until (+ (System/nanoTime) 5000000000)]
    (loop []
      (when-not (pred)
        (when (>= (System/nanoTime) until)
          (throw (ex-info "Recovery transport barrier timed out" {})))
        (Thread/sleep 5)
        (recur)))))

(defn- with-client
  ([f] (with-client {} f))
  ([opts f]
   (let [root (u/tmp-dir (str "client-recovery-transport-" (random-uuid)))
         port (allocate-port)
         srv (server/create {:root root :port port})
         proxy (#'chaos/start-proxy! port)
         c (atom nil)]
     (try
       (server/start srv)
       (reset! c (client/new-client
                   (str "dtlv://datalevin:datalevin@127.0.0.1:" (:port proxy))
                   (merge {:pool-size 1 :time-out 10000} opts)))
       (f @c proxy srv)
       (finally
         (#'chaos/policy! proxy {:mode :pass})
         (#'chaos/cut-flows! proxy)
         (when @c (.disposeClient ^IClientInternals @c))
         (server/stop srv)
         (#'chaos/stop-proxy! proxy)
         (u/delete-files root))))))

(deftest authentication-timeout-certification-retries-unobserved-replies
  (with-client {:time-out 500}
    (fn [c proxy ^Server srv]
      (let [replies (atom 0)]
        (session/remove-client {:clients-fn (fn [_] (.-clients srv))
                                :sys-conn-fn (fn [_] (.-sys-conn srv))}
                               srv (client/get-id c))
        (#'chaos/policy!
          proxy {:mode :authentication-timeout
                 :observe
                 (fn [direction type _]
                   (when (and (= direction :reply) (= type :authentication)
                              (<= (long (swap! replies inc)) 4))
                     ;; Let the first four short requests expire before the
                     ;; proxy can withhold their replies. Those attempts must
                     ;; not count as exercising the scheduled transport fault.
                     (let [flows (vec (filter #(= :authentication @(:request-type %))
                                              (.values ^ConcurrentHashMap (:flows proxy))))]
                       (await! #(every? (fn [flow]
                                          (.get ^AtomicBoolean (:closed? flow)))
                                        flows)))))})
        (#'chaos/exercise-authentication-timeout! {:admin c :proxy proxy} 0)
        (is (> @replies 4))
        (is (pos? (get @(:hits proxy) :authentication-timeout 0)))
        (is (not (client/disconnected? c)))))))

(deftest disconnect-cancels-provisional-recovery-transports
  (doseq [[kind phase] [[:renewal :authentication] [:renewal :set-client-id]
                       [:ha :authentication] [:ha :set-client-id] [:ha :open-kv]]]
    (testing (str (name kind) " / " (name phase))
      (with-client
        (fn [c proxy _]
          (let [pool (client/get-pool c)
                id (client/get-id c)
                ^ClientState state (.clientState ^IClientInternals c)
                entered (promise)
                release (promise)
                candidate (atom nil)
                endpoint (str "127.0.0.1:" (:port proxy))
                attempt #(if (= kind :renewal)
                           (.get ^AtomicReference (.-renewal state))
                           (.get ^ConcurrentHashMap (.-creating state) endpoint))
                recover! #(binding [cr/*request* (#'client/request-budget
                                                  c {:type :transact-kv :args ["db"]})]
                            (outcome
                              (fn []
                                (if (= kind :renewal)
                                  (.renewSession ^IClientInternals c pool (cr/deadline))
                                  (#'client/prepare-retry-client
                                    {:type :transact-kv :args ["db"]}
                                    (#'client/client-routing-context c)
                                    "127.0.0.1" (:port proxy) client/disconnect
                                    (fn [context host port]
                                      (let [created (#'client/new-client-for-endpoint
                                                      context host port)]
                                        (reset! candidate created)
                                        created)))))))]
            (#'chaos/policy!
              proxy {:mode :pass
                     :observe (fn [direction type _]
                                (when (and (= direction :reply) (= type phase))
                                  (deliver entered true)
                                  (when-not (deref release 10000 false)
                                    (throw (ex-info "Recovery reply barrier timed out" {})))))})
            (let [owner (future (recover!))]
              (try
                (is (true? (deref entered 5000 false)))
                ;; Wait for the caller's selector, so closing just the socket
                ;; cannot accidentally pass without waking a transport wait.
                (await! #(some (fn [transport]
                                 (and (instance? Selector transport)
                                      (.isOpen ^Selector transport)))
                               (get-in (attempt) [:transport-owner :resources])))
                (let [resources (vec (get-in (attempt) [:transport-owner :resources]))
                      waiter (future (recover!))
                      closing (future (outcome #(client/disconnect c)))]
                  (is (nil? (deref closing 1000 ::timeout)))
                  (doseq [task [owner waiter]]
                    (let [error (deref task 1000 ::timeout)]
                      (is (instance? Exception error))
                      (is (= :client-disconnected (:termination-reason (ex-data error))))))
                  (is (not (realized? release)) "the proxy is still withholding its reply")
                  (is (every? #(not (if (instance? Selector %)
                                     (.isOpen ^Selector %)
                                     (.isOpen ^Channel %))) resources))
                  (is (client/disconnected? c))
                  (is (identical? pool (client/get-pool c)))
                  (is (= id (client/get-id c)))
                  (is (nil? (attempt)))
                  (is (nil? (#'client/cached-retry-client c endpoint)))
                  (when @candidate (is (client/disconnected? @candidate))))
                (finally
                  (deliver release true)
                  (#'chaos/cut-flows! proxy)
                  (deref owner 5000 nil))))))))))
