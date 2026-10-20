(ns datalevin.server.resilience-test
  (:require
   [clojure.test :refer [deftest is use-fixtures]]
   [datalevin.client :as client]
   [datalevin.core :as d]
   [datalevin.server :as server]
   [datalevin.server.session :as session]
   [datalevin.test.core :refer [allocate-port db-fixture]]
   [datalevin.util :as u])
  (:import
   [datalevin.server Server]
   [java.lang.management ManagementFactory ThreadInfo]
   [java.net InetSocketAddress StandardSocketOptions]
   [java.nio ByteBuffer]
   [java.nio.channels ServerSocketChannel SocketChannel]
   [java.util UUID]
   [java.util.concurrent ExecutorService]))

(use-fixtures :once db-fixture)

(defn- await-condition! [pred]
  (let [deadline (+ (System/nanoTime) 5000000000)]
    (loop []
      (when-not (pred)
        (when (>= (System/nanoTime) deadline)
          (throw (ex-info "Resilience test timed out" {})))
        (Thread/sleep 5)
        (recur)))))

(defn- session-deps [^Server srv]
  {:sys-conn-fn (constantly (.-sys-conn srv))
   :clients-fn (constantly (.-clients srv))
   :user-roles-fn (constantly #{})
   :user-permissions-fn (constantly [])
   :user-eid-fn (constantly 1)
   :now-ms-fn (constantly 1)
   :idle-timeout-fn (constantly 172800000)})

(defn- seed-session! [^Server srv]
  (let [id (UUID/randomUUID)]
    (session/add-client (session-deps srv) srv "127.0.0.1" id "datalevin")
    id))

(defn- uri [port]
  (str "dtlv://datalevin:datalevin@127.0.0.1:" port))

(deftest blocked-session-expiry-does-not-block-new-connections
  (let [root (u/tmp-dir (str "server-blocked-sweep-" (UUID/randomUUID)))
        port (allocate-port)
        ^Server srv (server/create {:root root :port port})
        id (seed-session! srv)
        entered (promise)
        release (promise)
        holder (future
                 (session/update-client
                   (session-deps srv) srv id
                   (fn [current]
                     (deliver entered (.getId (Thread/currentThread)))
                     @release
                     current)))
        connecting (atom nil)]
    (try
      (let [owner-id (deref entered 5000 nil)]
        (is (some? owner-id))
        (server/start srv)
        ;; Prove the sweep is waiting for this real session's computation lock.
        (await-condition!
          #(some (fn [^ThreadInfo info] (= owner-id (.getLockOwnerId info)))
                 (.dumpAllThreads (ManagementFactory/getThreadMXBean) false false)))
        (reset! connecting
                (future
                  (with-open [channel (doto (SocketChannel/open)
                                        (.connect (InetSocketAddress. "127.0.0.1" (int port))))]
                    (let [conn (client/->Connection channel 3000 (ByteBuffer/allocate 65536))]
                      (try
                        ;; Registration exercises accept and request handling
                        ;; without competing for the locked session's map bin.
                        (client/send-n-receive conn {:type :set-client-id :client-id id})
                        (finally (client/close conn)))))))
        (is (= :set-client-id-ok (:type (deref @connecting 4000 ::timeout))))
        (is (not (realized? release)))
        (deliver release true)
        @holder
        (await-condition! #(nil? (get (.-clients srv) id))))
      (finally
        (deliver release true)
        @holder
        (when @connecting
          (future-cancel @connecting))
        (server/stop srv)
        (is (.isTerminated ^ExecutorService
                           (:session-maintenance (.-execution srv))))
        (u/delete-files root)))))

(deftest restored-weekend-session-batch-isolated-from-a-malformed-session
  (let [root (u/tmp-dir (str "server-weekend-sweep-" (UUID/randomUUID)))
        port (allocate-port)
        opts {:root root :port port}
        ^Server original (server/create opts)
        bad-id (UUID/randomUUID)]
    (try
      (dotimes [_ 66] (seed-session! original))
      (d/transact-kv
        (session/session-lmdb (.-sys-conn original))
        [[:put session/session-dbi bad-id
          {:last-active :malformed :last-active-checkpoint-until 1 :stores {}}
          :uuid :data]])
      (server/stop original)
      (let [^Server restored (server/create opts)]
        (try
          (is (= 67 (count (.-clients restored))))
          (server/start restored)
          (let [c (client/new-client (uri port) {:pool-size 3 :time-out 3000})]
            (try
              (await-condition! #(= #{bad-id (client/get-id c)}
                                    (set (keys (.-clients restored)))))
              (dotimes [_ 20] (is (= [] (client/list-databases c))))
              (is (= #{bad-id (client/get-id c)}
                     (set (keys (session/load-sessions (.-sys-conn restored))))))
              (finally (client/close-pool (client/get-pool c)))))
          (finally (server/stop restored))))
      (finally
        (server/stop original)
        (u/delete-files root)))))

(deftest same-client-recovers-after-outage-and-server-session-expiry
  (let [root (u/tmp-dir (str "client-outage-recovery-" (UUID/randomUUID)))
        port (allocate-port)
        opts {:root root :port port}
        ^Server original (server/create opts)
        c-v (atom nil)]
    (try
      (server/start original)
      (let [c (client/new-client (uri port) {:pool-size 3 :time-out 5000})
            pool (client/get-pool c)
            id (client/get-id c)
            connections (vec (repeatedly 3 #(client/get-connection pool)))]
        (reset! c-v c)
        (doseq [conn connections]
          (client/close conn)
          (client/release-connection pool conn))
        (server/stop original)
        (dotimes [_ 6]
          (is (thrown? Exception (client/list-databases c)))
          (is (false? (client/disconnected? c))))
        (let [^Server restored (server/create opts)]
          (try
            (session/remove-client (session-deps restored) restored id)
            (server/start restored)
            (is (= [] (client/list-databases c)))
            (is (not= id (client/get-id c)))
            (is (client/closed-pool? pool))
            ;; Borrow every replacement slot simultaneously, proving full capacity.
            (let [pool (client/get-pool c)
                  connections (vec (repeatedly 3 #(client/get-connection pool)))]
              (try
                (is (= 3 (count (set connections))))
                (doseq [conn connections]
                  (is (= :command-complete
                         (:type (client/send-n-receive
                                  conn {:type :list-databases :args []})))))
                (finally
                  (doseq [conn connections] (client/release-connection pool conn)))))
            (finally (server/stop restored)))))
      (finally
        (when @c-v (client/close-pool (client/get-pool @c-v)))
        (server/stop original)
        (u/delete-files root)))))

(defn- timeout-data [error]
  (some #(when (= :socket/timeout (:error (ex-data %)))
           (select-keys (ex-data %) [:error :phase :timeout-ms]))
        (take-while some? (iterate ex-cause error))))

(deftest client-send-timeout-closes-a-socket-when-the-server-stops-reading
  (doseq [send! [client/send-n-receive client/send-only]]
    (with-open [listener (doto (ServerSocketChannel/open)
                          (.setOption StandardSocketOptions/SO_RCVBUF (Integer/valueOf 1024))
                          (.bind (InetSocketAddress. "127.0.0.1" 0)))
                channel (doto (SocketChannel/open)
                          (.setOption StandardSocketOptions/SO_SNDBUF (Integer/valueOf 1024))
                          (.connect (.getLocalAddress listener))
                          (.configureBlocking false))
                peer (.accept listener)]
      (is (.isOpen peer))
      (let [conn (client/->Connection channel 200 (ByteBuffer/allocate 16777216))
            request {:type :get-value :args [(byte-array 8388608)]}
            result (future (try (send! conn request)
                                (catch Exception e e)))]
        (try
          (let [error (deref result 3000 ::timeout)]
            (is (instance? Exception error))
            (when (instance? Exception error)
              (is (= {:error :socket/timeout :phase :send :timeout-ms 200}
                     (timeout-data error))))
            (is (not (.isOpen channel))))
          (finally
            (.close channel)
            (future-cancel result)
            (client/close conn)))))))
