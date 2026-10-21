(ns datalevin.server.transaction-read-timeout-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.client :as client]
            [datalevin.core :as d]
            [datalevin.lmdb :as lmdb]
            [datalevin.server :as server]
            [datalevin.test.core :refer [allocate-port db-fixture]]
            [datalevin.util :as u])
  (:import [datalevin.remote KVStore]
           [datalevin.server Server]
           [java.net InetSocketAddress]
           [java.nio ByteBuffer]
           [java.nio.channels SelectionKey SocketChannel]
           [java.util.concurrent Semaphore]
           [java.util.concurrent.atomic AtomicLong]))

(use-fixtures :once db-fixture)

(defn- await! [pred]
  (let [deadline (+ (System/nanoTime) 5000000000)]
    (loop []
      (when-not (pred)
        (when (>= (System/nanoTime) deadline)
          (throw (ex-info "Transaction read timeout test timed out" {})))
        (Thread/sleep 5)
        (recur)))))

(defn- with-server [f]
  (let [root (u/tmp-dir (str "transaction-read-timeout-" (random-uuid)))
        port (allocate-port)
        ^Server srv (server/create {:root root :host "127.0.0.1" :port port
                                    :read-timeout-ms 1000})
        uri (str "dtlv://datalevin:datalevin@127.0.0.1:" port "/")]
    (try
      (server/start srv)
      (f srv uri port)
      (finally
        (server/stop srv)
        (is (empty? (:connection-threads (.-execution srv))))
        (u/delete-files root)))))

(defn- read-deadline ^long [^SelectionKey key]
  (.get ^AtomicLong (:read-deadline (:connection-lease @(.attachment key)))))

(defn- pause-transaction! [^Server srv db-name]
  (let [{:keys [runner runner-skey]} (get (.-dbs srv) db-name)
        ^SelectionKey key runner-skey]
    (Thread/sleep 1500)
    (is (some? runner))
    (is (identical? runner (:runner (get (.-dbs srv) db-name)))
        "the native transaction survives the idle network timeout")
    (is (.isValid key) "the transaction keeps its original socket")
    key))

(deftest idle-kv-and-datalog-transactions-survive-the-read-timeout
  (with-server
    (fn [^Server srv uri _]
      (let [kv (d/open-kv (str uri "idle-kv") {:client-opts {:pool-size 1 :time-out 3000}})]
        (try
          (d/open-dbi kv "data")
          (testing "KV pauses before its first operation and after a staged write"
            (let [owner (d/with-transaction-kv [tx kv]
                          (pause-transaction! srv "idle-kv")
                          (d/transact-kv tx "data" [[:put :key :value]])
                          (let [owner (pause-transaction! srv "idle-kv")]
                            (is (= :value (d/get-value tx "data" :key)))
                            owner))]
              (is (= :value (d/get-value kv "data" :key)))
              (await! #(not (.isValid ^SelectionKey owner)))
              (is (nil? (:runner (get (.-dbs srv) "idle-kv")))
                  "commit restores the ordinary idle socket deadline")))
          (finally (d/close-kv kv))))
      (let [conn (d/create-conn (str uri "idle-datalog") {:value {}}
                                {:client-opts {:pool-size 1 :time-out 3000}})]
        (try
          (testing "Datalog uses the same idle transaction exemption"
            (let [owner (d/with-transaction [tx conn]
                          (pause-transaction! srv "idle-datalog")
                          (d/transact! tx [{:db/id 1 :value 42}])
                          (let [owner (pause-transaction! srv "idle-datalog")]
                            (is (= 42 (:value (d/pull @tx [:value] 1))))
                            owner))]
              (is (= 42 (:value (d/pull @conn [:value] 1))))
              (await! #(not (.isValid ^SelectionKey owner)))
              (is (nil? (:runner (get (.-dbs srv) "idle-datalog"))))))
          (finally (d/close conn)))))))

(deftest partial-frames-and-copy-in-still-expire-inside-transactions
  (with-server
    (fn [^Server srv uri port]
      (let [^KVStore kv (d/open-kv (str uri "stalled-tx")
                                  {:client-opts {:pool-size 1 :time-out 3000}})]
        (try
          (d/open-dbi kv "data")
          (doseq [kind [:partial-frame :copy-in]]
            (testing (name kind)
              (let [ch (doto (SocketChannel/open
                              (InetSocketAddress. "127.0.0.1" (int port)))
                         (.configureBlocking false))
                    conn (client/->Connection ch 3000 (ByteBuffer/allocate 65536))
                    request! #(client/send-n-receive conn %)]
                (try
                  (is (= :set-client-id-ok
                         (:type (request! {:type :set-client-id
                                           :client-id (client/get-id (.-client kv))}))))
                  (is (= :command-complete
                         (:type (request! {:type :open-transact-kv :args ["stalled-tx"]}))))
                  (is (= :command-complete
                         (:type (request! {:type :transact-kv :mode :request :writing? true
                                           :args ["stalled-tx" "data" [[:put :key :staged]]
                                                  :data :data]}))))
                  (let [owner (pause-transaction! srv "stalled-tx")]
                    (case kind
                      :partial-frame (.write ch (ByteBuffer/wrap (byte-array [1])))
                      :copy-in (is (= :copy-in-response
                                      (:type (request! {:type :transact-kv :mode :copy-in
                                                        :writing? true
                                                        :args ["stalled-tx" "data" nil
                                                               :data :data]})))))
                    (await! #(pos? (read-deadline owner)))
                    (await! #(nil? (:runner (get (.-dbs srv) "stalled-tx"))))
                    (is (not (.isValid ^SelectionKey owner)))
                    (is (= 1 (.availablePermits
                               ^Semaphore (:lock (get (.-dbs srv) "stalled-tx")))))
                    (is (nil? (d/get-value kv "data" :key)) "the staged write was aborted"))
                  (finally (client/close conn))))))
          (d/with-transaction-kv [tx kv]
            (d/transact-kv tx "data" [[:put :key :committed]]))
          (is (= :committed (d/get-value kv "data" :key)))
          (finally (d/close-kv kv)))))))

(deftest an-explicit-body-timeout-still-aborts-the-idle-transaction
  (with-server
    (fn [^Server srv uri _]
      (let [kv (d/open-kv (str uri "body-timeout")
                         {:client-opts {:pool-size 1 :time-out 3000}})
            owner (atom nil)]
        (try
          (d/open-dbi kv "data")
          (let [error (try
                        (d/with-transaction-kv [tx kv {:timeout-ms 300}]
                          (reset! owner (:runner-skey (get (.-dbs srv) "body-timeout")))
                          (d/transact-kv tx "data" [[:put :key :staged]])
                          (Thread/sleep 1500))
                        nil
                        (catch Exception e e))]
            (is (lmdb/explicit-transaction-timeout-error? error)))
          (is (nil? (:runner (get (.-dbs srv) "body-timeout"))))
          (is (nil? (d/get-value kv "data" :key)))
          (await! #(not (.isValid ^SelectionKey @owner)))
          (finally (d/close-kv kv)))))))
