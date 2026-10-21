(ns datalevin.client-recovery-test
  (:require
   [clojure.test :refer [deftest is use-fixtures]]
   [datalevin.client :as client]
   [datalevin.client-recovery-chaos :as chaos]
   [datalevin.core :as d]
   [datalevin.kv :as kv]
   [datalevin.server :as server]
   [datalevin.server.session :as session]
   [datalevin.test.core :refer [allocate-port db-fixture server-fixture]]
   [datalevin.util :as u])
  (:import
   [datalevin.binding.cpp CppLMDB]
   [datalevin.client Connection]
   [datalevin.remote KVStore]
   [datalevin.server Server]
   [java.nio.channels SocketChannel]
   [java.util HashMap]))

(use-fixtures :once db-fixture)

(deftest concurrent-session-renewal-keeps-original-clients-recoverable
  ;; A stalled authentication must obey the short client timeout. In particular,
  ;; it must not impose initial authentication's 60-second cold-start allowance
  ;; on every caller queued behind renewal. The proxy proves this fault occurs.
  (let [result (chaos/run-chaos! {:seed 17 :pool-size 3 :cycles 2
                                :fault-order [:authentication-timeout :expire-sessions]})]
    (is (= #{:authentication-timeout :expire-sessions} (:coverage result)))
    (is (= 3 (:clients-created result)))
    (is (= 5 (:transport-clients result)))))

(deftest administrative-requests-work-after-database-reader-cache-initialization
  (server-fixture
    (fn []
      (let [^KVStore kv (d/open-kv "dtlv://datalevin:datalevin@localhost/reader-cache"
                                  {:client-opts {:pool-size 3}})]
        (try
          ;; Creating the dedicated transaction client initializes the shared
          ;; reader cache even when the DB has no custom native-value reader.
          ;; Administrative requests have no database argument or cache key.
          (doseq [c [(.-client kv) (.-tx-client kv)]]
            (is (= ["reader-cache"] (client/list-databases c))))
          (finally (d/close-kv kv)))))))

(deftest a-single-connection-pool-isolates-explicit-transactions
  (server-fixture
    (fn []
      (let [^KVStore kv (d/open-kv "dtlv://datalevin:datalevin@localhost/transaction-isolation"
                                  {:client-opts {:pool-size 1 :time-out 3000}})
            entered (promise)
            release (promise)
            tx (atom nil)]
        (try
          (d/open-dbi kv "data")
          (d/transact-kv kv "data" [[:put 1 1]] :long :long)
          (reset! tx (future
                       (d/with-transaction-kv [writing kv]
                         (d/transact-kv writing "data" [[:put 1 2]] :long :long)
                         (deliver entered true)
                         @release)))
          (is (true? (deref entered 5000 false)))
          (let [c (.-client kv)
                tx-client (.-tx-client kv)
                pool (client/get-pool c)
                tx-pool (client/get-pool tx-client)
                _ (when (identical? pool tx-pool)
                    (throw (ex-info "Ordinary requests share the transaction owner's pool" {})))
                ^Connection ordinary (client/get-connection pool)
                ^Connection owner (client/get-connection tx-pool)]
            (try
              ;; Check transport isolation before issuing a read. Sharing the
              ;; transaction socket can deadlock a lazy DBI open in native code.
              (when (identical? ordinary owner)
                (throw (ex-info "An ordinary request borrowed the transaction owner's connection" {})))
              (is (not= (.getLocalAddress ^SocketChannel (.-ch ordinary))
                        (.getLocalAddress ^SocketChannel (.-ch owner))))
              (finally
                (client/release-connection pool ordinary)
                (client/release-connection tx-pool owner))))
          (is (= 1 (d/get-value kv "data" 1 :long :long))
              "an ordinary read sees the committed snapshot while a transaction is pending")
          (deliver release true)
          (is (not= ::timeout (deref @tx 5000 ::timeout)))
          (is (= 2 (d/get-value kv "data" 1 :long :long)))
          (finally
            (deliver release true)
            (when @tx (deref @tx 5000 nil))
            (d/close-kv kv)))))))

(deftest restored-transactions-open-the-durable-catalog-before-owning-the-writer
  (let [root (u/tmp-dir (str "restored-transaction-catalog-" (random-uuid)))
        port (allocate-port)
        opts {:root root :port port}
        srv (atom (server/create opts))
        store (atom nil)
        db-name "catalog"]
    (try
      (server/start @srv)
      (let [^KVStore remote (d/open-kv
                             (str "dtlv://datalevin:datalevin@127.0.0.1:"
                                  port "/" db-name)
                             {:client-opts {:time-out 3000}})]
        (reset! store remote)
        (d/open-dbi remote "remembered")
        (d/open-dbi remote "unlisted")
        (d/transact-kv remote "unlisted" [[:put 1 7]] :long :long)
        ;; A saved session is a hint, not the durable DBI catalog. Reproduce an
        ;; incomplete saved list while preserving the live application handle.
        (let [^Server original @srv
              deps {:sys-conn-fn (constantly (.-sys-conn original))
                    :clients-fn (constantly (.-clients original))
                    :idle-timeout-fn (constantly 172800000)}]
          (doseq [[id saved] (.-clients original)
                  :when (contains? (:stores saved) db-name)]
            (session/update-client deps original id
                                   #(update-in % [:stores db-name :dbis]
                                               disj "unlisted"))))
        (server/stop @srv)
        (reset! srv (server/create opts))
        (let [^CppLMDB native (kv/raw-lmdb (#'server/get-kv-store @srv db-name))
              ^HashMap handles (.-dbis native)]
          (is (not (.containsKey handles "unlisted")))
          (server/start @srv)
          (d/with-transaction-kv [writing remote]
            ;; Fail before a native read if the handle is missing: attempting
            ;; lazy catalog restoration here would deadlock on our own writer.
            (when-not (.containsKey handles "unlisted")
              (throw (ex-info "Transaction acquired the writer before restoring DBIs" {})))
            (is (= 7 (d/get-value writing "unlisted" 1 :long :long)))
            (d/transact-kv writing "unlisted" [[:put 1 8]] :long :long))
          (is (= 8 (d/get-value remote "unlisted" 1 :long :long)))))
      (finally
        (when @store (d/close-kv @store))
        (server/stop @srv)
        (u/delete-files root)))))
