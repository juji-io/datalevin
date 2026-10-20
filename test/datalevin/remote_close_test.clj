(ns datalevin.remote-close-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [datalevin.client :as client]
   [datalevin.interface :as i]
   [datalevin.remote :as remote]
   [datalevin.server :as server]
   [datalevin.test.core :refer [allocate-port db-fixture]]
   [datalevin.util :as u])
  (:import
   [datalevin.client Connection]
   [datalevin.remote DatalogStore KVStore]
   [java.nio.channels SocketChannel]))

(use-fixtures :once db-fixture)

(deftest failed-remote-closes-release-owned-pools
  (doseq [kind [:kv :datalog]
          owns-client? [true false]]
    (testing (str (name kind) ", owns client " owns-client?)
      (let [root (u/tmp-dir (str "remote-close-" (random-uuid)))
            port (allocate-port)
            srv (server/create {:root root :port port})
            uri (str "dtlv://datalevin:datalevin@localhost:" port "/close-test")
            opts {:client-opts {:pool-size 1 :time-out 1000}}]
        (try
          (server/start srv)
          (let [c (client/new-client uri (:client-opts opts))
                store (case kind
                        :kv (remote/open-kv c uri opts owns-client?)
                        :datalog (remote/open c uri nil opts owns-client?))
                tx-client (case kind
                            :kv (.-tx-client ^KVStore store)
                            :datalog (.-tx-client ^DatalogStore store))
                pool (client/get-pool c)
                tx-pool (client/get-pool tx-client)
                ^Connection tx-conn (client/get-connection tx-pool)
                ^SocketChannel tx-channel (.-ch tx-conn)
                close! (case kind :kv i/close-kv :datalog i/close)
                closed? (case kind :kv i/closed-kv? :datalog i/closed?)]
            (client/release-connection tx-pool tx-conn)
            (try
              (is (not (identical? c tx-client)))
              (server/stop srv)
              (let [failure (try (close! store) (catch Throwable t t))]
                (is (instance? Exception failure)))
              (is (closed? store))
              (is (= owns-client? (client/closed-pool? pool)))
              (is (= owns-client? (client/disconnected? c)))
              (is (client/closed-pool? tx-pool))
              (is (client/disconnected? tx-client))
              (is (not (.isOpen tx-channel)))
              (is (nil? (close! store)))
              (finally
                (client/close-pool pool)
                (client/close-pool tx-pool))))
          (finally
            (server/stop srv)
            (u/delete-files root)))))))

(defn- cleanup-client
  [failure]
  (let [closed? (atom false)
        attempts (atom 0)]
    {:closed? closed?
     :attempts attempts
     :client (reify client/IClient
               (disconnected? [_] @closed?)
               (disconnect [_]
                 (swap! attempts inc)
                 (reset! closed? true)
                 (when failure (throw failure))))}))

(deftest remote-close-preserves-primary-error-and-attempts-both-cleanups
  (doseq [rpc-error [nil (ex-info "Close RPC failed" {})
                    (AssertionError. "Close RPC failed")]]
    (let [client-error (ex-info "Client cleanup failed" {})
          tx-error (AssertionError. "Transaction client cleanup failed")
          main (cleanup-client client-error)
          tx (cleanup-client tx-error)
          failure (try
                    (#'remote/close-remote-store!
                     (:client main) (:client tx) true "close-test"
                     #(when rpc-error (throw rpc-error)))
                    (catch Throwable t t))]
      (is (identical? (or rpc-error client-error) failure))
      (is (= (if rpc-error [client-error tx-error] [tx-error])
             (vec (.getSuppressed ^Throwable failure))))
      (is @(:closed? main))
      (is @(:closed? tx))
      (is (= 1 @(:attempts main) @(:attempts tx))))))

(deftest remote-close-respects-borrowed-and-shared-clients
  (doseq [owns-client? [true false]
          shared? [true false]]
    (testing (str "owns client " owns-client? ", shared client " shared?)
      (let [main (cleanup-client nil)
            tx (if shared? main (cleanup-client nil))
            failure (ex-info "Close RPC failed" {})]
        (is (identical? failure
                        (try
                          (#'remote/close-remote-store!
                           (:client main) (:client tx) owns-client? "close-test"
                           #(throw failure))
                          (catch Throwable t t))))
        (is (= (if owns-client? 1 0) @(:attempts main)))
        (is (= (if shared? owns-client? true) @(:closed? tx)))))))
