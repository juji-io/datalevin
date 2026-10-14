(ns datalevin.remote-chatty-cleanup-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [datalevin.client :as client]
            [datalevin.interface :as i]
            [datalevin.remote :as remote])
  (:import [java.util.concurrent ConcurrentHashMap]
           [java.util.concurrent.atomic AtomicBoolean]))

(use-fixtures
  :each
  (fn [f]
    (remote/reset-chatty-kv-stats!)
    (try (f) (finally (remote/reset-chatty-kv-stats!)))))

(defn- streak-state ^ConcurrentHashMap [] @#'remote/chatty-kv-seq-state)

(defn- detect! [db-name]
  (#'remote/detect-chatty-kv! db-name "data" :get-value))

(defn- fake-client [disconnected? close-fault]
  (reify client/IClient
    (request [_ req]
      (when (and close-fault (#{:close :close-kv} (:type req)))
        (throw close-fault))
      {:type :command-complete :result 2})
    (disconnect [_] nil)
    (disconnected? [_] disconnected?)
    (get-pool [_] nil)
    (get-id [_] nil)))

(defn- store [kind db-name disconnected? close-fault]
  (let [client (fake-client disconnected? close-fault)]
    (case kind
      :kv (remote/->KVStore "dtlv://mock" db-name client)
      :datalog (remote/->DatalogStore
                 "dtlv://mock" db-name client (volatile! nil) false
                 (volatile! {}) (AtomicBoolean.) false (AtomicBoolean.)))))

(defn- close-store! [kind store]
  (case kind :kv (i/close-kv store) :datalog (i/close store)))

(defn- db-streaks [db-name]
  (filter (fn [[[_ db]]] (= db db-name)) (into {} (streak-state))))

(deftest expired-thread-streaks-are-removed-on-detection
  (binding [remote/*chatty-kv-detect-window-ms* 10000]
    (let [state (streak-state)
          fresh [-1 "active" "data" :get-value]
          now (System/currentTimeMillis)]
      ;; Dead reader threads cannot revisit their old keys. A different reader
      ;; must reclaim these entries while preserving other active streaks.
      (dotimes [tid 100]
        (.put state [tid "expired" "data" :get-value] {:ts 0 :streak 2}))
      (.put state fresh {:ts now :streak 2})
      (detect! "new-reader")
      (is (empty? (db-streaks "expired")))
      (is (= {:ts now :streak 2} (.get state fresh)))
      (is (= 2 (.size state))))))

(deftest expired-streaks-restart-below-the-detection-threshold
  (binding [remote/*chatty-kv-detect-threshold* 3
            remote/*chatty-kv-detect-window-ms* 10000]
    (let [state (streak-state)
          key [(.getId (Thread/currentThread)) "restart" "data" :get-value]]
      (.put state key {:ts 0 :streak 20})
      (detect! "restart")
      (is (= 1 (:streak (.get state key))))
      (is (empty? (remote/chatty-kv-stats)))
      (detect! "restart")
      (detect! "restart")
      (is (= {["restart" "data" :get-value] 1} (remote/chatty-kv-stats))))))

(deftest store-teardown-clears-only-its-database-diagnostics
  (doseq [kind [:kv :datalog]
          disconnected? [false true]]
    (remote/reset-chatty-kv-stats!)
    (binding [remote/*chatty-kv-detect-threshold* 1
              remote/*chatty-kv-detect-window-ms* 10000]
      (let [store (store kind "closing" disconnected? nil)]
        (detect! "closing")
        (detect! "remaining")
        (close-store! kind store)
        (is (empty? (db-streaks "closing")))
        (is (= 1 (count (db-streaks "remaining"))))
        (is (= {["remaining" "data" :get-value] 1} (remote/chatty-kv-stats)))
        (close-store! kind store)
        (is (= {["remaining" "data" :get-value] 1} (remote/chatty-kv-stats)))))))

(deftest failed-remote-close-still-clears-diagnostics
  (doseq [kind [:kv :datalog]]
    (remote/reset-chatty-kv-stats!)
    (binding [remote/*chatty-kv-detect-threshold* 1]
      (let [fault (AssertionError. "remote close failed")
            store (store kind "closing" false fault)]
        (detect! "closing")
        (is (thrown-with-msg? AssertionError #"remote close failed"
                              (close-store! kind store)))
        (is (empty? (db-streaks "closing")))
        (is (empty? (remote/chatty-kv-stats)))))))

(deftest closing-a-datalog-parent-clears-its-kv-view-diagnostics
  (binding [remote/*chatty-kv-detect-threshold* 1]
    (let [parent (store :datalog "parent" false nil)
          kv (remote/datalog-kv parent)]
      (is (= 2 (i/get-value kv "data" 1 :long :long true)))
      (is (= {["parent" "data" :get-value] 1} (remote/chatty-kv-stats)))
      (i/close parent)
      (is (empty? (db-streaks "parent")))
      (is (empty? (remote/chatty-kv-stats))))))
