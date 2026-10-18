(ns datalevin.query.source-permissions-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.query.source :as source]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.permissions :as permissions])
  (:import [java.io Closeable]))

(defn- error [f]
  (try (f) nil (catch clojure.lang.ExceptionInfo e (ex-data e))))

(defn- close! [^Closeable resource] (.close resource))

(defn- live-usage [ctl]
  (dissoc (control/usage ctl) :external-requests))

(defn- request-limiter [ctl maximum]
  (source/request-limiter ctl {:max-concurrent-requests maximum :rate-limit nil}))

(def default-info {:consumption {:opens :many :max-open-cursors 1}})
(def once-info {:consumption {:opens :once :max-open-cursors 1}})
(def independent-info
  {:consumption-groups [{:id :orders :opens :once :max-open-cursors 1}
                        {:id :customers :opens :many :max-open-cursors 1}]})

(defn- alternative
  ([] {:properties {:replay :unstable}})
  ([group] {:properties {:replay :unstable :consumption-group group}}))

(deftest default-permissions-share-attempts-across-aliases-and-alternatives
  (let [ctl (control/create)
        calls (atom [])]
    (try
      (with-open [permissions (permissions/consumption ctl once-info)]
        (is (thrown? Exception
                     (with-open [_open (permissions/reserve-open! permissions (alternative) {})]
                       (swap! calls conj :first-alias)
                       (throw (Exception. "failed provider open")))))
        (doseq [replay [:identical :unstable]]
          (is (= :query-source/unsupported
                 (:error (error #(with-open [_open (permissions/reserve-open!
                                                    permissions {:properties {:replay replay}} {})]
                                   (swap! calls conj :other-alias)))))))
        (is (= [:first-alias] @calls)))
      (is (every? zero? (vals (live-usage ctl))))
      (finally (control/close! ctl)))))

(deftest default-concurrency-and-explicit-independent-resources
  (let [ctl (control/create)]
    (try
      (with-open [permissions (permissions/consumption ctl default-info)]
        (let [first-open (permissions/reserve-open! permissions (alternative) {})]
          (is (= :query-source/limit (:error (error #(permissions/reserve-open! permissions (alternative) {})))))
          (close! first-open)
          (close! first-open)
          (with-open [_open (permissions/reserve-open! permissions (alternative) {})]
            (is true))))
      (with-open [permissions (permissions/consumption ctl independent-info)
                  orders (permissions/reserve-open! permissions (alternative :orders) {:consumption-group :orders})
                  customers (permissions/reserve-open! permissions (alternative :customers) {:consumption-group :customers})]
        (close! orders)
        (is (= :query-source/unsupported
               (:error (error #(permissions/reserve-open! permissions (alternative :orders) {:consumption-group :orders})))))
        (close! customers)
        (with-open [_open (permissions/reserve-open! permissions (alternative :customers) {:consumption-group :customers})]
          (is true)))
      (is (every? zero? (vals (live-usage ctl))))
      (finally (control/close! ctl)))))

(deftest malformed-consumption-and-crossed-group-opens
  (let [ctl (control/create)]
    (try
      (doseq [info [{} (merge default-info independent-info)
                    {:consumption {:opens :once :max-open-cursors 2}}
                    {:consumption {:opens :many :max-open-cursors 0}}
                    {:consumption {:opens :reopen :max-open-cursors 1}}
                    {:consumption {:opens :many :max-open-cursors 1 :id :invented}}
                    {:consumption-groups [{:id :one :opens :once :max-open-cursors 1}]}
                    {:consumption-groups [{:id :same :opens :once :max-open-cursors 1}
                                          {:id :same :opens :many :max-open-cursors 1}]}]]
        (is (= :query-source/invalid (:error (error #(permissions/consumption ctl info))))))
      (with-open [default (permissions/consumption ctl default-info)
                  explicit (permissions/consumption ctl independent-info)]
        (doseq [[permissions alt request]
                [[default (alternative :orders) {:consumption-group :orders}]
                 [default (alternative) {:consumption-group nil}]
                 [default {:properties {:consumption-group nil}} {}]
                 [explicit (alternative) {}]
                 [explicit (alternative :orders) {:consumption-group :customers}]
                 [explicit (alternative :unknown) {:consumption-group :unknown}]]]
          (is (= :query-source/invalid (:error (error #(permissions/reserve-open! permissions alt request)))))))
      (is (every? zero? (vals (live-usage ctl))))
      (finally (control/close! ctl)))))

(deftest closing-permissions-retains-live-open-accounting
  (let [ctl (control/create)
        permissions (permissions/consumption ctl default-info)
        open (permissions/reserve-open! permissions (alternative) {})
        before (:retained-bytes (control/usage ctl))]
    (try
      (close! permissions)
      (is (= before (:retained-bytes (control/usage ctl))))
      (is (= :closed (:state (error #(permissions/reserve-open! permissions (alternative) {})))))
      (close! open)
      (close! open)
      (close! permissions)
      (is (every? zero? (vals (live-usage ctl))))
      (finally (close! open) (close! permissions) (control/close! ctl)))))

(deftest actual-setup-pages-retries-and-splits-share-one-request-ledger
  (let [ctl (control/create {:limits {:external-requests 4}})
        calls (atom [])]
    (try
      (with-open [limiter (request-limiter ctl 2)
                  permissions (permissions/consumption ctl independent-info)]
        (let [request! (fn [kind]
                         (with-open [_permit (source/acquire-request! limiter)]
                           (swap! calls conj kind)
                           (when (= :retry kind) (throw (Exception. "backend failure")))))]
          (request! :setup)
          (with-open [_orders (permissions/reserve-open! permissions (alternative :orders) {:consumption-group :orders})
                      _customers (permissions/reserve-open! permissions (alternative :customers) {:consumption-group :customers})]
            (request! :page)
            (is (thrown? Exception (request! :retry)))
            (request! :split))
          (is (= :external-requests (:category (error #(request! :extra-page)))))
          (is (= [:setup :page :retry :split] @calls))
          (is (= 4 (:external-requests (control/usage ctl))))
          (is (zero? (:concurrent-requests (control/usage ctl))))))
      (is (every? zero? (vals (live-usage ctl))))
      (finally (control/close! ctl)))))

(deftest session-and-query-concurrency-permits-wait-without-charging-starts
  (doseq [shared-session? [true false]]
    (let [ctl (control/create {:limits {:concurrent-requests (if shared-session? 2 1)}})
          child (control/create {:parent ctl})
          first-limiter (request-limiter ctl 1)
          second-limiter (if shared-session? first-limiter (request-limiter child 2))
          first-permit (source/acquire-request! first-limiter)
          entered (promise)
          waiter (future (deliver entered true)
                         (with-open [_permit (source/acquire-request! second-limiter)] :opened))]
      (try
        (is (= true (deref entered 3000 :hung)))
        (is (= :waiting (deref waiter 50 :waiting)))
        (is (= 1 (:external-requests (control/usage ctl))))
        (close! first-permit)
        (close! first-permit)
        (is (= :opened (deref waiter 3000 :hung)))
        (is (= 2 (:external-requests (control/usage ctl))))
        (is (zero? (:concurrent-requests (control/usage ctl))))
        (finally
          (close! first-permit)
          (close! first-limiter)
          (close! second-limiter)
          (future-cancel waiter)
          (control/close! child)
          (control/close! ctl)))
      (is (every? zero? (vals (live-usage ctl)))))))

(deftest rolling-rate-limit-expires-and-does-not-refund-on-release
  (let [ctl (control/create)]
    (try
      (with-open [limiter (source/request-limiter ctl {:max-concurrent-requests 2
                                                      :rate-limit {:requests 1 :per-ms 100}})]
        (let [before (System/nanoTime)]
          (with-open [_permit (source/acquire-request! limiter)] (is true))
          (with-open [_permit (source/acquire-request! limiter)]
            (is (<= 100000000 (- (System/nanoTime) before))))
          (is (= 2 (:external-requests (control/usage ctl))))
          (is (zero? (:concurrent-requests (control/usage ctl))))))
      (is (every? zero? (vals (live-usage ctl))))
      (finally (control/close! ctl)))))

(deftest cancellation-timeout-and-close-interrupt-permit-waits
  (doseq [reason [:cancel :timeout :limiter-close :control-close]
          rate? [false true]]
    (let [ctl (control/create (when (= :timeout reason) {:timeout-ms 500}))
          limiter (source/request-limiter ctl {:max-concurrent-requests 1
                                               :rate-limit (when rate? {:requests 1 :per-ms 10000})})
          first-permit (source/acquire-request! limiter)
          _ (when rate? (close! first-permit))
          entered (promise)
          waiter (future (deliver entered true)
                         (error #(with-open [_permit (source/acquire-request! limiter)] :unexpected-open)))]
      (try
        (is (= true (deref entered 3000 :hung)))
        (is (= :waiting (deref waiter 20 :waiting)))
        (case reason
          :cancel (control/cancel! ctl {:error :query-source/cancelled :phase :execution})
          :timeout nil
          :limiter-close (close! limiter)
          :control-close (control/close! ctl))
        (let [result (deref waiter 3000 :hung)]
          (is (not= :hung result))
          (is (= (case reason :cancel :query-source/cancelled :timeout :query/timeout :query-source/invalid)
                 (:error result))))
        (is (= 1 (:external-requests (control/usage ctl))))
        (finally (close! first-permit) (close! limiter) (future-cancel waiter) (control/close! ctl)))
      (is (every? zero? (vals (live-usage ctl)))))))

(deftest closing-a-request-limiter-keeps-live-permits-accounted
  (let [ctl (control/create)
        limiter (request-limiter ctl 1)
        permit (source/acquire-request! limiter)
        before (:retained-bytes (control/usage ctl))]
    (try
      (close! limiter)
      (is (= before (:retained-bytes (control/usage ctl))))
      (close! permit)
      (close! permit)
      (is (every? zero? (vals (live-usage ctl))))
      (finally (close! permit) (close! limiter) (control/close! ctl)))))

(deftest malformed-request-constraints-and-memory-failures-release-charges
  (let [ctl (control/create)]
    (try
      (doseq [constraints [nil {} {:max-concurrent-requests 1}
                           {:max-concurrent-requests 0 :rate-limit nil}
                           {:max-concurrent-requests 1 :rate-limit false}
                           {:max-concurrent-requests 1 :rate-limit {:requests 1 :per-ms 0}}
                           {:max-concurrent-requests 1 :rate-limit {:requests 1 :per-ms 1 :other true}}
                           {:max-concurrent-requests 1 :rate-limit nil :other true}]]
        (is (= :query-source/invalid (:error (error #(source/request-limiter ctl constraints))))))
      (is (every? zero? (vals (live-usage ctl))))
      (finally (control/close! ctl))))
  (let [ctl (control/create {:limits {:retained-bytes 1}})]
    (try
      (is (= :retained-bytes (:category (error #(request-limiter ctl 1)))))
      (is (= :retained-bytes (:category (error #(permissions/consumption ctl once-info)))))
      (is (every? zero? (vals (control/usage ctl))))
      (finally (control/close! ctl)))))

(deftest one-shot-open-attempts-are-atomic-under-contention
  (let [ctl (control/create)
        permissions (permissions/consumption ctl once-info)
        start (promise)
        attempted (atom 0)
        workers (mapv (fn [_]
                        (future @start
                                (error #(with-open [_open (permissions/reserve-open! permissions (alternative) {})]
                                          (swap! attempted inc)))))
                      (range 12))]
    (try
      (deliver start true)
      (let [results (mapv #(deref % 3000 :hung) workers)]
        (is (= 1 @attempted))
        (is (= 1 (count (filter nil? results))))
        (is (= 11 (count (filter #(= :query-source/unsupported (:error %)) results)))))
      (finally (deliver start true) (run! future-cancel workers) (close! permissions) (control/close! ctl)))))

(deftest memory-admission-failure-does-not-spend-an-unstarted-request-or-open
  (let [ctl (control/create {:limits {:retained-bytes 16384}})]
    (try
      (with-open [limiter (source/request-limiter ctl {:max-concurrent-requests 1
                                                      :rate-limit {:requests 1 :per-ms 1000}})]
        (let [free (- 16384 (:retained-bytes (control/usage ctl)))
              held (control/reserve! ctl {:retained-bytes (- free 1280)})
              before (control/usage ctl)]
          (try
            (is (= :retained-bytes (:category (error #(source/acquire-request! limiter)))))
            (is (= before (control/usage ctl)))
            (finally (control/release! held))))
        (with-open [_permit (source/acquire-request! limiter)]
          (is (= 1 (:external-requests (control/usage ctl))))))
      (with-open [permissions (permissions/consumption ctl once-info)]
        (let [free (- 16384 (:retained-bytes (control/usage ctl)))
              held (control/reserve! ctl {:retained-bytes (- free 256)})
              before (control/usage ctl)]
          (try
            (is (= :retained-bytes (:category (error #(permissions/reserve-open! permissions (alternative) {})))))
            (is (= before (control/usage ctl)))
            (finally (control/release! held))))
        (with-open [_open (permissions/reserve-open! permissions (alternative) {})]
          (is true)))
      (is (every? zero? (vals (live-usage ctl))))
      (finally (control/close! ctl)))))

(deftest interruption-cancels-a-blocked-request-without-spending-another-start
  (let [ctl (control/create)
        limiter (request-limiter ctl 1)
        first-permit (source/acquire-request! limiter)
        entered (promise) result (promise)
        worker (doto (Thread. ^Runnable
                              (fn []
                                (deliver entered true)
                                (deliver result (error #(with-open [_permit (source/acquire-request! limiter)]
                                                          :unexpected-open)))))
                 (.setDaemon true) (.start))]
    (try
      (is (= true (deref entered 3000 :hung)))
      (is (= :waiting (deref result 20 :waiting)))
      (.interrupt worker)
      (is (= :query-source/cancelled (:error (deref result 3000 :hung))))
      (is (= 1 (:external-requests (control/usage ctl))))
      (finally
        (.interrupt worker)
        (close! first-permit)
        (close! limiter)
        (control/close! ctl)
        (.join worker 3000)))
    (is (not (.isAlive worker)))
    (is (every? zero? (vals (live-usage ctl))))))
