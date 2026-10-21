(ns datalevin.remote-open-cleanup-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [datalevin.client :as client]
   [datalevin.remote :as remote]
   [datalevin.test.core :refer [server-fixture]])
  (:import
   [java.util Map]))

(use-fixtures :once server-fixture)

(def ^:private client-opts {:pool-size 1 :time-out 3000})
(def ^:private opts {:client-opts client-opts})
(def ^:private root-uri "dtlv://datalevin:datalevin@localhost")

(defn- channels []
  (set (.keySet ^Map @#'client/connection-wire-opts)))

(defn- outcome [f]
  (try (f) (catch Throwable t t)))

(defn- open-store [kind uri]
  (case kind
    :kv (remote/open-kv uri opts)
    :datalog (remote/open uri nil opts)))

(defn- open-with-client [kind c uri owns-client?]
  (case kind
    :kv (remote/open-kv c uri opts owns-client?)
    :datalog (remote/open c uri nil opts owns-client?)))

(deftest missing-database-name-is-rejected-before-client-creation
  (doseq [open! [#(remote/open-kv % opts)
                 #(remote/open % nil opts)
                 #(remote/open % nil)]
          suffix ["" "/"]]
    (let [before (channels)
          failure (outcome #(open! (str root-uri suffix)))]
      (is (instance? clojure.lang.ExceptionInfo failure))
      (is (= "URI should contain a database name" (ex-message failure)))
      (is (= before (channels)))))
  ;; Even an unreachable server must not be contacted for an invalid URI.
  (doseq [kind [:kv :datalog]]
    (is (= "URI should contain a database name"
           (ex-message (outcome #(open-store kind
                                            "dtlv://invalid:invalid@127.0.0.1:1")))))))

(deftest permission-rejection-releases-only-constructor-owned-clients
  (let [admin (client/new-client root-uri client-opts)]
    (try
      (client/create-user admin "restricted" "secret")
      (doseq [[kind db-type] [[:kv :key-value] [:datalog :datalog]]]
        (let [db-name (str "private-" (name kind))
              uri (str "dtlv://restricted:secret@localhost/" db-name)]
          (client/create-database admin db-name db-type)
          (testing (name kind)
            (doseq [pool-size [1 3]]
              (let [before (channels)
                    open-opts {:client-opts (assoc client-opts :pool-size pool-size)}]
                (dotimes [_ 2]
                  (let [failure (outcome #(case kind
                                           :kv (remote/open-kv uri open-opts)
                                           :datalog (remote/open uri nil open-opts)))]
                    (is (instance? clojure.lang.ExceptionInfo failure))
                    (is (re-find #"permission" (ex-message failure)))
                    (is (= before (channels)))))))
            (let [borrowed (client/new-client uri client-opts)]
              (try
                (let [before (channels)
                      failure (outcome #(open-with-client kind borrowed uri false))]
                  (is (instance? clojure.lang.ExceptionInfo failure))
                  (is (re-find #"permission" (ex-message failure)))
                  (is (not (client/disconnected? borrowed)))
                  (is (not (client/closed-pool? (client/get-pool borrowed))))
                  (is (= before (channels))))
                (finally (client/close-client borrowed)))))))
      (finally (client/close-client admin)))))

(deftest transaction-client-construction-failure-unwinds-store-opening
  (doseq [kind [:kv :datalog]
          owns-client? [true false]]
    (testing (str (name kind) ", owns client " owns-client?)
      (let [before (channels)
            uri (str root-uri "/transaction-open-" (name kind))
            transport (client/new-client uri client-opts)
            pool (client/get-pool transport)
            ;; The existing pool can open the DB, but the dedicated client's
            ;; fresh authentication fails with these credentials.
            c (client/->Client "datalevin" "incorrect-password" "localhost"
                               client/*default-port* 1 3000
                               (client/get-id transport) pool)]
        (try
          (let [opened (channels)
                failure (outcome #(open-with-client kind c uri owns-client?))]
            (is (instance? clojure.lang.ExceptionInfo failure))
            (is (re-find #"Authentication failure" (ex-message failure)))
            (is (= owns-client? (client/closed-pool? pool)))
            (is (= (if owns-client? before opened) (channels))))
          (finally (client/disconnect c) (client/disconnect transport)))))))

(deftest store-construction-failure-releases-both-acquired-clients
  (doseq [owns-client? [true false]]
    (let [before (channels)
          uri (str root-uri "/store-construction")
          c (client/new-client uri client-opts)
          tx-client (atom nil)
          failure (AssertionError. "Store construction failed")]
      (try
        (let [opened (channels)]
          (is (identical?
                failure
                (outcome
                  #(#'remote/open-remote-store!
                    uri c opts owns-client?
                    (fn [c db-name _query]
                      (client/open-database c db-name "kv"))
                    (fn [_c tx _db-name _query _db-info]
                      (reset! tx-client tx)
                      (throw failure))))))
          (is (some? @tx-client))
          (is (client/disconnected? @tx-client))
          (is (client/closed-pool? (client/get-pool @tx-client)))
          (is (= owns-client? (client/disconnected? c)))
          (is (= (if owns-client? before opened) (channels))))
        (finally
          (when @tx-client (client/disconnect @tx-client))
          (client/disconnect c))))))

(deftest failed-client-configuration-releases-its-new-pool
  (doseq [pool-size [1 3]]
    (let [before (channels)
          failure (outcome #(client/new-client
                             root-uri (assoc client-opts
                                             :pool-size pool-size
                                             :ha-write-retry-timeout-ms "invalid")))]
      (is (instance? ClassCastException failure))
      (is (= before (channels))))))

(deftest failed-open-preserves-error-when-client-cleanup-fails
  (doseq [failure [(ex-info "Database open failed" {})
                   (AssertionError. "Database open failed")]]
    (let [cleanup-error (ex-info "Client cleanup failed" {})
          attempts (atom 0)
          c (reify client/IClient
              (disconnected? [_] false)
              (disconnect [_]
                (swap! attempts inc)
                (throw cleanup-error)))]
      (is (identical? failure
                      (outcome #(#'remote/open-remote-store!
                                 (str root-uri "/failure") c opts true
                                 (fn [& _] (throw failure))
                                 (fn [& _] (is false "must not construct a store"))))))
      (is (= [cleanup-error] (vec (.getSuppressed ^Throwable failure))))
      (is (= 1 @attempts)))))
