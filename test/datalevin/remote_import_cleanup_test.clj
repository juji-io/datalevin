(ns datalevin.remote-import-cleanup-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.client :as client]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.server :as server]
            [datalevin.test.core :refer [allocate-port db-fixture]]
            [datalevin.util :as u])
  (:import [datalevin.server Server]))

(use-fixtures :each db-fixture)

(defn- with-import-server
  [f]
  (let [root (u/tmp-dir (str "import-cleanup-" (random-uuid)))
        port (allocate-port)
        srv (server/create {:root root :port port})]
    (binding [client/*default-port* port]
      (try
        (server/start srv)
        (f srv)
        (finally (server/stop srv) (u/delete-files root))))))

(defn- session-ids
  [^Server srv]
  (set (keys (.-clients srv))))

(defn- await-session-ids
  [srv expected]
  ;; Disconnect is sent without a response, so allow the server to process it.
  (let [deadline (+ (System/currentTimeMillis) 5000)]
    (loop []
      (cond
        (= expected (session-ids srv)) true
        (>= (System/currentTimeMillis) deadline) false
        :else (do (Thread/sleep 5) (recur))))))

(def ^:private schema
  {:item/value {:db/valueType :db.type/long}})

(def ^:private failing-imports
  [[:request 1 [(d/datom 100 :item/value 100)
                (d/datom 101 :item/value "bad")]]
   [:copy-in c/+wire-datom-batch-size+
    (conj (mapv #(d/datom (+ 100 %) :item/value %)
                (range c/+wire-datom-batch-size+))
          (d/datom (+ 100 c/+wire-datom-batch-size+) :item/value "bad"))]
   [:invalid-batch 0 []]])

(deftest failed-imports-release-owned-client-sessions
  (with-import-server
    (fn [srv]
      (doseq [wal? [false true] pool-size [1 3]]
        (let [uri (str "dtlv://datalevin:datalevin@localhost/failures-" wal? "-" pool-size)
              opts {:wal? wal? :validate-data? true
                    :client-opts {:pool-size pool-size :time-out 5000}}
              conn (d/create-conn uri schema opts)]
          (try
            (d/transact! conn [{:db/id 1 :item/value 1}])
            (let [baseline (session-ids srv)]
              (doseq [[mode batch-size datoms] failing-imports]
                (testing (str mode ", WAL " wal? ", pool size " pool-size)
                  (dotimes [_ 3]
                    (let [failure (try
                                    (binding [c/*fill-db-batch-size* batch-size]
                                      (d/init-db datoms uri nil opts))
                                    (catch Exception e e))]
                      (is (instance? clojure.lang.ExceptionInfo failure))
                      (is (re-find (if (= mode :invalid-batch)
                                     #"Invalid fill-db batch size" #"Invalid data")
                                   (or (ex-message failure) "")))))
                  (is (await-session-ids srv baseline))
                  (is (= baseline (session-ids srv)))
                  (is (= #{[1]} (d/q '[:find ?e :where [?e :item/value]] @conn)))))
              (testing "the caller's existing connection remains usable"
                (is (= 2 (get (:tempids (d/transact! conn [{:db/id -1 :item/value 2}]))
                              -1)))))
            (finally (d/close conn))))))))

(deftest successful-import-retains-sessions-until-the-returned-db-closes
  (with-import-server
    (fn [srv]
      (doseq [pool-size [1 3]]
        (let [uri (str "dtlv://datalevin:datalevin@localhost/success-" pool-size)
              opts {:client-opts {:pool-size pool-size :time-out 5000}}
              conn (d/create-conn uri schema opts)]
          (try
            (let [baseline (session-ids srv)
                  imported (d/init-db [(d/datom 100 :item/value 100)] uri nil opts)]
              (try
                (is (= (+ (count baseline) 2)
                       (count (session-ids srv))))
                (is (= 100 (d/q '[:find ?v . :where [100 :item/value ?v]] imported)))
                (is (= 100 (d/q '[:find ?v . :where [100 :item/value ?v]] @conn)))
                (finally (d/close-db imported)))
              (is (await-session-ids srv baseline))
              (is (= baseline (session-ids srv)))
              (is (= 101 (get (:tempids (d/transact! conn [{:db/id -1 :item/value 101}]))
                              -1))))
            (finally (d/close conn))))))))
