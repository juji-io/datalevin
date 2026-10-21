(ns datalevin.server.transaction-sampling-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.async :as a]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.interface :as i]
            [datalevin.lmdb :as l]
            [datalevin.server :as server]
            [datalevin.storage :as st]
            [datalevin.test.core :refer [allocate-port db-fixture]]
            [datalevin.util :as u])
  (:import [datalevin.server Server]
           [datalevin.storage Store]))

(use-fixtures :once db-fixture)

(deftest sampling-cannot-replace-a-remote-transaction
  (let [root (u/tmp-dir (str "transaction-sampling-" (random-uuid)))
        port (allocate-port)
        ^Server srv (server/create {:root root :port port})
        uri (str "dtlv://datalevin:datalevin@localhost:" port "/sessions")]
    (try
      (server/start srv)
      (let [conn (d/create-conn uri {:session/id {:db/unique :db.unique/identity}
                                   :session/revision {:db/valueType :db.type/long}}
                                {:auto-entity-time? true})]
        (try
          (d/transact! conn [{:session/id "browser" :session/revision 0}])
          (doseq [abort? [false true]]
            (testing (if abort? "rollback" "commit")
              (d/with-transaction [tx conn]
                (let [^Store store (:store (get (.-dbs srv) "sessions"))
                      lmdb (.-lmdb store)
                      writer @(l/write-txn lmdb)
                      sampled (future (i/analyze store :session/revision))]
                  (is (not= ::timeout (deref sampled 5000 ::timeout))
                      "advisory sampling finishes while the writer is open")
                  (is (identical? writer @(l/write-txn lmdb)))
                  (is (= "browser" (:session/id (d/pull @tx '[*] [:session/id "browser"]))))
                  (let [foreign (future
                                  (try
                                    (i/transact-kv lmdb c/meta [[:put :foreign :value]])
                                    :written
                                    (catch Exception e e)))
                        result (deref foreign 5000 ::timeout)]
                    (is (instance? Exception result)
                        "a foreign writer cannot join the native transaction"))
                  (d/transact! tx [{:session/id "browser" :session/revision (if abort? 2 1)}])
                  (is (= (if abort? 2 1)
                         (:session/revision (d/pull @tx '[*] [:session/id "browser"]))))
                  (when abort? (d/abort-transact tx))))
              (is (nil? (:runner (get (.-dbs srv) "sessions"))))
              (is (nil? @(l/write-txn (.-lmdb ^Store (:store (get (.-dbs srv) "sessions"))))))
              ;; A subsequent transaction must be admitted without restarting.
              (d/with-transaction [tx conn]
                (is (= 1 (:session/revision (d/pull @tx '[*] [:session/id "browser"])))))))
          (finally (d/close conn))))
      (finally
        (server/stop srv)
        (u/delete-files root)))))

(deftest concurrent-session-requests-with-background-sampling
  (let [root (u/tmp-dir (str "concurrent-sessions-" (random-uuid)))
        port (allocate-port)
        ^Server srv (server/create {:root root :port port})
        uri (str "dtlv://datalevin:datalevin@localhost:" port "/sessions")
        connections (atom [])
        schema {:account/id {:db/unique :db.unique/identity}
                :session/id {:db/unique :db.unique/identity}
                :session/revision {:db/valueType :db.type/long}
                :session/clock-ms {:db/valueType :db.type/long}
                :session/account {:db/valueType :db.type/ref}
                :session/document {:db/valueType :db.type/idoc}}
        document {:csrf "browser-token" :identity {:user 101 :roles #{:visitor}}}]
    (try
      (server/start srv)
      (dotimes [_ 4]
        (swap! connections conj
               (d/create-conn uri schema {:auto-entity-time? true})))
      (d/transact! (first @connections)
                   [{:account/id 101}
                    {:session/id "browser" :session/revision 0
                     :session/document document :session/account [:account/id 101]}])
      (let [^Store store (:store (get (.-dbs srv) "sessions"))
            executor (a/get-executor)
            ready (promise)
            requests
            (mapv (fn [conn]
                    (future
                      @ready
                      (dotimes [_ 12]
                        (d/with-transaction [tx conn]
                          (let [row (d/pull @tx '[*] [:session/id "browser"])
                                revision (inc (:session/revision row))]
                            (assert (= document (:session/document row)))
                            (d/transact! tx [{:db/id (:db/id row)
                                             :session/revision revision}])
                            ;; Submit the same work the production scheduler
                            ;; uses while this remote transaction owns the writer.
                            (let [sample (a/exec executor (st/->SamplingWork store executor))]
                              (assert (not= ::timeout (deref sample 5000 ::timeout))))
                            (let [updated (d/pull @tx '[*] [:session/id "browser"])]
                              (assert (= revision (:session/revision updated)))
                              (d/transact! tx [{:db/id (:db/id row)
                                               :session/clock-ms (:db/updated-at updated)}])))))
                      :completed))
                  @connections)]
        (is (true? (:background-sampling? (i/opts store))))
        (is (some? @(.-scheduled-sampling store)))
        (deliver ready true)
        (is (= (vec (repeat 4 :completed))
               (mapv #(deref % 30000 ::timeout) requests)))
        (is (= 48 (:session/revision
                   (d/pull @(first @connections) '[*] [:session/id "browser"])))))
      (doseq [conn @connections] (d/close conn))
      (reset! connections [])
      ;; A replacement application connection reads the committed session and
      ;; can write again against the same running server.
      (let [replacement (d/create-conn uri)]
        (try
          (d/with-transaction [tx replacement]
            (let [row (d/pull @tx '[*] [:session/id "browser"])]
              (is (= 48 (:session/revision row)))
              (is (= document (:session/document row)))
              (d/transact! tx [{:db/id (:db/id row) :session/revision 49}])))
          (is (= 49 (:session/revision
                     (d/pull @replacement '[*] [:session/id "browser"]))))
          (finally (d/close replacement))))
      (finally
        (doseq [conn @connections] (d/close conn))
        (server/stop srv)
        (u/delete-files root)))))
