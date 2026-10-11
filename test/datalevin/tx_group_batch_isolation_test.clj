(ns datalevin.tx-group-batch-isolation-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.core :as d]
            [datalevin.interface :as i]
            [datalevin.storage :as s]
            [datalevin.test.core :refer [db-fixture]]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.phase :as phase]
            [datalevin.util :as u])
  (:import [datalevin.tx_group.batch Collector]
           [java.util.concurrent ConcurrentLinkedQueue]))

(use-fixtures :each db-fixture)

(def ^:private options
  {:wal? true :wal-durability-profile :strict :write-batch-size 8
   :snapshot-scheduler? false :wal-segment-prealloc? false})

(defn- await-queued! [^Collector collector n]
  (let [^ConcurrentLinkedQueue queue (.-ready collector)]
    (is (loop [attempt 0]
          (cond
            (= n (.size queue)) true
            (= 2000 attempt) false
            :else (do (Thread/sleep 5) (recur (inc attempt))))))))

(defn- change-while-queued! [conn boundary ops]
  (let [lmdb (d/datalog-kv conn)
        collector (get-in @(i/kv-info lmdb) [:independent-control :collector])
        entered (promise) release (promise) jobs (atom [])
        unobserve (phase/observe!
                    (fn [event context]
                      (when (and (= event boundary)
                                 (identical? collector (batch/batch-collector context))
                                 (not (realized? entered)))
                        (deliver entered true)
                        (assert (deref release 10000 false)))))]
    (try
      ;; At publication, the seed has committed and the tested requests form a
      ;; new batch. At sealing, the seed remains the batch's initial KV member;
      ;; Datalog requests join it later during native preparation.
      (swap! jobs conj (future (try
                                (if (= boundary :batch-sealed)
                                  (d/transact-kv lmdb "control" [[:put 1 1]] :long :long)
                                  (batch/submit! collector {:op (fn [_] :seed)}))
                                (catch Throwable t t))))
      (is (deref entered 10000 false))
      (doseq [[idx op] (map-indexed vector ops)]
        (swap! jobs conj (future (try (op) (catch Throwable t t))))
        (await-queued! collector (inc idx)))
      (i/assoc-opt (:store @conn) :search-opts {:indexing-mode :sync})
      (let [before-lsn (:last-committed-lsn (d/txlog-watermarks lmdb))]
        (deliver release true)
        {:before-lsn before-lsn
         :seed (deref (first @jobs) 10000 ::timeout)
         :requests (mapv #(deref % 10000 ::timeout) (rest @jobs))})
      (finally
        (deliver release true)
        (doseq [job @jobs] (deref job 10000 nil))
        (unobserve)))))

(defn- assert-indexing-change! [result]
  (is (instance? clojure.lang.ExceptionInfo result))
  (when (instance? clojure.lang.ExceptionInfo result)
    (is (= :txlog/indexing-mode-changed (:error (ex-data result))))
    (is (= :not-committed (:outcome (ex-data result))))
    (is (true? (:retryable? (ex-data result))))))

(defn- collected! [conn ops]
  (let [collector (get-in @(i/kv-info (d/datalog-kv conn)) [:independent-control :collector])
        entered (promise) release (promise) jobs (atom [])
        unobserve (phase/observe!
                    (fn [event b]
                      (when (and (= :joint-publication event)
                                 (identical? collector (batch/batch-collector b))
                                 (not (realized? entered)))
                        (deliver entered true)
                        (assert (deref release 10000 false)))))]
    (try
      ;; Pause the preceding seed after commit so all tested requests are
      ;; collected into the next batch before any of their bodies run.
      (swap! jobs conj (future (try
                                (batch/submit! collector {:op (fn [_] :seed)})
                                (catch Throwable t t))))
      (is (deref entered 10000 false))
      (doseq [[idx op] (map-indexed vector ops)]
        (swap! jobs conj (future (try (op) (catch Throwable t t))))
        (await-queued! collector (inc idx)))
      (let [before-lsn (:last-committed-lsn (d/txlog-watermarks (d/datalog-kv conn)))]
        (deliver release true)
        (is (= :seed (deref (first @jobs) 10000 ::timeout)))
        {:before-lsn before-lsn
         :results (mapv #(deref % 10000 ::timeout) (rest @jobs))})
      (finally
        (deliver release true)
        (doseq [job @jobs] (deref job 10000 nil))
        (unobserve)))))

(deftest indexing-mode-changes-in-bodies-validate-before-secondary-mutation
  (doseq [members [1 2]
          explicit? [false true]
          update! [#(i/assoc-opt % :search-opts {:indexing-mode :sync})
                   #(i/assoc-opts % {:search-opts {:indexing-mode :sync}})]]
    (testing (str members " members, explicit transaction " explicit?)
      (let [dir (u/tmp-dir (str "batch-body-indexing-mode-" (random-uuid)))
            conn (d/create-conn dir {:text {:db/valueType :db.type/string
                                           :db/fulltext true}}
                                (assoc options :search-opts {:indexing-mode :async}))
            calls (atom [])]
        (try
          (d/transact! conn [{:db/id 1 :text "hello"}])
          (d/wait-for-secondary-index conn {:timeout-ms 10000})
          (is (= 1 (count (d/fulltext-datoms @conn "hello"))))
          (let [before @conn
                change! (fn [db]
                          (swap! calls conj :first)
                          (update! (:store db))
                          [[:db/add 1 :text "goodbye"]])
                {:keys [before-lsn results]}
                (collected!
                  conn
                  (cond->
                    [#(if explicit?
                        (d/with-transaction [tx conn]
                          (d/transact! tx (change! @tx)))
                        (d/transact! conn [[:db.fn/call change!]]))]
                    (= members 2)
                    (conj #(d/with-transaction [tx conn]
                             (swap! calls conj :second)
                             (d/transact! tx [{:db/id 2 :text "second"}])))))]
            ;; Even a lone async-admitted member must reject synchronous work:
            ;; the collector can admit later requests during preparation.
            (doseq [result results] (assert-indexing-change! result))
            (is (= [:first] @calls))
            (is (identical? before @conn))
            (is (= before-lsn
                   (:last-committed-lsn (d/txlog-watermarks (d/datalog-kv conn)))))
            (is (= "hello" (:text (d/entity @conn 1))))
            (is (= 1 (count (d/fulltext-datoms @conn "hello"))))
            (is (empty? (d/fulltext-datoms @conn "goodbye")))
            (is (nil? (:text (d/entity @conn 2)))))
          ;; A sync-admitted, isolated body may still change options and write.
          (update! (:store @conn))
          (d/with-transaction [tx conn]
            (i/assoc-opt (:store @tx) :search-opts {:indexing-mode :async})
            (update! (:store @tx))
            (d/transact! tx [{:db/id 2 :text "later"}]))
          (is (= 1 (count (d/fulltext-datoms @conn "later"))))
          (d/close conn)
          (let [reopened (d/create-conn dir)]
            (try
              (is (= "hello" (:text (d/entity @reopened 1))))
              (is (= 1 (count (d/fulltext-datoms @reopened "hello"))))
              (is (empty? (d/fulltext-datoms @reopened "goodbye")))
              (is (= 1 (count (d/fulltext-datoms @reopened "later"))))
              (finally (d/close reopened))))
          (finally (d/close conn) (u/delete-files dir)))))))

(defn- change-during-resolution! [conn update! ops]
  (let [collector (get-in @(i/kv-info (d/datalog-kv conn)) [:independent-control :collector])
        entered (promise) release (promise) jobs (atom [])
        changed? (atom false) change-job (atom nil)
        unobserve (phase/observe!
                    (fn [event b]
                      (when (and (#{:batch-sealed :resolution-start} event)
                                 (identical? collector (batch/batch-collector b)))
                        (case event
                          :batch-sealed
                          (when-not (realized? entered)
                            (deliver entered true)
                            (assert (deref release 10000 false)))
                          :resolution-start
                          (when (compare-and-set! changed? false true)
                            (let [store (:store @conn)]
                              (reset! change-job (future (update! store)))
                              ;; Option publication precedes persistence, whose
                              ;; writer lock is held by this resolving batch.
                              (is (loop [attempt 0]
                                    (cond
                                      (s/synchronous-secondary-indexing? store) true
                                      (= attempt 2000) false
                                      :else (do (Thread/sleep 5) (recur (inc attempt))))))
                              (is (not (realized? @change-job)))))
                          nil))))]
    (try
      (doseq [[idx op] (map-indexed vector ops)]
        (swap! jobs conj (future (try (op) (catch Throwable t t))))
        (if (zero? (long idx))
          (is (deref entered 10000 false))
          (await-queued! collector idx)))
      (deliver release true)
      (let [results (mapv #(deref % 10000 ::timeout) @jobs)]
        (is @changed?)
        (when-let [job @change-job] (is (not= ::timeout (deref job 10000 ::timeout))))
        results)
      (finally
        (deliver release true)
        (doseq [job @jobs] (deref job 10000 nil))
        (when-let [job @change-job] (deref job 10000 nil))
        (unobserve)))))

(deftest writing-store-validates-the-options-copied-after-queue-checks
  (doseq [explicit? [false true]
          update! [#(i/assoc-opt % :search-opts {:indexing-mode :sync})
                   #(i/assoc-opts % {:search-opts {:indexing-mode :sync}})]]
    (let [dir (u/tmp-dir (str "batch-option-publication-" (random-uuid)))
          conn (d/create-conn dir {:text {:db/valueType :db.type/string
                                         :db/fulltext true}}
                              (assoc options :search-opts {:indexing-mode :async}))
          calls (atom [])]
      (try
        (d/transact! conn [{:db/id 1 :text "hello"}])
        (d/wait-for-secondary-index conn {:timeout-ms 10000})
        (is (= 1 (count (d/fulltext-datoms @conn "hello"))))
        (let [before @conn
              results (change-during-resolution!
                        conn update!
                        [#(if explicit?
                            (d/with-transaction [tx conn]
                              (swap! calls conj :first)
                              (d/transact! tx [{:db/id 1 :text "goodbye"}]))
                            (d/transact! conn [{:db/id 1 :text "goodbye"}]))
                         #(d/with-transaction [_ conn]
                            (swap! calls conj :failure)
                            (throw (ex-info "reject second" {})))])]
          (doseq [result results] (assert-indexing-change! result))
          (is (empty? @calls))
          (is (identical? before @conn)))
        (is (= "hello" (:text (d/entity @conn 1))))
        (is (= 1 (count (d/fulltext-datoms @conn "hello"))))
        (is (empty? (d/fulltext-datoms @conn "goodbye")))
        ;; Newly admitted sync writes still commit after the clean rejection.
        (d/transact! conn [{:db/id 2 :text "later"}])
        (is (= 1 (count (d/fulltext-datoms @conn "later"))))
        (d/close conn)
        (let [reopened (d/create-conn dir)]
          (try
            (is (= "hello" (:text (d/entity @reopened 1))))
            (is (= 1 (count (d/fulltext-datoms @reopened "hello"))))
            (is (empty? (d/fulltext-datoms @reopened "goodbye")))
            (finally (d/close reopened))))
        (finally (d/close conn) (u/delete-files dir))))))

(deftest indexing-mode-is-rechecked-before-sealed-and-collected-bodies
  (doseq [boundary [:joint-publication :batch-sealed]
          explicit? [false true]]
    (testing (str boundary ", explicit transaction " explicit?)
      (let [dir (u/tmp-dir (str "batch-isolation-" (random-uuid)))
            conn (d/create-conn dir {:text {:db/valueType :db.type/string
                                           :db/fulltext true}} options)
            lmdb (d/datalog-kv conn)
            calls (atom [])
            failure (ex-info "later request failed" {})]
        (try
          (d/open-dbi lmdb "control")
          (d/transact! conn [{:db/id 1 :text "hello"}])
          (is (= 1 (count (d/fulltext-datoms @conn "hello"))))
          (i/assoc-opt (:store @conn) :search-opts {:indexing-mode :async})
          (let [before @conn
                {:keys [before-lsn seed requests]}
                (change-while-queued!
                  conn boundary
                  [#(if explicit?
                      (d/with-transaction [tx conn]
                        (swap! calls conj :first)
                        (d/transact! tx [[:db/retract 1 :text "hello"]]))
                      (d/transact! conn [[:db/retract 1 :text "hello"]]))
                   #(d/with-transaction [_ conn]
                      (swap! calls conj :failure)
                      (throw failure))])]
            (doseq [result requests] (assert-indexing-change! result))
            (if (= boundary :batch-sealed)
              (assert-indexing-change! seed)
              (is (= :seed seed)))
            (is (empty? @calls))
            (is (identical? before @conn))
            (is (= "hello" (:text (d/entity @conn 1))))
            (is (= 1 (count (d/fulltext-datoms @conn "hello"))))
            (is (zero? (d/entries lmdb "control")))
            (is (= before-lsn (:last-committed-lsn (d/txlog-watermarks lmdb))))
            (is (batch/serving?
                  (get-in @(i/kv-info lmdb) [:independent-control :collector]))))
          ;; A fresh request sees sync mode at admission and commits alone.
          (d/transact! conn [[:db/retract 1 :text "hello"]])
          (is (nil? (:text (d/entity @conn 1))))
          (is (empty? (d/fulltext-datoms @conn "hello")))
          (finally (d/close conn) (u/delete-files dir)))))))
