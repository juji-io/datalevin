(ns datalevin.transaction-function-view-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.core :as d]
            [datalevin.interface :as i]
            [datalevin.interpret :as inter]
            [datalevin.kv :as kv]
            [datalevin.lmdb :as l]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.batch.rmw :as rmw]
            [datalevin.tx-group.phase :as phase]
            [datalevin.util :as u]))

(defn- collected! [conn ops]
  (let [collector (get-in @(i/kv-info (d/datalog-kv conn))
                          [:independent-control :collector])
        queue (.-ready ^datalevin.tx_group.batch.Collector collector)
        entered (promise) release (promise) jobs (atom [])
        unobserve (phase/observe!
                    (fn [event b]
                      (when (and (= :batch-sealed event)
                                 (identical? collector (batch/batch-collector b))
                                 (not (realized? entered)))
                        (deliver entered true)
                        (assert (deref release 10000 false)))))]
    (try
      (doseq [[idx op] (map-indexed vector ops)]
        (swap! jobs conj (future (try (op) (catch Throwable t t))))
        (when (zero? (long idx)) (is (deref entered 10000 false)))
        (is (loop [attempt 0]
              (cond (= idx (.size ^java.util.Queue queue)) true
                    (= attempt 1000) false
                    :else (do (Thread/sleep 5) (recur (inc attempt)))))))
      (deliver release true)
      (mapv #(deref % 10000 ::timeout) @jobs)
      (finally
        (deliver release true)
        (doseq [job @jobs] (deref job 10000 nil))
        (unobserve)))))

(defn- error-code [f]
  (try (f) nil (catch Exception e (:error (ex-data e)))))

(deftest transaction-functions-cannot-revive-preceding-request-handles
  (doseq [reuse? [true false]
          boundary [:tx-fn :ensure]]
    (testing (str boundary ", resolver reuse " reuse?)
      (let [dir (u/tmp-dir (str "transaction-function-view-" (random-uuid)))
            opts {:wal? true :wal-durability-profile :strict
                  :wal-segment-prealloc? false :snapshot-scheduler? false
                  :background-sampling? false}
            conn (d/create-conn dir {:value {:db/valueType :db.type/long}} opts)
            lmdb (d/datalog-kv conn)
            escaped (atom [])
            tx-fn (fn [db before after]
                    (is (= before (:value (d/entity db 1))))
                    (let [view (d/datalog-kv db)]
                      (doseq [old @escaped]
                        (is (not (identical? (kv/raw-lmdb old) (kv/raw-lmdb view))))
                        (doseq [op [#(d/transact-kv old "probe" [[:put 99 99]] :long :long)
                                    #(d/get-value old "probe" 2 :long :long)
                                    #(l/write-txn old)
                                    #(d/with-transaction-kv [_ old] :revived)
                                    #(i/abort-transact-kv (kv/raw-lmdb old))]]
                          (is (= :txlog/transaction-view-invalidated (error-code op)))))
                      (swap! escaped conj view)
                      (d/with-transaction-kv [nested view]
                        (d/transact-kv nested "probe" [[:put after after]] :long :long))
                      (is (= after (d/get-value view "probe" after :long :long))))
                    [[:db/add 1 :value after]])
            request (fn [before after]
                      (d/transact!
                        conn
                        (if (= boundary :tx-fn)
                          [[:db.fn/call tx-fn before after]]
                          [[:db/add 1 :value after]
                           [:db/ensure (fn [db n] (tx-fn db n n) true) after]])))]
        (try
          (d/open-dbi lmdb "probe")
          (d/transact! conn [{:db/id 1 :value 0}])
          (binding [rmw/*reuse-datalog-member-setup?* reuse?]
            (let [results (collected!
                            conn
                            [#(d/transact! conn [[:db/add 1 :value 1]])
                             #(request 1 2)
                             #(request 2 3)
                             #(d/transact! conn [[:db.fn/cas 1 :value 3 4]])
                             #(request 4 5)
                             #(d/transact! conn [{:db/id 1 :value 6}])])]
              (is (every? map? results) (pr-str results))))
          (is (= 3 (count @escaped)))
          (is (= [6] (mapv :v (d/datoms @conn :eav 1 :value))))
          (is (nil? (d/get-value lmdb "probe" 99 :long :long)))
          (doseq [n [2 3 5]] (is (= n (d/get-value lmdb "probe" n :long :long))))
          (doseq [view @escaped]
            (is (= :txlog/transaction-view-invalidated
                   (error-code #(d/transact-kv view "probe" [[:put 99 99]] :long :long)))))
          (d/close conn)
          (let [reopened (d/create-conn dir nil opts)]
            (try
              (is (= [6] (mapv :v (d/datoms @reopened :eav 1 :value))))
              (is (nil? (d/get-value (d/datalog-kv reopened) "probe" 99 :long :long)))
              (doseq [n [2 3 5]]
                (is (= n (d/get-value (d/datalog-kv reopened) "probe" n :long :long))))
              (finally (d/close reopened))))
          (finally (d/close conn) (u/delete-files dir)))))))

(deftest pending-custom-values-survive-retiring-a-resolver-view
  (let [dir (u/tmp-dir (str "transaction-function-custom-view-" (random-uuid)))
        conn (d/create-conn dir nil
                            {:wal? true :wal-durability-profile :strict
                             :wal-segment-prealloc? false :snapshot-scheduler? false
                             :background-sampling? false})]
    (try
      (d/register-type conn :app/value
                       {:index {:type :long :order-fn (inter/inter-fn [v] (:rank v))}})
      (d/update-schema conn {:value {:db/valueType :app/value
                                     :db/cardinality :db.cardinality/many}})
      (d/transact! conn [{:db/id 1 :value [{:rank 0}]}])
      (let [tx-fn (fn [_ n] [[:db/add 1 :value {:rank n}]])
            results (collected! conn
                                [#(d/transact! conn [[:db.fn/call tx-fn 1]])
                                 #(d/transact! conn [[:db.fn/call tx-fn 2]])])]
        (is (every? map? results)
            (pr-str (mapv #(when (instance? Throwable %) (ex-data %)) results)))
        (is (= #{{:rank 0} {:rank 1} {:rank 2}}
               (set (map :v (d/datoms @conn :eav 1 :value))))))
      (finally (d/close conn) (u/delete-files dir)))))
