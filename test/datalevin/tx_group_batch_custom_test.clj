(ns datalevin.tx-group-batch-custom-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [datalevin.core :as d]
            [datalevin.interface :as i]
            [datalevin.kv :as kv]
            [datalevin.lmdb :as l]
            [datalevin.test.core :refer [db-fixture]]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.phase :as phase]
            [datalevin.udf :as udf]
            [datalevin.util :as u]))

(use-fixtures :each db-fixture)

(def ^:private options
  {:wal? true :wal-durability-profile :strict :wal-segment-prealloc? false
   :snapshot-scheduler? false :background-sampling? false})

(defn- with-custom-conn [resolver f]
  (let [dir (u/tmp-dir (str "batch-custom-capture-" (random-uuid)))
        runtime (udf/create-registry)
        conn (d/create-conn (str dir "/source") nil
                            (assoc options :runtime-opts {:udf-registry runtime}))
        native (d/datalog-kv conn)
        descriptor {:udf/lang :test :udf/kind :order-fn :udf/id :test/rank}]
    (try
      (d/open-dbi native "audit")
      (udf/register-resolver! runtime :test resolver)
      (d/register-type conn :test/value {:index {:type :long :order-fn descriptor}})
      (d/update-schema conn {:value {:db/valueType :test/value
                                    :db/cardinality :db.cardinality/many}})
      (f conn native dir)
      (finally (d/close conn) (u/delete-files dir)))))

(defn- audit-resolver [{:keys [kv]} _]
  (fn [v]
    (when (l/writing? kv)
      (d/transact-kv kv "audit" [[:put (:rank v) "seen"]] :long :string))
    (:rank v)))

(defn- forms [ranks]
  (mapv #(vector :db/add 1 :value {:rank %}) ranks))

(defn- replay-audit [native dir]
  (let [replay (d/open-kv (str dir "/replay") {:wal? false})]
    (try
      (doseq [{:keys [rows lsn]} (kv/open-tx-log-rows native 0)]
        (kv/replay-txlog-rows! replay rows lsn))
      (d/get-range replay "audit" [:all] :long :string)
      (finally (d/close-kv replay)))))

(defn- collected! [conn ops]
  (let [collector (get-in @(i/kv-info (d/datalog-kv conn))
                          [:independent-control :collector])
        queue (.-ready ^datalevin.tx_group.batch.Collector collector)
        entered (promise) release (promise) jobs (atom [])
        batches (atom [])
        unobserve (phase/observe!
                    (fn [event b]
                      (when (and (= event :batch-sealed)
                                 (identical? collector (batch/batch-collector b))
                                 (not (realized? entered)))
                        (deliver entered true)
                        (assert (deref release 10000 false)))
                      (when (and (= event :resolution-start)
                                 (identical? collector (batch/batch-collector b)))
                        (swap! batches conj (batch/batch-count b)))))]
    (try
      (doseq [[idx op] (map-indexed vector ops)]
        (swap! jobs conj (future (try (op) (catch Throwable t t))))
        (when (zero? (long idx)) (is (deref entered 10000 false))))
      (is (loop [attempt 0]
            (cond
              (= (dec (count ops)) (.size ^java.util.Queue queue)) true
              (= attempt 2000) false
              :else (do (Thread/sleep 5) (recur (inc attempt))))))
      (deliver release true)
      (let [results (mapv #(deref % 10000 ::timeout) @jobs)]
        (is (some #(= (count ops) %) @batches))
        results)
      (finally
        (deliver release true)
        (doseq [job @jobs] (deref job 10000 nil))
        (unobserve)))))

(deftest custom-order-callback-writes-are-captured-for-replay
  (doseq [explicit? [false true]]
    (with-custom-conn audit-resolver
      (fn [conn native dir]
        (is (map? (if explicit?
                    (d/with-transaction [tx conn] (d/transact! tx (forms [1 2])))
                    (d/transact! conn (forms [1 2])))))
        (is (= [[1 "seen"] [2 "seen"]]
               (d/get-range native "audit" [:all] :long :string)
               (replay-audit native dir)))))))

(deftest custom-order-resolvers-follow-the-active-batch-member
  (let [handles (atom []) errors (atom [])]
    (with-custom-conn
      (fn [{:keys [kv] :as context} descriptor]
        (when (l/writing? kv) (swap! handles conj kv))
        (let [order (audit-resolver context descriptor)]
          (fn [v]
            (when (and (l/writing? kv) (= 3 (:rank v)))
              (try
                (d/transact-kv (first @handles) "audit" [[:put 99 "expired"]]
                               :long :string)
                (swap! errors conj :unexpected-success)
                (catch Exception e (swap! errors conj (:error (ex-data e))))))
            (order v))))
      (fn [conn native dir]
        (let [reports (collected! conn [#(d/transact! conn (forms [1 2]))
                                       #(d/transact! conn (forms [3 4]))
                                       #(d/transact! conn (forms [5 6]))])]
          (is (every? map? reports) (pr-str reports)))
        (is (seq @errors))
        (is (every? #{:txlog/transaction-view-invalidated} @errors))
        (is (<= 3 (count (distinct @handles))))
        (doseq [handle @handles]
          (is (thrown? clojure.lang.ExceptionInfo
                       (d/transact-kv handle "audit" [[:put 99 "expired"]]
                                      :long :string))))
        (is (= (mapv #(vector % "seen") (range 1 7))
               (d/get-range native "audit" [:all] :long :string)
               (replay-audit native dir)))))))

(deftest custom-order-callback-writes-roll-back-with-the-batch
  (with-custom-conn audit-resolver
    (fn [conn native dir]
      (let [lsn (:last-committed-lsn (d/txlog-watermarks native))
            results (collected! conn [#(d/transact! conn (forms [1 2]))
                                     #(d/with-transaction [_ conn]
                                        (throw (ex-info "reject batch" {})))])]
        (is (every? #(instance? Throwable %) results))
        (is (= lsn (:last-committed-lsn (d/txlog-watermarks native)))))
      (is (empty? (d/get-range native "audit" [:all] :long :string)))
      (is (empty? (replay-audit native dir)))
      (is (empty? (d/datoms @conn :eav 1 :value))))))
