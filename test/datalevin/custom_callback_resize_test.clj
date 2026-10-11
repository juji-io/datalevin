(ns datalevin.custom-callback-resize-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.bits :as b]
            [datalevin.core :as d]
            [datalevin.kv :as kv]
            [datalevin.lmdb :as l]
            [datalevin.test.core :refer [db-fixture]]
            [datalevin.tx-group.phase :as phase]
            [datalevin.udf :as udf]
            [datalevin.util :as u])
  (:import [java.util Arrays]))

(use-fixtures :each db-fixture)

(deftest serializer-map-growth-preserves-the-storage-writer
  (doseq [wal? [true false]
          explicit? [false true]]
    (testing (str "WAL " wal? ", explicit transaction " explicit?)
      (let [dir (u/tmp-dir (str "custom-callback-resize-" (random-uuid)))
            registry (udf/create-registry)
            opts {:wal? wal? :wal-durability-profile :strict
                  :kv-opts {:mapsize 1} :wal-segment-prealloc? false
                  :snapshot-scheduler? false :background-sampling? false
                  :runtime-opts {:udf-registry registry}}
            conn (d/create-conn (str dir "/source") nil opts)
            native (d/datalog-kv conn)
            descriptor (fn [kind] {:udf/lang :test :udf/kind kind
                                   :udf/id :test/value})
            payload (byte-array (* 2 1024 1024) (byte 42))
            handles (atom []) calls (atom 0) resizes (atom 0)
            unobserve (phase/observe!
                        (fn [event _]
                          (when (= :native-resized event) (swap! resizes inc))))]
        (try
          (d/open-dbi native "audit")
          (udf/register-resolver!
            registry :test
            (fn [{:keys [kv]} {:keys [udf/kind]}]
              (case kind
                :order-fn :rank
                :deserializer b/deserialize
                :serializer
                (fn [v]
                  (when (l/writing? kv)
                    (swap! handles conj kv)
                    (swap! calls inc)
                    (d/transact-kv kv "audit" [[:put (:rank v) payload]] :long :bytes)
                    ;; Both the callback and subsequent custom datom storage
                    ;; must be able to read the replacement native writer.
                    (is (Arrays/equals ^bytes payload
                                       ^bytes (d/get-value kv "audit" (:rank v)
                                                          :long :bytes))))
                  (b/serialize v)))))
          (d/register-type conn :test/value
                           {:index {:type :long :order-fn (descriptor :order-fn)}
                            :payload {:serialize (descriptor :serializer)
                                      :deserialize (descriptor :deserializer)}})
          (d/update-schema conn {:value {:db/valueType :test/value}
                                 :ordinary {:db/valueType :db.type/long}})
          (let [transact #(d/transact! % [[:db/add 1 :value {:rank 1}]
                                        [:db/add 1 :ordinary 10]])]
            (is (map? (if explicit?
                        (d/with-transaction [tx conn] (transact tx))
                        (transact conn)))))
          (is (= {:rank 1} (:value (d/entity @conn 1))))
          (is (= 10 (:ordinary (d/entity @conn 1))))
          (is (Arrays/equals ^bytes payload
                             ^bytes (d/get-value native "audit" 1 :long :bytes)))
          (when wal?
            (is (pos? @resizes))
            (is (= 1 @calls))
            (doseq [handle @handles]
              (is (= :txlog/transaction-view-invalidated
                     (try (d/get-value handle "audit" 1 :long :bytes)
                          :unexpected-success
                          (catch clojure.lang.ExceptionInfo e
                            (:error (ex-data e)))))))
            (let [replay (d/open-kv (str dir "/replay") {:wal? false})]
              (try
                (doseq [{:keys [rows lsn]} (kv/open-tx-log-rows native 0)]
                  (kv/replay-txlog-rows! replay rows lsn))
                (is (Arrays/equals ^bytes payload
                                   ^bytes (d/get-value replay "audit" 1 :long :bytes)))
                (finally (d/close-kv replay))))
            (let [replayed (d/create-conn (str dir "/replay") nil
                                          (assoc opts :wal? false))]
              (try
                (is (= {:rank 1} (:value (d/entity @replayed 1))))
                (is (= 10 (:ordinary (d/entity @replayed 1))))
                (finally (d/close replayed)))))
          ;; The collector remains usable after the callback's resize.
          (d/transact! conn [[:db/add 1 :ordinary 11]])
          (d/close conn)
          (let [reopened (d/create-conn (str dir "/source") nil opts)]
            (try
              (is (= {:rank 1} (:value (d/entity @reopened 1))))
              (is (= 11 (:ordinary (d/entity @reopened 1))))
              (is (Arrays/equals ^bytes payload
                                 ^bytes (d/get-value (d/datalog-kv reopened)
                                                    "audit" 1 :long :bytes)))
              (finally (d/close reopened))))
          (finally
            (unobserve)
            (d/close conn)
            (u/delete-files dir)))))))
