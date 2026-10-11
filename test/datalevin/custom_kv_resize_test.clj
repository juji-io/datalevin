(ns datalevin.custom-kv-resize-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.bits :as b]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.interpret :as inter]
            [datalevin.kv :as kv]
            [datalevin.lmdb :as l]
            [datalevin.test.core :refer [db-fixture]]
            [datalevin.tx-group.phase :as phase]
            [datalevin.udf :as udf]
            [datalevin.util :as u])
  (:import [java.util Arrays Random]))

(use-fixtures :each db-fixture)

(deftest custom-kv-batch-reads-use-the-refreshed-writer
  (doseq [wal? [true false]
          explicit? [false true]
          custom-keys? [false true]]
    (testing (str "WAL " wal? ", explicit " explicit? ", custom keys " custom-keys?)
      (let [dir (u/tmp-dir (str "custom-kv-resize-" (random-uuid)))
            opts {:wal? wal? :wal-durability-profile :strict :mapsize 1
                  :wal-segment-prealloc? false :snapshot-scheduler? false}
            db (d/open-kv (str dir "/source") opts)
            payload (byte-array (* 2 1024 1024))
            _ (.nextBytes (Random. 123) payload)
            keys (if custom-keys? [{:rank 1} {:rank 2} {:rank 3}] [1 2 3])
            kt (if custom-keys? :test/value :long)
            resizes (atom 0) calls (atom 0) retained (atom nil)
            unobserve (phase/observe!
                        (fn [event _]
                          (when (= event :native-resized) (swap! resizes inc))))
            check! (fn [handle]
                     (is (empty? (d/get-list handle "items" (keys 0) kt :test/value)))
                     (is (empty? (d/get-list handle "items" (keys 2) kt :test/value)))
                     (let [values (d/get-list handle "items" (keys 1) kt :test/value)]
                       (is (= [2] (mapv :rank values)))
                       (is (Arrays/equals ^bytes payload ^bytes (:payload (first values)))))
                     ;; Deleted keys and items must release their payloads.
                     (is (= (if custom-keys? 2 1) (d/entries handle c/custom-values))))]
        (try
          (d/register-type db :test/value
                           {:index {:type :long
                                    :order-fn (inter/inter-fn [v] (:rank v))}})
          (d/open-list-dbi db "items"
                           (cond-> {:value-type :test/value}
                             custom-keys? (assoc :key-type :test/value)))
          (d/put-list-items db "items" (keys 0) [{:rank 1}] kt :test/value)
          (let [write! (fn [tx]
                         (swap! calls inc)
                         (d/transact-kv tx "items"
                                        [[:put-list (keys 1) [{:rank 2 :payload payload}]]
                                         [:del (keys 0)]
                                         [:put-list (keys 2) [{:rank 3}]]
                                         [:del-list (keys 2) [{:rank 3}]]]
                                        kt :test/value))]
            (is (= :transacted
                   (if explicit?
                     (d/with-transaction-kv [tx db]
                       (reset! retained tx)
                       (let [result (write! tx)]
                         (check! tx)
                         result))
                     (write! db)))))
          (check! db)
          (when wal?
            (is (pos? @resizes))
            (is (= 1 @calls))
            (when explicit?
              (is (thrown? clojure.lang.ExceptionInfo
                           (d/transact-kv @retained "items" [[:del (keys 1)]]
                                          kt :test/value))))
            (let [replay (d/open-kv (str dir "/replay") {:wal? false})]
              (try
                (doseq [{:keys [rows lsn]} (kv/open-tx-log-rows db 0)]
                  (kv/replay-txlog-rows! replay rows lsn))
                (finally (d/close-kv replay))))
            (let [replayed (d/open-kv (str dir "/replay") {:wal? false})]
              (try (check! replayed)
                   (finally (d/close-kv replayed)))))
          ;; A later write proves the collector remains usable after resizing.
          (d/open-dbi db "probe")
          (d/transact-kv db "probe" [[:put 1 42]] :long :long)
          (d/close-kv db)
          (let [reopened (d/open-kv (str dir "/source") opts)]
            (try
              (check! reopened)
              (is (= 42 (d/get-value reopened "probe" 1 :long :long)))
              (finally (d/close-kv reopened))))
          (finally
            (unobserve)
            (d/close-kv db)
            (u/delete-files dir)))))))

(deftest custom-read-snapshots-follow-order-callback-map-growth
  (doseq [mapsize [1 32]
          operation [:point :range :get-list :list-range :filter]]
    (testing (str "map " mapsize ", read " operation)
      (let [dir (u/tmp-dir (str "custom-read-resize-" (random-uuid)))
            runtime (udf/create-registry)
            opts {:wal? true :wal-durability-profile :strict :mapsize mapsize
                  :wal-segment-prealloc? false :snapshot-scheduler? false
                  :runtime-opts {:udf-registry runtime}}
            db (d/open-kv (str dir "/source") opts)
            desc (fn [kind] {:udf/lang :test :udf/kind kind :udf/id :test/read})
            audit (byte-array (* 2 1024 1024))
            _ (.nextBytes (Random. 42) audit)
            armed (atom false) calls (atom []) bodies (atom 0) resizes (atom 0)
            retained (atom nil)
            unobserve (phase/observe!
                        (fn [event _]
                          (when (= event :native-resized) (swap! resizes inc))))
            kr [:closed {:rank 1} {:rank 2}]
            vr [:closed {:rank 11} {:rank 22}]
            pairs [[{:rank 1} {:rank 11}] [{:rank 2} {:rank 22}]]
            expected-calls (cond-> [[:test/key 1]]
                             (#{:range :list-range :filter} operation) (conj [:test/key 2])
                             (= operation :list-range) (into [[:test/item 11] [:test/item 22]]))]
        (try
          (d/open-dbi db "audit")
          (d/open-dbi db "probe")
          (udf/register-resolver!
            runtime :test
            (fn [{:keys [kv kind type-name]} _]
              (let [txn @(l/write-txn kv)]
                (fn [value]
                  (when (and (l/writing? kv) (not (identical? txn @(l/write-txn kv))))
                    (throw (ex-info "Read codec retained an earlier native writer" {:kind kind})))
                  (case kind
                    :serializer (b/serialize value)
                    :deserializer (b/deserialize value)
                    :order-fn
                    (do
                      (when (l/writing? kv)
                        (swap! calls conj [type-name (:rank value)])
                        (when (compare-and-set! armed true false)
                          (d/transact-kv kv "audit" [[:put 0 audit]] :long :bytes)))
                      (:rank value)))))))
          (doseq [type [:test/key :test/item]]
            (d/register-type db type
                             {:index {:type :long :order-fn (desc :order-fn)}
                              :payload {:serialize (desc :serializer)
                                        :deserialize (desc :deserializer)}}))
          (d/open-list-dbi db "items" {:key-type :test/key :value-type :test/item})
          (d/transact-kv db "items"
                         [[:put-list {:rank 1} [{:rank 11}]]
                          [:put-list {:rank 2} [{:rank 22}]]]
                         :test/key :test/item)
          (reset! calls [])
          (reset! resizes 0)
          (let [result
                (d/with-transaction-kv [tx db]
                  (reset! retained tx)
                  (swap! bodies inc)
                  (reset! armed true)
                  (let [result (case operation
                                 :point (d/get-value tx "items" {:rank 1} :test/key :test/item)
                                 :range (d/get-range tx "items" kr :test/key :test/item)
                                 :get-list (d/get-list tx "items" {:rank 1} :test/key :test/item)
                                 :list-range (d/list-range tx "items" kr :test/key vr :test/item)
                                 :filter (d/range-filter tx "items" (fn [_ _] true) kr
                                                         :test/key :test/item false false))]
                    (d/transact-kv tx "probe" [[:put 1 42]] :long :long)
                    result))]
            (is (= (case operation :point {:rank 11} :get-list [{:rank 11}] pairs) result)))
          (is (= expected-calls @calls) "completed bounds must not run again")
          (is (= 1 @bodies))
          (is (= (= mapsize 1) (pos? @resizes)))
          (is (Arrays/equals ^bytes audit ^bytes (d/get-value db "audit" 0 :long :bytes)))
          (is (= 42 (d/get-value db "probe" 1 :long :long)))
          (is (thrown? clojure.lang.ExceptionInfo
                       (d/get-value @retained "audit" 0 :long :bytes)))
          (let [replay (d/open-kv (str dir "/replay") {:wal? false})]
            (try
              (doseq [{:keys [rows lsn]} (kv/open-tx-log-rows db 0)]
                (kv/replay-txlog-rows! replay rows lsn))
              (is (Arrays/equals ^bytes audit ^bytes (d/get-value replay "audit" 0 :long :bytes)))
              (is (= 42 (d/get-value replay "probe" 1 :long :long)))
              (finally (d/close-kv replay))))
          (d/close-kv db)
          (let [reopened (d/open-kv (str dir "/source") opts)]
            (try
              (is (= pairs (d/get-range reopened "items" [:all] :test/key :test/item)))
              (finally (d/close-kv reopened))))
          (finally
            (unobserve)
            (d/close-kv db)
            (u/delete-files dir)))))))

(deftest custom-kv-codecs-refresh-after-serializer-map-growth
  (doseq [custom-keys? [false true]
          explicit? [false true]
          mapsize [1 32]]
    (testing (str "custom keys " custom-keys? ", explicit " explicit? ", map " mapsize)
      (let [dir (u/tmp-dir (str "custom-kv-codec-resize-" (random-uuid)))
            runtime (udf/create-registry)
            opts {:wal? true :wal-durability-profile :strict :mapsize mapsize
                  :wal-segment-prealloc? false :snapshot-scheduler? false
                  :runtime-opts {:udf-registry runtime}}
            db (d/open-kv (str dir "/source") opts)
            desc (fn [kind] {:udf/lang :test :udf/kind kind :udf/id :test/codec})
            audit (byte-array (* 2 1024 1024) (byte 42))
            calls (atom []) writers (atom {}) handles (atom []) resizes (atom 0)
            unobserve (phase/observe!
                        (fn [event _]
                          (when (= event :native-resized) (swap! resizes inc))))
            read-values (fn [handle]
                          (if custom-keys?
                            (mapv first (d/get-range handle "items" [:all] :test/value :long))
                            (d/get-list handle "items" 1 :long :test/value)))
            rows (if custom-keys?
                   [[:put {:rank 1} 1] [:put {:rank 2} 2] [:del {:rank 0}]]
                   [[:put-list 1 [{:rank 1} {:rank 2}]] [:del-list 1 [{:rank 0}]]])
            kt (if custom-keys? :test/value :long)
            vt (if custom-keys? :long :test/value)]
        (try
          (d/open-dbi db "audit")
          (udf/register-resolver!
            runtime :test
            (fn [{:keys [kv kind]} _]
              (let [txn @(l/write-txn kv)]
                (when (l/writing? kv)
                  (swap! writers update kind (fnil conj #{}) txn)
                  (swap! handles conj kv))
                (fn [value]
                  (when (and (l/writing? kv) (not (identical? txn @(l/write-txn kv))))
                    (throw (ex-info "Codec retained an earlier native writer" {:kind kind})))
                  (case kind
                    ;; Collisions require matching to decode an existing payload.
                    :order-fn 0
                    :deserializer (b/deserialize value)
                    :serializer
                    (do
                      (when (and (l/writing? kv) (pos? (long (:rank value))))
                        ;; Materialize the deserializer before this serializer
                        ;; replaces its native writer, then force a collision scan.
                        (read-values kv)
                        (swap! calls conj (:rank value))
                        (d/transact-kv kv "audit" [[:put (:rank value) audit]] :long :bytes))
                      (b/serialize value)))))))
          (d/register-type db :test/value
                           {:index {:type :long :order-fn (desc :order-fn)}
                            :payload {:serialize (desc :serializer)
                                      :deserialize (desc :deserializer)}})
          (if custom-keys?
            (d/open-dbi db "items" {:key-type :test/value})
            (d/open-list-dbi db "items" {:value-type :test/value}))
          (d/transact-kv db "items"
                         (if custom-keys? [[:put {:rank 0} 0]] [[:put-list 1 [{:rank 0}]]])
                         kt vt)
          (reset! writers {})
          (reset! handles [])
          (reset! calls [])
          (reset! resizes 0)
          (let [write! #(d/transact-kv % "items" rows kt vt)]
            (is (= :transacted (if explicit?
                                (d/with-transaction-kv [tx db] (write! tx))
                                (write! db)))))
          (is (= [1 2] (mapv :rank (read-values db))))
          (is (= [1 2] @calls) "completed serializer writes must run once")
          (is (= 2 (d/entries db c/custom-values)))
          (is (seq (:deserializer @writers)))
          (if (= mapsize 1)
            (do (is (pos? @resizes))
                (is (< 1 (count (:deserializer @writers)))))
            (is (zero? @resizes)))
          (doseq [handle @handles]
            (is (thrown? clojure.lang.ExceptionInfo
                         (d/get-value handle "audit" 1 :long :bytes))))
          (let [replay (d/open-kv (str dir "/replay") {:wal? false})]
            (try
              (doseq [{:keys [rows lsn]} (kv/open-tx-log-rows db 0)]
                (kv/replay-txlog-rows! replay rows lsn))
              (finally (d/close-kv replay))))
          (doseq [path [(str dir "/replay") (str dir "/source")]]
            (when (= path (str dir "/source")) (d/close-kv db))
            (let [reopened (d/open-kv path (assoc opts :wal? (= path (str dir "/source"))))]
              (try
                (is (= [1 2] (mapv :rank (read-values reopened))))
                (doseq [rank [1 2]]
                  (is (Arrays/equals ^bytes audit
                                     ^bytes (d/get-value reopened "audit" rank :long :bytes))))
                (finally (d/close-kv reopened)))))
          (finally
            (unobserve)
            (d/close-kv db)
            (u/delete-files dir)))))))
