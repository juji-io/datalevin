(ns datalevin.wal-kv-test
  (:require
   [clojure.java.io :as io]
   [datalevin.lmdb :as l]
   [datalevin.bits :as b]
   [datalevin.wal :as wal]
   [datalevin.interface :as if]
   [datalevin.util :as u]
   [datalevin.constants :as c]
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.string :as s]
   [clojure.test :refer [deftest testing is use-fixtures]]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.clojure-test :as test]
   [clojure.test.check.properties :as prop])
  (:import
   [java.util UUID]))

(use-fixtures :each db-fixture)

(defn- last-wal-id
  [lmdb]
  (:last-committed-wal-tx-id (if/kv-wal-watermarks lmdb)))

(defn- assert-kv-wal-post-commit-failpoint
  [step phase]
  (let [dir  (u/tmp-dir (str "kv-wal-fp-" (name step) "-" (name phase) "-"
                             (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                             :kv-wal? true})]
    (try
      (if/open-dbi lmdb "a")
      (let [base-id ^long (last-wal-id lmdb)
            ex      (try
                      (binding [c/*failpoint* {:step  step
                                               :phase phase
                                               :fn    #(throw (ex-info "fp-post" {}))}]
                        (if/transact-kv lmdb [[:put "a" 1 "x"]]))
                      nil
                      (catch Exception e e))]
        (is (instance? clojure.lang.ExceptionInfo ex))
        (if (= step :step-4)
          (is (nil? (if/get-value lmdb "a" 1)))
          (is (= "x" (if/get-value lmdb "a" 1))))
        (is (= (u/long-inc base-id) (last-wal-id lmdb)))

        ;; After post-commit failure, WAL ids must continue from durable state.
        (if/transact-kv lmdb [[:put "a" 2 "y"]])
        (is (= (+ base-id 2) (last-wal-id lmdb)))
        (let [records (vec (wal/read-wal-records dir (dec base-id)
                                                 (+ base-id 2)))]
          (is (= #{base-id (inc base-id) (+ base-id 2)}
                 (set (map :wal/tx-id records))))))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))

(deftest kv-wal-disabled-by-default-test
  (let [dir  (u/tmp-dir (str "kv-wal-default-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags (conj c/default-env-flags :nosync)})]
    (try
      (if/open-dbi lmdb "a")
      (if/transact-kv lmdb [[:put "a" 1 "x"]])
      (is (= {:last-committed-wal-tx-id 0
              :last-indexed-wal-tx-id   0
              :last-committed-user-tx-id 0}
             (if/kv-wal-watermarks lmdb)))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))

(deftest kv-wal-append-test
  (let [dir  (u/tmp-dir (str "kv-wal-append-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                             :kv-wal? true})]
    (try
      (if/open-dbi lmdb "a")
      (let [base-id ^long (last-wal-id lmdb)]
        (if/transact-kv lmdb [[:put "a" 1 "x"]
                              [:put "a" 2 "y"]])
        (let [wm      (if/kv-wal-watermarks lmdb)
              wal-id  (:last-committed-wal-tx-id wm)
              idx-id  (:last-indexed-wal-tx-id wm)
              user-id (:last-committed-user-tx-id wm)
              records (vec (wal/read-wal-records dir base-id wal-id))
              record  (last records)]
          (is (= (inc base-id) wal-id))
          (is (= 0 idx-id))
          (is (= wal-id user-id))
          (is (= wal-id (:wal/tx-id record)))
          (is (= 2 (count (:wal/ops record))))
          (is (= #{"a"} (set (map :dbi (:wal/ops record)))))
          (is (every? #(= :put (:op %)) (:wal/ops record))))

        (if/transact-kv lmdb [[:put "a" 3 "z"]])
        (is (= (+ base-id 2) (last-wal-id lmdb))))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))

(deftest kv-wal-failpoint-step3-before-test
  (let [dir  (u/tmp-dir (str "kv-wal-fp-step3-before-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                             :kv-wal? true})]
    (try
      (if/open-dbi lmdb "a")
      (let [base-id (last-wal-id lmdb)
            ex      (try
                      (binding [c/*failpoint* {:step :step-3
                                               :phase :before
                                               :fn #(throw (ex-info "fp-before" {}))}]
                        (if/transact-kv lmdb [[:put "a" 1 "x"]]))
                      nil
                      (catch Exception e e))]
        (is (instance? clojure.lang.ExceptionInfo ex))
        (is (nil? (if/get-value lmdb "a" 1)))
        (is (= base-id (last-wal-id lmdb))))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))

(deftest kv-wal-failpoint-step3-after-test
  (let [dir  (u/tmp-dir (str "kv-wal-fp-step3-after-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                             :kv-wal? true})]
    (try
      (if/open-dbi lmdb "a")
      (let [base-id ^long (last-wal-id lmdb)
            ex      (try
                      (binding [c/*failpoint* {:step  :step-3
                                               :phase :after
                                               :fn    #(throw (ex-info "fp-after" {}))}]
                        (if/transact-kv lmdb [[:put "a" 1 "x"]]))
                      nil
                      (catch Exception e e))]
        (is (instance? clojure.lang.ExceptionInfo ex))
        ;; Step-3 failpoint after commit must not roll back durable effects.
        (is (nil? (if/get-value lmdb "a" 1)))
        (is (= (inc base-id) (last-wal-id lmdb)))

        ;; After post-commit failure, subsequent tx should continue from next WAL id.
        (if/transact-kv lmdb [[:put "a" 2 "y"]])
        (is (= (+ base-id 2) (last-wal-id lmdb)))
        (let [records (vec (wal/read-wal-records dir (dec base-id)
                                                 (+ base-id 2)))]
          (is (= #{base-id (inc base-id) (+ base-id 2)}
                 (set (map :wal/tx-id records))))))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))

(deftest kv-wal-failpoint-step4-after-test
  (assert-kv-wal-post-commit-failpoint :step-4 :after))

(deftest kv-wal-failpoint-step5-after-test
  (assert-kv-wal-post-commit-failpoint :step-5 :after))

(deftest kv-wal-failpoint-step6-after-test
  (assert-kv-wal-post-commit-failpoint :step-6 :after))

(deftest kv-wal-failpoint-step7-after-test
  (assert-kv-wal-post-commit-failpoint :step-7 :after))

(deftest kv-wal-failpoint-step8-after-nonfatal-test
  (let [dir  (u/tmp-dir (str "kv-wal-fp-step8-after-" (UUID/randomUUID)))
        lmdb (l/open-kv
               dir
               {:flags                  (conj c/default-env-flags :nosync)
                :kv-wal?                true
                :wal-meta-flush-max-txs 1})]
    (try
      (if/open-dbi lmdb "a")
      (let [base-id ^long (last-wal-id lmdb)]
        (is (= :transacted
               (binding [c/*failpoint* {:step  :step-8
                                        :phase :after
                                        :fn    #(throw (ex-info "fp-step8" {}))}]
                 (if/transact-kv lmdb [[:put "a" 1 "x"]]))))
        (is (= "x" (if/get-value lmdb "a" 1)))
        (is (= (inc base-id) (last-wal-id lmdb))))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))

(deftest kv-wal-meta-publish-test
  (let [dir       (u/tmp-dir (str "kv-wal-meta-" (UUID/randomUUID)))
        lmdb      (l/open-kv
                    dir
                    {:flags                  (conj c/default-env-flags :nosync)
                     :kv-wal?                true
                     :wal-meta-flush-max-txs 1
                     :wal-meta-flush-max-ms  60000})
        meta-path (str dir u/+separator+ c/wal-dir u/+separator+ c/wal-meta-file)]
    (try
      (if/open-dbi lmdb "a")
      (let [base-id ^long (last-wal-id lmdb)]
        (if/transact-kv lmdb [[:put "a" 1 "x"]])
        (is (.exists (io/file meta-path)))
        (let [meta (wal/read-wal-meta dir)]
          (is (= (inc base-id) (get meta c/last-committed-wal-tx-id)))
          (is (= 0 (get meta c/last-indexed-wal-tx-id)))
          (is (= (inc base-id) (get meta c/last-committed-user-tx-id)))
          (is (true? (get meta :wal/enabled?)))
          (is (pos? (long (or (get meta c/wal-meta-revision) 0))))
          (is (>= (long (or (get meta c/committed-last-modified-ms) -1)) 0))))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))

(deftest kv-wal-meta-flush-cadence-test
  (let [dir       (u/tmp-dir (str "kv-wal-meta-cadence-" (UUID/randomUUID)))
        lmdb      (l/open-kv
                    dir
                    {:flags                  (conj c/default-env-flags :nosync)
                     :kv-wal?                true
                     :wal-meta-flush-max-txs 3
                     :wal-meta-flush-max-ms  60000})
        meta-path (str dir u/+separator+ c/wal-dir u/+separator+
                       c/wal-meta-file)]
    (try
      (if/open-dbi lmdb "a")
      (let [base-id ^long (last-wal-id lmdb)]
        (if/transact-kv lmdb [[:put "a" 1 "x"]])
        (is (not (.exists (io/file meta-path))))
        (if/transact-kv lmdb [[:put "a" 2 "y"]])
        (is (.exists (io/file meta-path)))
        (if/transact-kv lmdb [[:put "a" 3 "z"]])
        (let [meta (wal/read-wal-meta dir)]
          (is (= (+ base-id 2) (get meta c/last-committed-wal-tx-id)))
          (is (= 0 (get meta c/last-indexed-wal-tx-id)))
          (is (= (+ base-id 2) (get meta c/last-committed-user-tx-id)))))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))

(deftest kv-wal-replay-test
  (let [dir  (u/tmp-dir (str "kv-wal-replay-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                             :kv-wal? true})]
    (try
      (if/open-dbi lmdb "a")
      (if/transact-kv lmdb [[:put "a" 1 "x"]])
      (if/transact-kv lmdb [[:put "a" 2 "y"]
                            [:del "a" 1]])
      (let [committed-id (last-wal-id lmdb)]
        (is (= committed-id (:last-committed-wal-tx-id
                              (if/kv-wal-watermarks lmdb))))
        (binding [c/*trusted-apply* true
                  c/*bypass-wal*    true]
          (if/transact-kv lmdb c/kv-info
                          [[:put c/last-indexed-wal-tx-id 0]]
                          :data :data))
        (is (= 0 (if/get-value lmdb c/kv-info
                               c/last-indexed-wal-tx-id :data :data)))
        ;; Applied marker starts at 0; replay should apply WAL records.
        (is (nil? (if/get-value lmdb c/kv-info
                                c/applied-wal-tx-id :data :data)))

        (let [res (l/replay-kv-wal! lmdb)]
          (is (= {:from 0 :to committed-id :applied committed-id} res)))
        (is (= committed-id (if/get-value lmdb c/kv-info
                                          c/last-indexed-wal-tx-id :data :data)))
        (is (= committed-id (if/get-value lmdb c/kv-info
                                          c/applied-wal-tx-id :data :data)))
        (is (= committed-id (:last-committed-wal-tx-id
                              (if/kv-wal-watermarks lmdb))))

        ;; Replay must not append additional WAL records.
        (is (= committed-id (count (wal/read-wal-records dir 0 committed-id))))
        (is (nil? (if/get-value lmdb "a" 1)))
        (is (= "y" (if/get-value lmdb "a" 2))))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))

(deftest kv-wal-replay-list-ops-test
  (let [dir  (u/tmp-dir (str "kv-wal-replay-list-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                             :kv-wal? true})]
    (try
      (if/open-list-dbi lmdb "list")
      (if/transact-kv lmdb [[:put-list "list" "a" [1 2 3] :string :long]])
      (if/transact-kv lmdb [[:del-list "list" "a" [1] :string :long]])
      (is (= [2 3] (if/get-list lmdb "list" "a" :string :long)))

      ;; Simulate base lagging behind committed WAL, then replay.
      ;; At this point, the base is cleared but the overlay still provides the data.
      (if/clear-dbi lmdb "list")
      (is (= [2 3] (if/get-list lmdb "list" "a" :string :long)))
      ;; Reset watermarks to simulate base lagging behind WAL.
      ;; Use trusted apply to avoid emitting WAL records for kv-info setup.
      (binding [c/*trusted-apply* true
                  c/*bypass-wal*    true]
        (if/transact-kv lmdb c/kv-info
                        [[:put c/last-indexed-wal-tx-id 0]
                         [:put c/applied-wal-tx-id 0]]
                        :data :data))

      (let [committed-id (last-wal-id lmdb)]
        (is (= {:from 0 :to committed-id :applied committed-id}
               (l/replay-kv-wal! lmdb))))
      (is (= [2 3] (if/get-list lmdb "list" "a" :string :long)))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))

(deftest kv-wal-dbi-guard-test
  (let [dir  (u/tmp-dir (str "kv-wal-guard-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                             :kv-wal? true})]
    (try
      (let [ex (try
                 (if/transact-kv lmdb [[:put "nonexistent-dbi" 1 (byte-array [1]) :id :raw]])
                 nil
                 (catch Exception e e))]
        (is (instance? clojure.lang.ExceptionInfo ex))
        (is (s/includes? (ex-message ex) "not open")))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))

(deftest kv-wal-replay-bounded-and-noop-test
  (let [dir   (u/tmp-dir (str "kv-wal-replay-bounded-" (UUID/randomUUID)))
        lmdb  (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                              :kv-wal? true})
        dir2  (u/tmp-dir (str "kv-wal-replay-noop-" (UUID/randomUUID)))
        lmdb2 (l/open-kv dir2 {:flags (conj c/default-env-flags :nosync)})]
    (try
      (if/open-dbi lmdb "a")
      (let [base-id (last-wal-id lmdb)]
        (if/transact-kv lmdb [[:put "a" 1 "x"]])
        (if/transact-kv lmdb [[:put "a" 2 "y"]])
        (binding [c/*trusted-apply* true
                  c/*bypass-wal*    true]
          (if/transact-kv lmdb c/kv-info
                          [[:put c/last-indexed-wal-tx-id 0]
                           [:put c/applied-wal-tx-id 0]]
                          :data :data))

        (is (= {:from 0 :to base-id :applied base-id}
               (l/replay-kv-wal! lmdb base-id)))
        (is (= base-id (if/get-value lmdb c/kv-info
                                     c/last-indexed-wal-tx-id :data :data)))
        (is (= base-id (if/get-value lmdb c/kv-info
                                     c/applied-wal-tx-id :data :data)))

        (is (= {:from base-id :to base-id :applied 0}
               (l/replay-kv-wal! lmdb base-id)))

        (is (= {:from 0 :to 0 :applied 0}
               (l/replay-kv-wal! lmdb2))))
      (finally
        (if/close-kv lmdb)
        (if/close-kv lmdb2)
        (u/delete-files dir)
        (u/delete-files dir2)))))

(deftest kv-wal-replay-legacy-applied-marker-fallback-test
  (let [dir  (u/tmp-dir (str "kv-wal-replay-legacy-applied-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                             :kv-wal? true})]
    (try
      (if/open-dbi lmdb "a")
      (if/transact-kv lmdb [[:put "a" 1 "x"]])
      (if/transact-kv lmdb [[:put "a" 2 "y"]])

      ;; Simulate a pre-separation marker store: legacy key exists, new key absent.
      ;; Use trusted apply to avoid emitting WAL records for kv-info setup.
      (binding [c/*trusted-apply* true
                  c/*bypass-wal*    true]
        (if/transact-kv lmdb c/kv-info
                        [[:del c/applied-wal-tx-id]
                         [:put c/legacy-applied-tx-id 0]
                         [:put c/last-indexed-wal-tx-id 0]]
                        :data :data))

      (let [committed-id (last-wal-id lmdb)]
        (is (= {:from 0 :to committed-id :applied committed-id}
               (l/replay-kv-wal! lmdb)))
        (is (= committed-id
               (if/get-value lmdb c/kv-info c/applied-wal-tx-id :data :data)))
        (is (= committed-id
               (if/get-value lmdb c/kv-info
                             c/last-indexed-wal-tx-id :data :data))))
      (is (= "x" (if/get-value lmdb "a" 1)))
      (is (= "y" (if/get-value lmdb "a" 2)))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))

(deftest kv-wal-overlay-merge-and-prune-test
  (let [dir  (u/tmp-dir (str "kv-wal-overlay-merge-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                             :kv-wal? true})]
    (try
      (if/open-dbi lmdb "a")
      (if/transact-kv lmdb [[:put "a" 1 "x"]
                            [:put "a" 2 "y"]
                            [:put "a" 3 "z"]])
      (if/transact-kv lmdb [[:del "a" 2]])

      ;; Simulate WAL tail visibility when base is stale.
      (if/clear-dbi lmdb "a")
      (is (= "x" (if/get-value lmdb "a" 1)))
      (is (nil? (if/get-value lmdb "a" 2)))
      (is (= "z" (if/get-value lmdb "a" 3)))
      (is (= [[1 "x"] [3 "z"]]
             (vec (if/get-range lmdb "a" [:all]))))
      (is (= [[3 "z"] [1 "x"]]
             (vec (if/get-range lmdb "a" [:all-back]))))
      (is (= [[1 "x"]]
             (vec (if/get-range lmdb "a" [:closed 1 2]))))
      (is (= ["x" "z"]
             (vec (if/get-range lmdb "a" [:all] :data :data true))))
      (is (= [1 "x"] (if/get-first lmdb "a" [:all] :data :data false)))
      (is (= true (if/get-first lmdb "a" [:all] :data :ignore true)))
      (is (= [[1 "x"] [3 "z"]]
             (vec (if/get-first-n lmdb "a" 5 [:all] :data :data false))))
      (is (= [1 3] (vec (if/key-range lmdb "a" [:all] :data))))
      (is (= 2 (if/key-range-count lmdb "a" [:all] :data)))
      (is (= 2 (if/range-count lmdb "a" [:all] :data)))
      (is (= [3 "z"]
             (if/get-some lmdb "a" (fn [k _] (= k 3))
                          [:all] :data :data false false)))
      (is (= [[1 "x"]]
             (vec (if/range-filter lmdb "a" (fn [k _] (= k 1))
                                   [:all] :data :data false false))))
      (is (= ["x!"]
             (vec (if/range-keep lmdb "a"
                                 (fn [k v] (when (= k 1) (str v "!")))
                                 [:all] :data :data false))))
      (is (= "x!"
             (if/range-some lmdb "a"
                            (fn [k v] (when (= k 1) (str v "!")))
                            [:all] :data :data false)))
      (is (= 1
             (if/range-filter-count lmdb "a" (fn [k _] (= k 1))
                                    [:all] :data :data false)))
      (let [visited  (volatile! [])
            kvisited (volatile! [])]
        (if/visit lmdb "a" (fn [k v] (vswap! visited conj [k v]))
                  [:all] :data :data false)
        (if/visit-key-range lmdb "a" (fn [k] (vswap! kvisited conj k))
                            [:all] :data false)
        (is (= [[1 "x"] [3 "z"]] @visited))
        (is (= [1 3] @kvisited)))
      (is (= 0 (if/get-rank lmdb "a" 1 :data)))
      (is (= 1 (if/get-rank lmdb "a" 3 :data)))
      (is (nil? (if/get-rank lmdb "a" 2 :data)))
      (is (= [1 "x"] (if/get-by-rank lmdb "a" 0 :data :data false)))
      (is (= [3 "z"] (if/get-by-rank lmdb "a" 1 :data :data false)))
      (is (nil? (if/get-by-rank lmdb "a" 2 :data :data false)))
      (is (= [[1 "x"] [3 "z"]]
             (vec (if/sample-kv lmdb "a" 2 :data :data false))))
      (is (= ["x" "z"]
             (vec (if/sample-kv lmdb "a" 2 :data :data true))))
      (is (= [[1 "x"] [3 "z"]]
             (->> (if/range-seq lmdb "a" [:all] :data :data false
                                {:batch-size 1})
                  (apply concat)
                  vec)))
      (let [sampled (volatile! [])]
        (if/visit-key-sample lmdb "a" (long-array [0 1]) 0 0
                             (fn [k] (vswap! sampled conj k))
                             [:all] :data false)
        (is (= [1 3] @sampled)))

      ;; Replay catches base up and prunes committed overlay <= indexed watermark.
      ;; Use trusted apply to avoid emitting WAL records for kv-info setup.
      (binding [c/*trusted-apply* true
                  c/*bypass-wal*    true]
        (if/transact-kv lmdb c/kv-info
                        [[:put c/last-indexed-wal-tx-id 0]
                         [:put c/applied-wal-tx-id 0]]
                        :data :data))
      (let [committed-id (last-wal-id lmdb)]
        (is (= {:indexed-wal-tx-id committed-id
                :committed-wal-tx-id committed-id
                :drained? true}
               (if/flush-kv-indexer! lmdb))))

      ;; With pruned overlay, clearing base again leaves no values visible.
      (if/clear-dbi lmdb "a")
      (is (nil? (if/get-value lmdb "a" 1)))
      (is (empty? (if/get-range lmdb "a" [:all])))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))

(deftest kv-wal-overlay-merge-and-prune-dupsort-test
  (let [dir  (u/tmp-dir (str "kv-wal-overlay-dupsort-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                             :kv-wal? true})]
    (try
      (if/open-list-dbi lmdb "l")
      (if/put-list-items lmdb "l" "a" [1 2 3] :string :long)
      (if/put-list-items lmdb "l" "b" [4 5] :string :long)
      (if/del-list-items lmdb "l" "a" [2] :string :long)
      (if/put-list-items lmdb "l" "a" [6] :string :long)
      (if/del-list-items lmdb "l" "b" :string)
      (if/put-list-items lmdb "l" "c" [7] :string :long)

      ;; Simulate WAL tail visibility when base is stale.
      (if/clear-dbi lmdb "l")
      (is (= 1 (if/get-value lmdb "l" "a" :string :long)))
      (is (nil? (if/get-value lmdb "l" "b" :string :long)))
      (is (= [["a" 1] ["a" 3] ["a" 6] ["c" 7]]
             (vec (if/get-range lmdb "l" [:all] :string :long))))
      (is (= [["c" 7] ["a" 1] ["a" 3] ["a" 6]]
             (vec (if/get-range lmdb "l" [:all-back] :string :long))))
      (is (= [["a" 1] ["a" 3] ["a" 6]]
             (vec (if/get-range lmdb "l" [:closed "a" "b"] :string :long))))
      (is (= [["a" nil] ["a" nil] ["a" nil] ["c" nil]]
             (vec (if/get-range lmdb "l" [:all] :string :ignore false))))
      (is (= ["a" 1]
             (if/get-first lmdb "l" [:all] :string :long false)))
      (is (= [["a" 1] ["a" 3] ["a" 6]]
             (vec (if/get-first-n lmdb "l" 3 [:all] :string :long false))))
      (is (= ["a" "c"] (vec (if/key-range lmdb "l" [:all] :string))))
      (is (= 2 (if/key-range-count lmdb "l" [:all] :string)))
      (is (= 4 (if/range-count lmdb "l" [:all] :string)))
      (is (= ["c" 7]
             (if/get-some lmdb "l" (fn [_ v] (= v 7))
                          [:all] :string :long false false)))
      (is (= [["a" 6]]
             (vec (if/range-filter lmdb "l"
                                   (fn [k v] (and (= k "a") (= v 6)))
                                   [:all] :string :long false false))))
      (let [kvisited (volatile! [])]
        (if/visit-key-range lmdb "l" (fn [k] (vswap! kvisited conj k))
                            [:all] :string false)
        (is (= ["a" "c"] @kvisited)))
      (is (= [1 3 6] (vec (if/get-list lmdb "l" "a" :string :long))))
      (is (= [] (vec (if/get-list lmdb "l" "b" :string :long))))
      (is (= 3 (if/list-count lmdb "l" "a" :string)))
      (is (= 0 (if/list-count lmdb "l" "b" :string)))
      (is (if/in-list? lmdb "l" "a" 3 :string :long))
      (is (not (if/in-list? lmdb "l" "a" 2 :string :long)))
      (is (= 3 (b/read-buffer (if/near-list lmdb "l" "a" 2 :string :long)
                              :long)))
      (is (nil? (if/near-list lmdb "l" "b" 1 :string :long)))
      (is (= [["a" 1] ["a" 3] ["a" 6] ["c" 7]]
             (vec (if/list-range lmdb "l" [:all] :string [:all] :long))))
      (is (= [["a" 6] ["a" 3] ["a" 1] ["c" 7]]
             (vec (if/list-range lmdb "l" [:all] :string [:all-back] :long))))
      (is (= ["a" 1]
             (if/list-range-first lmdb "l" [:all] :string [:all] :long)))
      (is (= [["a" 1] ["a" 3]]
             (vec (if/list-range-first-n lmdb "l" 2 [:all] :string
                                         [:all] :long))))
      (is (= 4 (if/list-range-count lmdb "l" [:all] :string [:all] :long)))
      (is (= 2 (if/list-range-count lmdb "l" [:all] :string [:all] :long 2)))
      (is (= 2 (if/key-range-list-count lmdb "l" [:all] :string)))
      (is (= 1 (if/key-range-list-count lmdb "l" [:all] :string 1)))
      (is (= [["a" 6]]
             (vec (if/list-range-filter lmdb "l"
                                        (fn [k v] (and (= k "a") (= v 6)))
                                        [:all] :string [:all] :long false))))
      (is (= [6]
             (vec (if/list-range-keep lmdb "l"
                                      (fn [k v] (when (and (= k "a") (= v 6))
                                                  v))
                                      [:all] :string [:all] :long false))))
      (is (= 6
             (if/list-range-some lmdb "l"
                                 (fn [k v] (when (and (= k "a") (= v 6))
                                             v))
                                 [:all] :string [:all] :long false)))
      (is (= 1
             (if/list-range-filter-count lmdb "l"
                                         (fn [k v] (and (= k "a") (= v 6)))
                                         [:all] :string [:all] :long false)))
      (let [vals (volatile! [])]
        (if/visit-list lmdb "l" (fn [v] (vswap! vals conj v))
                       "a" :string :long false)
        (is (= [1 3 6] @vals)))
      (let [visited (volatile! [])]
        (if/visit-list-range lmdb "l" (fn [k v] (vswap! visited conj [k v]))
                             [:all] :string [:all] :long false)
        (is (= [["a" 1] ["a" 3] ["a" 6] ["c" 7]] @visited)))
      (let [visited (volatile! [])]
        (if/visit-list-key-range lmdb "l" (fn [k v] (vswap! visited conj [k v]))
                                 [:all] :string :long false)
        (is (= [["a" 1] ["a" 3] ["a" 6] ["c" 7]] @visited)))
      (let [sampled (volatile! [])]
        (if/visit-list-sample lmdb "l" (long-array [1 3]) 0 0
                              (fn [k v] (vswap! sampled conj [k v]))
                              [:all] :string :long false)
        (is (= [["a" 3] ["c" 7]] @sampled)))
      ;; Replay catches base up and prunes committed overlay <= indexed watermark.
      ;; Use trusted apply to avoid emitting WAL records for kv-info setup.
      (binding [c/*trusted-apply* true
                  c/*bypass-wal*    true]
        (if/transact-kv lmdb c/kv-info
                        [[:put c/last-indexed-wal-tx-id 0]
                         [:put c/applied-wal-tx-id 0]]
                        :data :data))
      (let [committed-id (last-wal-id lmdb)]
        (is (= {:indexed-wal-tx-id committed-id
                :committed-wal-tx-id committed-id
                :drained? true}
               (if/flush-kv-indexer! lmdb))))

      ;; With pruned overlay, clearing base again leaves no values visible.
      (if/clear-dbi lmdb "l")
      (is (nil? (if/get-value lmdb "l" "a" :string :long)))
      (is (empty? (if/get-range lmdb "l" [:all] :string :long)))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))

(deftest kv-wal-watermarks-and-flush-indexer-test
  (let [dir   (u/tmp-dir (str "kv-wal-watermarks-" (UUID/randomUUID)))
        lmdb  (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                              :kv-wal? true})
        dir2  (u/tmp-dir (str "kv-wal-watermarks-noop-" (UUID/randomUUID)))
        lmdb2 (l/open-kv dir2 {:flags (conj c/default-env-flags :nosync)})]
    (try
      (if/open-dbi lmdb "a")
      (let [base-id ^long (last-wal-id lmdb)]
        (if/transact-kv lmdb [[:put "a" 1 "x"]])
        (if/transact-kv lmdb [[:put "a" 2 "y"]])
        (let [committed-id (+ base-id 2)
              partial-id   (inc base-id)]
          (is (= {:last-committed-wal-tx-id  committed-id
                  :last-indexed-wal-tx-id    0
                  :last-committed-user-tx-id committed-id}
                 (l/kv-wal-watermarks lmdb)))

          (binding [c/*trusted-apply* true
                    c/*bypass-wal*    true]
            (if/transact-kv lmdb c/kv-info
                            [[:put c/last-indexed-wal-tx-id 0]]
                            :data :data))

          (is (= {:indexed-wal-tx-id   partial-id
                  :committed-wal-tx-id committed-id
                  :drained?            false}
                 (l/flush-kv-indexer! lmdb partial-id)))
          (is (= {:indexed-wal-tx-id   committed-id
                  :committed-wal-tx-id committed-id
                  :drained?            true}
                 (l/flush-kv-indexer! lmdb)))
          (is (= {:last-committed-wal-tx-id  committed-id
                  :last-indexed-wal-tx-id    committed-id
                  :last-committed-user-tx-id committed-id}
                 (l/kv-wal-watermarks lmdb)))))

      (is (= {:last-committed-wal-tx-id  0
              :last-indexed-wal-tx-id    0
              :last-committed-user-tx-id 0}
             (l/kv-wal-watermarks lmdb2)))
      (is (= {:indexed-wal-tx-id   0
              :committed-wal-tx-id 0
              :drained?            true}
             (l/flush-kv-indexer! lmdb2)))
      (finally
        (if/close-kv lmdb)
        (if/close-kv lmdb2)
        (u/delete-files dir)
        (u/delete-files dir2)))))

(deftest kv-wal-admin-test
  (let [dir  (u/tmp-dir (str "datalevin-kv-wal-admin-test-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                             :kv-wal? true})]
    (try
      (if/open-dbi lmdb "misc")
      (let [base-id ^long (:last-committed-wal-tx-id
                           (if/kv-wal-watermarks lmdb))]
        (if/transact-kv lmdb [[:put "misc" 1 "x"]])
        (if/transact-kv lmdb [[:put "misc" 2 "y"]])
        (let [committed-id ^long (+ ^long base-id 2)
              partial-id   (inc ^long base-id)]
          (is (= {:last-committed-wal-tx-id  committed-id
                  :last-indexed-wal-tx-id    0
                  :last-committed-user-tx-id committed-id}
                 (if/kv-wal-watermarks lmdb)))

          (binding [c/*trusted-apply* true
                    c/*bypass-wal*    true]
            (if/transact-kv lmdb c/kv-info
                            [[:put c/last-indexed-wal-tx-id 0]]
                            :data :data))

          (is (= {:indexed-wal-tx-id   partial-id
                  :committed-wal-tx-id committed-id
                  :drained?            false}
                 (l/flush-kv-indexer! lmdb partial-id)))
          (is (= {:indexed-wal-tx-id   committed-id
                  :committed-wal-tx-id committed-id
                  :drained?            true}
                 (l/flush-kv-indexer! lmdb)))))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))

;; ---------------------------------------------------------------------------
;; Generative / fuzz tests
;; ---------------------------------------------------------------------------

(def ^:private gen-typed-kv
  "Generator for [key value key-type value-type] tuples covering several
   WAL-serialisable types.  Each variant produces values that round-trip
   cleanly through both LMDB and the WAL codec."
  (gen/one-of
    [(gen/tuple gen/large-integer gen/large-integer
               (gen/return :long) (gen/return :long))
     (gen/tuple (gen/not-empty gen/string-alphanumeric)
                (gen/not-empty gen/string-alphanumeric)
                (gen/return :string) (gen/return :string))
     (gen/tuple gen/keyword-ns gen/large-integer
                (gen/return :keyword) (gen/return :long))
     (gen/tuple gen/large-integer gen/keyword-ns
                (gen/return :long) (gen/return :keyword))
     (gen/tuple gen/large-integer
                (gen/fmap double gen/large-integer)
                (gen/return :long) (gen/return :double))]))

(def ^:private gen-put-op
  "Generate a single :put op map."
  (gen/fmap (fn [[k v kt vt]]
              {:op :put :dbi "a" :k k :v v :kt kt :vt vt})
            gen-typed-kv))

(def ^:private gen-tx
  "Generate a transaction: a non-empty vector of :put ops."
  (gen/not-empty (gen/vector gen-put-op 1 6)))

(def ^:private gen-tx-sequence
  "Generate a sequence of 1-5 transactions."
  (gen/not-empty (gen/vector gen-tx 1 5)))

(defn- ops->kv-entries
  "Convert an op map to the vector form that transact-kv expects."
  [{:keys [op dbi k v kt vt]}]
  (case op
    :put [:put dbi k v kt vt]
    :del [:del dbi k kt]))

(defn- expected-state
  "Compute the expected key->value map from a sequence of transactions.
   Last-write-wins per [dbi kt k] triple."
  [txs]
  (reduce (fn [acc tx]
            (reduce (fn [m {:keys [op dbi k v kt vt]}]
                      (case op
                        :put (assoc m [dbi kt k] {:v v :vt vt})
                        :del (dissoc m [dbi kt k])))
                    acc tx))
          {} txs))

(test/defspec kv-wal-fuzz-write-read-test
  100
  (prop/for-all
    [txs gen-tx-sequence]
    (let [dir  (u/tmp-dir (str "kv-wal-fuzz-" (UUID/randomUUID)))
          lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                               :kv-wal? true})]
      (try
        (if/open-dbi lmdb "a")
        (let [base-id  ^long (last-wal-id lmdb)
              _        (doseq [tx txs]
                         (if/transact-kv lmdb (mapv ops->kv-entries tx)))
              final-id ^long (last-wal-id lmdb)
              expected (expected-state txs)
              n-txs    (count txs)]

          ;; 1. WAL id advanced by exactly the number of transactions
          (is (= (+ base-id n-txs) final-id)
              "WAL tx-id should advance by number of transactions")

          ;; 2. Every expected value is readable from the KV store
          (let [vals-ok
                (every? (fn [[[dbi kt k] {:keys [v vt]}]]
                          (= v (if/get-value lmdb dbi k kt vt)))
                        expected)]
            (is vals-ok "All expected values should be readable"))

          ;; 3. WAL records round-trip: correct number of records and total ops
          (let [records   (vec (wal/read-wal-records dir base-id final-id))
                total-ops (reduce + (map (comp count :wal/ops) records))
                input-ops (reduce + (map count txs))]
            (is (= n-txs (count records))
                "WAL should contain one record per transaction")
            (is (= input-ops total-ops)
                "Total WAL ops should equal total input ops")
            (is (= (set (range (inc base-id) (inc final-id)))
                   (set (map :wal/tx-id records)))
                "WAL tx-ids should be contiguous"))

          ;; 4. Replay: reset indexed marker, replay WAL, verify values survive
          (binding [c/*trusted-apply* true
                    c/*bypass-wal*    true]
            (if/transact-kv lmdb c/kv-info
                            [[:put c/last-indexed-wal-tx-id 0]
                             [:put c/applied-wal-tx-id 0]]
                            :data :data))
          (let [replay-res (l/replay-kv-wal! lmdb)]
            (is (= final-id (:applied replay-res))
                "Replay should apply up to the committed WAL id"))
          (let [vals-after-replay
                (every? (fn [[[dbi kt k] {:keys [v vt]}]]
                          (= v (if/get-value lmdb dbi k kt vt)))
                        expected)]
            (is vals-after-replay
                "All values should survive WAL replay")))
        (finally
          (if/close-kv lmdb)
          (u/delete-files dir))))))

(test/defspec kv-wal-fuzz-put-del-test
  100
  (prop/for-all
    [puts (gen/not-empty
            (gen/vector
              (gen/tuple gen/large-integer gen/string-alphanumeric)
              1 10))]
    (let [dir  (u/tmp-dir (str "kv-wal-fuzz-pd-" (UUID/randomUUID)))
          lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                               :kv-wal? true})]
      (try
        (if/open-dbi lmdb "a")
        (let [base-id (last-wal-id lmdb)
              ;; Deduplicate: last-write-wins per key
              put-map (reduce (fn [m [k v]] (assoc m k v)) {} puts)
              ;; Transaction 1: put all keys
              _       (if/transact-kv lmdb
                        (mapv (fn [[k v]] [:put "a" k v :long :string]) puts))
              ;; Pick roughly half the unique keys to delete
              all-keys (keys put-map)
              to-del   (take (max 1 (quot (count all-keys) 2)) all-keys)
              _        (if/transact-kv lmdb
                         (mapv (fn [k] [:del "a" k :long]) to-del))
              del-set  (set to-del)
              alive    (remove (fn [[k _]] (del-set k)) put-map)
              final-id (last-wal-id lmdb)]

          ;; Deleted keys should be gone
          (is (every? (fn [k]
                        (nil? (if/get-value lmdb "a" k :long :string)))
                      to-del)
              "Deleted keys should return nil")

          ;; Surviving keys should still be readable
          (is (every? (fn [[k v]]
                        (= v (if/get-value lmdb "a" k :long :string)))
                      alive)
              "Non-deleted keys should be readable")

          ;; WAL should have exactly 2 records (one put-batch, one del-batch)
          (let [records (vec (wal/read-wal-records dir base-id final-id))]
            (is (= 2 (count records))
                "Two WAL records: puts then dels")
            (is (= (count puts)
                   (count (:wal/ops (first records))))
                "Put record should have one op per input put")
            (is (= (count to-del)
                   (count (:wal/ops (second records))))
                "Del record should have one op per deleted key")
            ;; Verify op types in WAL records
            (is (every? #(= :put (:op %)) (:wal/ops (first records))))
            (is (every? #(= :del (:op %)) (:wal/ops (second records))))))
        (finally
          (if/close-kv lmdb)
          (u/delete-files dir))))))

;; ---------------------------------------------------------------------------
;; Fuzz tests for wrapped KV iterators (overlay merge)
;; ---------------------------------------------------------------------------

(def ^:private gen-small-long
  "Small longs (0–30) for key/value generation.  Kept small to maximize
   collision probability between base and overlay."
  (gen/choose 0 30))

;; --- Non-dupsort helpers ---

(def ^:private gen-key-iter-op
  (gen/one-of
    [(gen/fmap (fn [[k v]] {:op :put :k k :v v})
              (gen/tuple gen-small-long gen-small-long))
     (gen/fmap (fn [k] {:op :del :k k})
              gen-small-long)]))

(def ^:private gen-key-iter-ops
  (gen/not-empty (gen/vector gen-key-iter-op 1 30)))

(defn- apply-key-iter-ops
  "Apply put/del ops to a sorted-map reference model (key → value)."
  [m ops]
  (reduce (fn [acc {:keys [op k v]}]
            (case op
              :put (assoc acc k v)
              :del (dissoc acc k)))
          m ops))

(defn- kv-ops->txn [dbi ops]
  (mapv (fn [{:keys [op k v]}]
          (case op
            :put [:put dbi k v :long :long]
            :del [:del dbi k :long]))
        ops))

(defn- verify-key-iter
  "Verify key iteration against sorted-map reference."
  [lmdb dbi ref-map]
  ;; get-range :all
  (is (= (vec ref-map)
         (vec (if/get-range lmdb dbi [:all] :long :long)))
      "get-range :all")
  ;; get-range :all-back
  (is (= (if (empty? ref-map) [] (vec (rseq ref-map)))
         (vec (if/get-range lmdb dbi [:all-back] :long :long)))
      "get-range :all-back")
  ;; key-range
  (is (= (vec (keys ref-map))
         (vec (if/key-range lmdb dbi [:all] :long)))
      "key-range :all")
  ;; range-count
  (is (= (count ref-map)
         (if/range-count lmdb dbi [:all] :long))
      "range-count :all")
  ;; visit-key-range
  (let [visited (volatile! [])]
    (if/visit-key-range lmdb dbi (fn [k] (vswap! visited conj k))
                        [:all] :long false)
    (is (= (vec (keys ref-map)) @visited) "visit-key-range :all"))
  ;; visit (key+value)
  (let [visited (volatile! [])]
    (if/visit lmdb dbi (fn [k v] (vswap! visited conj [k v]))
              [:all] :long :long false)
    (is (= (vec ref-map) @visited) "visit :all"))
  ;; Parametric :closed range
  (when (>= (count ref-map) 2)
    (let [ks  (vec (keys ref-map))
          lo  (first ks)
          hi  (last ks)]
      (is (= (vec (subseq ref-map >= lo <= hi))
             (vec (if/get-range lmdb dbi [:closed lo hi] :long :long)))
          "get-range :closed")
      (when (> (count ks) 2)
        (let [mid (nth ks (quot (count ks) 2))]
          (is (= (vec (subseq ref-map >= mid))
                 (vec (if/get-range lmdb dbi [:at-least mid] :long :long)))
              "get-range :at-least")
          (is (= (vec (subseq ref-map <= mid))
                 (vec (if/get-range lmdb dbi [:at-most mid] :long :long)))
              "get-range :at-most")
          (is (= (vec (subseq ref-map > mid))
                 (vec (if/get-range lmdb dbi [:greater-than mid] :long :long)))
              "get-range :greater-than"))))))

(test/defspec kv-wal-fuzz-key-iter-test
  50
  (prop/for-all
    [ops1 gen-key-iter-ops
     ops2 gen-key-iter-ops]
    (let [dir  (u/tmp-dir (str "kv-wal-fuzz-ki-" (UUID/randomUUID)))
          lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                                :kv-wal? true})]
      (try
        (if/open-dbi lmdb "a")

        ;; Phase 1: overlay-only (base is empty)
        (if/transact-kv lmdb (kv-ops->txn "a" ops1))
        (let [ref1 (apply-key-iter-ops (sorted-map) ops1)]
          (verify-key-iter lmdb "a" ref1)

          ;; Phase 2: replay to base, prune overlay, add new overlay
          (binding [c/*trusted-apply* true
                  c/*bypass-wal*    true]
            (if/transact-kv lmdb c/kv-info
                            [[:put c/last-indexed-wal-tx-id 0]
                             [:put c/applied-wal-tx-id 0]]
                            :data :data))
          (if/flush-kv-indexer! lmdb)

          (if/transact-kv lmdb (kv-ops->txn "a" ops2))
          (let [ref2 (apply-key-iter-ops ref1 ops2)]
            (verify-key-iter lmdb "a" ref2)))
        true
        (finally
          (if/close-kv lmdb)
          (u/delete-files dir))))))

;; --- Dupsort helpers ---

(def ^:private gen-list-iter-op
  (gen/frequency
    [[5 (gen/fmap (fn [[k vs]]
                    {:op :put-list :k k :vs (vec (distinct vs))})
                  (gen/tuple gen-small-long
                             (gen/not-empty (gen/vector gen-small-long 1 5))))]
     [3 (gen/fmap (fn [[k vs]]
                    {:op :del-list :k k :vs (vec (distinct vs))})
                  (gen/tuple gen-small-long
                             (gen/not-empty (gen/vector gen-small-long 1 3))))]
     [2 (gen/fmap (fn [k] {:op :wipe :k k})
                  gen-small-long)]]))

(def ^:private gen-list-iter-ops
  (gen/not-empty (gen/vector gen-list-iter-op 1 20)))

(defn- apply-list-iter-ops
  "Apply list ops to a sorted-map of key → sorted-set-of-values."
  [m ops]
  (reduce (fn [acc {:keys [op k vs]}]
            (case op
              :put-list (update acc k
                                (fn [s] (into (or s (sorted-set)) vs)))
              :del-list (if-let [s (get acc k)]
                          (let [new-s (reduce disj s vs)]
                            (if (empty? new-s) (dissoc acc k)
                                (assoc acc k new-s)))
                          acc)
              :wipe     (dissoc acc k)))
          m ops))

(defn- flatten-list-ref
  "Flatten sorted-map {k → sorted-set} to [[k v] …] pairs."
  ([m] (flatten-list-ref m false false))
  ([m key-back? val-back?]
   (vec (let [key-seq (if key-back?
                        (when (seq m) (rseq m))
                        (seq m))]
          (for [[k vs] key-seq
                v (if val-back? (rseq vs) (seq vs))]
            [k v])))))

(defn- list-ops->txn [dbi ops]
  (mapv (fn [{:keys [op k vs]}]
          (case op
            :put-list [:put-list dbi k vs :long :long]
            :del-list [:del-list dbi k vs :long :long]
            :wipe     [:del dbi k :long]))
        ops))

(defn- verify-list-iter
  "Verify list iteration (wrap-list-iterable) against reference model."
  [lmdb dbi ref-map]
  ;; list-range [:all] [:all]
  (is (= (flatten-list-ref ref-map)
         (vec (if/list-range lmdb dbi [:all] :long [:all] :long)))
      "list-range :all :all")
  ;; list-range [:all-back] [:all]
  (is (= (flatten-list-ref ref-map true false)
         (vec (if/list-range lmdb dbi [:all-back] :long [:all] :long)))
      "list-range :all-back :all")
  ;; list-range [:all] [:all-back]
  (is (= (flatten-list-ref ref-map false true)
         (vec (if/list-range lmdb dbi [:all] :long [:all-back] :long)))
      "list-range :all :all-back")
  ;; list-range-count
  (is (= (reduce + 0 (map count (vals ref-map)))
         (if/list-range-count lmdb dbi [:all] :long [:all] :long))
      "list-range-count :all :all")
  ;; visit-list-range
  (let [visited (volatile! [])]
    (if/visit-list-range lmdb dbi (fn [k v] (vswap! visited conj [k v]))
                         [:all] :long [:all] :long false)
    (is (= (flatten-list-ref ref-map) @visited)
        "visit-list-range :all :all"))
  ;; Parametric value sub-range for a key that exists
  (when (seq ref-map)
    (let [[k vs] (first ref-map)
          v-lo   (first vs)
          v-hi   (last vs)
          expected (vec (for [v (subseq vs >= v-lo <= v-hi)] [k v]))]
      (is (= expected
             (vec (if/list-range lmdb dbi [:closed k k] :long
                                 [:closed v-lo v-hi] :long)))
          "list-range :closed key :closed val"))))

(defn- verify-list-key-range-iter
  "Verify list-key-range iteration (wrap-list-key-range-full-val-iterable)."
  [lmdb dbi ref-map]
  ;; visit-list-key-range [:all]
  (let [visited (volatile! [])]
    (if/visit-list-key-range lmdb dbi (fn [k v] (vswap! visited conj [k v]))
                             [:all] :long :long false)
    (is (= (flatten-list-ref ref-map) @visited)
        "visit-list-key-range :all"))
  ;; visit-list-key-range [:all-back] — underlying iterator is forward-only
  (let [visited (volatile! [])]
    (if/visit-list-key-range lmdb dbi (fn [k v] (vswap! visited conj [k v]))
                             [:all-back] :long :long false)
    (is (= (flatten-list-ref ref-map) @visited)
        "visit-list-key-range :all-back"))
  ;; Parametric :closed key range
  (when (>= (count ref-map) 2)
    (let [lo      (first (keys ref-map))
          hi      (last (keys ref-map))
          sub     (into (sorted-map) (subseq ref-map >= lo <= hi))
          visited (volatile! [])]
      (if/visit-list-key-range lmdb dbi (fn [k v] (vswap! visited conj [k v]))
                               [:closed lo hi] :long :long false)
      (is (= (flatten-list-ref sub) @visited)
          "visit-list-key-range :closed"))))

(test/defspec kv-wal-fuzz-list-iter-test
  50
  (prop/for-all
    [ops1 gen-list-iter-ops
     ops2 gen-list-iter-ops]
    (let [dir  (u/tmp-dir (str "kv-wal-fuzz-li-" (UUID/randomUUID)))
          lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                                :kv-wal? true})]
      (try
        (if/open-list-dbi lmdb "l")

        ;; Phase 1: overlay-only
        (if/transact-kv lmdb (list-ops->txn "l" ops1))
        (let [ref1 (apply-list-iter-ops (sorted-map) ops1)]
          (verify-list-iter lmdb "l" ref1)
          (verify-list-key-range-iter lmdb "l" ref1)

          ;; Phase 2: replay to base, prune overlay, add new overlay
          (binding [c/*trusted-apply* true
                  c/*bypass-wal*    true]
            (if/transact-kv lmdb c/kv-info
                            [[:put c/last-indexed-wal-tx-id 0]
                             [:put c/applied-wal-tx-id 0]]
                            :data :data))
          (if/flush-kv-indexer! lmdb)

          (if/transact-kv lmdb (list-ops->txn "l" ops2))
          (let [ref2 (apply-list-iter-ops ref1 ops2)]
            (verify-list-iter lmdb "l" ref2)
            (verify-list-key-range-iter lmdb "l" ref2)))
        true
        (finally
          (if/close-kv lmdb)
          (u/delete-files dir))))))

;; ---------------------------------------------------------------------------
;; TX-Log API tests
;; ---------------------------------------------------------------------------

(deftest test-open-tx-log-basic
  (let [dir  (u/tmp-dir (str "kv-wal-txlog-basic-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                              :kv-wal? true})]
    (try
      (if/open-dbi lmdb "a")
      (let [base-id ^long (last-wal-id lmdb)]
        (if/transact-kv lmdb [[:put "a" 1 "x"]])
        (if/transact-kv lmdb [[:put "a" 2 "y"]])
        (if/transact-kv lmdb [[:put "a" 3 "z"]])
        (let [records (vec (if/open-tx-log lmdb base-id))]
          (is (= 3 (count records)))
          (is (every? #(contains? % :wal/tx-id) records))
          (is (every? #(contains? % :wal/ops) records))
          (is (= (set (range (inc base-id) (+ base-id 4)))
                 (set (map :wal/tx-id records))))))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))

(deftest test-open-tx-log-range
  (let [dir  (u/tmp-dir (str "kv-wal-txlog-range-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                              :kv-wal? true})]
    (try
      (if/open-dbi lmdb "a")
      (let [base-id ^long (last-wal-id lmdb)]
        (if/transact-kv lmdb [[:put "a" 1 "x"]])
        (if/transact-kv lmdb [[:put "a" 2 "y"]])
        (if/transact-kv lmdb [[:put "a" 3 "z"]])
        ;; Read only the first two records
        (let [records (vec (if/open-tx-log lmdb base-id (+ base-id 2)))]
          (is (= 2 (count records)))
          (is (= #{(+ base-id 1) (+ base-id 2)}
                 (set (map :wal/tx-id records)))))
        ;; Read from middle
        (let [records (vec (if/open-tx-log lmdb (+ base-id 1)))]
          (is (= 2 (count records)))
          (is (= #{(+ base-id 2) (+ base-id 3)}
                 (set (map :wal/tx-id records))))))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))

(deftest test-open-tx-log-empty
  (let [dir  (u/tmp-dir (str "kv-wal-txlog-empty-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                              :kv-wal? true})]
    (try
      (if/open-dbi lmdb "a")
      (if/transact-kv lmdb [[:put "a" 1 "x"]])
      (let [committed (last-wal-id lmdb)
            records   (vec (if/open-tx-log lmdb committed))]
        (is (empty? records)))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))

(deftest test-open-tx-log-wal-disabled
  (let [dir  (u/tmp-dir (str "kv-wal-txlog-disabled-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags (conj c/default-env-flags :nosync)})]
    (try
      (if/open-dbi lmdb "a")
      (if/transact-kv lmdb [[:put "a" 1 "x"]])
      (let [records (vec (if/open-tx-log lmdb 0))]
        (is (empty? records)))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))

(deftest test-gc-wal-segments-basic
  (let [dir  (u/tmp-dir (str "kv-wal-gc-basic-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags                  (conj c/default-env-flags :nosync)
                              :kv-wal?                true
                              :wal-segment-max-bytes  1})]
    (try
      (if/open-dbi lmdb "a")
      ;; Write enough txs to force segment rotation (max 1 byte per segment)
      (dotimes [i 5]
        (if/transact-kv lmdb [[:put "a" i (str "v" i)]]))
      ;; Should have multiple segment files
      (let [files-before (count (wal/segment-files dir))]
        (is (> files-before 1))
        ;; Advance indexed watermark via replay
        (binding [c/*trusted-apply* true
                  c/*bypass-wal*    true]
          (if/transact-kv lmdb c/kv-info
                          [[:put c/last-indexed-wal-tx-id 0]
                           [:put c/applied-wal-tx-id 0]]
                          :data :data))
        (if/flush-kv-indexer! lmdb)
        ;; Now GC should delete old segments
        (let [result (if/gc-wal-segments! lmdb)]
          (is (pos? (:deleted result)))
          (is (pos? (:retained result)))
          ;; Active segment should always remain
          (is (>= (count (wal/segment-files dir)) 1))))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))

(deftest test-gc-wal-segments-retain
  (let [dir  (u/tmp-dir (str "kv-wal-gc-retain-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags                  (conj c/default-env-flags :nosync)
                              :kv-wal?                true
                              :wal-segment-max-bytes  1})]
    (try
      (if/open-dbi lmdb "a")
      (dotimes [i 5]
        (if/transact-kv lmdb [[:put "a" i (str "v" i)]]))
      ;; Advance indexed watermark
      (binding [c/*trusted-apply* true
                c/*bypass-wal*    true]
        (if/transact-kv lmdb c/kv-info
                        [[:put c/last-indexed-wal-tx-id 0]
                         [:put c/applied-wal-tx-id 0]]
                        :data :data))
      (if/flush-kv-indexer! lmdb)
      ;; retain-wal-id = 0 should prevent GC from deleting anything
      ;; since gc watermark = min(indexed, retain) = 0
      (let [result (if/gc-wal-segments! lmdb 0)]
        (is (zero? (:deleted result))))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))

(deftest test-gc-wal-segments-active-segment
  (let [dir  (u/tmp-dir (str "kv-wal-gc-active-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                              :kv-wal? true})]
    (try
      (if/open-dbi lmdb "a")
      (if/transact-kv lmdb [[:put "a" 1 "x"]])
      ;; With only one segment, GC should never delete it
      (binding [c/*trusted-apply* true
                c/*bypass-wal*    true]
        (if/transact-kv lmdb c/kv-info
                        [[:put c/last-indexed-wal-tx-id 0]
                         [:put c/applied-wal-tx-id 0]]
                        :data :data))
      (if/flush-kv-indexer! lmdb)
      (let [result (if/gc-wal-segments! lmdb)]
        (is (zero? (:deleted result)))
        (is (= 1 (:retained result)))
        (is (= 1 (count (wal/segment-files dir)))))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))
