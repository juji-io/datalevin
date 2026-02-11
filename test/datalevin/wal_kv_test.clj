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
   [java.util UUID]
   [datalevin.lmdb IListRandKeyValIterable IListRandKeyValIterator]))

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
      (let [base-id (last-wal-id lmdb)
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
        (is (= (inc base-id) (last-wal-id lmdb)))

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
      (let [base-id (last-wal-id lmdb)]
        (if/transact-kv lmdb [[:put "a" 1 "x"]
                              [:put "a" 2 "y"]])
        (let [wm     (if/kv-wal-watermarks lmdb)
              wal-id (:last-committed-wal-tx-id wm)
              idx-id (:last-indexed-wal-tx-id wm)
              user-id (:last-committed-user-tx-id wm)
              records (vec (wal/read-wal-records dir base-id wal-id))
              record (last records)]
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
      (let [base-id (last-wal-id lmdb)
            ex      (try
                      (binding [c/*failpoint* {:step :step-3
                                               :phase :after
                                               :fn #(throw (ex-info "fp-after" {}))}]
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
        lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                             :kv-wal? true
                             :wal-meta-flush-max-txs 1})]
    (try
      (if/open-dbi lmdb "a")
      (let [base-id (last-wal-id lmdb)]
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
        lmdb      (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                                  :kv-wal? true
                                  :wal-meta-flush-max-txs 1
                                  :wal-meta-flush-max-ms 60000})
        meta-path (str dir u/+separator+ c/wal-dir u/+separator+ c/wal-meta-file)]
    (try
      (if/open-dbi lmdb "a")
      (let [base-id (last-wal-id lmdb)]
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
        lmdb      (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                                  :kv-wal? true
                                  :wal-meta-flush-max-txs 3
                                  :wal-meta-flush-max-ms 60000})
        meta-path (str dir u/+separator+ c/wal-dir u/+separator+ c/wal-meta-file)]
    (try
      (if/open-dbi lmdb "a")
      (let [base-id (last-wal-id lmdb)]
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
        (binding [c/*trusted-apply* true]
          (if/transact-kv lmdb c/kv-info
                          [[:put c/last-indexed-wal-tx-id 0]]
                          :attr :id))
        (is (= 0 (if/get-value lmdb c/kv-info
                               c/last-indexed-wal-tx-id :attr :id)))
        ;; Applied marker starts at 0; replay should apply WAL records.
        (is (nil? (if/get-value lmdb c/kv-info
                                c/applied-wal-tx-id :attr :id)))

        (let [res (l/replay-kv-wal! lmdb)]
          (is (= {:from 0 :to committed-id :applied committed-id} res)))
        (is (= committed-id (if/get-value lmdb c/kv-info
                                          c/last-indexed-wal-tx-id :attr :id)))
        (is (= committed-id (if/get-value lmdb c/kv-info
                                          c/applied-wal-tx-id :attr :id)))
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
      (binding [c/*trusted-apply* true]
        (if/transact-kv lmdb c/kv-info
                        [[:put c/last-indexed-wal-tx-id 0]
                         [:put c/applied-wal-tx-id 0]]
                        :attr :id))

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
                 (if/transact-kv lmdb [[:put c/kv-wal 1 (byte-array [1]) :id :raw]])
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
        (binding [c/*trusted-apply* true]
          (if/transact-kv lmdb c/kv-info
                          [[:put c/last-indexed-wal-tx-id 0]
                           [:put c/applied-wal-tx-id 0]]
                          :attr :id))

        (is (= {:from 0 :to base-id :applied base-id}
               (l/replay-kv-wal! lmdb base-id)))
        (is (= base-id (if/get-value lmdb c/kv-info
                                     c/last-indexed-wal-tx-id :attr :id)))
        (is (= base-id (if/get-value lmdb c/kv-info
                                     c/applied-wal-tx-id :attr :id)))

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
      (binding [c/*trusted-apply* true]
        (if/transact-kv lmdb c/kv-info
                        [[:del c/applied-wal-tx-id]
                         [:put c/legacy-applied-tx-id 0]
                         [:put c/last-indexed-wal-tx-id 0]]
                        :attr :id))

      (let [committed-id (last-wal-id lmdb)]
        (is (= {:from 0 :to committed-id :applied committed-id}
               (l/replay-kv-wal! lmdb)))
        (is (= committed-id
               (if/get-value lmdb c/kv-info c/applied-wal-tx-id :attr :id)))
        (is (= committed-id
               (if/get-value lmdb c/kv-info
                             c/last-indexed-wal-tx-id :attr :id))))
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
      (binding [c/*trusted-apply* true]
        (if/transact-kv lmdb c/kv-info
                        [[:put c/last-indexed-wal-tx-id 0]
                         [:put c/applied-wal-tx-id 0]]
                        :attr :id))
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
      (let [vals (volatile! [])
            op   (fn [^IListRandKeyValIterable iterable]
                   (with-open [^IListRandKeyValIterator iter
                               (l/val-iterator iterable)]
                     (loop [next? (l/seek-key iter "a" :string)]
                       (when next?
                         (vswap! vals conj (b/read-buffer (l/next-val iter)
                                                          :long))
                         (recur (l/has-next-val iter))))))]
        (if/operate-list-val-range lmdb "l" op [:all] :long)
        (is (= [1 3 6] @vals)))
      (let [vals (volatile! [])
            op   (fn [^IListRandKeyValIterable iterable]
                   (with-open [^IListRandKeyValIterator iter
                               (l/val-iterator iterable)]
                     (loop [next? (l/seek-key iter "a" :string)]
                       (when next?
                         (vswap! vals conj (b/read-buffer (l/next-val iter)
                                                          :long))
                         (recur (l/has-next-val iter))))))]
        (if/operate-list-val-range lmdb "l" op [:closed 2 6] :long)
        (is (= [3 6] @vals)))

      ;; Replay catches base up and prunes committed overlay <= indexed watermark.
      ;; Use trusted apply to avoid emitting WAL records for kv-info setup.
      (binding [c/*trusted-apply* true]
        (if/transact-kv lmdb c/kv-info
                        [[:put c/last-indexed-wal-tx-id 0]
                         [:put c/applied-wal-tx-id 0]]
                        :attr :id))
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
      (let [base-id (last-wal-id lmdb)]
        (if/transact-kv lmdb [[:put "a" 1 "x"]])
        (if/transact-kv lmdb [[:put "a" 2 "y"]])
        (let [committed-id (+ base-id 2)
              partial-id   (inc base-id)]
          (is (= {:last-committed-wal-tx-id committed-id
                  :last-indexed-wal-tx-id 0
                  :last-committed-user-tx-id committed-id}
                 (l/kv-wal-watermarks lmdb)))

          (binding [c/*trusted-apply* true]
            (if/transact-kv lmdb c/kv-info
                            [[:put c/last-indexed-wal-tx-id 0]]
                            :attr :id))

          (is (= {:indexed-wal-tx-id partial-id
                  :committed-wal-tx-id committed-id
                  :drained? false}
                 (l/flush-kv-indexer! lmdb partial-id)))
          (is (= {:indexed-wal-tx-id committed-id
                  :committed-wal-tx-id committed-id
                  :drained? true}
                 (l/flush-kv-indexer! lmdb)))
          (is (= {:last-committed-wal-tx-id committed-id
                  :last-indexed-wal-tx-id committed-id
                  :last-committed-user-tx-id committed-id}
                 (l/kv-wal-watermarks lmdb)))))

      (is (= {:last-committed-wal-tx-id 0
              :last-indexed-wal-tx-id 0
              :last-committed-user-tx-id 0}
             (l/kv-wal-watermarks lmdb2)))
      (is (= {:indexed-wal-tx-id 0
              :committed-wal-tx-id 0
              :drained? true}
             (l/flush-kv-indexer! lmdb2)))
      (finally
        (if/close-kv lmdb)
        (if/close-kv lmdb2)
        (u/delete-files dir)
        (u/delete-files dir2)))))

(deftest kv-wal-admin-test
  (let [dir  (u/tmp-dir (str "datalevin-kv-wal-admin-test-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags (conj c/default-env-flags :nosync)
                             :kv-wal? true})]
    (try
      (if/open-dbi lmdb "misc")
      (let [base-id (:last-committed-wal-tx-id
                     (if/kv-wal-watermarks lmdb))]
        (if/transact-kv lmdb [[:put "misc" 1 "x"]])
        (if/transact-kv lmdb [[:put "misc" 2 "y"]])
        (let [committed-id (+ base-id 2)
              partial-id   (inc base-id)]
          (is (= {:last-committed-wal-tx-id committed-id
                  :last-indexed-wal-tx-id 0
                  :last-committed-user-tx-id committed-id}
                 (if/kv-wal-watermarks lmdb)))

          (binding [c/*trusted-apply* true]
            (if/transact-kv lmdb c/kv-info
                            [[:put c/last-indexed-wal-tx-id 0]]
                            :attr :id))

          (is (= {:indexed-wal-tx-id partial-id
                  :committed-wal-tx-id committed-id
                  :drained? false}
                 (l/flush-kv-indexer! lmdb partial-id)))
          (is (= {:indexed-wal-tx-id committed-id
                  :committed-wal-tx-id committed-id
                  :drained? true}
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
        (let [base-id   (last-wal-id lmdb)
              _         (doseq [tx txs]
                          (if/transact-kv lmdb (mapv ops->kv-entries tx)))
              final-id  (last-wal-id lmdb)
              expected  (expected-state txs)
              n-txs     (count txs)]

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
          (let [records    (vec (wal/read-wal-records dir base-id final-id))
                total-ops  (reduce + (map (comp count :wal/ops) records))
                input-ops  (reduce + (map count txs))]
            (is (= n-txs (count records))
                "WAL should contain one record per transaction")
            (is (= input-ops total-ops)
                "Total WAL ops should equal total input ops")
            (is (= (set (range (inc base-id) (inc final-id)))
                   (set (map :wal/tx-id records)))
                "WAL tx-ids should be contiguous"))

          ;; 4. Replay: reset indexed marker, replay WAL, verify values survive
          (binding [c/*trusted-apply* true]
            (if/transact-kv lmdb c/kv-info
                            [[:put c/last-indexed-wal-tx-id 0]
                             [:put c/applied-wal-tx-id 0]]
                            :attr :id))
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
