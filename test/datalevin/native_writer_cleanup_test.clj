(ns datalevin.native-writer-cleanup-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.binding.cpp :as cpp]
            [datalevin.core :as d]
            [datalevin.interface :as i]
            [datalevin.kv :as kv]
            [datalevin.lmdb :as l]
            [datalevin.test.core :refer [db-fixture]]
            [datalevin.tx-state.lifetime :as lifetime]
            [datalevin.util :as u])
  (:import [datalevin.binding.cpp Rtx]
           [datalevin.cpp Util$MapFullException]
           [java.util.concurrent.atomic AtomicBoolean]))

(use-fixtures :each db-fixture)

(defn- caught [f]
  (try (f) (catch Throwable e e)))

(defn- closed? [^Rtx rtx]
  (.get ^AtomicBoolean (.-closed? rtx)))

(defn- with-kv [f]
  (let [dir (u/tmp-dir (str "native-writer-cleanup-" (random-uuid)))
        db (d/open-kv dir {:wal? false})
        raw (kv/raw-lmdb db)]
    (try
      (d/open-dbi db "migration-entities")
      (f db raw)
      (finally
        ;; Clean up even when testing the unfixed failure path.
        (when @(l/write-txn raw)
          (i/abort-transact-kv raw)
          (i/close-transact-kv raw))
        (d/close-kv db)
        (u/delete-files dir)))))

(defn- assert-next-write! [db raw]
  (let [writer (i/open-transact-kv raw)]
    (i/transact-kv writer "migration-entities" [[:put :next 42]])
    (is (= :committed (i/close-transact-kv raw))))
  (is (= 42 (d/get-value db "migration-entities" :next))))

(deftest commit-errors-release-original-and-replacement-writers
  (doseq [replace? [false true]]
    (testing (str "replacement writer " replace?)
      (with-kv
        (fn [db raw]
          (let [writer (i/open-transact-kv raw)
                original @(l/write-txn raw)
                replacement (atom nil)
                fault (AssertionError. "commit callback failed")]
            (i/transact-kv writer "migration-entities" [[:put :staged :original]])
            (is (identical?
                  fault
                  (binding [cpp/*before-write-commit-fn*
                            (fn [_]
                              (when replace?
                                (let [next-writer (i/open-transact-kv raw)]
                                  (reset! replacement @(l/write-txn raw))
                                  (i/transact-kv next-writer "migration-entities"
                                                 [[:put :staged :replacement]])))
                              (throw fault))]
                    (caught #(i/close-transact-kv raw)))))
            (is (nil? @(l/write-txn raw)))
            (is (closed? original))
            (when replace? (is (closed? @replacement)))
            (is (= :txlog/transaction-view-invalidated
                   (:error (ex-data (caught #(i/get-value writer "migration-entities"
                                                         :staged))))))
            (is (nil? (d/get-value db "migration-entities" :staged)))
            (assert-next-write! db raw)))))))

(deftest failed-writer-reset-detaches-the-closed-transaction
  (with-kv
    (fn [_ raw]
      (let [guard (lifetime/create)
            _ (vswap! (i/kv-info raw) assoc :native-lifetime guard)
            writer (i/open-transact-kv raw)
            original @(l/write-txn raw)]
        ;; A fence can arrive between releasing the old writer's lease and
        ;; acquiring the replacement. No new native transaction may start.
        (lifetime/fence! guard)
        (is (= :txlog/native-fenced
               (:error (ex-data (caught #(l/reset-write raw))))))
        (is (closed? original))
        (is (nil? @(l/write-txn raw)))
        (is (zero? (:native-users (lifetime/state guard))))
        (is (= :txlog/transaction-view-invalidated
               (:error (ex-data (caught #(i/get-value writer "migration-entities"
                                                     :staged))))))))))

(deftest commit-map-growth-releases-the-failed-writer
  (with-kv
    (fn [db raw]
      (let [writer (i/open-transact-kv raw)
            original @(l/write-txn raw)]
        (i/transact-kv writer "migration-entities" [[:put :staged :aborted]])
        (is (true?
              (:resized
                (ex-data
                  (binding [cpp/*before-write-commit-fn*
                            (fn [_] (throw (Util$MapFullException. "commit map full")))]
                    (caught #(i/close-transact-kv raw)))))))
        (is (nil? @(l/write-txn raw)))
        (is (closed? original))
        (is (nil? (d/get-value db "migration-entities" :staged)))
        (assert-next-write! db raw)))))
