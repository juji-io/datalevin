(ns datalevin.native-rank-cleanup-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [datalevin.core :as d]
            [datalevin.test.core :refer [db-fixture]]
            [datalevin.util :as u])
  (:import [org.bytedeco.javacpp Pointer]))

(use-fixtures :each db-fixture)

(defn- with-kv [opts f]
  (let [dir (u/tmp-dir (str "native-rank-cleanup-" (random-uuid)))
        db (d/open-kv dir {:wal? false})]
    (try
      (d/open-dbi db "rank" opts)
      (d/transact-kv db [[:put "rank" 30 300 :long :long]
                         [:put "rank" 10 100 :long :long]
                         [:put "rank" 20 200 :long :long]])
      ;; Populate the reusable reader and drain unrelated temporary pointers
      ;; before measuring individual public API calls.
      (d/get-value db "rank" 10 :long :long)
      (System/gc)
      (Pointer/deallocateReferences)
      (f db)
      (finally
        (d/close-kv db)
        (u/delete-files dir)))))

(defn- balanced-rank [db k]
  (let [bytes (Pointer/totalBytes)
        pointers (Pointer/totalCount)]
    (try
      (d/get-rank db "rank" k :long)
      (finally
        ;; No GC between the call and these checks: cleanup must be immediate,
        ;; including MDB_NOTFOUND and native errors.
        (is (= bytes (Pointer/totalBytes)) "get-rank retained native bytes")
        (is (= pointers (Pointer/totalCount)) "get-rank retained native pointers")))))

(deftest rank-lookups-release-temporary-native-pointers
  (with-kv
    nil
    (fn [db]
      (dotimes [_ 20]
        (doseq [[k rank] [[10 0] [20 1] [30 2] [99 nil]]]
          (is (= rank (balanced-rank db k))))))))

(deftest native-rank-errors-release-temporary-native-pointers
  (with-kv
    {:flags #{:create}}
    (fn [db]
      (dotimes [_ 20]
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Fail to get-rank"
                              (balanced-rank db 10)))))))
