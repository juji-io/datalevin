(ns datalevin.native-dbi-cleanup-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [datalevin.util :as u])
  (:import [datalevin.cpp Dbi Env Txn Util$DTLVException]
           [datalevin.dtlvnative DTLV]
           [org.bytedeco.javacpp Pointer]))

(defn- with-env [f]
  (let [dir (u/tmp-dir (str "native-dbi-cleanup-" (random-uuid)))]
    (u/create-dirs dir)
    (let [^Env env (Env/create dir (* 1024 1024) 16 1 DTLV/MDB_NOTLS)]
      (try
        (let [^Dbi dbi (Dbi/create env "existing" DTLV/MDB_CREATE)]
          (try (f env dbi dir) (finally (.close dbi))))
        (finally
          (.close env)
          (u/delete-files dir))))))

(use-fixtures
  :once
  (fn [f]
    ;; Load JNI and drain unrelated allocations before measuring failures.
    (with-env (fn [_ _ _]))
    (System/gc)
    (Pointer/deallocateReferences)
    (f)))

(defn- balanced-call [f]
  (let [bytes (Pointer/totalBytes)
        pointers (Pointer/totalCount)]
    (try
      (f)
      (finally
        ;; No GC: constructor failures must immediately release the IntPointer.
        (is (= bytes (Pointer/totalBytes)) "DBI creation retained native bytes")
        (is (= pointers (Pointer/totalCount)) "DBI creation retained native pointers")))))

(deftest failed-dbi-opens-release-the-pointer-and-owned-writer
  (with-env
    (fn [^Env env _ _]
      (dotimes [_ 20]
        (is (thrown-with-msg? Util$DTLVException #"MDB_DBS_FULL"
                             (balanced-call #(Dbi/create env "overflow" DTLV/MDB_CREATE))))
        ;; The failed constructor must release the writer for another DBI open.
        (balanced-call
          #(let [^Dbi dbi (Dbi/create env "existing" 0)]
             (try (is (pos? (.get dbi))) (finally (.close dbi)))))))))

(deftest failed-transaction-begins-release-the-dbi-pointer
  (with-env
    (fn [^Env env _ dir]
      (.close env)
      (let [^Env readonly (Env/create dir (* 1024 1024) 16 1
                                     (bit-or DTLV/MDB_NOTLS DTLV/MDB_RDONLY))]
        (try
          (dotimes [_ 20]
            (is (thrown? Util$DTLVException
                         (balanced-call #(Dbi/create readonly "existing" 0)))))
          (finally (.close readonly)))))))

(deftest failed-dbi-opens-preserve-caller-owned-transactions
  (with-env
    (fn [^Env env _ _]
      (let [^Txn txn (Txn/create env)]
        (try
          (dotimes [_ 20]
            (is (thrown-with-msg? Util$DTLVException #"MDB_DBS_FULL"
                                 (balanced-call #(Dbi/open txn "overflow" DTLV/MDB_CREATE))))
            (is (pos? (.id txn))))
          (balanced-call
            #(let [^Dbi dbi (Dbi/open txn "existing" 0)]
               (try (is (pos? (.get dbi))) (finally (.close dbi)))))
          (.commit txn)
          (finally (.close txn)))))))
