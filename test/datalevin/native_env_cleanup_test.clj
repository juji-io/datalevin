(ns datalevin.native-env-cleanup-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.util :as u])
  (:import [datalevin.cpp Env Txn Util$DTLVException]
           [datalevin.dtlvnative DTLV]))

(defn- create-env [dir readers]
  (Env/create dir (* 1024 1024) readers 4 DTLV/MDB_NOTLS))

(defn- assert-can-open! [dir]
  (let [^Env env (create-env dir 16)]
    (try
      (is (not (.isClosed env)))
      (let [^Txn txn (Txn/createReadOnly env)]
        (try
          (is (not (neg? (.id txn))))
          (finally (.close txn))))
      (finally (.close env)))
    (.close env)
    (is (.isClosed env))))

(deftest environment-setup-errors-preserve-the-native-error
  (let [dir (u/tmp-dir (str "native-env-setup-failure-" (random-uuid)))]
    (u/create-dirs dir)
    (try
      (dotimes [_ 20]
        (is (thrown-with-msg? Util$DTLVException #"Invalid argument"
                              (create-env dir 0))))
      (assert-can-open! dir)
      (finally (u/delete-files dir)))))

(deftest environment-open-errors-allow-a-later-successful-open
  (let [dir (u/tmp-dir (str "native-env-open-failure-" (random-uuid)))]
    (try
      (dotimes [_ 20]
        (is (thrown? Util$DTLVException (create-env dir 16))))
      (u/create-dirs dir)
      (assert-can-open! dir)
      (finally (u/delete-files dir)))))
