(ns datalevin.map-resize-test
  (:require
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.test :refer [deftest testing are is use-fixtures]]
   [datalevin.core :as d]
   [datalevin.datom :as dd]
   [datalevin.interpret :as i]
   [datalevin.util :as u]
   [datalevin.constants :as c :refer [tx0]])
  (:import
   [java.util UUID]))

(use-fixtures :each db-fixture)

(deftest million-txns-map-resize-test
  (let [dir  (u/tmp-dir (str "million-txns-test-" (UUID/randomUUID)))
        conn (d/create-conn
               dir {}
               {:kv-opts {:flags (conj c/default-env-flags :nosync)}})]

    (dotimes [i 1000000] (d/transact! conn [{:foo i}]))
    (is (= 1000000 (count (d/datoms @conn :eav))))

    ;; TODO this will segfault in native when close db in graalvm
    (when-not (u/graal?)
      ;; this will blow through 100 MiB boundary
      (dotimes [i 1000000] (d/transact! conn [{:foo i}]))
      (is (= 2000000 (count (d/datoms @conn :eav)))))

    (d/close conn)
    (u/delete-files dir)))

(deftest map-resize-clear-test
  (let [dir (u/tmp-dir (str "clear-test-" (UUID/randomUUID)))]
    (dotimes [_ 10]
      (let [conn (d/create-conn
                   dir {:buggy/key {:db/valueType :db.type/string
                                    :db/unique    :db.unique/identity}}
                   {:kv-opts {:mapsize 1
                              :flags   (conj c/default-env-flags :nosync)}})]
        (d/transact! conn (for [i (range 100000)]
                            {:buggy/key  (format "%20d" i)
                             :buggy/val  (format "bubba-%d" i)
                             :buggy/time (System/currentTimeMillis)}))
        (is (= 300000 (count (d/datoms (d/db conn) :eav))))
        (d/close conn)
        (is (d/closed? conn))

        (let [conn1 (d/create-conn dir)]
          (d/clear conn1)
          (is (d/closed? conn1)))

        (let [conn2 (d/create-conn dir)]
          (is (= 0 (count (d/datoms (d/db conn2) :eav))))
          (d/transact! conn2 [{:buggy/key  (format "%20d" 100001)
                               :buggy/val  (format "bubba-%d" 100)
                               :buggy/time (System/currentTimeMillis)}])

          (is (= 3 (count (d/datoms (d/db conn2) :eav))))
          (d/close conn2))

        (let [conn1 (d/create-conn dir)]
          (d/clear conn1)
          (is (d/closed? conn1)))))
    (u/delete-files dir)))
