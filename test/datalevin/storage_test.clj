(ns datalevin.storage-test
  (:require [datalevin.storage :as sut]
            [datalevin.datom :as d]
            [datalevin.bits :as b]
            [taoensso.timbre :as log]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.clojure-test :as test]
            [clojure.test.check.properties :as prop]
            [datalevin.constants :as c]
            [clojure.test :refer [deftest is use-fixtures]])
  (:import [java.util UUID]
           [datalevin.storage Store]
           [org.lmdbjava CursorIterable$KeyVal]))

(def ^:dynamic ^Store store nil)

(defn store-test-fixture
  [f]
  (let [dir (str "/tmp/store-test-" (UUID/randomUUID))]
    (with-redefs [store (sut/open dir)]
      (f)
      (sut/close store)
      (b/delete-files dir))))


(use-fixtures :each store-test-fixture)

(deftest basic-ops-test
  (is (= c/gt0 (sut/max-gt store)))
  (is (= 1 (sut/max-aid store)))
  (is (= c/implicit-schema (sut/schema store)))
  (is (= c/e0 (sut/init-max-eid store)))
  (let [a :a/b
        p {:db/valueType :db.type/uuid
           :db/index     true}
        v (UUID/randomUUID)
        d (d/datom c/e0 a v c/tx0)
        s (assoc (sut/schema store) a (assoc p :db/aid 1))]
    (sut/swap-attr store a merge p)
    (is (= s (sut/schema store)))
    (sut/insert store d)
    (is (= 1 (sut/datom-count store c/eav)))
    (is (= 1 (sut/datom-count store c/aev)))
    (is (= 1 (sut/datom-count store c/ave)))
    (is (= 1 (sut/datom-count store c/vae)))
    )
  )
