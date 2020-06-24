(ns datalevin.storage-test
  (:require [datalevin.storage :as sut]
            [datalevin.datom :as d]
            [datalevin.bits :as b]
            [taoensso.timbre :as log]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.clojure-test :as test]
            [clojure.test.check.properties :as prop]
            [datalevin.constants :as c]
            [clojure.test :refer [deftest is]])
  (:import [java.util UUID]
           [org.lmdbjava CursorIterable$KeyVal]))

(def ^:dynamic store nil)

(defn store-test-fixture
  [f]
  (let [dir (str "/tmp/store-test-" (UUID/randomUUID))]
    (with-redefs [store (sut/open dir)]
      (f)
      (sut/close store)
      (b/delete-files dir))))

(def anunnaki [])
