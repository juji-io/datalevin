(ns datalevin.vector-test
  (:require
   [datalevin.vector :as sut]
   [datalevin.util :as u]
   [datalevin.core :as d]
   [datalevin.constants :as c]
   [datalevin.test.core :refer [db-fixture]]
   [clojure.test :refer [deftest is use-fixtures]])
  (:import
   [datalevin.dtlvnative DTLV DTLV$usearch_init_options_t DTLV$usearch_index_t]
   [datalevin.cpp VecIdx]
   [datalevin.vector VectorIndex]
   [java.util UUID Random]))

(use-fixtures :each db-fixture)

(def random (Random.))

(defn- rand-float-vec
  [^long n]
  #_(let [v (float-array n)]
      (dotimes [i n] (aset v i (.nextFloat ^Random random)))
      v)
  (VecIdx/randomVector (int n)))

(deftest basic-ops-test
  (let [dir   (u/tmp-dir (str "test-" (UUID/randomUUID)))
        lmdb  (d/open-kv dir)
        n     200
        index ^VectorIndex (sut/new-vector-index lmdb
                                                 {:dimensions       n
                                                  :connectivity     3
                                                  :expansion-add    40
                                                  :expansion-search 16})]
    (is (= 0 (sut/vec-count index)))
    (is (= (.-fname index) (str dir u/+separator+ c/default-domain ".vi")))
    (is (= (.-dimensions index) n))
    ;; (is (= (.-metric-type index) c/default-metric-type))
    ;; (is (= (.-quantization index) c/default-quantization))
    ;; (is (= (.-connectivity index) c/default-connectivity))
    ;; (is (= (.-expansion-add index) c/default-expansion-add))
    ;; (is (= (.-expansion-search index) c/default-expansion-search))

    (sut/add-vec index :ok (rand-float-vec n))

    (sut/close-vecs index)
    (d/close-kv lmdb)
    (u/delete-files dir)))
