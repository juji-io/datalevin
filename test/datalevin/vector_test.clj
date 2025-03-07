(ns datalevin.vector-test
  (:require
   [datalevin.vector :as sut]
   [datalevin.util :as u]
   [datalevin.core :as d]
   [datalevin.constants :as c]
   [datalevin.test.core :refer [db-fixture]]
   [clojure.test :refer [deftest is use-fixtures]])
  (:import
   [datalevin.vector VectorIndex]
   [java.util UUID Random]))

(use-fixtures :each db-fixture)

(def random (Random.))

(defn- rand-float-vec
  [^long n]
  (let [v (float-array n)]
    (dotimes [i n] (aset v i (.nextFloat ^Random random)))
    v))

(deftest basic-ops-test
  (let [dir   (u/tmp-dir (str "test-" (UUID/randomUUID)))
        lmdb  (d/open-kv dir)
        n     200
        v1    (rand-float-vec n)
        v2    (rand-float-vec n)
        index ^VectorIndex (sut/new-vector-index lmdb {:dimensions n})]
    (is (= 0 (sut/vec-count index)))
    (is (= (.-fname index) (str dir u/+separator+ c/default-domain ".vi")))
    (is (= (.-dimensions index) n))
    (is (= (.-metric-type index) c/default-metric-type))
    (is (= (.-quantization index) c/default-quantization))
    (is (= (.-connectivity index) c/default-connectivity))
    (is (= (.-expansion-add index) c/default-expansion-add))
    (is (= (.-expansion-search index) c/default-expansion-search))

    (sut/add-vec index :ok v1)
    (is (= 1 (sut/vec-count index)))
    (is (sut/vec-indexed? index :ok))
    (is (= [:ok] (sut/search-vec index v1)))
    (is (= [[:ok 0.0]] (sut/search-vec index v1 {:display :refs+dists})))

    (sut/close-vecs index)

    (let [index1 ^VectorIndex (sut/new-vector-index lmdb {:dimensions n})]
      (is (= 1 (sut/vec-count index1)))
      (is (sut/vec-indexed? index1 :ok))
      (is (= [:ok] (sut/search-vec index1 v1)))
      (is (= [[:ok 0.0]] (sut/search-vec index1 v1 {:display :refs+dists})))

      (sut/add-vec index1 :nice v2)
      (is (= 2 (sut/vec-count index1)))
      (is (sut/vec-indexed? index1 :nice))
      (is (= [:nice] (sut/search-vec index1 v2 {:top 1})))
      (is (= [[:nice 0.0]] (sut/search-vec index1 v2 {:top 1 :display :refs+dists})))
      (is (= [:nice :ok] (sut/search-vec index1 v2)))

      (sut/remove-vec index1 :ok)
      (is (= 1 (sut/vec-count index1)))
      (is (not (sut/vec-indexed? index1 :ok)))
      (is (= [:nice] (sut/search-vec index1 v2)))
      (is (= [[:nice 0.0]] (sut/search-vec index1 v2 {:display :refs+dists})))
      (is (= [:nice] (sut/search-vec index1 v2)))

      (sut/close-vecs index1))

    (let [index2 ^VectorIndex (sut/new-vector-index lmdb {:dimensions n})]
      (is (= 1 (sut/vec-count index2)))
      (is (sut/vec-indexed? index2 :nice))
      (is (= [:nice] (sut/search-vec index2 v2)))
      (is (= [[:nice 0.0]] (sut/search-vec index2 v2 {:display :refs+dists})))
      (sut/clear-vecs index2))

    (let [index3 ^VectorIndex (sut/new-vector-index lmdb {:dimensions n})]
      (is (= 0 (sut/vec-count index3)))
      (sut/close-vecs index3))

    (d/close-kv lmdb)
    (u/delete-files dir)))
