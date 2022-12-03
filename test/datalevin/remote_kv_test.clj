(ns datalevin.remote-kv-test
  (:require [datalevin.remote :as sut]
            [datalevin.lmdb :as l]
            [datalevin.util :as u]
            [datalevin.datom :as d]
            [datalevin.core :as dc]
            [datalevin.interpret :as i]
            [datalevin.test.core :refer [server-fixture]]
            [clojure.test :refer [is testing deftest use-fixtures]])
  (:import [java.util UUID Arrays]))

(use-fixtures :each server-fixture)

(deftest kv-store-ops-test
  (let [dir   "dtlv://datalevin:datalevin@localhost/testkv"
        store (sut/open-kv dir)]
    (is (instance? datalevin.remote.KVStore store))
    (l/open-dbi store "a")
    (l/open-dbi store "b")
    (l/open-dbi store "c" {:key-size (inc Long/BYTES) :val-size (inc Long/BYTES)})
    (l/open-dbi store "d")

    (testing "list dbis"
      (is (= #{"a" "b" "c" "d"} (set (l/list-dbis store)))))

    (testing "transact-kv"
      (l/transact-kv store
                     [[:put "a" 1 2]
                      [:put "a" 'a 1]
                      [:put "a" 5 {}]
                      [:put "a" :annunaki/enki true :attr :data]
                      [:put "a" :datalevin ["hello" "world"]]
                      [:put "a" 42 (d/datom 1 :a/b {:id 4}) :long :datom]
                      [:put "b" 2 3]
                      [:put "b" (byte 0x01) #{1 2} :byte :data]
                      [:put "b" (byte-array [0x41 0x42]) :bk :bytes :data]
                      [:put "b" [-1 -235254457N] 5]
                      [:put "b" :a 4]
                      [:put "b" :bv (byte-array [0x41 0x42 0x43]) :data :bytes]
                      [:put "b" 1 :long :long :data]
                      [:put "b" :long 1 :data :long]
                      [:put "b" 2 3 :long :long]
                      [:put "b" "ok" 42 :string :int]
                      [:put "d" 3.14 :pi :double :keyword]]))

    (testing "entries"
      (is (= 4 (:entries (l/stat store))))
      (is (= 6 (:entries (l/stat store "a"))))
      (is (= 6 (l/entries store "a")))
      (is (= 10 (l/entries store "b"))))

    (testing "get-value"
      (is (= 2 (l/get-value store "a" 1)))
      (is (= [1 2] (l/get-value store "a" 1 :data :data false)))
      (is (= true (l/get-value store "a" :annunaki/enki :attr :data)))
      (is (= (d/datom 1 :a/b {:id 4}) (l/get-value store "a" 42 :long :datom)))
      (is (nil? (l/get-value store "a" 2)))
      (is (nil? (l/get-value store "b" 1)))
      (is (= 5 (l/get-value store "b" [-1 -235254457N])))
      (is (= 1 (l/get-value store "a" 'a)))
      (is (= {} (l/get-value store "a" 5)))
      (is (= ["hello" "world"] (l/get-value store "a" :datalevin)))
      (is (= 3 (l/get-value store "b" 2)))
      (is (= 4 (l/get-value store "b" :a)))
      (is (= #{1 2} (l/get-value store "b" (byte 0x01) :byte)))
      (is (= :bk (l/get-value store "b" (byte-array [0x41 0x42]) :bytes)))
      (is (Arrays/equals ^bytes (byte-array [0x41 0x42 0x43])
                         ^bytes (l/get-value store "b" :bv :data :bytes)))
      (is (= :long (l/get-value store "b" 1 :long :data)))
      (is (= 1 (l/get-value store "b" :long :data :long)))
      (is (= 3 (l/get-value store "b" 2 :long :long)))
      (is (= 42 (l/get-value store "b" "ok" :string :int)))
      (is (= :pi (l/get-value store "d" 3.14 :double :keyword))))

    (testing "delete"
      (l/transact-kv store [[:del "a" 1]
                            [:del "a" :non-exist]])
      (is (nil? (l/get-value store "a" 1))))

    (testing "entries-again"
      (is (= 5 (l/entries store "a")))
      (is (= 10 (l/entries store "b"))))

    (testing "non-existent dbi"
      (is (thrown? Exception (l/get-value store "z" 1))))

    (testing "handle val overflow automatically"
      (l/transact-kv store [[:put "c" 1 (range 50000)]])
      (is (= (range 50000) (l/get-value store "c" 1))))

    (testing "key overflow throws"
      (is (thrown? Exception (l/transact-kv store [[:put "a" (range 1000) 1]]))))

    (testing "close then re-open, clear and drop"
      (l/close-kv store)
      (is (l/closed-kv? store))
      (let [store (sut/open-kv dir)]
        (is (= ["hello" "world"] (l/get-value store "a" :datalevin)))
        (l/clear-dbi store "a")
        (is (nil? (l/get-value store "a" :datalevin)))
        (l/drop-dbi store "a")
        (is (thrown? Exception (l/get-value store "a" 1)))
        (l/close-kv store)))

    (testing "range and filter queries"
      (let [store (sut/open-kv dir)]
        (l/open-dbi store "r" {:key-size (inc Long/BYTES) :val-size (inc Long/BYTES)})
        (let [ks   (shuffle (range 0 10000))
              vs   (map inc ks)
              txs  (map (fn [k v] [:put "r" k v :long :long]) ks vs)
              pred (i/inter-fn [kv]
                               (let [^long k (dc/read-buffer (dc/k kv) :long)]
                                 (< 10 k 20)))
              fks  (range 11 20)
              fvs  (map inc fks)
              res  (map (fn [k v] [k v]) fks fvs)
              rc   (count res)]
          (l/transact-kv store txs)
          (is (= rc (l/range-filter-count store "r" pred [:all] :long)))
          (is (= fvs (l/range-filter store "r" pred [:all] :long :long true)))
          (is (= res (l/range-filter store "r" pred [:all] :long :long)))
          (is (= 12 (l/get-some store "r" pred [:all] :long :long true)))
          (is (= [0 1] (l/get-first store "r" [:all] :long :long)))
          (is (= 10000 (l/range-count store "r" [:all] :long)))
          (is (= (range 1 10001)
                 (l/get-range store "r" [:all] :long :long true))))
        (l/close-kv store)))))

(deftest copy-test
  (let [src    "dtlv://datalevin:datalevin@localhost/copytest"
        rstore (sut/open-kv src)
        dst    (u/tmp-dir (str "copy-test-" (UUID/randomUUID)))]
    (l/open-dbi rstore "z")
    (let [ks  (shuffle (range 0 10000))
          vs  (map inc ks)
          txs (map (fn [k v] [:put "z" k v :long :long]) ks vs)]
      (l/transact-kv rstore txs))
    (l/copy rstore dst)
    (let [cstore (l/open-kv dst)]
      (l/open-dbi cstore "z")
      (is (= (l/get-range rstore "z" [:all] :long :long)
             (l/get-range cstore "z" [:all] :long :long)))
      (l/close-kv cstore))
    (l/close-kv rstore)))
