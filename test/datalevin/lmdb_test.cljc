(ns datalevin.lmdb-test
  (:require [datalevin.lmdb :as l]
            [datalevin.bits :as b]
            [datalevin.interpret :as i]
            [datalevin.util :as u]
            [datalevin.constants :as c]
            [datalevin.datom :as d]
            [clojure.test :refer [deftest testing is]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.clojure-test :as test]
            [clojure.test.check.properties :as prop]
            [taoensso.nippy :as nippy])
  (:import [java.util UUID Arrays HashMap]
           [org.eclipse.collections.impl.map.mutable.primitive IntShortHashMap]
           [java.lang Long]))

(if (u/graal?)
  (require 'datalevin.binding.graal)
  (require 'datalevin.binding.java))

(deftest basic-ops-test
  (let [dir  (u/tmp-dir (str "lmdb-test-" (UUID/randomUUID)))
        lmdb (l/open-kv dir)]
    (l/open-dbi lmdb "a")
    (l/open-dbi lmdb "b")
    (l/open-dbi lmdb "c" (inc Long/BYTES) (inc Long/BYTES))
    (l/open-dbi lmdb "d")

    (testing "list dbis"
      (is (= #{"a" "b" "c" "d"} (set (l/list-dbis lmdb)))))

    (testing "transact-kv"
      (l/transact-kv lmdb
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
                      [:put "d" 3.14 :pi :double :keyword]
                      [:put "d" #inst "1969-01-01" "nice year" :instant :string]
                      ]))

    (testing "entries"
      (is (= 4 (:entries (l/stat lmdb))))
      (is (= 6 (:entries (l/stat lmdb "a"))))
      (is (= 6 (l/entries lmdb "a")))
      (is (= 10 (l/entries lmdb "b"))))

    (testing "get-value"
      (is (= 2 (l/get-value lmdb "a" 1)))
      (is (= [1 2] (l/get-value lmdb "a" 1 :data :data false)))
      (is (= true (l/get-value lmdb "a" :annunaki/enki :attr :data)))
      (is (= (d/datom 1 :a/b {:id 4}) (l/get-value lmdb "a" 42 :long :datom)))
      (is (nil? (l/get-value lmdb "a" 2)))
      (is (nil? (l/get-value lmdb "b" 1)))
      (is (= 5 (l/get-value lmdb "b" [-1 -235254457N])))
      (is (= 1 (l/get-value lmdb "a" 'a)))
      (is (= {} (l/get-value lmdb "a" 5)))
      (is (= ["hello" "world"] (l/get-value lmdb "a" :datalevin)))
      (is (= 3 (l/get-value lmdb "b" 2)))
      (is (= 4 (l/get-value lmdb "b" :a)))
      (is (= #{1 2} (l/get-value lmdb "b" (byte 0x01) :byte)))
      (is (= :bk (l/get-value lmdb "b" (byte-array [0x41 0x42]) :bytes)))
      (is (Arrays/equals ^bytes (byte-array [0x41 0x42 0x43])
                         ^bytes (l/get-value lmdb "b" :bv :data :bytes)))
      (is (= :long (l/get-value lmdb "b" 1 :long :data)))
      (is (= 1 (l/get-value lmdb "b" :long :data :long)))
      (is (= 3 (l/get-value lmdb "b" 2 :long :long)))
      (is (= 42 (l/get-value lmdb "b" "ok" :string :int)))
      (is (= :pi (l/get-value lmdb "d" 3.14 :double :keyword)))
      (is (= "nice year" (l/get-value lmdb "d" #inst "1969-01-01" :instant :string)))
      )

    (testing "delete"
      (l/transact-kv lmdb [[:del "a" 1]
                           [:del "a" :non-exist]])
      (is (nil? (l/get-value lmdb "a" 1))))

    (testing "entries-again"
      (is (= 5 (l/entries lmdb "a")))
      (is (= 10 (l/entries lmdb "b"))))

    (testing "non-existent dbi"
      (is (thrown? Exception (l/get-value lmdb "z" 1))))

    (testing "handle val overflow automatically"
      (l/transact-kv lmdb [[:put "c" 1 (range 100000)]])
      (is (= (range 100000) (l/get-value lmdb "c" 1))))

    (testing "key overflow throws"
      (is (thrown? Exception (l/transact-kv lmdb [[:put "a" (range 1000) 1]]))))

    (testing "close then re-open, clear and drop"
      (let [dir (l/dir lmdb)]
        (l/close-kv lmdb)
        (is (l/closed-kv? lmdb))
        (let [lmdb  (l/open-kv dir)
              dbi-a (l/open-dbi lmdb "a")]
          (is (= "a" (l/dbi-name dbi-a)))
          (is (= ["hello" "world"] (l/get-value lmdb "a" :datalevin)))
          (l/clear-dbi lmdb "a")
          (is (nil? (l/get-value lmdb "a" :datalevin)))
          (l/drop-dbi lmdb "a")
          (is (thrown? Exception (l/get-value lmdb "a" 1)))
          (l/close-kv lmdb))))
    (u/delete-files dir)))

(deftest read-during-transaction-test
  (let [dir  (u/tmp-dir (str "lmdb-ctx-test-" (UUID/randomUUID)))
        lmdb (l/open-kv dir)]
    (l/open-dbi lmdb "a")
    (l/open-dbi lmdb "d")

    (l/open-transact-kv lmdb)

    (testing "get-value"
      (is (nil? (l/get-value lmdb "a" 1 :data :data false true)))
      (l/transact-kv lmdb
                     [[:put "a" 1 2]
                      [:put "a" 'a 1]
                      [:put "a" 5 {}]
                      [:put "a" :annunaki/enki true :attr :data]
                      [:put "a" :datalevin ["hello" "world"]]
                      [:put "a" 42 (d/datom 1 :a/b {:id 4}) :long :datom]])

      (is (= [1 2] (l/get-value lmdb "a" 1 :data :data false true)))
      ;; non-writing txn will still read pre-transaction values
      (is (nil? (l/get-value lmdb "a" 1 :data :data false false)))

      (is (nil? (l/get-value lmdb "d" #inst "1969-01-01" :instant :string
                             true true)))
      (l/transact-kv lmdb
                     [[:put "d" 3.14 :pi :double :keyword]
                      [:put "d" #inst "1969-01-01" "nice year" :instant :string]])
      (is (= "nice year"
             (l/get-value lmdb "d" #inst "1969-01-01" :instant :string
                          true true)))
      (is (nil? (l/get-value lmdb "d" #inst "1969-01-01" :instant :string
                             true false))))

    (l/close-transact-kv lmdb)

    (testing "entries after transaction"
      (is (= 6 (l/entries lmdb "a")))
      (is (= 2 (l/entries lmdb "d"))))
    (u/delete-files dir)))

(deftest reentry-test
  (let [dir  (u/tmp-dir (str "lmdb-test-" (UUID/randomUUID)))
        lmdb (l/open-kv dir)]
    (l/open-dbi lmdb "a")
    (l/transact-kv lmdb [[:put "a" :old 1]])
    (is (= 1 (l/get-value lmdb "a" :old)))
    (let [res (future
                (let [lmdb2 (l/open-kv dir)]
                  (l/open-dbi lmdb2 "a")
                  (is (= 1 (l/get-value lmdb2 "a" :old)))
                  (l/transact-kv lmdb2 [[:put "a" :something 1]])
                  (is (= 1 (l/get-value lmdb2 "a" :something)))
                  (is (= 1 (l/get-value lmdb "a" :something)))
                  ;; should not close this
                  ;; https://github.com/juji-io/datalevin/issues/7
                  (l/close-kv lmdb2)
                  1))]
      (is (= 1 @res)))
    (is (thrown-with-msg? Exception #"multiple LMDB"
                          (l/get-value lmdb "a" :something)))
    (l/close-kv lmdb)
    (u/delete-files dir)))

(deftest get-first-test
  (let [dir  (u/tmp-dir (str "lmdb-test-" (UUID/randomUUID)))
        lmdb (l/open-kv dir)]
    (l/open-dbi lmdb "a")
    (l/open-dbi lmdb "c" (inc Long/BYTES) (inc Long/BYTES))
    (let [ks  (shuffle (range 0 1000))
          vs  (map inc ks)
          txs (map (fn [k v] [:put "c" k v :long :long]) ks vs)]
      (l/transact-kv lmdb txs)
      (is (= [0 1] (l/get-first lmdb "c" [:all] :long :long)))
      (is (= [0 nil] (l/get-first lmdb "c" [:all] :long :ignore)))
      (is (= [999 1000] (l/get-first lmdb "c" [:all-back] :long :long)))
      (is (= [9 10] (l/get-first lmdb "c" [:at-least 9] :long :long)))
      (is (= [10 11] (l/get-first lmdb "c" [:greater-than 9] :long :long)))
      (is (= true (l/get-first lmdb "c" [:greater-than 9] :long :ignore true)))
      (is (nil? (l/get-first lmdb "c" [:greater-than 1000] :long :ignore)))
      (l/transact-kv lmdb [[:put "a" 0xff 1 :byte]
                           [:put "a" 0xee 2 :byte]
                           [:put "a" 0x11 3 :byte]])
      (is (= 3 (l/get-first lmdb "a" [:all] :byte :data true)))
      (is (= 1 (l/get-first lmdb "a" [:all-back] :byte :data true))))
    (l/close-kv lmdb)
    (u/delete-files dir)))

(deftest get-range-no-gap-test
  (let [dir  (u/tmp-dir (str "lmdb-test-" (UUID/randomUUID)))
        lmdb (l/open-kv dir)]
    (l/open-dbi lmdb "c" (inc Long/BYTES) (inc Long/BYTES))
    (let [ks   (shuffle (range 0 1000))
          vs   (map inc ks)
          txs  (map (fn [k v] [:put "c" k v :long :long]) ks vs)
          res  (sort-by first (map (fn [k v] [k v]) ks vs))
          rres (reverse res)
          rc   (count res)]
      (l/transact-kv lmdb txs)
      (is (= res (l/get-range lmdb "c" [:all] :long :long)))
      (is (= rc (l/range-count lmdb "c" [:all] :long)))
      (is (= rres (l/get-range lmdb "c" [:all-back] :long :long)))
      (is (= (->> res (drop 990))
             (l/get-range lmdb "c" [:at-least 990] :long :long)))
      (is (= [] (l/get-range lmdb "c" [:greater-than 1500] :long :ignore)))
      (is (= 0 (l/range-count lmdb "c" [:greater-than 1500] :long)))
      (is (= res
             (l/get-range lmdb "c" [:less-than Long/MAX_VALUE] :long :long)))
      (is (= rc (l/range-count lmdb "c" [:less-than Long/MAX_VALUE] :long)))
      (is (= (take 10 res)
             (l/get-range lmdb "c" [:at-most 9] :long :long)))
      (is (= (->> res (drop 10) (take 100))
             (l/get-range lmdb "c" [:closed 10 109] :long :long)))
      (is (= (->> res (drop 10) (take 100) (map second))
             (l/get-range lmdb "c" [:closed 10 109] :long :long true))))
    (l/close-kv lmdb)
    (u/delete-files dir)))

(deftest get-range-gap-test
  (let [dir        (u/tmp-dir (str "lmdb-test-" (UUID/randomUUID)))
        db         (l/open-kv dir)
        misc-table "misc-test-table"]
    (l/open-dbi db misc-table (b/type-size :long) (b/type-size :long))
    (l/transact-kv db
                   [[:put misc-table 1 1 :long :long]
                    [:put misc-table 2 2 :long :long]
                    [:put misc-table 4 4 :long :long]
                    [:put misc-table 8 8 :long :long]
                    [:put misc-table 16 16 :long :long]
                    [:put misc-table 32 32 :long :long]
                    [:put misc-table 64 64 :long :long]
                    [:put misc-table 128 128 :long :long]
                    [:put misc-table 256 256 :long :long]
                    [:put misc-table 512 512 :long :long]
                    [:put misc-table 1024 1024 :long :long]
                    [:put misc-table 2048 2048 :long :long]
                    [:put misc-table 4096 4096 :long :long]
                    [:put misc-table 8192 8192 :long :long]
                    [:put misc-table 16384 16384 :long :long]])
    (is (= [1 2 4 8 16 32 64 128 256 512 1024 2048 4096 8192 16384]
           (l/get-range db misc-table [:all] :long :long true)))
    (is (= [16384 8192 4096 2048 1024 512 256 128 64 32 16 8 4 2 1]
           (l/get-range db misc-table [:all-back] :long :long true)))
    (is (= [1024 2048 4096 8192 16384]
           (l/get-range db misc-table [:at-least 1000] :long :long true)))
    (is (= [1024 2048 4096 8192 16384]
           (l/get-range db misc-table [:at-least 1024] :long :long true)))
    (is (= [16 8 4 2 1]
           (l/get-range db misc-table [:at-most-back 16] :long :long true)))
    (is (= [16 8 4 2 1]
           (l/get-range db misc-table [:at-most-back 17] :long :long true)))
    (is (= [1 2 4 8 16]
           (l/get-range db misc-table [:at-most 16] :long :long true)))
    (is (= [1 2 4 8 16]
           (l/get-range db misc-table [:at-most 17] :long :long true)))
    (is (= [16384 8192 4096 2048]
           (l/get-range db misc-table [:at-least-back 2048] :long :long true)))
    (is (= [16384 8192 4096 2048]
           (l/get-range db misc-table [:at-least-back 2000] :long :long true)))
    (is (= [2] (l/get-range db misc-table [:closed 2 2] :long :long true)))
    (is (= [] (l/get-range db misc-table [:closed 2 1] :long :long true)))
    (is (= [2 4 8 16 32]
           (l/get-range db misc-table [:closed 2 32] :long :long true)))
    (is (= [4 8 16] (l/get-range db misc-table [:closed 3 30] :long :long true)))
    (is (= [1 2 4 8 16 32] (l/get-range db misc-table [:closed 0 40]
                                        :long :long true)))
    (is (= [] (l/get-range db misc-table [:closed-back 2 32] :long :long true)))
    (is (= [32 16 8 4 2] (l/get-range db misc-table [:closed-back 32 2]
                                      :long :long true)))
    (is (= [16 8 4] (l/get-range db misc-table [:closed-back 30 3]
                                 :long :long true)))
    (is (= [32 16 8 4 2 1] (l/get-range db misc-table [:closed-back 40 0]
                                        :long :long true)))
    (is (= [2 4 8 16] (l/get-range db misc-table [:closed-open 2 32]
                                   :long :long true)))
    (is (= [4 8 16] (l/get-range db misc-table [:closed-open 3 30]
                                 :long :long true)))
    (is (= [1 2 4 8 16 32] (l/get-range db misc-table [:closed-open 0 40]
                                        :long :long true)))
    (is (= [] (l/get-range db misc-table [:closed-open-back 2 32]
                           :long :long true)))
    (is (= [32 16 8 4] (l/get-range db misc-table
                                    [:closed-open-back 32 2] :long :long true)))
    (is (= [16 8 4] (l/get-range db misc-table
                                 [:closed-open-back 30 3] :long :long true)))
    (is (= [32 16 8 4 2 1] (l/get-range db misc-table
                                        [:closed-open-back 40 0]
                                        :long :long true)))
    (is (= [4096 8192 16384] (l/get-range db misc-table
                                          [:greater-than 2048]
                                          :long :long true)))
    (is (= [2048 4096 8192 16384] (l/get-range db misc-table
                                               [:greater-than 2000]
                                               :long :long true)))
    (is (= [8 4 2 1] (l/get-range db misc-table
                                  [:less-than-back 16] :long :long true)))
    (is (= [16 8 4 2 1] (l/get-range db misc-table
                                     [:less-than-back 17] :long :long true)))
    (is (= [1 2 4 8] (l/get-range db misc-table
                                  [:less-than 16] :long :long true)))
    (is (= [1 2 4 8 16] (l/get-range db misc-table
                                     [:less-than 17] :long :long true)))
    (is (= [16384 8192 4096] (l/get-range db misc-table
                                          [:greater-than-back 2048]
                                          :long :long true)))
    (is (= [16384 8192 4096 2048] (l/get-range db misc-table
                                               [:greater-than-back 2000]
                                               :long :long true)))
    (is (= [] (l/get-range db misc-table [:open 2 2]
                           :long :long true)))
    (is (= [] (l/get-range db misc-table [:open 2 1]
                           :long :long true)))
    (is (= [4 8 16] (l/get-range db misc-table [:open 2 32] :long :long true)))
    (is (= [4 8 16] (l/get-range db misc-table [:open 3 30] :long :long true)))
    (is (= [1 2 4 8 16 32] (l/get-range db misc-table [:open 0 40]
                                        :long :long true)))
    (is (= [] (l/get-range db misc-table [:open-back 2 2] :long :long true)))
    (is (= [] (l/get-range db misc-table [:open-back 2 1] :long :long true)))
    (is (= [16 8 4] (l/get-range db misc-table [:open-back 32 2]
                                 :long :long true)))
    (is (= [16 8 4] (l/get-range db misc-table [:open-back 30 3]
                                 :long :long true)))
    (is (= [32 16 8 4 2 1] (l/get-range db misc-table [:open-back 40 0]
                                        :long :long true)))
    (is (= [4 8 16 32] (l/get-range db misc-table [:open-closed 2 32]
                                    :long :long true)))
    (is (= [4 8 16] (l/get-range db misc-table [:open-closed 3 30]
                                 :long :long true)))
    (is (= [1 2 4 8 16 32] (l/get-range db misc-table [:open-closed 0 40]
                                        :long :long true)))
    (is (= [] (l/get-range db misc-table [:open-closed-back 2 32]
                           :long :long true)))
    (is (= [16 8 4 2] (l/get-range db misc-table [:open-closed-back 32 2]
                                   :long :long true)))
    (is (= [16 8 4] (l/get-range db misc-table [:open-closed-back 30 3]
                                 :long :long true)))
    (is (= [32 16 8 4 2 1] (l/get-range db misc-table
                                        [:open-closed-back 40 0]
                                        :long :long true)))
    (l/close-kv db)
    (u/delete-files dir)))

(deftest get-some-test
  (let [dir  (u/tmp-dir (str "lmdb-test-" (UUID/randomUUID)))
        lmdb (l/open-kv dir)]
    (l/open-dbi lmdb "c" (inc Long/BYTES) (inc Long/BYTES))
    (let [ks   (shuffle (range 0 100))
          vs   (map inc ks)
          txs  (map (fn [k v] [:put "c" k v :long :long]) ks vs)
          pred (i/inter-fn [kv]
                           (let [^long k (b/read-buffer (l/k kv) :long)]
                             (> k 15)))]
      (l/transact-kv lmdb txs)
      (is (= 17 (l/get-some lmdb "c" pred [:all] :long :long true)))
      (is (= [16 17] (l/get-some lmdb "c" pred [:all] :long :long))))
    (l/close-kv lmdb)
    (u/delete-files dir)))

(deftest range-filter-test
  (let [dir  (u/tmp-dir (str "lmdb-test-" (UUID/randomUUID)))
        lmdb (l/open-kv dir)]
    (l/open-dbi lmdb "c" (inc Long/BYTES) (inc Long/BYTES))
    (let [ks   (shuffle (range 0 100))
          vs   (map inc ks)
          txs  (map (fn [k v] [:put "c" k v :long :long]) ks vs)
          pred (i/inter-fn [kv]
                           (let [^long k (b/read-buffer (l/k kv) :long)]
                             (< 10 k 20)))
          fks  (range 11 20)
          fvs  (map inc fks)
          res  (map (fn [k v] [k v]) fks fvs)
          rc   (count res)]
      (l/transact-kv lmdb txs)
      (is (= fvs (l/range-filter lmdb "c" pred [:all] :long :long true)))
      (is (= rc (l/range-filter-count lmdb "c" pred [:all] :long)))
      (is (= res (l/range-filter lmdb "c" pred [:all] :long :long)))
      (is (= fks (map first
                      (l/range-filter lmdb "c" pred [:all] :long :ignore false))))

      (let [hm      (HashMap.)
            visitor (i/inter-fn [kv]
                                (let [^long k (b/read-buffer (l/k kv) :long)
                                      ^long v (b/read-buffer (l/v kv) :long)]
                                  (.put hm k v)))]
        (l/visit lmdb "c" visitor [:closed 11 19] :long)
        (is (= (into {} res) hm))))
    (l/close-kv lmdb)
    (u/delete-files dir)))

(deftest multi-threads-get-value-test
  (let [dir  (u/tmp-dir (str "lmdb-test-" (UUID/randomUUID)))
        lmdb (l/open-kv dir)]
    (l/open-dbi lmdb "a")
    (let [ks  (shuffle (range 0 100000))
          vs  (map inc ks)
          txs (map (fn [k v] [:put "a" k v :long :long]) ks vs)]
      (l/transact-kv lmdb txs)
      (is (= vs (pmap #(l/get-value lmdb "a" % :long :long) ks))))
    (l/close-kv lmdb)
    (u/delete-files dir)))

(deftest multi-threads-put-test
  (let [dir  (u/tmp-dir (str "lmdb-test-" (UUID/randomUUID)))
        lmdb (l/open-kv dir)]
    (l/open-dbi lmdb "a")
    (let [ks  (shuffle (range 0 10000))
          vs  (map inc ks)
          txs (map (fn [k v] [:put "a" k v :long :long]) ks vs)]
      (dorun (pmap #(l/transact-kv lmdb [%]) txs))
      (is (= vs (map #(l/get-value lmdb "a" % :long :long) ks))))
    (l/close-kv lmdb)
    (u/delete-files dir)))

;; generative tests

(test/defspec datom-ops-generative-test
  100
  (prop/for-all [k gen/large-integer
                 e gen/large-integer
                 a gen/keyword-ns
                 v gen/any-equatable]
                (let [dir    (u/tmp-dir (str "lmdb-test-" (UUID/randomUUID)))
                      lmdb   (l/open-kv dir)
                      _      (l/open-dbi lmdb "a")
                      d      (d/datom e a v e)
                      _      (l/transact-kv lmdb [[:put "a" k d :long :datom]])
                      put-ok (= d (l/get-value lmdb "a" k :long :datom))
                      _      (l/transact-kv lmdb [[:del "a" k :long]])
                      del-ok (nil? (l/get-value lmdb "a" k :long))]
                  (l/close-kv lmdb)
                  (u/delete-files dir)
                  (is (and put-ok del-ok)))))


(defn- data-size-less-than?
  [^long limit data]
  (< (alength ^bytes (nippy/freeze data)) limit))

(test/defspec data-ops-generative-test
  100
  (prop/for-all [k (gen/such-that (partial data-size-less-than? c/+max-key-size+)
                                  gen/any-equatable)
                 v (gen/such-that (partial data-size-less-than? c/+default-val-size+)
                                  gen/any-equatable)]
                (let [dir    (u/tmp-dir (str "lmdb-test-" (UUID/randomUUID)))
                      lmdb   (l/open-kv dir)
                      _      (l/open-dbi lmdb "a")
                      _      (l/transact-kv lmdb [[:put "a" k v]])
                      put-ok (= v (l/get-value lmdb "a" k))
                      _      (l/transact-kv lmdb [[:del "a" k]])
                      del-ok (nil? (l/get-value lmdb "a" k))]
                  (l/close-kv lmdb)
                  (u/delete-files dir)
                  (is (and put-ok del-ok)))))

(test/defspec bytes-ops-generative-test
  100
  (prop/for-all [^bytes k (gen/not-empty gen/bytes)
                 ^bytes v (gen/not-empty gen/bytes)]
                (let [dir    (u/tmp-dir (str "lmdb-test-" (UUID/randomUUID)))
                      lmdb   (l/open-kv dir)
                      _      (l/open-dbi lmdb "a")
                      _      (l/transact-kv lmdb [[:put "a" k v :bytes :bytes]])
                      put-ok (Arrays/equals v
                                            ^bytes
                                            (l/get-value
                                              lmdb "a" k :bytes :bytes))
                      _      (l/transact-kv lmdb [[:del "a" k :bytes]])
                      del-ok (nil? (l/get-value lmdb "a" k :bytes))]
                  (l/close-kv lmdb)
                  (u/delete-files dir)
                  (is (and put-ok del-ok)))))

(test/defspec long-ops-generative-test
  100
  (prop/for-all [^long k gen/large-integer
                 ^long v gen/large-integer]
                (let [dir    (u/tmp-dir (str "lmdb-test-" (UUID/randomUUID)))
                      lmdb   (l/open-kv dir)
                      _      (l/open-dbi lmdb "a")
                      _      (l/transact-kv lmdb [[:put "a" k v :long :long]])
                      put-ok (= v ^long (l/get-value lmdb "a" k :long :long))
                      _      (l/transact-kv lmdb [[:del "a" k :long]])
                      del-ok (nil? (l/get-value lmdb "a" k)) ]
                  (l/close-kv lmdb)
                  (u/delete-files dir)
                  (is (and put-ok del-ok)))))

(deftest inverted-list-basic-ops-test
  (let [dir     (u/tmp-dir (str "inverted-test-" (UUID/randomUUID)))
        lmdb    (l/open-kv dir)
        pred    (i/inter-fn
                  [kv]
                  (let [^long v (b/read-buffer (l/v kv) :long)]
                    (odd? v)))
        sum     (volatile! 0)
        visitor (i/inter-fn
                  [kv]
                  (let [^long v (b/read-buffer (l/v kv) :long)]
                    (vswap! sum #(+ ^long %1 ^long %2) v)))]
    (l/open-list lmdb "inverted")

    (l/put-list-items lmdb "inverted" "a" [1 2 3 4] :string :long)
    (l/put-list-items lmdb "inverted" "b" [5 6 7] :string :long)

    (is (= [["a" 1] ["a" 2] ["a" 3] ["a" 4] ["b" 5] ["b" 6] ["b" 7]]
           (l/get-range lmdb "inverted" [:all] :string :long) ))
    (is (= [["a" 1] ["a" 2] ["a" 3] ["a" 4] ["b" 5] ["b" 6] ["b" 7]]
           (l/get-range lmdb "inverted" [:closed "a" "b"] :string :long) ))
    (is (= [["b" 5] ["b" 6] ["b" 7]]
           (l/get-range lmdb "inverted" [:closed "b" "b"] :string :long) ))
    (is (= [["b" 5] ["b" 6] ["b" 7]]
           (l/get-range lmdb "inverted" [:open-closed "a" "b"] :string :long) ))

    (is (= (l/list-count lmdb "inverted" "a" :string) 4))
    (is (= (l/list-count lmdb "inverted" "b" :string) 3))

    (is (not (l/in-list? lmdb "inverted" "a" 7 :string :long)))
    (is (l/in-list? lmdb "inverted" "b" 7 :string :long))

    (is (= (l/get-list lmdb "inverted" "a" :string :long) [1 2 3 4]))

    (l/visit-list lmdb "inverted" visitor "a" :string)
    (is (= @sum 10))

    (l/del-list-items lmdb "inverted" "a" :string)

    (is (= (l/list-count lmdb "inverted" "a" :string) 0))
    (is (not (l/in-list? lmdb "inverted" "a" 1 :string :long)))
    (is (nil? (l/get-list lmdb "inverted" "a" :string :long)))

    (l/put-list-items lmdb "inverted" "b" [1 2 3 4] :string :long)

    (is (= (l/list-count lmdb "inverted" "b" :string) 7))
    (is (l/in-list? lmdb "inverted" "b" 1 :string :long))

    (l/del-list-items lmdb "inverted" "b" [1 2] :string :long)

    (is (= (l/list-count lmdb "inverted" "b" :string) 5))
    (is (not (l/in-list? lmdb "inverted" "b" 1 :string :long)))

    (is (= (l/filter-list lmdb "inverted" "b" pred :string :long) [3 5 7]))
    (is (= (l/filter-list-count lmdb "inverted" "b" pred :string) 3))

    (l/close-kv lmdb)
    (u/delete-files dir)))
