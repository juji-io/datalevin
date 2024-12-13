(ns datalevin.scan-test
  (:require
   [datalevin.lmdb :as l]
   [datalevin.bits :as b]
   [datalevin.interpret :as i]
   [datalevin.util :as u]
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.test :refer [deftest testing is use-fixtures]]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.clojure-test :as test]
   [clojure.test.check.properties :as prop]
   [datalevin.constants :as c])
  (:import
   [java.util UUID HashMap]
   [java.lang Long AutoCloseable]))

(use-fixtures :each db-fixture)

(deftest get-first-test
  (let [dir  (u/tmp-dir (str "lmdb-test-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags (conj c/default-env-flags :nosync)})]
    (l/open-dbi lmdb "a")
    (l/open-dbi lmdb "c" {:key-size (inc Long/BYTES) :val-size (inc Long/BYTES)})
    (let [ks  (shuffle (range 0 1000))
          vs  (map inc ks)
          txs (map (fn [k v] [:put "c" k v :long :long]) ks vs)]
      (l/transact-kv lmdb txs)
      (is (= [0 1] (l/get-first lmdb "c" [:all] :long :long)))
      (is (= [[0 1] [1 2]] (l/get-first-n lmdb "c" 2 [:all] :long :long)))
      (is (= [0 nil] (l/get-first lmdb "c" [:all] :long :ignore)))
      (is (= [999 1000] (l/get-first lmdb "c" [:all-back] :long :long)))
      (is (= [[999 1000] [998 999]] (l/get-first-n lmdb "c" 2 [:all-back] :long :long)))
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

(deftest range-seq-test
  (let [dir  (u/tmp-dir (str "range-seq-test-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags (conj c/default-env-flags :nosync)})]
    (l/open-dbi lmdb "a")

    (l/transact-kv lmdb [[:put "a" 0 0 :long :long]
                         [:put "a" 1 1 :long :long]
                         [:put "a" 2 2 :long :long]
                         [:put "a" 3 3 :long :long]
                         [:put "a" 4 4 :long :long]])
    (with-open [^AutoCloseable vs
                (l/range-seq lmdb "a" [:all] :long :long true
                             {:batch-size 2})]
      (is (= 10 (reduce + (seq vs)))))

    (with-open [^AutoCloseable vs
                (l/range-seq lmdb "a" [:all] :long :long true)]
      (is (= [0 1 2 3 4] (into [] cat vs)) ))

    (with-open [^AutoCloseable vs
                (l/range-seq lmdb "a" [:all] :long :long true)]
      (is (= [1 2 3 4 5] (map inc vs)) ))

    (with-open [^AutoCloseable vs
                (l/range-seq lmdb "a" [:all] :long :long true)]
      (is (= [[0 1] [2 3] [4]] (partition-all 2 vs)) ))

    (with-open [^AutoCloseable vs
                (l/range-seq lmdb "a" [:all] :long :long true {:batch-size 2})]
      (let [products (atom 0)]
        (doseq [v vs] (swap! products + v))
        (is (= @products 10))))

    (l/close-kv lmdb)
    (u/delete-files dir)))

(deftest range-no-gap-test
  (let [dir  (u/tmp-dir (str "lmdb-test-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags (conj c/default-env-flags :nosync)})]
    (l/open-dbi lmdb "c"
                {:key-size (inc Long/BYTES) :val-size (inc Long/BYTES)})
    (let [ks   (shuffle (range 0 1000))
          vs   (map inc ks)
          txs  (map (fn [k v] [:put "c" k v :long :long]) ks vs)
          res  (sort-by first (map (fn [k v] [k v]) ks vs))
          rres (reverse res)
          rc   (count res)]
      (l/transact-kv lmdb txs)
      (is (= res (l/get-range lmdb "c" [:all] :long :long)))
      (is (= (range 0 1000) (l/key-range lmdb "c" [:all] :long)))
      (with-open [^AutoCloseable rs
                  (l/range-seq lmdb "c" [:all] :long :long)]
        (is (= res (seq rs))))

      (is (= rc (l/range-count lmdb "c" [:all] :long)))

      (is (= rres (l/get-range lmdb "c" [:all-back] :long :long)))
      (is (= (reverse (range 0 1000))
             (l/key-range lmdb "c" [:all-back] :long)))
      (with-open [^AutoCloseable rs
                  (l/range-seq lmdb "c" [:all-back] :long :long)]
        (is (= (seq rs) rres)))

      (is (= (->> res (drop 990))
             (l/get-range lmdb "c" [:at-least 990] :long :long)))
      (is (= (range 990 1000) (l/key-range lmdb "c" [:at-least 990] :long)))
      (with-open [^AutoCloseable rs
                  (l/range-seq lmdb "c" [:at-least 990] :long :long)]
        (is (= (seq rs) (->> res (drop 990)))))

      (is (= []
             (l/get-range lmdb "c" [:greater-than 1500] :long :ignore)))
      (is (= [] (l/key-range lmdb "c" [:greater-than 1500] :long)))
      (with-open [^AutoCloseable rs
                  (l/range-seq lmdb "c" [:greater-than 1500] :long :ignore)]
        (is (= (seq rs) [])))

      (is (= 0 (l/range-count lmdb "c" [:greater-than 1500] :long)))

      (is (= res
             (l/get-range lmdb "c" [:less-than Long/MAX_VALUE] :long :long)))
      (is (= (range 0 1000)
             (l/key-range lmdb "c" [:less-than Long/MAX_VALUE] :long)))
      (with-open [^AutoCloseable rs
                  (l/range-seq lmdb "c" [:less-than Long/MAX_VALUE]
                               :long :long)]
        (is (= (seq rs) res)))

      (is (= rc (l/range-count lmdb "c" [:less-than Long/MAX_VALUE] :long)))

      (is (= (take 10 res)
             (l/get-range lmdb "c" [:at-most 9] :long :long)))
      (is (= (range 0 10)
             (l/key-range lmdb "c" [:at-most 9] :long)))
      (with-open [^AutoCloseable rs
                  (l/range-seq lmdb "c" [:at-most 9] :long :long)]
        (is (= (seq rs) (take 10 res))))

      (is (= (->> res (drop 10) (take 100))
             (l/get-range lmdb "c" [:closed 10 109] :long :long)))
      (is (= (range 10 110)
             (l/key-range lmdb "c" [:closed 10 109] :long)))
      (with-open [^AutoCloseable rs
                  (l/range-seq lmdb "c" [:closed 10 109] :long :long)]
        (is (= (seq rs) (->> res (drop 10) (take 100)))))

      (is (= (->> res (drop 10) (take 100) (map second))
             (l/get-range lmdb "c" [:closed 10 109] :long :long true)))
      (with-open [^AutoCloseable rs
                  (l/range-seq lmdb "c" [:closed 10 109] :long :long true)]
        (is (= (seq rs) (->> res (drop 10) (take 100) (map second))))))

    (l/close-kv lmdb)
    (u/delete-files dir)))

(deftest range-gap-test
  (let [dir        (u/tmp-dir (str "lmdb-test-" (UUID/randomUUID)))
        db         (l/open-kv dir {:flags (conj c/default-env-flags :nosync)})
        misc-table "misc-test-table"]
    (l/open-dbi db misc-table {:key-size (b/type-size :long)
                               :val-size (b/type-size :long)})
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
    (with-open [^AutoCloseable rs
                (l/range-seq db misc-table [:all] :long :long true)]
      (is (= (seq rs)
             [1 2 4 8 16 32 64 128 256 512 1024 2048 4096 8192 16384])))
    (is (= [16384 8192 4096 2048 1024 512 256 128 64 32 16 8 4 2 1]
           (l/get-range db misc-table [:all-back] :long :long true)))
    (with-open [^AutoCloseable rs
                (l/range-seq db misc-table [:all-back] :long :long true)]
      (is (= (seq rs)
             [16384 8192 4096 2048 1024 512 256 128 64 32 16 8 4 2 1] )))
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
    (with-open [^AutoCloseable rs
                (l/range-seq db misc-table [:at-most 17] :long :long true)]
      (is (= [1 2 4 8 16] (seq rs) )))

    (is (= [16384 8192 4096 2048]
           (l/get-range db misc-table [:at-least-back 2048] :long :long true)))


    (is (= [16384 8192 4096 2048]
           (l/get-range db misc-table [:at-least-back 2000] :long :long true)))

    (is (= [2] (l/get-range db misc-table [:closed 2 2] :long :long true)))

    (is (= [] (l/get-range db misc-table [:closed 2 1] :long :long true)))

    (with-open [^AutoCloseable rs
                (l/range-seq db misc-table [:closed 2 1] :long :long true)]
      (is (= (seq rs) [])))

    (is (= [2 4 8 16 32]
           (l/get-range db misc-table [:closed 2 32] :long :long true)))

    (with-open [^AutoCloseable rs
                (l/range-seq db misc-table [:closed 2 32] :long :long true)]
      (is (= [2 4 8 16 32] (seq rs))))

    (is (= [4 8 16]
           (l/get-range db misc-table [:closed 3 30] :long :long true)))

    (is (= [1 2 4 8 16 32]
           (l/get-range db misc-table [:closed 0 40] :long :long true)))


    (is (= []
           (l/get-range db misc-table [:closed-back 2 32] :long :long true)))

    (is (= [32 16 8 4 2]
           (l/get-range db misc-table [:closed-back 32 2] :long :long true)))

    (with-open [^AutoCloseable rs
                (l/range-seq db misc-table [:closed-back 32 2]
                             :long :long true)]
      (is (= [32 16 8 4 2] (seq rs))))

    (is (= [16 8 4]
           (l/get-range db misc-table [:closed-back 30 3] :long :long true)))

    (is (= [32 16 8 4 2 1]
           (l/get-range db misc-table [:closed-back 40 0] :long :long true)))

    (is (= [2 4 8 16]
           (l/get-range db misc-table [:closed-open 2 32] :long :long true)))

    (with-open [^AutoCloseable rs
                (l/range-seq db misc-table [:closed-open 2 32]
                             :long :long true)]
      (is (= (seq rs) [2 4 8 16])))

    (is (= [4 8 16]
           (l/get-range db misc-table [:closed-open 3 30] :long :long true)))

    (is (= [1 2 4 8 16 32]
           (l/get-range db misc-table [:closed-open 0 40] :long :long true)))

    (is (= []
           (l/get-range db misc-table [:closed-open-back 2 32]
                        :long :long true)))

    (with-open [^AutoCloseable rs
                (l/range-seq db misc-table [:closed-open-back 2 32]
                             :long :long true) ]
      (is (= (seq rs) [])))

    (is (= [32 16 8 4]
           (l/get-range db misc-table [:closed-open-back 32 2]
                        :long :long true)))
    (is (= [16 8 4]
           (l/get-range db misc-table [:closed-open-back 30 3]
                        :long :long true)))
    (is (= [32 16 8 4 2 1]
           (l/get-range db misc-table [:closed-open-back 40 0]
                        :long :long true)))
    (is (= [4096 8192 16384]
           (l/get-range db misc-table [:greater-than 2048] :long :long true)))
    (is (= [2048 4096 8192 16384]
           (l/get-range db misc-table [:greater-than 2000] :long :long true)))

    (with-open [^AutoCloseable rs
                (l/range-seq db misc-table [:greater-than 2000]
                             :long :long true)]
      (is (= (seq rs) [2048 4096 8192 16384])))

    (is (= [8 4 2 1]
           (l/get-range db misc-table [:less-than-back 16] :long :long true)))
    (is (= [16 8 4 2 1]
           (l/get-range db misc-table [:less-than-back 17] :long :long true)))

    (with-open [^AutoCloseable rs
                (l/range-seq db misc-table
                             [:less-than-back 17] :long :long true)]
      (is (= (seq rs) [16 8 4 2 1])))

    (is (= [1 2 4 8]
           (l/get-range db misc-table [:less-than 16] :long :long true)))
    (is (= [1 2 4 8 16]
           (l/get-range db misc-table [:less-than 17] :long :long true)))
    (is (= [16384 8192 4096]
           (l/get-range db misc-table [:greater-than-back 2048]
                        :long :long true)))
    (is (= [16384 8192 4096 2048]
           (l/get-range db misc-table [:greater-than-back 2000]
                        :long :long true)))
    (is (= []
           (l/get-range db misc-table [:open 2 2] :long :long true)))
    (is (= [] (l/get-range db misc-table [:open 2 1]
                           :long :long true)))
    (is (= [4 8 16]
           (l/get-range db misc-table [:open 2 32] :long :long true)))
    (is (= [4 8 16]
           (l/get-range db misc-table [:open 3 30] :long :long true)))
    (is (= [1 2 4 8 16 32]
           (l/get-range db misc-table [:open 0 40] :long :long true)))
    (is (= [] (l/get-range db misc-table [:open-back 2 2] :long :long true)))
    (is (= [] (l/get-range db misc-table [:open-back 2 1] :long :long true)))
    (is (= [16 8 4]
           (l/get-range db misc-table [:open-back 32 2] :long :long true)))
    (is (= [16 8 4]
           (l/get-range db misc-table [:open-back 30 3] :long :long true)))
    (is (= [32 16 8 4 2 1]
           (l/get-range db misc-table [:open-back 40 0] :long :long true)))
    (is (= [4 8 16 32]
           (l/get-range db misc-table [:open-closed 2 32] :long :long true)))
    (is (= [4 8 16]
           (l/get-range db misc-table [:open-closed 3 30] :long :long true)))
    (is (= [1 2 4 8 16 32]
           (l/get-range db misc-table [:open-closed 0 40] :long :long true)))
    (is (= []
           (l/get-range db misc-table [:open-closed-back 2 32]
                        :long :long true)))
    (is (= [16 8 4 2]
           (l/get-range db misc-table [:open-closed-back 32 2]
                        :long :long true)))
    (is (= [16 8 4]
           (l/get-range db misc-table [:open-closed-back 30 3]
                        :long :long true)))
    (is (= [32 16 8 4 2 1]
           (l/get-range db misc-table [:open-closed-back 40 0]
                        :long :long true)))
    (with-open [^AutoCloseable rs
                (l/range-seq db misc-table
                             [:open-closed-back 40 0] :long :long true)]
      (is (= (seq rs) [32 16 8 4 2 1])))
    (l/close-kv db)
    (u/delete-files dir)))

(deftest get-some-test
  (let [dir  (u/tmp-dir (str "lmdb-test-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags (conj c/default-env-flags :nosync)})]
    (l/open-dbi lmdb "c"
                {:key-size (inc Long/BYTES) :val-size (inc Long/BYTES)})
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

(deftest range-filter-keep-some-test
  (let [dir  (u/tmp-dir (str "lmdb-test-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags (conj c/default-env-flags :nosync)})]
    (l/open-dbi lmdb "c"
                {:key-size (inc Long/BYTES) :val-size (inc Long/BYTES)})
    (let [ks    (shuffle (range 0 100))
          vs    (map inc ks)
          txs   (map (fn [k v] [:put "c" k v :long :long]) ks vs)
          pred  (i/inter-fn [kv]
                  (let [^long k (b/read-buffer (l/k kv) :long)]
                    (< 10 k 20)))
          pred1 (i/inter-fn [kv]
                  (let [^long k (b/read-buffer (l/k kv) :long)]
                    (when (< 10 k 13) k)))
          pred2 (i/inter-fn [k v] (< 10 k 20))
          pred3 (i/inter-fn [k v] (when (< 10 k 13) k))
          fks   (range 11 20)
          fvs   (map inc fks)
          res   (map (fn [k v] [k v]) fks fvs)
          rc    (count res)]
      (l/transact-kv lmdb txs)
      (is (= fvs (l/range-filter lmdb "c" pred [:all] :long :long true)))
      (is (= rc (l/range-filter-count lmdb "c" pred [:all] :long)))
      (is (= res (l/range-filter lmdb "c" pred [:all] :long :long)))
      (is (= [11 12] (l/range-keep lmdb "c" pred1 [:all] :long :long)))
      (is (= 11 (l/range-some lmdb "c" pred1 [:all] :long :long)))
      (is (= res (l/range-filter lmdb "c" pred2 [:all] :long :long false false)))
      (is (= [11 12] (l/range-keep lmdb "c" pred3 [:all] :long :long false)))
      (is (= 11 (l/range-some lmdb "c" pred3 [:all] :long :long false)))
      (is (= fks (map first
                      (l/range-filter lmdb "c" pred [:all]
                                      :long :ignore false))))

      (let [hm      (HashMap.)
            visitor (i/inter-fn [kv]
                      (let [^long k (b/read-buffer (l/k kv) :long)
                            ^long v (b/read-buffer (l/v kv) :long)]
                        (.put hm k v)))]
        (l/visit lmdb "c" visitor [:closed 11 19] :long)
        (is (= (into {} res) hm))))
    (l/close-kv lmdb)
    (u/delete-files dir)))

(deftest get-range-transduce-test
  (let [dir  (u/tmp-dir (str "lmdb-test-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags (conj c/default-env-flags :nosync)})]
    (l/open-dbi lmdb "a")
    (l/transact-kv lmdb [[:put "a" 1 2]
                         [:put "a" 3 4]
                         [:put "a" 5 6]])
    (is (= [[1 2] [3 4] [5 6]]
           (sequence (map identity) (l/get-range lmdb "a" [:all]))
           (eduction (map identity) (l/get-range lmdb "a" [:all]))))
    (l/close-kv lmdb)
    (u/delete-files dir)))

(deftest list-fns-test
  (let [dir   (u/tmp-dir (str "lmdb-test-" (UUID/randomUUID)))
        lmdb  (l/open-kv dir {:flags (conj c/default-env-flags :nosync)})
        pred  (i/inter-fn [kv]
                (let [^long v (b/read-buffer (l/v kv) :long)]
                  (even? v)))
        pred1 (i/inter-fn [kv]
                (let [^long v (b/read-buffer (l/v kv) :long)]
                  (when (even? v) v)))]
    (l/open-list-dbi lmdb "a")
    (l/put-list-items lmdb "a" 1 [3 4 3 2] :long :long)
    (l/put-list-items lmdb "a" 2 [7 9 4 3 2] :long :long)
    (l/put-list-items lmdb "a" 4 [14 13 20] :long :long)
    (l/put-list-items lmdb "a" 5 [7 9 14 30 12] :long :long)
    (l/put-list-items lmdb "a" 8 [17 9 4 3 12] :long :long)

    (is (= 3 (l/list-count lmdb "a" 1 :long))) ;; set behavior
    (is (l/in-list? lmdb "a" 8 17 :long :long))
    (is (= [2 3 4] (l/get-list lmdb "a" 1 :long :long)))

    (l/del-list-items lmdb "a" 8 [17 12] :long :long)
    (is (not (l/in-list? lmdb "a" 8 17 :long :long)))

    (is (= []
           (l/list-range lmdb "a" [:open-back 10 8] :long
                         [:less-than-back 8] :long)))
    (is (nil? (l/list-range-first lmdb "a" [:open-back 10 8] :long
                                  [:less-than-back 8] :long)))
    (is (= [] (l/list-range lmdb "a" [:greater-than 12] :long
                            [:greater-than 5] :long)))
    (is (= [] (l/list-range lmdb "a" [:less-than 2] :long
                            [:greater-than 5] :long)))
    (is (= []
           (l/list-range lmdb "a" [:closed-back 30 10] :long
                         [:closed-open-back 10 2] :long)))
    (is (= []
           (l/list-range lmdb "a" [:closed 0 30] :long
                         [:at-least 50] :long)))
    (is (= [[1 2] [1 3] [1 4] [2 2] [2 3] [2 4]]
           (l/list-range lmdb "a" [:closed 0 4] :long
                         [:closed 0 5] :long)))
    (is (= [[2 4] [2 3] [2 2] [1 4] [1 3] [1 2] ]
           (l/list-range lmdb "a" [:at-most-back 2] :long
                         [:at-most-back 5] :long)))
    (is (= [[5 30] [5 14] [5 12] [5 9] [5 7] [4 20] [4 14] [4 13]
            [2 9] [2 7]]
           (l/list-range lmdb "a" [:less-than-back 7] :long
                         [:greater-than-back 5] :long)))
    (is (= [[8 9]]
           (l/list-range lmdb "a" [:at-least-back 7] :long
                         [:at-least-back 5] :long)))
    (is (= [[2 4] [2 3] [2 2] [1 4] [1 3] [1 2] ]
           (l/list-range lmdb "a" [:at-most-back 3] :long
                         [:at-most-back 5] :long)))
    (is (= [[1 3] [1 2] [2 3] [2 2]]
           (l/list-range lmdb "a" [:at-most 2] :long
                         [:at-most-back 3] :long)))
    (is (= [[2 4]]
           (l/list-range lmdb "a" [:closed 2 2] :long
                         [:closed 4 5] :long)))
    (is (= [[4 14] [4 13] [5 14] [5 12] [5 9] [5 7]]
           (l/list-range lmdb "a" [:open-closed 2 5] :long
                         [:closed-back 14 5] :long)))
    (is (= [[2 9] [2 7] [2 4] [1 4]]
           (l/list-range lmdb "a" [:closed-back 3 0] :long
                         [:closed-back 10 4] :long)))
    (is (= [[2 9] [2 7]]
           (l/list-range lmdb "a" [:at-most 3] :long
                         [:at-least-back 7] :long)))
    (is (= [[8 9]]
           (l/list-range lmdb "a" [:open 5 13] :long
                         [:open-back 17 5] :long)))
    (is (= [[2 7]]
           (l/list-range lmdb "a" [:less-than-back 3] :long
                         [:closed-open-back 7 5] :long)))
    (is (= [[5 30]]
           (l/list-range lmdb "a" [:closed-back 10 0] :long
                         [:closed-open-back 30 20] :long)))
    (is (= [[1 4] [1 3] [2 4] [2 3]]
           (l/list-range lmdb "a" [:less-than 3] :long
                         [:open-closed-back 5 3] :long)))
    (is (= [[5 14] [5 30] [4 14] [4 20]]
           (l/list-range lmdb "a" [:open-back 7 3] :long
                         [:greater-than 13] :long)))
    (is (= [[8 4] [8 3]]
           (l/list-range lmdb "a" [:open 5 10] :long
                         [:less-than-back 8] :long)))
    (is (= [[8 4] [8 3]]
           (l/list-range lmdb "a" [:open 5 10] :long
                         [:less-than-back 8] :long)))
    (is (= [[8 3] [8 4] [8 9]]
           (l/list-range lmdb "a" [:open 5 10] :long [:all] :long)))
    (is (= [[1 3] [1 4]]
           (l/list-range lmdb "a" [:closed 0 1] :long [:closed 3 4] :long)))
    (is (= [[8 3] [8 4]]
           (l/list-range lmdb "a" [:closed-open-back 15 5] :long
                         [:less-than 8] :long)))
    (is (= [[1 2] [1 3] [1 4] [2 2] [2 3] [2 4]]
           (l/list-range lmdb "a" [:less-than 5] :long
                         [:less-than 5] :long)))
    (is (= [[8 9]]
           (l/list-range lmdb "a" [:greater-than 5] :long
                         [:greater-than 5] :long)))
    (is (= [[2 2] [2 3] [2 4] [1 2] [1 3] [1 4]]
           (l/list-range lmdb "a" [:closed-back 5 0] :long
                         [:closed 0 5] :long)))
    (is (= [[2 4] [2 3] [2 2] [1 4] [1 3] [1 2]]
           (l/list-range lmdb "a" [:closed-back 5 0] :long
                         [:closed-back 5 0] :long)))
    (is (= 6
           (l/list-range-count lmdb "a" [:closed-back 5 0] :long
                               [:closed-back 5 0] :long)))
    (is (= 3
           (l/list-range-count lmdb "a" [:closed-back 5 0] :long
                               [:closed-back 5 0] :long 3)))
    (is (= 1 (l/list-range-count lmdb "a" [:greater-than 3] :long
                                 [:greater-than 20] :long)))
    (is (= [5 30]
           (l/list-range-first lmdb "a" [:greater-than 3] :long
                               [:greater-than 20] :long)))
    (is (= [[5 30]]
           (l/list-range-first-n lmdb "a" 2 [:greater-than 3] :long
                                 [:greater-than 20] :long)))
    (is (= [[4 13][4 14]]
           (l/list-range-first-n lmdb "a" 2 [:greater-than 3] :long
                                 [:greater-than 10] :long)))
    (is (= [[2 2] [2 4]]
           (l/list-range-filter lmdb "a" pred [:closed 2 2] :long
                                [:all] :long)))
    (is (= [2 4] (l/list-range-keep lmdb "a" pred1 [:closed 2 2] :long
                                    [:all] :long)))
    (is (= 2 (l/list-range-some lmdb "a" pred1 [:closed 2 2] :long
                                [:all] :long)))
    (is (l/list-range-some lmdb "a" pred [:closed 2 2] :long [:all] :long))
    (is (= 5 (l/list-range-filter-count
               lmdb "a" pred [:open 2 6] :long [:greater-than 5] :long)))
    (is (= 2 (l/list-range-filter-count
               lmdb "a" pred [:open 2 6] :long [:greater-than 5] :long
               true 2)))
    (l/close-kv lmdb)
    (u/delete-files dir)))

(def range-types
  #{:all :all-back :at-least :at-most-back :at-most :at-least-back
    :closed :closed-back :closed-open :closed-open-back :greater-than
    :less-than-back :less-than :greater-than-back :open :open-back
    :open-closed :open-closed-back})

(test/defspec list-range-generative-test
  100
  (prop/for-all
    [ss (gen/sorted-set gen/nat {:num-elements 4})
     ranges (gen/vector (gen/elements range-types) 2)
     knoise gen/small-integer
     vnoise gen/small-integer]
    (let [dir  (u/tmp-dir (str "list-test-" (UUID/randomUUID)))
          lmdb (l/open-kv dir {:flags (conj c/default-env-flags :nosync)})
          _    (l/open-list-dbi lmdb "a")
          top  20
          _    (dotimes [i top]
                 (dotimes [j top]
                   (l/transact-kv
                     lmdb [[:put "a" i j :long :long]])))

          [^long n1 ^long n2 ^long n3 ^long n4] (vec ss)

          n3'             (+ ^long n3 ^long knoise)
          kstart          (min n1 n3')
          kend            (max n1 n3')
          n4'             (+ ^long n4 ^long vnoise)
          vstart          (min n2 n4')
          vend            (max n2 n4')
          [krange vrange] ranges

          res
          (fn [rt ^long start ^long end]
            (case rt
              :all               (range top)
              :all-back          (reverse (range top))
              :at-least          (range (if (< 0 start) start 0) top)
              :at-most-back      (reverse
                                   (range (if (< end top) (inc end) top)))
              :at-most           (range (if (< end top) (inc end) top))
              :at-least-back     (reverse
                                   (range (if (< 0 start) start 0) top))
              :closed            (range (if (< 0 start) start 0)
                                        (if (< end top) (inc end) top))
              :closed-back       (reverse
                                   (range (if (< 0 start) start 0)
                                          (if (< end top) (inc end) top)))
              :closed-open       (range (if (< 0 start) start 0)
                                        (if (< end top) end top))
              :closed-open-back  (reverse
                                   (range (if (<= 0 start) (inc start) 0)
                                          (if (< end top) (inc end) top)))
              :greater-than      (range (if (<= 0 start) (inc start) 0)
                                        top)
              :less-than-back    (reverse
                                   (range 0 (if (< end top) end top)))
              :less-than         (range 0 (if (< end top) end top))
              :greater-than-back (reverse
                                   (range (if (<= 0 start)
                                            (inc start) 0)
                                          top))
              :open              (range (if (<= 0 start) (inc start) 0)
                                        (if (< end top) end top))
              :open-back         (reverse
                                   (range (if (<= 0 start) (inc start) 0)
                                          (if (< end top) end top)))
              :open-closed       (range (if (<= 0 start) (inc start) 0)
                                        (if (< end top) (inc end) top))
              :open-closed-back  (reverse
                                   (range (if (< 0 start) start 0)
                                          (if (< end top) end top)))))
          info (fn [rt s e]
                 (case rt
                   :all               [:all]
                   :all-back          [:all-back]
                   :at-least          [:at-least s]
                   :at-most-back      [:at-most-back e]
                   :at-most           [:at-most e]
                   :at-least-back     [:at-least-back s]
                   :closed            [:closed s e]
                   :closed-back       [:closed-back e s]
                   :closed-open       [:closed-open s e]
                   :closed-open-back  [:closed-open-back e s]
                   :greater-than      [:greater-than s]
                   :less-than-back    [:less-than-back e]
                   :less-than         [:less-than e]
                   :greater-than-back [:greater-than-back s]
                   :open              [:open s e]
                   :open-back         [:open-back e s]
                   :open-closed       [:open-closed s e]
                   :open-closed-back  [:open-closed-back e s]))]
      (try
        (let [expected (for [i (res krange kstart kend)
                             j (res vrange vstart vend)]
                         [i j])
              got      (l/list-range lmdb "a"
                                     (info krange kstart kend) :long
                                     (info vrange vstart vend) :long)]
          (is (= expected got)))
        (finally
          (l/close-kv lmdb)
          (u/delete-files dir))))))
