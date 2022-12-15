(ns datalevin.scan-test
  (:require [datalevin.lmdb :as l]
            [datalevin.bits :as b]
            [datalevin.interpret :as i]
            [datalevin.util :as u]
            [clojure.test :refer [deftest testing is]])
  (:import [java.util UUID HashMap]
           [java.lang Long AutoCloseable]))

(if (u/graal?)
  (require 'datalevin.binding.graal)
  (require 'datalevin.binding.java))

(deftest get-first-test
  (let [dir  (u/tmp-dir (str "lmdb-test-" (UUID/randomUUID)))
        lmdb (l/open-kv dir)]
    (l/open-dbi lmdb "a")
    (l/open-dbi lmdb "c" {:key-size (inc Long/BYTES) :val-size (inc Long/BYTES)})
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

(deftest range-seq-test
  (let [dir  (u/tmp-dir (str "range-seq-test-" (UUID/randomUUID)))
        lmdb (l/open-kv dir)]
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
        lmdb (l/open-kv dir)]
    (l/open-dbi lmdb "c" {:key-size (inc Long/BYTES) :val-size (inc Long/BYTES)})
    (let [ks   (shuffle (range 0 1000))
          vs   (map inc ks)
          txs  (map (fn [k v] [:put "c" k v :long :long]) ks vs)
          res  (sort-by first (map (fn [k v] [k v]) ks vs))
          rres (reverse res)
          rc   (count res)]
      (l/transact-kv lmdb txs)
      (is (= res (l/get-range lmdb "c" [:all] :long :long)))
      (with-open [^AutoCloseable rs (l/range-seq lmdb "c" [:all] :long :long)]
        (is (= res (seq rs))))

      (is (= rc (l/range-count lmdb "c" [:all] :long)))

      (is (= rres (l/get-range lmdb "c" [:all-back] :long :long)))
      (with-open [^AutoCloseable rs (l/range-seq lmdb "c" [:all-back] :long :long)]
        (is (= (seq rs) rres)))

      (is (= (->> res (drop 990))
             (l/get-range lmdb "c" [:at-least 990] :long :long)))
      (with-open [^AutoCloseable rs (l/range-seq lmdb "c" [:at-least 990] :long :long)]
        (is (= (seq rs) (->> res (drop 990)))))

      (is (= [] (l/get-range lmdb "c" [:greater-than 1500] :long :ignore)))
      (with-open [^AutoCloseable rs
                  (l/range-seq lmdb "c" [:greater-than 1500] :long :ignore)]
        (is (= (seq rs) [])))

      (is (= 0 (l/range-count lmdb "c" [:greater-than 1500] :long)))

      (is (= res
             (l/get-range lmdb "c" [:less-than Long/MAX_VALUE] :long :long)))
      (with-open [^AutoCloseable rs
                  (l/range-seq lmdb "c" [:less-than Long/MAX_VALUE] :long :long)]
        (is (= (seq rs) res)))

      (is (= rc (l/range-count lmdb "c" [:less-than Long/MAX_VALUE] :long)))

      (is (= (take 10 res)
             (l/get-range lmdb "c" [:at-most 9] :long :long)))
      (with-open [^AutoCloseable rs
                  (l/range-seq lmdb "c" [:at-most 9] :long :long)]
        (is (= (seq rs) (take 10 res))))

      (is (= (->> res (drop 10) (take 100))
             (l/get-range lmdb "c" [:closed 10 109] :long :long)))
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
        db         (l/open-kv dir)
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

    (is (= [4 8 16] (l/get-range db misc-table [:closed 3 30] :long :long true)))

    (is (= [1 2 4 8 16 32] (l/get-range db misc-table [:closed 0 40]
                                        :long :long true)))


    (is (= [] (l/get-range db misc-table [:closed-back 2 32] :long :long true)))


    (is (= [32 16 8 4 2] (l/get-range db misc-table [:closed-back 32 2]
                                      :long :long true)))

    (with-open [^AutoCloseable rs
                (l/range-seq db misc-table [:closed-back 32 2]
                             :long :long true)]
      (is (= [32 16 8 4 2] (seq rs))))

    (is (= [16 8 4] (l/get-range db misc-table [:closed-back 30 3]
                                 :long :long true)))


    (is (= [32 16 8 4 2 1] (l/get-range db misc-table [:closed-back 40 0]
                                        :long :long true)))

    (is (= [2 4 8 16] (l/get-range db misc-table [:closed-open 2 32]
                                   :long :long true)))

    (with-open [^AutoCloseable rs
                (l/range-seq db misc-table [:closed-open 2 32]
                             :long :long true)]
      (is (= (seq rs) [2 4 8 16])))

    (is (= [4 8 16] (l/get-range db misc-table [:closed-open 3 30]
                                 :long :long true)))


    (is (= [1 2 4 8 16 32] (l/get-range db misc-table [:closed-open 0 40]
                                        :long :long true)))

    (is (= [] (l/get-range db misc-table [:closed-open-back 2 32]
                           :long :long true)))

    (with-open [^AutoCloseable rs
                (l/range-seq db misc-table [:closed-open-back 2 32]
                             :long :long true) ]
      (is (= (seq rs) [])))

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

    (with-open [^AutoCloseable rs
                (l/range-seq db misc-table
                             [:greater-than 2000] :long :long true)]
      (is (= (seq rs) [2048 4096 8192 16384])))

    (is (= [8 4 2 1] (l/get-range db misc-table
                                  [:less-than-back 16] :long :long true)))


    (is (= [16 8 4 2 1] (l/get-range db misc-table
                                     [:less-than-back 17] :long :long true)))

    (with-open [^AutoCloseable rs
                (l/range-seq db misc-table
                             [:less-than-back 17] :long :long true)]
      (is (= (seq rs) [16 8 4 2 1])))

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

    (with-open [^AutoCloseable rs
                (l/range-seq db misc-table
                             [:open-closed-back 40 0] :long :long true)]
      (is (= (seq rs) [32 16 8 4 2 1])))

    (l/close-kv db)
    (u/delete-files dir)))

(deftest get-some-test
  (let [dir  (u/tmp-dir (str "lmdb-test-" (UUID/randomUUID)))
        lmdb (l/open-kv dir)]
    (l/open-dbi lmdb "c" {:key-size (inc Long/BYTES) :val-size (inc Long/BYTES)})
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
    (l/open-dbi lmdb "c" {:key-size (inc Long/BYTES) :val-size (inc Long/BYTES)})
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
