(ns datalevin.compress-test
  (:require
   [datalevin.constants :as c]
   [datalevin.interface :as if]
   [datalevin.bits :as b]
   [datalevin.lmdb :as l]
   [datalevin.interface :as if]
   [datalevin.util :as u]
   [datalevin.binding.cpp :as cpp]
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.test :refer [deftest is use-fixtures]]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.clojure-test :as test]
   [clojure.test.check.properties :as prop])
  (:import
   [java.util UUID Arrays]
   [java.io File]
   [java.nio.file Files Paths]
   [java.lang Long]
   [datalevin.lmdb IListRandKeyValIterable IListRandKeyValIterator]))

(use-fixtures :each db-fixture)

(deftest sample-key-freqs-test
  (let [dir  (u/tmp-dir (str "sample-keys-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags (conj c/default-env-flags :nosync)})
        m    10
        ks   (shuffle (range 0 m))
        txs  #(map (fn [k v] [:put % k v :long :long]) ks ks)

        n    1000000
        ks1  (shuffle (range 0 n))
        txs1 #(map (fn [k v] [:put % k v :long :long]) ks1 ks1)]

    (if/open-dbi lmdb "u")
    (if/transact-kv lmdb (txs "u"))
    (let [freqs (cpp/sample-key-freqs lmdb)]
      (is (nil? freqs)))

    (if/open-dbi lmdb "v")
    (if/transact-kv lmdb (txs1 "v"))
    (let [^longs freqs (cpp/sample-key-freqs lmdb)]
      (is (= (alength freqs) c/+key-compress-num-symbols+))
      (is (< (* 2 ^long c/*compress-sample-size*) (aget freqs 0)))
      (is (< (aget freqs 1) (aget freqs 0)))
      (is (< (count (filter #(< 1 ^long %) (seq freqs)))
             (count (filter #(= ^long % 1) (seq freqs))))))

    (if/close-kv lmdb)
    (u/delete-files dir)))

(deftest list-sample-freqs-test
  (let [dir  (u/tmp-dir (str "sample-list-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags (conj c/default-env-flags :nosync)})
        m    100
        n    1000
        ks   (shuffle (range 0 m))
        vs   (shuffle (range 0 n))
        txs  #(for [k ks v vs] [:put % k v :id :id])]

    (if/open-list-dbi lmdb "l")
    (if/transact-kv lmdb (txs "l"))
    (let [^longs freqs (cpp/sample-key-freqs lmdb)]
      (is (= (alength freqs) c/+key-compress-num-symbols+))
      (is (< (* 2 ^long c/*compress-sample-size*) (aget freqs 0)))
      (is (< (aget freqs 1) (aget freqs 0)))
      (is (< (count (filter #(< 1 ^long %) (seq freqs)))
             (count (filter #(= ^long % 1) (seq freqs))))))

    (if/close-kv lmdb)
    (u/delete-files dir)))

#_(deftest kv-default-compressor-test
    (let [orig-dir (u/tmp-dir (str "kv-origin-" (UUID/randomUUID)))
          orig-kv  (l/open-kv orig-dir {:flags (conj c/default-env-flags :nosync)})
          comp-dir (u/tmp-dir (str "kv-compress-" (UUID/randomUUID)))
          comp-kv  (l/open-kv comp-dir {:flags        (conj c/default-env-flags :nosync)
                                        :key-compress :hu})]
      (if/open-dbi orig-kv "a")
      (if/open-dbi comp-kv "a")
      (dotimes [i 100]
        (let [bs (.getBytes (u/random-string (inc (rand-int 64))) "US-ASCII")]
          (if/transact-kv orig-kv [[:put "a" bs i :bytes :id]])
          (if/transact-kv comp-kv [[:put "a" bs i :bytes :id]])
          (is (= i
                 (if/get-value orig-kv "a" bs :bytes :id)
                 (if/get-value comp-kv "a" bs :bytes :id)))))
      (is (< (.length (File. (str comp-dir u/+separator+ c/data-file-name)))
             (.length (File. (str orig-dir u/+separator+ c/data-file-name)))))))
