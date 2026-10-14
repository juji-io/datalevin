(ns datalevin.native-sample-cleanup-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.bits :as b]
            [datalevin.core :as d]
            [datalevin.interface :as i]
            [datalevin.lmdb :as l]
            [datalevin.test.core :refer [db-fixture]]
            [datalevin.util :as u])
  (:import [java.lang AutoCloseable]
           [java.util Iterator]
           [org.bytedeco.javacpp Pointer SizeTPointer]))

(use-fixtures :each db-fixture)

(defn- with-samples-db [counted? f]
  (let [dir (u/tmp-dir (str "native-sample-cleanup-" (random-uuid)))
        db (d/open-kv dir {:wal? false})]
    (try
      (i/open-dbi db "keys" (when-not counted? {:flags #{:create}}))
      (i/open-list-dbi db "lists"
                       (when-not counted? {:flags #{:create :dupsort}}))
      (i/transact-kv db (mapv #(vector :put "keys" % (* 10 (long %)) :long :long)
                            [10 20 30 40 50]))
      (i/put-list-items db "lists" 10 [100 101 102] :long :long)
      (i/put-list-items db "lists" 20 [200 201 202] :long :long)
      (f db)
      (finally
        (d/close-kv db)
        (u/delete-files dir)))))

(defn- with-cursor [db name f]
  (let [dbi (i/get-dbi db name)
        rtx (i/get-rtx db)
        cur (l/get-cursor dbi rtx)]
    (try
      (f dbi rtx cur)
      (finally
        (l/return-cursor dbi cur)
        (i/return-rtx db rtx)))))

(defn- sample-iterable [list? dbi rtx cur indices k-range]
  ((if list? l/iterate-list-sample l/iterate-key-sample)
   dbi rtx cur indices k-range :long))

(defn- balanced-create [^Iterable iterable]
  (let [bytes (Pointer/totalBytes)
        pointers (Pointer/totalCount)]
    (try
      (.iterator iterable)
      (finally
        ;; The indices must already be freed at creation, without GC or close.
        (is (= bytes (Pointer/totalBytes)) "iterator creation retained native bytes")
        (is (= pointers (Pointer/totalCount)) "iterator creation retained native pointers")))))

(defn- read-samples [^Iterator iter]
  (loop [result []]
    (if (.hasNext iter)
      (let [kv (.next iter)]
        (recur (conj result [(b/read-buffer (l/k kv) :long)
                             (b/read-buffer (l/v kv) :long)])))
      result)))

(deftest sample-indices-close-before-iteration
  (with-samples-db
    true
    (fn [db]
      (doseq [[list? name expected]
              [[false "keys" [[10 100] [30 300] [50 500]]]
               [true "lists" [[10 100] [10 102] [20 201]]]]]
        (testing name
          (with-cursor db name
            (fn [dbi rtx cur]
              (doseq [[indices k-range result]
                      [[(long-array [0 2 4]) [:all] expected]
                       [(long-array []) [:all] []]
                       [(long-array [0 2 4]) [:closed 90 100] []]]]
                (let [iterable (sample-iterable list? dbi rtx cur indices k-range)]
                  (dotimes [_ 10]
                    (with-open [^AutoCloseable iter (balanced-create iterable)]
                      ;; Reuse a same-sized allocation before reading, so a
                      ;; native iterator retaining the freed input is exposed.
                      (with-open [overwrite (SizeTPointer. (long (alength ^longs indices)))]
                        (dotimes [n (alength ^longs indices)]
                          (.put overwrite n Long/MAX_VALUE)))
                      (is (= result (read-samples iter))))))))))))))

(deftest failed-sample-creation-releases-indices
  (with-samples-db
    false
    (fn [db]
      ;; Rank iterators reject uncounted DBIs through the real native API.
      (doseq [[list? name] [[false "keys"] [true "lists"]]]
        (testing name
          (with-cursor db name
            (fn [dbi rtx cur]
              (let [iterable (sample-iterable list? dbi rtx cur
                                              (long-array [0 2 4]) [:all])]
                (dotimes [_ 10]
                  (is (thrown? RuntimeException (balanced-create iterable))))))))))))
