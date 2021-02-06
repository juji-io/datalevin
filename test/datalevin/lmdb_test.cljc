(ns datalevin.lmdb-test
  (:require [datalevin.lmdb :as l]
            [datalevin.bits :as b]
            [datalevin.util :as u]
            [datalevin.binding.graal]
            [datalevin.binding.java]
            [datalevin.constants :as c]
            [datalevin.datom :as d]
            [clojure.test :refer [deftest testing is]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.clojure-test :as test]
            [clojure.test.check.properties :as prop]
            [taoensso.nippy :as nippy])
  (:import [java.util UUID Arrays]))

(deftest basic-ops-test
  (let [dir  (u/tmp-dir (str "lmdb-test-" (UUID/randomUUID)))
        lmdb (l/open-lmdb dir)]
    (.open-dbi lmdb "a")
    (.open-dbi lmdb "b")
    (.open-dbi lmdb "c" (inc Long/BYTES) (inc Long/BYTES))
    (.open-dbi lmdb "d")

    (testing "transact"
      (.transact lmdb
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
      (is (= 6 (.entries lmdb "a")))
      (is (= 10 (.entries lmdb "b"))))

    (testing "get-value"
      (is (= 2 (.get-value lmdb "a" 1)))
      (is (= [1 2] (.get-value lmdb "a" 1 :data :data false)))
      (is (= true (.get-value lmdb "a" :annunaki/enki :attr :data)))
      (is (= (d/datom 1 :a/b {:id 4}) (.get-value lmdb "a" 42 :long :datom)))
      (is (nil? (.get-value lmdb "a" 2)))
      (is (nil? (.get-value lmdb "b" 1)))
      (is (= 5 (.get-value lmdb "b" [-1 -235254457N])))
      (is (= 1 (.get-value lmdb "a" 'a)))
      (is (= {} (.get-value lmdb "a" 5)))
      (is (= ["hello" "world"] (.get-value lmdb "a" :datalevin)))
      (is (= 3 (.get-value lmdb "b" 2)))
      (is (= 4 (.get-value lmdb "b" :a)))
      (is (= #{1 2} (.get-value lmdb "b" (byte 0x01) :byte)))
      (is (= :bk (.get-value lmdb "b" (byte-array [0x41 0x42]) :bytes)))
      (is (Arrays/equals ^bytes (byte-array [0x41 0x42 0x43])
                         ^bytes (.get-value lmdb "b" :bv :data :bytes)))
      (is (= :long (.get-value lmdb "b" 1 :long :data)))
      (is (= 1 (.get-value lmdb "b" :long :data :long)))
      (is (= 3 (.get-value lmdb "b" 2 :long :long)))
      (is (= 42 (.get-value lmdb "b" "ok" :string :int)))
      (is (= :pi (.get-value lmdb "d" 3.14 :double :keyword))))

    (testing "delete"
      (.transact lmdb [[:del "a" 1]
                       [:del "a" :non-exist]])
      (is (nil? (.get-value lmdb "a" 1))))

    (testing "entries-again"
      (is (= 5 (.entries lmdb "a")))
      (is (= 10 (.entries lmdb "b"))))

    (testing "non-existent dbi"
      (is (thrown-with-msg? Exception #"open-dbi" (.get-value lmdb "z" 1))))

    (testing "handle val overflow automatically"
      (.transact lmdb [[:put "c" 1 (range 100000)]])
      (is (= (range 100000) (.get-value lmdb "c" 1))))

    (testing "key overflow throws"
      (is (thrown-with-msg? Exception #"BufferOverflow"
                            (.transact lmdb [[:put "a" (range 1000) 1]]))))

    (testing "close then re-open, clear and drop"
      (let [dir (.-dir lmdb)]
        (.close-env lmdb)
        (is (.closed? lmdb))
        (let [lmdb  (l/open-lmdb dir)
              dbi-a (.open-dbi lmdb "a")]
          (is (= "a" (.dbi-name dbi-a)))
          (is (= ["hello" "world"] (.get-value lmdb "a" :datalevin)))
          (.clear-dbi lmdb "a")
          (is (nil? (.get-value lmdb "a" :datalevin)))
          (.drop-dbi lmdb "a")
          (is (thrown-with-msg? Exception #"open-dbi" (.get-value lmdb "a" 1)))
          (.close-env lmdb))))
    (b/delete-files dir)))


(deftest reentry-test
  (let [dir  (u/tmp-dir (str "lmdb-test-" (UUID/randomUUID)))
        lmdb (l/open-lmdb dir)]
    (.open-dbi lmdb "a")
    (.transact lmdb [[:put "a" :old 1]])
    (is (= 1 (.get-value lmdb "a" :old)))
    (let [res (future
                (let [lmdb2 (l/open-lmdb dir)]
                  (.open-dbi lmdb2 "a")
                  (is (= 1 (.get-value lmdb2 "a" :old)))
                  (.transact lmdb2 [[:put "a" :something 1]])
                  (is (= 1 (.get-value lmdb2 "a" :something)))
                  (is (= 1 (.get-value lmdb "a" :something)))
                  ;; should not close this
                  ;; https://github.com/juji-io/datalevin/issues/7
                  (.close-env lmdb2)
                  1))]
      (is (= 1 @res)))
    (is (thrown-with-msg? Exception #"multiple LMDB"
                          (.get-value lmdb "a" :something)))
    (.close-env lmdb)
    (b/delete-files dir)))

(deftest get-first-test
  (let [dir  (u/tmp-dir (str "lmdb-test-" (UUID/randomUUID)))
        lmdb (l/open-lmdb dir)]
    (.open-dbi lmdb "a")
    (.open-dbi lmdb "c" (inc Long/BYTES) (inc Long/BYTES))
    (let [ks  (shuffle (range 0 1000))
          vs  (map inc ks)
          txs (map (fn [k v] [:put "c" k v :long :long]) ks vs)]
      (.transact lmdb txs)
      (is (= [0 1] (.get-first lmdb "c" [:all] :long :long)))
      (is (= [0 nil] (.get-first lmdb "c" [:all] :long :ignore)))
      (is (= [999 1000] (.get-first lmdb "c" [:all-back] :long :long)))
      (is (= [9 10] (.get-first lmdb "c" [:at-least 9] :long :long)))
      (is (= [10 11] (.get-first lmdb "c" [:greater-than 9] :long :long)))
      (is (= true (.get-first lmdb "c" [:greater-than 9] :long :ignore true)))
      (is (nil? (.get-first lmdb "c" [:greater-than 1000] :long :ignore)))
      (.transact lmdb [[:put "a" 0xff 1 :byte]
                       [:put "a" 0xee 2 :byte]
                       [:put "a" 0x11 3 :byte]])
      (is (= 3 (.get-first lmdb "a" [:all] :byte :data true)))
      (is (= 1 (.get-first lmdb "a" [:all-back] :byte :data true))))
    (.close-env lmdb)
    (b/delete-files dir)))

(deftest get-range-test
  (let [dir  (u/tmp-dir (str "lmdb-test-" (UUID/randomUUID)))
        lmdb (l/open-lmdb dir)]
    (.open-dbi lmdb "c" (inc Long/BYTES) (inc Long/BYTES))
    (let [ks  (shuffle (range 0 1000))
          vs  (map inc ks)
          txs (map (fn [k v] [:put "c" k v :long :long]) ks vs)
          res (sort-by first (map (fn [k v] [k v]) ks vs))
          rc  (count res)]
      (.transact lmdb txs)
      (is (= [] (.get-range lmdb "c" [:greater-than 1500] :long :ignore)))
      (is (= 0 (.range-count lmdb "c" [:greater-than 1500] :long)))
      (is (= res (.get-range lmdb "c" [:all] :long :long)))
      (is (= rc (.range-count lmdb "c" [:all] :long)))
      (is (= res
             (.get-range lmdb "c" [:less-than Long/MAX_VALUE] :long :long)))
      (is (= rc (.range-count lmdb "c" [:less-than Long/MAX_VALUE] :long)))
      (is (= (take 10 res)
             (.get-range lmdb "c" [:at-most 9] :long :long)))
      (is (= (->> res (drop 10) (take 100))
             (.get-range lmdb "c" [:closed 10 109] :long :long)))
      (is (= (->> res (drop 990))
             (.get-range lmdb "c" [:at-least 990] :long :long)))
      (is (= (->> res (drop 10) (take 100) (map second))
             (.get-range lmdb "c" [:closed 10 109] :long :long true))))
    (.close-env lmdb)
    (b/delete-files dir)))

(deftest get-some-test
  (let [dir  (u/tmp-dir (str "lmdb-test-" (UUID/randomUUID)))
        lmdb (l/open-lmdb dir)]
    (.open-dbi lmdb "c" (inc Long/BYTES) (inc Long/BYTES))
    (let [ks   (shuffle (range 0 100))
          vs   (map inc ks)
          txs  (map (fn [k v] [:put "c" k v :long :long]) ks vs)
          pred (fn [kv]
                 (let [^long k (b/read-buffer (l/k kv) :long)]
                   (> k 15)))]
      (.transact lmdb txs)
      (is (= 17 (.get-some lmdb "c" pred [:all] :long :long true)))
      (is (= [16 17] (.get-some lmdb "c" pred [:all] :long :long))))
    (.close-env lmdb)
    (b/delete-files dir)))

(deftest range-filter-test
  (let [dir  (u/tmp-dir (str "lmdb-test-" (UUID/randomUUID)))
        lmdb (l/open-lmdb dir)]
    (.open-dbi lmdb "c" (inc Long/BYTES) (inc Long/BYTES))
    (let [ks   (shuffle (range 0 100))
          vs   (map inc ks)
          txs  (map (fn [k v] [:put "c" k v :long :long]) ks vs)
          pred (fn [kv]
                 (let [^long k (b/read-buffer (l/k kv) :long)]
                   (< 10 k 20)))
          fks  (range 11 20)
          fvs  (map inc fks)
          res  (map (fn [k v] [k v]) fks fvs)
          rc   (count res)]
      (.transact lmdb txs)
      (is (= fvs (.range-filter lmdb "c" pred [:all] :long :long true)))
      (is (= rc (.range-filter-count lmdb "c" pred [:all] :long)))
      (is (= res (.range-filter lmdb "c" pred [:all] :long :long))))
    (.close-env lmdb)
    (b/delete-files dir)))

(deftest multi-threads-get-value-test
  (let [dir  (u/tmp-dir (str "lmdb-test-" (UUID/randomUUID)))
        lmdb (l/open-lmdb dir)]
    (.open-dbi lmdb "a")
    (let [ks  (shuffle (range 0 100000))
          vs  (map inc ks)
          txs (map (fn [k v] [:put "a" k v :long :long]) ks vs)]
      (.transact lmdb txs)
      (is (= vs (pmap #(.get-value lmdb "a" % :long :long) ks))))
    (.close-env lmdb)
    (b/delete-files dir)))

;; generative tests

(test/defspec datom-ops-generative-test
  100
  (prop/for-all [k gen/large-integer
                 e gen/large-integer
                 a gen/keyword-ns
                 v gen/any-equatable]
                (let [dir    (u/tmp-dir (str "lmdb-test-" (UUID/randomUUID)))
                      lmdb   (l/open-lmdb dir)
                      _      (.open-dbi lmdb "a")
                      d      (d/datom e a v e)
                      _      (.transact lmdb [[:put "a" k d :long :datom]])
                      put-ok (= d (.get-value lmdb "a" k :long :datom))
                      _      (.transact lmdb [[:del "a" k :long]])
                      del-ok (nil? (.get-value lmdb "a" k :long))]
                  (.close-env lmdb)
                  (b/delete-files dir)
                  (is (and put-ok del-ok)))))


(defn- data-size-less-than?
  [^long limit data]
  (< (alength ^bytes (nippy/fast-freeze data)) limit))

(test/defspec data-ops-generative-test
  100
  (prop/for-all [k (gen/such-that (partial data-size-less-than? c/+max-key-size+)
                                  gen/any-equatable)
                 v (gen/such-that (partial data-size-less-than? c/+default-val-size+)
                                  gen/any-equatable)]
                (let [dir    (u/tmp-dir (str "lmdb-test-" (UUID/randomUUID)))
                      lmdb   (l/open-lmdb dir)
                      _      (.open-dbi lmdb "a")
                      _      (.transact lmdb [[:put "a" k v]])
                      put-ok (= v (.get-value lmdb "a" k))
                      _      (.transact lmdb [[:del "a" k]])
                      del-ok (nil? (.get-value lmdb "a" k))]
                  (.close-env lmdb)
                  (b/delete-files dir)
                  (is (and put-ok del-ok)))))

(test/defspec bytes-ops-generative-test
  100
  (prop/for-all [^bytes k (gen/not-empty gen/bytes)
                 ^bytes v (gen/not-empty gen/bytes)]
                (let [dir    (u/tmp-dir (str "lmdb-test-" (UUID/randomUUID)))
                      lmdb   (l/open-lmdb dir)
                      _      (.open-dbi lmdb "a")
                      _      (.transact lmdb [[:put "a" k v :bytes :bytes]])
                      put-ok (Arrays/equals v
                                            ^bytes
                                            (.get-value
                                              lmdb "a" k :bytes :bytes))
                      _      (.transact lmdb [[:del "a" k :bytes]])
                      del-ok (nil? (.get-value lmdb "a" k :bytes))]
                  (.close-env lmdb)
                  (b/delete-files dir)
                  (is (and put-ok del-ok)))))

(test/defspec long-ops-generative-test
  100
  (prop/for-all [^long k gen/large-integer
                 ^long v gen/large-integer]
                (let [dir    (u/tmp-dir (str "lmdb-test-" (UUID/randomUUID)))
                      lmdb   (l/open-lmdb dir)
                      _      (.open-dbi lmdb "a")
                      _      (.transact lmdb [[:put "a" k v :long :long]])
                      put-ok (= v ^long (.get-value lmdb "a" k :long :long))
                      _      (.transact lmdb [[:del "a" k :long]])
                      del-ok (nil? (.get-value lmdb "a" k)) ]
                  (.close-env lmdb)
                  (b/delete-files dir)
                  (is (and put-ok del-ok)))))
