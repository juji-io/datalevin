(ns datalevin.remote-kv-test
  (:require
   [datalevin.remote :as sut]
   [datalevin.lmdb :as l]
   [datalevin.constants :as c]
   [datalevin.bits :as b]
   [datalevin.util :as u]
   [datalevin.datom :as d]
   [datalevin.core :as dc]
   [datalevin.interpret :as i]
   [datalevin.test.core :refer [server-fixture]]
   [clojure.test :refer [is testing deftest use-fixtures]])
  (:import
   [java.util UUID Arrays]))

(use-fixtures :each server-fixture)

(deftest kv-store-ops-test
  (let [dir   "dtlv://datalevin:datalevin@localhost/testkv"
        store (sut/open-kv dir)]
    (is (instance? datalevin.remote.KVStore store))

    (l/open-dbi store "a")
    (l/open-dbi store "b")
    (l/open-dbi store "c" {:key-size (inc Long/BYTES)
                           :val-size (inc Long/BYTES)})
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
                      [:put "d" 3.14 :pi :double :keyword]
                      [:put "d" #inst "1969-01-01" "nice year" :instant :string]
                      [:put "d" [-1 0 1 2 3 4] 1 [:long]]
                      [:put "d" [:a :b :c :d] [1 2 3] [:keyword] [:long]]
                      [:put "d" [-1 "heterogeneous" :datalevin/tuple] 2
                       [:long :string :keyword]]
                      [:put "d"  [:ok -0.687 "nice"] [2 4]
                       [:keyword :double :string] [:long]]]))

    (testing "entries"
      (is (= 5 (:entries (l/stat store))))
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
      (is (= :pi (l/get-value store "d" 3.14 :double :keyword)))
      (is (= "nice year"
             (l/get-value store "d" #inst "1969-01-01" :instant :string))))

    (testing "get-first and get-first-n"
      (is (= [1 2] (l/get-first store "a" [:closed 1 10] :data)))
      (is (= [[1 2] [5 {}]] (l/get-first-n store "a" 2 [:closed 1 10] :data)))
      (is (= [[1 2] [5 {}]] (l/get-first-n store "a" 3 [:closed 1 10] :data))))

    (testing "delete"
      (l/transact-kv store [[:del "a" 1]
                            [:del "a" :non-exist]
                            [:del "a" "random things that do not exist"]])
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
      (is (thrown? Exception
                   (l/transact-kv store [[:put "a" (range 1000) 1]]))))

    (testing "close then re-open, clear and drop"
      (l/close-kv store)
      (is (l/closed-kv? store))
      (let [store (sut/open-kv dir)]
        (l/open-dbi store "a")
        (is (= ["hello" "world"] (l/get-value store "a" :datalevin)))
        (l/clear-dbi store "a")
        (is (nil? (l/get-value store "a" :datalevin)))
        (l/drop-dbi store "a")
        (is (thrown? Exception (l/get-value store "a" 1)))
        (l/close-kv store)))

    (testing "range and filter queries"
      (let [store (sut/open-kv dir)]
        (l/open-dbi store "r" {:key-size (inc Long/BYTES)
                               :val-size (inc Long/BYTES)})
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
          (is (= rc (l/range-filter-count store "r" pred [:all] :long nil)))
          (is (= fvs (l/range-filter store "r" pred [:all] :long :long true)))
          (is (= res (l/range-filter store "r" pred [:all] :long :long)))
          (is (= 12 (l/get-some store "r" pred [:all] :long :long true)))
          (is (= [0 1] (l/get-first store "r" [:all] :long :long)))
          (is (= 10000 (l/range-count store "r" [:all] :long)))
          (is (= (range 1 10001)
                 (l/get-range store "r" [:all] :long :long true))))
        (l/close-kv store)))))

(deftest list-basic-ops-test
  (let [dir     (str "dtlv://datalevin:datalevin@localhost/" (UUID/randomUUID))
        lmdb    (l/open-kv dir)
        sum     (volatile! 0)
        visitor (i/inter-fn
                    [vb]
                  (let [^long v (b/read-buffer vb :long)]
                    (vswap! sum #(+ ^long %1 ^long %2) v)))]
    (l/open-list-dbi lmdb "l")

    (l/put-list-items lmdb "l" "a" [1 2 3 4] :string :long)
    (l/put-list-items lmdb "l" "b" [5 6 7] :string :long)
    (l/put-list-items lmdb "l" "c" [3 6 9] :string :long)

    (is (= (l/entries lmdb "l") 10))

    (is (= [["a" 1] ["a" 2] ["a" 3] ["a" 4] ["b" 5] ["b" 6] ["b" 7]
            ["c" 3] ["c" 6] ["c" 9]]
           (l/get-range lmdb "l" [:all] :string :long)))
    (is (= [["a" 1] ["a" 2] ["a" 3] ["a" 4] ["b" 5] ["b" 6] ["b" 7]]
           (l/get-range lmdb "l" [:closed "a" "b"] :string :long)))
    (is (= [["b" 5] ["b" 6] ["b" 7]]
           (l/get-range lmdb "l" [:closed "b" "b"] :string :long)))
    (is (= [["b" 5] ["b" 6] ["b" 7]]
           (l/get-range lmdb "l" [:open-closed "a" "b"] :string :long)))
    (is (= [["c" 3] ["c" 6] ["c" 9] ["b" 5] ["b" 6] ["b" 7]
            ["a" 1] ["a" 2] ["a" 3] ["a" 4]]
           (l/get-range lmdb "l" [:all-back] :string :long)))

    (is (= ["a" 1]
           (l/get-first lmdb "l" [:closed "a" "a"] :string :long)))
    (is (= [["a" 1] ["a" 2]]
           (l/get-first-n lmdb "l" 2 [:closed "a" "c"] :string :long)))
    (is (= [["a" 1] ["a" 2]]
           (l/list-range-first-n lmdb "l" 2 [:closed "a" "c"] :string
                                 [:closed 1 5] :long)))
    (is (= [3 6 9]
           (l/get-list lmdb "l" "c" :string :long)))

    (is (= [["a" 1] ["a" 2] ["a" 3] ["a" 4] ["b" 5] ["b" 6] ["b" 7]
            ["c" 3] ["c" 6] ["c" 9]]
           (l/list-range lmdb "l" [:all] :string [:all] :long)))
    (is (= [["a" 2] ["a" 3] ["a" 4] ["c" 3]]
           (l/list-range lmdb "l" [:closed "a" "c"] :string
                         [:closed 2 4] :long)))
    (is (= [["c" 9] ["c" 6] ["c" 3] ["b" 7] ["b" 6] ["b" 5]
            ["a" 4] ["a" 3] ["a" 2] ["a" 1]]
           (l/list-range lmdb "l" [:all-back] :string [:all-back] :long)))
    (is (= [["c" 3]]
           (l/list-range lmdb "l" [:at-least "b"] :string
                         [:at-most-back 4] :long)))

    (is (= [["b" 5]]
           (l/list-range lmdb "l" [:open "a" "c"] :string
                         [:less-than 6] :long)))

    (is (= (l/list-count lmdb "l" "a" :string) 4))
    (is (= (l/list-count lmdb "l" "b" :string) 3))

    (is (not (l/in-list? lmdb "l" "a" 7 :string :long)))
    (is (l/in-list? lmdb "l" "b" 7 :string :long))

    (is (= (l/get-list lmdb "l" "a" :string :long) [1 2 3 4]))
    (is (= (l/get-list lmdb "l" "a" :string :long) [1 2 3 4]))

    (l/visit-list lmdb "l" visitor "a" :string)
    (is (= @sum 10))

    (l/del-list-items lmdb "l" "a" :string)

    (is (= (l/list-count lmdb "l" "a" :string) 0))
    (is (not (l/in-list? lmdb "l" "a" 1 :string :long)))
    (is (empty? (l/get-list lmdb "l" "a" :string :long)))

    (l/put-list-items lmdb "l" "b" [1 2 3 4] :string :long)

    (is (= [1 2 3 4 5 6 7]
           (l/get-list lmdb "l" "b" :string :long)))
    (is (= (l/list-count lmdb "l" "b" :string) 7))
    (is (l/in-list? lmdb "l" "b" 1 :string :long))

    (l/del-list-items lmdb "l" "b" [1 2] :string :long)

    (is (= (l/list-count lmdb "l" "b" :string) 5))
    (is (not (l/in-list? lmdb "l" "b" 1 :string :long)))
    (is (= [3 4 5 6 7]
           (l/get-list lmdb "l" "b" :string :long)))
    (l/close-kv lmdb)))

(deftest list-string-test
  (let [dir   (str "dtlv://datalevin:datalevin@localhost/" (UUID/randomUUID))
        lmdb  (l/open-kv dir {:flags (conj c/default-env-flags :nosync)})
        pred  (i/inter-fn [kv]
                          (let [^String v (b/read-buffer (l/v kv) :string)]
                            (< (count v) 5)))
        pred1 (i/inter-fn [kv]
                          (let [^String v (b/read-buffer (l/v kv) :string)]
                            (when (< (count v) 5) v)))]
    (l/open-list-dbi lmdb "str")
    (is (l/list-dbi? lmdb "str"))

    (l/put-list-items lmdb "str" "a" ["abc" "hi" "defg" ] :string :string)
    (l/put-list-items lmdb "str" "b" ["hello" "world" "nice"] :string :string)

    (is (= [["a" "abc"] ["a" "defg"] ["a" "hi"]
            ["b" "hello"] ["b" "nice"] ["b" "world"]]
           (l/get-range lmdb "str" [:all] :string :string)))
    (is (= [["a" "abc"] ["a" "defg"] ["a" "hi"]
            ["b" "hello"] ["b" "nice"] ["b" "world"]]
           (l/get-range lmdb "str" [:closed "a" "b"] :string :string)))
    (is (= [["b" "hello"] ["b" "nice"] ["b" "world"]]
           (l/get-range lmdb "str" [:closed "b" "b"] :string :string)))
    (is (= [["b" "hello"] ["b" "nice"] ["b" "world"]]
           (l/get-range lmdb "str" [:open-closed "a" "b"] :string :string)))

    (is (= [["b" "nice"]]
           (l/list-range-filter lmdb "str" pred [:greater-than "a"] :string
                                [:all] :string)))

    (is (= ["nice"]
           (l/list-range-keep lmdb "str" pred1 [:greater-than "a"] :string
                              [:all] :string)))
    (is (= "nice"
           (l/list-range-some lmdb "str" pred1 [:greater-than "a"] :string
                              [:all] :string)))

    (is (= ["a" "abc"]
           (l/get-first lmdb "str" [:closed "a" "a"] :string :string)))

    (is (= (l/list-count lmdb "str" "a" :string) 3))
    (is (= (l/list-count lmdb "str" "b" :string) 3))

    (is (not (l/in-list? lmdb "str" "a" "hello" :string :string)))
    (is (l/in-list? lmdb "str" "b" "hello" :string :string))

    (is (= (l/get-list lmdb "str" "a" :string :string)
           ["abc" "defg" "hi"]))

    (l/del-list-items lmdb "str" "a" :string)

    (is (= (l/list-count lmdb "str" "a" :string) 0))
    (is (not (l/in-list? lmdb "str" "a" "hi" :string :string)))
    (is (empty? (l/get-list lmdb "str" "a" :string :string)))

    (l/put-list-items lmdb "str" "b" ["good" "peace"] :string :string)

    (is (= (l/list-count lmdb "str" "b" :string) 5))
    (is (l/in-list? lmdb "str" "b" "good" :string :string))

    (l/del-list-items lmdb "str" "b" ["hello" "world"] :string :string)

    (is (= (l/list-count lmdb "str" "b" :string) 3))
    (is (not (l/in-list? lmdb "str" "b" "world" :string :string)))

    (l/close-kv lmdb)))

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
    (l/close-kv rstore)
    (u/delete-files dst)))

(deftest re-index-test
  (let [dir  "dtlv://datalevin:datalevin@localhost/re-index"
        lmdb (sut/open-kv dir)]
    (l/open-dbi lmdb "misc")

    (l/transact-kv
      lmdb
      [[:put "misc" :datalevin "Hello, world!"]
       [:put "misc" 42 {:saying "So Long, and thanks for all the fish"
                        :source "The Hitchhiker's Guide to the Galaxy"}]])
    (let [lmdb1 (l/re-index lmdb {})]
      (l/open-dbi lmdb1 "misc")
      (is (= [[42
               {:saying "So Long, and thanks for all the fish",
                :source "The Hitchhiker's Guide to the Galaxy"}]
              [:datalevin "Hello, world!"]]
             (l/get-range lmdb1 "misc" [:all])))

      ;; TODO https://github.com/juji-io/datalevin/issues/212
      #_(l/visit
          lmdb1 "misc"
          (i/inter-fn
            [kv]
            (let [k (dc/read-buffer (dc/k kv) :data)]
              (when (= k 42)
                (l/transact-kv
                  lmdb1
                  [[:put "misc" 42 "Don't panic"]]))))

          [:all])
      #_(is (= [[42 "Don't panic"] [:datalevin "Hello, world!"]]
               (l/get-range lmdb1 "misc" [:all])))
      (l/close-kv lmdb1))))
