(ns datalevin.lmdb-test
  (:require
   [datalevin.lmdb :as l]
   [datalevin.bits :as b]
   [datalevin.interpret :as i]
   [datalevin.util :as u]
   [datalevin.core :as dc]
   [datalevin.constants :as c]
   [datalevin.datom :as d]
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.test :refer [deftest testing is use-fixtures]]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.clojure-test :as test]
   [clojure.test.check.properties :as prop])
  (:import
   [java.util UUID Arrays]
   [java.lang Long]
   [org.eclipse.collections.impl.list.mutable FastList]))

(if (u/graal?)
  (require 'datalevin.binding.graal)
  (require 'datalevin.binding.java))

(use-fixtures :each db-fixture)

(deftest basic-ops-test
  (let [dir  (u/tmp-dir (str "lmdb-test-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:spill-opts {:spill-threshold 50}})]

    (is (= 50 (-> lmdb l/opts :spill-opts :spill-threshold)))

    (l/open-dbi lmdb "a")
    (l/open-dbi lmdb "b")
    (l/open-dbi lmdb "c" {:key-size (inc Long/BYTES) :val-size (inc Long/BYTES)})
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
                      [:put "b" "ok" 42 :string :long]
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
      (is (= 42 (l/get-value lmdb "b" "ok" :string :long)))
      (is (= :pi (l/get-value lmdb "d" 3.14 :double :keyword)))
      (is (= "nice year" (l/get-value lmdb "d" #inst "1969-01-01" :instant :string)))
      )

    (testing "delete"
      (l/transact-kv lmdb [[:del "a" 1]
                           [:del "a" :non-exist]
                           [:del "a" "random things that do not exist"]])
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

(deftest multi-threads-get-value-test
  (let [dir  (u/tmp-dir (str "lmdb-test-" (UUID/randomUUID)))
        lmdb (l/open-kv dir)]
    (l/open-dbi lmdb "a")
    (let [ks  (shuffle (range 0 100000))
          vs  (map inc ks)
          txs (map (fn [k v] [:put "a" k v :long :long]) ks vs)]
      (l/transact-kv lmdb txs)
      (is (= 100000 (l/entries lmdb "a")))
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
      (is (= 10000 (l/entries lmdb "a")))
      (is (= vs (map #(l/get-value lmdb "a" % :long :long) ks))))
    (l/close-kv lmdb)
    (u/delete-files dir)))

;; generative tests

(test/defspec datom-ops-generative-test
  100
  (prop/for-all
    [k gen/large-integer
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
  (< (alength ^bytes (b/serialize data)) limit))

(test/defspec data-ops-generative-test
  100
  (prop/for-all
    [k (gen/such-that (partial data-size-less-than? c/+max-key-size+)
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
  (prop/for-all
    [^bytes k (gen/not-empty gen/bytes)
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

(deftest list-basic-ops-test
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
    (l/open-list-dbi lmdb "inverted")

    (l/put-list-items lmdb "inverted" "a" [1 2 3 4] :string :long)
    (l/put-list-items lmdb "inverted" "b" [5 6 7] :string :long)

    (is (= [["a" 1] ["a" 2] ["a" 3] ["a" 4] ["b" 5] ["b" 6] ["b" 7]]
           (l/get-range lmdb "inverted" [:all] :string :long)))
    (is (= [["b" 7] ["b" 6] ["b" 5] ["a" 4] ["a" 3] ["a" 2] ["a" 1]]
           (l/get-range lmdb "inverted" [:all-back] :string :long)))
    (is (= [["a" 1] ["a" 2] ["a" 3] ["a" 4] ["b" 5] ["b" 6] ["b" 7]]
           (l/get-range lmdb "inverted" [:closed "a" "b"] :string :long)))
    ;; TODO this doesn't work
    ;; (is (= [["b" 7] ["b" 6] ["b" 5] ["a" 4] ["a" 3] ["a" 2] ["a" 1]]
    ;;        (l/get-range lmdb "inverted" [:closed-back "b" "a"] :string :long)))
    (is (= [["b" 5] ["b" 6] ["b" 7]]
           (l/get-range lmdb "inverted" [:closed "b" "b"] :string :long)))
    (is (= [["b" 5] ["b" 6] ["b" 7]]
           (l/get-range lmdb "inverted" [:open-closed "a" "b"] :string :long)))

    (is (= ["a" 1]
           (l/get-first lmdb "inverted" [:closed "a" "a"] :string :long)))

    (is (= (l/list-count lmdb "inverted" "a" :string) 4))
    (is (= (l/list-count lmdb "inverted" "b" :string) 3))

    (is (not (l/in-list? lmdb "inverted" "a" 7 :string :long)))
    (is (l/in-list? lmdb "inverted" "b" 7 :string :long))

    ;; (is (= (l/get-list lmdb "inverted" "a" :string :long) [1 2 3 4]))

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

(deftest list-string-test
  (let [dir  (u/tmp-dir (str "string-list-test-" (UUID/randomUUID)))
        lmdb (l/open-kv dir)
        pred (i/inter-fn
               [kv]
               (let [^String v (b/read-buffer (l/v kv) :string)]
                 (< (count v) 5)))]
    (l/open-list-dbi lmdb "str")

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

    (is (= ["a" "abc"]
           (l/get-first lmdb "str" [:closed "a" "a"] :string :string)))

    (is (= (l/list-count lmdb "str" "a" :string) 3))
    (is (= (l/list-count lmdb "str" "b" :string) 3))

    (is (not (l/in-list? lmdb "str" "a" "hello" :string :string)))
    (is (l/in-list? lmdb "str" "b" "hello" :string :string))

    ;; (is (= (l/get-list lmdb "str" "a" :string :string)
    ;;        ["abc" "defg" "hi"]))

    (l/del-list-items lmdb "str" "a" :string)

    (is (= (l/list-count lmdb "str" "a" :string) 0))
    (is (not (l/in-list? lmdb "str" "a" "hi" :string :string)))
    ;; (is (nil? (l/get-list lmdb "str" "a" :string :string)))

    (l/put-list-items lmdb "str" "b" ["good" "peace"] :string :string)

    (is (= (l/list-count lmdb "str" "b" :string) 5))
    (is (l/in-list? lmdb "str" "b" "good" :string :string))

    (l/del-list-items lmdb "str" "b" ["hello" "world"] :string :string)

    (is (= (l/list-count lmdb "str" "b" :string) 3))
    (is (not (l/in-list? lmdb "str" "b" "world" :string :string)))

    ;; (is (= (l/filter-list lmdb "str" "b" pred :string :string)
    ;;        ["good" "nice"]))
    ;; (is (= (l/filter-list-count lmdb "str" "b" pred :string) 2))

    (l/close-kv lmdb)
    (u/delete-files dir)))

(deftest validate-data-test
  (let [dir  (u/tmp-dir (str "valid-data-" (UUID/randomUUID)))
        lmdb (l/open-kv dir)]
    (l/open-dbi lmdb "a" {:validate-data? true})
    (is (thrown-with-msg? Exception #"Invalid data"
                          (l/transact-kv lmdb [[:put "a" 1 2 :string]])))
    (is (thrown-with-msg? Exception #"Invalid data"
                          (l/transact-kv lmdb [[:put "a" 1 "b" :long :long]])))
    (is (thrown-with-msg? Exception #"Invalid data"
                          (l/transact-kv lmdb [[:put "a" 1 1 :float]])))
    (is (thrown-with-msg? Exception #"Invalid data"
                          (l/transact-kv lmdb [[:put "a" 1000 1 :byte]])))
    (is (thrown-with-msg? Exception #"Invalid data"
                          (l/transact-kv lmdb [[:put "a" 1 1 :bytes]])))
    (is (thrown-with-msg? Exception #"Invalid data"
                          (l/transact-kv lmdb [[:put "a" "b" 1 :keyword]])))
    (is (thrown-with-msg? Exception #"Invalid data"
                          (l/transact-kv lmdb [[:put "a" "b" 1 :symbol]])))
    (is (thrown-with-msg? Exception #"Invalid data"
                          (l/transact-kv lmdb [[:put "a" "b" 1 :boolean]])))
    (is (thrown-with-msg? Exception #"Invalid data"
                          (l/transact-kv lmdb [[:put "a" "b" 1 :instant]])))
    (is (thrown-with-msg? Exception #"Invalid data"
                          (l/transact-kv lmdb [[:put "a" "b" 1 :uuid]])))
    (l/close-kv lmdb)
    (u/delete-files dir)))

(deftest custom-data-test
  (let [dir  (u/tmp-dir (str "custom-data-" (UUID/randomUUID)))
        lmdb (l/open-kv dir)
        lst  (doto (FastList.) (.add 1))
        data {:lst lst}]
    (l/open-dbi lmdb "a")
    (l/transact-kv lmdb [[:put "a" 1 data]])
    (is (not= data (l/get-value lmdb "a" 1)))
    ;; TODO somehow this doesn't work in graal
    (when-not (u/graal?)
      (is (= data
             (binding [c/*data-serializable-classes*
                       #{"org.eclipse.collections.impl.list.mutable.FastList"}]
               (l/get-value lmdb "a" 1)))))
    (l/close-kv lmdb)
    (u/delete-files dir)))

(deftest read-during-transaction-test
  (let [dir   (u/tmp-dir (str "lmdb-ctx-test-" (UUID/randomUUID)))
        lmdb  (l/open-kv dir)
        lmdb1 (l/mark-write lmdb)]
    (l/open-dbi lmdb "a")
    (l/open-dbi lmdb "d")

    (l/open-transact-kv lmdb)

    (testing "get-value"
      (is (nil? (l/get-value lmdb1 "a" 1 :data :data false)))
      (l/transact-kv lmdb
                     [[:put "a" 1 2]
                      [:put "a" 'a 1]
                      [:put "a" 5 {}]
                      [:put "a" :annunaki/enki true :attr :data]
                      [:put "a" :datalevin ["hello" "world"]]
                      [:put "a" 42 (d/datom 1 :a/b {:id 4}) :long :datom]])

      (is (= [1 2] (l/get-value lmdb1 "a" 1 :data :data false)))
      ;; non-writing txn will still read pre-transaction values
      (is (nil? (l/get-value lmdb "a" 1 :data :data false)))

      (is (nil? (l/get-value lmdb1 "d" #inst "1969-01-01" :instant :string
                             true)))
      (l/transact-kv lmdb
                     [[:put "d" 3.14 :pi :double :keyword]
                      [:put "d" #inst "1969-01-01" "nice year" :instant :string]])
      (is (= "nice year"
             (l/get-value lmdb1 "d" #inst "1969-01-01" :instant :string
                          true)))
      (is (nil? (l/get-value lmdb "d" #inst "1969-01-01" :instant :string
                             true))))

    (l/close-transact-kv lmdb)

    (testing "entries after transaction"
      (is (= 6 (l/entries lmdb "a")))
      (is (= 2 (l/entries lmdb "d"))))

    (l/close-kv lmdb)
    (u/delete-files dir)))

(deftest with-txn-map-resize-test
  (let [dir  (u/tmp-dir (str "map-size-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:mapsize 1})
        data {:description "this is going to be bigger than 1MB"
              :numbers     (range 1000000)}]
    (l/open-dbi lmdb "a")

    (dc/with-transaction-kv [db lmdb]
      (l/transact-kv db [[:put "a" 0 :prior]])
      (is (= :prior (l/get-value db "a" 0)))
      (l/transact-kv db [[:put "a" 1 data]])
      (is (= data (l/get-value db "a" 1))))

    (is (= :prior (l/get-value lmdb "a" 0)))
    (is (= data (l/get-value lmdb "a" 1)))

    (l/close-kv lmdb)
    (u/delete-files dir)))

(deftest with-transaction-kv-test
  (let [dir  (u/tmp-dir (str "with-tx-kv-test-" (UUID/randomUUID)))
        lmdb (l/open-kv dir)]
    (l/open-dbi lmdb "a")

    (testing "new value is invisible to outside readers"
      (dc/with-transaction-kv [db lmdb]
        (is (nil? (l/get-value db "a" 1 :data :data false)))
        (l/transact-kv db [[:put "a" 1 2]
                           [:put "a" :counter 0]])
        (is (= [1 2] (l/get-value db "a" 1 :data :data false)))
        (is (nil? (l/get-value lmdb "a" 1 :data :data false))))
      (is (= [1 2] (l/get-value lmdb "a" 1 :data :data false))))

    (testing "abort"
      (dc/with-transaction-kv [db lmdb]
        (l/transact-kv db [[:put "a" 1 3]])
        (is (= [1 3] (l/get-value db "a" 1 :data :data false)))
        (l/abort-transact-kv db))
      (is (= [1 2] (l/get-value lmdb "a" 1 :data :data false))))

    (testing "concurrent writes do not overwrite each other"
      (let [count-f
            #(dc/with-transaction-kv [db lmdb]
               (let [^long now (l/get-value db "a" :counter)]
                 (l/transact-kv db [[:put "a" :counter (inc now)]])
                 (l/get-value db "a" :counter)))]
        (is (= (set [1 2 3])
               (set (pcalls count-f count-f count-f))))))

    (testing "nested concurrent writes"
      (let [count-f
            #(dc/with-transaction-kv [db lmdb]
               (let [^long now (l/get-value db "a" :counter)]
                 (dc/with-transaction-kv [db' db]
                   (l/transact-kv db' [[:put "a" :counter (inc now)]]))
                 (l/get-value db "a" :counter)))]
        (is (= (set [4 5 6])
               (set (pcalls count-f count-f count-f))))))

    (l/close-kv lmdb)
    (u/delete-files dir)))
