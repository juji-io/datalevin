(ns datalevin.remote-test
  (:require [datalevin.remote :as sut]
            [datalevin.server :as srv]
            [datalevin.storage :as st]
            [datalevin.lmdb :as l]
            [datalevin.datom :as d]
            [datalevin.bits :as b]
            [datalevin.constants :as c]
            [datalevin.util :as u]
            [clojure.test :refer [is are testing deftest use-fixtures]])
  (:import [java.util UUID Arrays]
           [datalevin.datom Datom]))

(defn server-fixture
  [f]
  (let [server (srv/create {:port c/default-port
                            :root (u/tmp-dir
                                    (str "remote-test-" (UUID/randomUUID)))})]
    (try
      (srv/start server)
      (f)
      (catch Exception e (throw e))
      (finally (srv/stop server)))))

(use-fixtures :each server-fixture)

(deftest dt-store-ops-test
  (let [dir   "dtlv://datalevin:datalevin@localhost/ops-test"
        store (sut/open dir)]
    (is (instance? datalevin.remote.DatalogStore store))
    (is (= c/implicit-schema (st/schema store)))
    (is (= c/e0 (st/init-max-eid store)))
    (let [a  :a/b
          v  (UUID/randomUUID)
          d  (d/datom c/e0 a v)
          s  (assoc (st/schema store) a {:db/aid 1})
          b  :b/c
          p1 {:db/valueType :db.type/uuid}
          v1 (UUID/randomUUID)
          d1 (d/datom c/e0 b v1)
          s1 (assoc s b (merge p1 {:db/aid 2}))
          c  :c/d
          p2 {:db/valueType :db.type/ref}
          v2 (long (rand c/emax))
          d2 (d/datom c/e0 c v2)
          s2 (assoc s1 c (merge p2 {:db/aid 3}))
          t1 (st/last-modified store)]
      (st/load-datoms store [d])
      (is (< t1 (st/last-modified store)))
      (is (= s (st/schema store)))
      (is (= 1 (st/datom-count store :eav)))
      (is (= 1 (st/datom-count store :ave)))
      (is (= 0 (st/datom-count store :vea)))
      (is (= [d] (st/fetch store d)))
      (is (= [d] (st/slice store :eavt d d)))
      (is (= true (st/populated? store :eav d d)))
      (is (= 1 (st/size store :eav d d)))
      (is (= d (st/head store :eav d d)))
      (st/swap-attr store b merge p1)
      (st/load-datoms store [d1])
      (is (= s1 (st/schema store)))
      (is (= 2 (st/datom-count store :eav)))
      (is (= 2 (st/datom-count store :ave)))
      (is (= 0 (st/datom-count store :vea)))
      (is (= [] (st/slice store :eav d (d/datom c/e0 :non-exist v1))))
      (is (= 0 (st/size store :eav d (d/datom c/e0 :non-exist v1))))
      (is (nil? (st/populated? store :eav d (d/datom c/e0 :non-exist v1))))
      (is (= d (st/head store :eav d d1)))
      (is (= 2 (st/size store :eav d d1)))
      (is (= [d d1] (st/slice store :eav d d1)))
      (is (= [d d1] (st/slice store :ave d d1)))
      (is (= [d1 d] (st/rslice store :eav d1 d)))
      (is (= [d d1] (st/slice store :eav
                              (d/datom c/e0 a nil)
                              (d/datom c/e0 nil nil))))
      (is (= [d1 d] (st/rslice store :eav
                               (d/datom c/e0 b nil)
                               (d/datom c/e0 nil nil))))
      (is (= 1 (st/size-filter store :eav
                               (fn [^Datom d] (= v (.-v d)))
                               (d/datom c/e0 nil nil)
                               (d/datom c/e0 nil nil))))
      (is (= d (st/head-filter store :eav
                               (fn [^Datom d] (= v (.-v d)))
                               (d/datom c/e0 nil nil)
                               (d/datom c/e0 nil nil))))
      (is (= [d] (st/slice-filter store :eav
                                  (fn [^Datom d] (= v (.-v d)))
                                  (d/datom c/e0 nil nil)
                                  (d/datom c/e0 nil nil))))
      (is (= [d1 d] (st/rslice store :ave d1 d)))
      (is (= [d d1] (st/slice store :ave
                              (d/datom c/e0 a nil)
                              (d/datom c/e0 nil nil))))
      (is (= [d1 d] (st/rslice store :ave
                               (d/datom c/e0 b nil)
                               (d/datom c/e0 nil nil))))
      (is (= [d] (st/slice-filter store :ave
                                  (fn [^Datom d] (= v (.-v d)))
                                  (d/datom c/e0 nil nil)
                                  (d/datom c/e0 nil nil))))
      (st/swap-attr store c merge p2)
      (st/load-datoms store [d2])
      (is (= s2 (st/schema store)))
      (is (= 3 (st/datom-count store c/eav)))
      (is (= 3 (st/datom-count store c/ave)))
      (is (= 1 (st/datom-count store c/vea)))
      (is (= [d2] (st/slice store :vea
                            (d/datom c/e0 nil v2)
                            (d/datom c/emax nil v2))))
      (st/load-datoms store [(d/delete d)])
      (is (= 2 (st/datom-count store c/eav)))
      (is (= 2 (st/datom-count store c/ave)))
      (is (= 1 (st/datom-count store c/vea)))
      (st/close store)
      (is (st/closed? store))
      (let [store (sut/open dir)]
        (is (= [d1] (st/slice store :eav d1 d1)))
        (st/load-datoms store [(d/delete d1)])
        (is (= 1 (st/datom-count store c/eav)))
        (st/load-datoms store [d d1])
        (is (= 3 (st/datom-count store c/eav)))
        (st/close store))
      (let [d     :d/e
            p3    {:db/valueType :db.type/long}
            s3    (assoc s2 d (merge p3 {:db/aid 4}))
            s4    (assoc s3 :f/g {:db/aid 5 :db/valueType :db.type/string})
            store (sut/open dir {d p3})]
        (is (= s3 (st/schema store)))
        (st/set-schema store {:f/g {:db/valueType :db.type/string}})
        (is (= s4 (st/schema store)))
        (st/close store)))))

(deftest dt-store-larger-test
  (let [dir   "dtlv://datalevin:datalevin@localhost/larger-test"
        end   1000000
        store (sut/open dir)
        vs    (range 0 end)
        txs   (mapv d/datom (range c/e0 (+ c/e0 end)) (repeat :id)
                    vs)
        pred  (fn [^Datom d] (odd? (.-v d)))]
    (is (instance? datalevin.remote.DatalogStore store))
    (st/load-datoms store txs)
    (is (= (d/datom c/e0 :id 0)
           (st/head store :eav (d/datom c/e0 :id nil)
                    (d/datom c/emax :id nil))))
    (is (= (d/datom (dec (+ c/e0 end) ) :id (dec (+ c/e0 end)))
           (st/tail store :ave (d/datom c/emax :id nil)
                    (d/datom c/e0 :id nil))))
    (is (= (filter pred txs)
           (st/slice-filter store :eav pred
                            (d/datom c/e0 nil nil)
                            (d/datom c/emax nil nil))))
    (is (= (reverse txs)
           (st/rslice store :eav
                      (d/datom c/emax nil nil) (d/datom c/e0 nil nil))))
    (st/close store)))

(deftest kv-store-ops-test
  (let [dir   "dtlv://datalevin:datalevin@localhost/testkv"
        store (sut/open-kv dir)]
    (is (instance? datalevin.remote.KVStore store))
    (l/open-dbi store "a")
    (l/open-dbi store "b")
    (l/open-dbi store "c" (inc Long/BYTES) (inc Long/BYTES))
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
      (l/transact-kv store [[:put "c" 1 (range 100000)]])
      (is (= (range 100000) (l/get-value store "c" 1))))

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
        (l/open-dbi store "r" (inc Long/BYTES) (inc Long/BYTES))
        (let [ks   (shuffle (range 0 10000))
              vs   (map inc ks)
              txs  (map (fn [k v] [:put "r" k v :long :long]) ks vs)
              pred (fn [kv]
                     (let [^long k (b/read-buffer (l/k kv) :long)]
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
