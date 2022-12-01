(ns datalevin.remote-test
  (:require [datalevin.remote :as sut]
            [datalevin.storage :as st]
            [datalevin.lmdb :as l]
            [datalevin.interpret :as i]
            [datalevin.datom :as d]
            [datalevin.core :as dc]
            [datalevin.db :as db]
            [datalevin.constants :as c]
            [datalevin.client :as cl]
            [datalevin.util :as u]
            [datalevin.test.core :refer [server-fixture]]
            [clojure.string :as str]
            [clojure.test :refer [is testing deftest use-fixtures]])
  (:import [java.util UUID Arrays HashMap]
           [datalevin.datom Datom]
           [java.lang Thread]))

(use-fixtures :each server-fixture)

(deftest dt-store-ops-test
  (testing "permission"
    (is (thrown? Exception (sut/open "dtlv://someone:wrong@localhost/nodb")))

    (let [client (cl/new-client "dtlv://datalevin:datalevin@localhost")]
      (cl/create-user client "someone" "secret")
      (cl/grant-permission client :datalevin.role/someone
                           :datalevin.server/create
                           :datalevin.server/database
                           nil)
      (is (= (count (cl/list-user-permissions client "someone")) 3))

      (cl/create-user client "viewer" "secret")
      (cl/grant-permission client :datalevin.role/viewer
                           :datalevin.server/view
                           :datalevin.server/database
                           nil)))

  (testing "datalog store ops"
    (let [dir   "dtlv://someone:secret@localhost/ops-test"
          store (sut/open dir)]
      (is (instance? datalevin.remote.DatalogStore store))
      (is (= c/implicit-schema (st/schema store)))
      (is (= c/e0 (st/init-max-eid store)))
      (let [a  :a/b
            v  (UUID/randomUUID)
            d  (d/datom c/e0 a v)
            s  (assoc (st/schema store) a {:db/aid 3})
            b  :b/c
            p1 {:db/valueType :db.type/uuid}
            v1 (UUID/randomUUID)
            d1 (d/datom c/e0 b v1)
            s1 (assoc s b (merge p1 {:db/aid 4}))
            c  :c/d
            p2 {:db/valueType :db.type/ref}
            v2 (long (rand c/emax))
            d2 (d/datom c/e0 c v2)
            s2 (assoc s1 c (merge p2 {:db/aid 5}))
            t1 (st/last-modified store)]
        (st/load-datoms store [d])
        (is (<= t1 (st/last-modified store)))
        (is (= s (st/schema store)))
        (is (= 1 (st/datom-count store :eav)))
        (is (= 1 (st/datom-count store :ave)))
        (is (= 0 (st/datom-count store :vea)))
        (is (= [d] (st/fetch store d)))
        (is (= [d] (st/slice store :eavt d d)))
        (is (= true (st/populated? store :eav d d)))
        (is (= 1 (st/size store :eav d d)))
        (is (= d (st/head store :eav d d)))
        (st/swap-attr store b (i/inter-fn [& ms] (apply merge ms)) p1)
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
                                 (i/inter-fn [^Datom d] (= v (dc/datom-v d)))
                                 (d/datom c/e0 nil nil)
                                 (d/datom c/e0 nil nil))))
        (is (= d (st/head-filter store :eav
                                 (i/inter-fn [^Datom d] (= v (dc/datom-v d)))
                                 (d/datom c/e0 nil nil)
                                 (d/datom c/e0 nil nil))))
        (is (= [d] (st/slice-filter store :eav
                                    (i/inter-fn [^Datom d] (= v (dc/datom-v d)))
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
                                    (i/inter-fn [^Datom d] (= v (dc/datom-v d)))
                                    (d/datom c/e0 nil nil)
                                    (d/datom c/e0 nil nil))))
        (st/swap-attr store c (i/inter-fn [& ms] (apply merge ms)) p2)
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
              s3    (assoc s2 d (merge p3 {:db/aid 6}))
              s4    (assoc s3 :f/g {:db/aid 7 :db/valueType :db.type/string})
              store (sut/open dir {d p3})]
          (is (= s3 (st/schema store)))
          (st/set-schema store {:f/g {:db/valueType :db.type/string}})
          (is (= s4 (st/schema store)))
          (st/close store)))))

  (testing "data viewer permission"
    (let [dir   "dtlv://viewer:secret@localhost/ops-test"
          store (sut/open dir)]
      (is (instance? datalevin.remote.DatalogStore store))
      (is (not= c/implicit-schema (st/schema store)))
      (is (= 3 (st/datom-count store c/eav)))

      (is (thrown? Exception (st/set-schema store {:o/p {}})))
      (is (thrown? Exception (st/load-datoms store []))))))

(deftest dt-store-larger-test
  (let [dir   "dtlv://datalevin:datalevin@localhost/larger-test"
        end   100000
        store (sut/open dir)
        vs    (range 0 end)
        txs   (mapv d/datom (range c/e0 (+ c/e0 end)) (repeat :id)
                    vs)
        pred  (i/inter-fn [d] (odd? (dc/datom-v d)))]
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

(deftest same-client-multiple-dbs-test
  (let [uri-str "dtlv://datalevin:datalevin@localhost"
        client  (cl/new-client uri-str)
        store1  (sut/open-kv client (str uri-str "/mykv") nil)
        store2  (sut/open client (str uri-str "/mydt") nil nil)]
    (is (instance? datalevin.remote.KVStore store1))
    (is (instance? datalevin.remote.DatalogStore store2))

    (dc/open-dbi store1 "a")
    (dc/transact-kv store1 [[:put "a" "hello" "world"]])
    (is (= (dc/get-value store1 "a" "hello") "world"))
    (dc/close-kv store1)

    (let [conn (dc/conn-from-db (db/new-db store2))]
      (dc/transact! conn [{:hello "world"}])
      (is (= (dc/q '[:find ?w .
                     :where
                     [_ :hello ?w]]
                   @conn)
             "world"))
      (dc/close conn))))

(deftest remote-basic-ops-test
  (let [schema
        {:sales/country           {:db/valueType :db.type/string},
         :juji.data/display?      {:db/valueType :db.type/boolean},
         :juji.data/origin-column {:db/valueType :db.type/long},
         :sales/company           {:db/valueType :db.type/string},
         :sales/top-product-use   {:db/valueType :db.type/string},
         :juji.data/of-attribute  {:db/valueType :db.type/keyword},
         :juji.data/references    {:db/valueType :db.type/keyword},
         :juji.data/value         {:db/valueType :db.type/string},
         :sales/year              {:db/valueType :db.type/long},
         :sales/total             {:db/valueType :db.type/long},
         :juji.data/synonyms      {:db/valueType   :db.type/string,
                                   :db/cardinality :db.cardinality/many}}

        schema-update
        {:regions/region  {:db/valueType :db.type/string}
         :regions/country {:db/valueType :db.type/string}}

        dir  "dtlv://datalevin:datalevin@localhost/core"
        conn (dc/create-conn dir schema)
        txs
        [{:juji.data/synonyms      ["company" "customer"],
          :juji.data/display?      true,
          :db/ident                :sales/company,
          :juji.data/origin-column 0,
          :db/id                   -1}
         {:juji.data/references    :regions/country,
          :db/ident                :sales/country,
          :juji.data/origin-column 1,
          :db/id                   -2}
         {:sales/year              2019,
          :juji.data/synonyms      ["total" "spending" "payment"],
          :juji.data/display?      true,
          :db/ident                :sales/total,
          :juji.data/origin-column 2,
          :db/id                   -3}
         {:sales/year              2018,
          :db/ident                :sales/total,
          :juji.data/origin-column 3,
          :db/id                   -4}
         {:db/ident                :sales/top-product-use,
          :juji.data/origin-column 4,
          :db/id                   -5}
         {:juji.data/synonyms     ["US" "u.s." "usa" "united states" "america" "U.S.A."],
          :juji.data/of-attribute :sales/country,
          :juji.data/value        "us",
          :db/id                  -6}
         {:juji.data/synonyms     ["U.K." "UK" "British" "England" "English"
                                   "United Kingdom"],
          :juji.data/of-attribute :sales/country,
          :juji.data/value        "u-k",
          :db/id                  -7}
         {:sales/year            2019,
          :db/id                 -8,
          :sales/company         "IBM",
          :sales/country         "US",
          :sales/top-product-use "SRM",
          :sales/total           5}
         {:sales/year            2018,
          :db/id                 -9,
          :sales/company         "IBM",
          :sales/country         "US",
          :sales/top-product-use "SRM",
          :sales/total           32}
         {:sales/year            2019,
          :db/id                 -10,
          :sales/company         "PwC",
          :sales/country         "Germany",
          :sales/top-product-use "CRM",
          :sales/total           89}
         {:sales/year            2018,
          :db/id                 -11,
          :sales/company         "PwC",
          :sales/country         "Germany",
          :sales/top-product-use "CRM",
          :sales/total           75}
         {:sales/year            2019,
          :db/id                 -12,
          :sales/company         "Goodwill",
          :sales/country         "US",
          :sales/top-product-use "ERP",
          :sales/total           23}
         {:sales/year            2018,
          :db/id                 -13,
          :sales/company         "Goodwill",
          :sales/country         "US",
          :sales/top-product-use "ERP",
          :sales/total           43}
         {:sales/year            2019,
          :db/id                 -14,
          :sales/company         "HSBC",
          :sales/country         "U.K.",
          :sales/top-product-use "SCM",
          :sales/total           74}
         {:sales/year            2018,
          :db/id                 -15,
          :sales/company         "HSBC",
          :sales/country         "U.K.",
          :sales/top-product-use "SCM",
          :sales/total           69}
         {:sales/year            2019,
          :db/id                 -16,
          :sales/company         "Unilever",
          :sales/country         "U.K.",
          :sales/top-product-use "CRM",
          :sales/total           34}
         {:sales/year            2018,
          :db/id                 -17,
          :sales/company         "Unilever",
          :sales/country         "U.K.",
          :sales/top-product-use "CRM",
          :sales/total           23}]]
    (dc/transact! conn txs)
    (is (= (count (dc/datoms @conn :eavt)) 83))
    (is (= (dc/q '[:find ?st .
                   :where
                   [?e :sales/company "Unilever"]
                   [?e :sales/year 2018]
                   [?e :sales/total ?st]]
                 @conn)
           23))
    (is (= (set (dc/q '[:find [(pull ?e [*]) ...]
                        :in $ ?ns-in
                        :where
                        [?e :db/ident ?v]
                        [(namespace ?v) ?ns]
                        [(= ?ns ?ns-in)]]
                      @conn "sales"))
           #{{:db/id                   4,
              :db/ident                :sales/top-product-use,
              :juji.data/origin-column 4}
             {:db/id                   3,
              :db/ident                :sales/total,
              :juji.data/display?      true,
              :juji.data/origin-column 3,
              :sales/year              2018,
              :juji.data/synonyms      ["payment" "spending" "total"]}
             {:db/id                   2,
              :db/ident                :sales/country,
              :juji.data/origin-column 1,
              :juji.data/references    :regions/country}
             {:db/id                   1,
              :db/ident                :sales/company,
              :juji.data/display?      true,
              :juji.data/origin-column 0,
              :juji.data/synonyms      ["company" "customer"]}}))
    (is (= (dc/q '[:find [?v ...]
                   :in $ ?a
                   :where
                   [_ ?a ?v]]
                 @conn
                 :sales/country)
           ["US" "Germany" "U.K."]))
    (is (= (dc/q '[:find ?company ?total
                   :in $ ?year
                   :where
                   [?e :sales/year ?year]
                   [?e :sales/total ?total]
                   [?e :sales/company ?company]]
                 @conn
                 2018)
           #{["IBM" 32]
             ["Unilever" 23]
             ["PwC" 75]
             ["Goodwill" 43]
             ["HSBC" 69]}))
    (is (= (dc/q '[:find (pull ?e pattern)
                   :in $ pattern
                   :where
                   [?e :juji.data/value ?n]]
                 @conn [:juji.data/value [:juji.data/synonyms]])
           [[#:juji.data{:value    "u-k",
                         :synonyms ["British" "England" "English" "U.K."
                                    "UK" "United Kingdom"]}]
            [#:juji.data{:value    "us",
                         :synonyms ["U.S.A." "US" "america" "u.s."
                                    "united states" "usa"]}]]))
    (is (not (:regions/country (dc/schema conn))))
    (dc/update-schema conn schema-update)
    (is (:regions/country (dc/schema conn)))
    (is (:db/created-at (dc/schema conn)))
    (is (dc/conn? conn))
    (is (thrown? Exception (dc/update-schema conn {} #{:sales/year})))
    (dc/clear conn)
    (let [conn1 (dc/create-conn dir)]
      (is (= 0 (count (dc/datoms @conn1 :eavt))))
      (dc/close conn1))))

(deftest remote-conn-schema-update-test
  (let [schema {:user/email {:db/unique    :db.unique/identity
                             :db/valueType :db.type/string}
                :user/notes {:db/valueType   :db.type/ref
                             :db/cardinality :db.cardinality/many}
                :note/text  {:db/valueType :db.type/string}}

        dir  "dtlv://datalevin:datalevin@localhost/schema"
        _    (dc/create-conn dir)
        conn (dc/create-conn dir schema {:auto-entity-time? true})]
    (dc/transact! conn [{:user/email "eva@example.com"
                         :user/notes [{:note/text "do this do that"}
                                      {:note/text "enough is enough!"}]}])
    (is (= (-> (dc/entity @conn [:user/email "eva@example.com"])
               dc/touch
               :user/email)
           "eva@example.com"))
    (dc/close conn)))

(deftest remote-local-identity-test
  (let [schema
        {:juji.data/original-form {:db/valueType :db.type/string, :db/aid 1},
         :retail/sku              {:db/valueType :db.type/string, :db/aid 2},
         :retail/color            {:db/valueType :db.type/string, :db/aid 3},
         :juji.data/nominal?      {:db/valueType :db.type/boolean, :db/aid 4},
         :juji.data/no-index?     {:db/valueType :db.type/boolean, :db/aid 5},
         :juji.data/display?      {:db/valueType :db.type/boolean, :db/aid 6},
         :retail/type             {:db/valueType :db.type/string, :db/aid 7},
         :juji.data/origin-column {:db/valueType :db.type/long, :db/aid 8},
         :juji.data/id?           {:db/valueType :db.type/boolean, :db/aid 9},
         :retail/brand            {:db/valueType :db.type/string, :db/aid 10},
         :juji.data/unit          {:db/valueType   :db.type/string,
                                   :db/cardinality :db.cardinality/many, :db/aid 11},
         :db/ident                {:db/unique    :db.unique/identity,
                                   :db/valueType :db.type/keyword, :db/aid 0},
         :retail/material         {:db/valueType :db.type/string, :db/aid 12},
         :juji.data/of-attribute  {:db/valueType :db.type/keyword, :db/aid 13},
         :retail/price            {:db/valueType :db.type/double, :db/aid 14},
         :juji.data/references    {:db/valueType :db.type/keyword, :db/aid 15},
         :juji.data/value         {:db/valueType :db.type/string, :db/aid 16},
         :retail/gender           {:db/valueType :db.type/string, :db/aid 17},
         :retail/country          {:db/valueType :db.type/string, :db/aid 18},
         :juji.data/synonyms      {:db/valueType   :db.type/string,
                                   :db/cardinality :db.cardinality/many, :db/aid 19}}
        txs
        [{:juji.data/synonyms      ["sku" "apparel" "garment" "clothe" "item"],
          :juji.data/display?      true,
          :db/ident                :retail/sku,
          :juji.data/origin-column 0,
          :juji.data/original-form "sku",
          :db/id                   -1}
         {:juji.data/synonyms      ["company" "designer"],
          :juji.data/display?      true,
          :db/ident                :retail/brand,
          :juji.data/origin-column 1,
          :juji.data/original-form "brand",
          :db/id                   -2}
         {:juji.data/synonyms      [],
          :juji.data/display?      true,
          :db/ident                :retail/type,
          :juji.data/origin-column 2,
          :juji.data/original-form "Type",
          :db/id                   -3}
         {:db/ident                :retail/gender,
          :juji.data/origin-column 3,
          :juji.data/original-form "Gender",
          :db/id                   -4}
         {:db/ident                :retail/color,
          :juji.data/origin-column 4,
          :juji.data/original-form "color",
          :db/id                   -5}
         {:db/ident                :retail/price,
          :juji.data/origin-column 5,
          :juji.data/original-form "price ($)",
          :db/id                   -6}
         {:juji.data/synonyms      ["make in"],
          :db/ident                :retail/country,
          :juji.data/origin-column 6,
          :juji.data/original-form "country",
          :db/id                   -7}
         {:db/ident                :retail/material,
          :juji.data/origin-column 7,
          :juji.data/original-form "material",
          :db/id                   -8}
         {:juji.data/synonyms      [],
          :juji.data/of-attribute  :retail/type,
          :juji.data/value         "short",
          :juji.data/original-form "Shorts",
          :db/id                   -9}
         {:juji.data/synonyms      [],
          :juji.data/of-attribute  :retail/type,
          :juji.data/value         "short",
          :juji.data/original-form "Shorts",
          :db/id                   -10}
         {:retail/sku      "s123456",
          :retail/color    "white",
          :retail/type     "tshirt",
          :retail/brand    "365life",
          :db/id           -11,
          :retail/material "cotton",
          :retail/price    9.99,
          :retail/gender   "male",
          :retail/country  "japan"}
         {:retail/sku      "s234567",
          :retail/color    "red",
          :retail/type     "short",
          :retail/brand    "alfani",
          :db/id           -12,
          :retail/material "cotton",
          :retail/price    19.99,
          :retail/gender   "male",
          :retail/country  "japan"}
         {:retail/sku      "s345678",
          :retail/color    "silver",
          :retail/type     "short",
          :retail/brand    "charter club",
          :db/id           -13,
          :retail/material "denim",
          :retail/price    12.99,
          :retail/gender   "male",
          :retail/country  "japan"}
         {:retail/sku      "s456789",
          :retail/color    "black",
          :retail/type     "dress",
          :retail/brand    "giani bernini",
          :db/id           -14,
          :retail/material "silk",
          :retail/price    45.98,
          :retail/gender   "female",
          :retail/country  "germany"}
         {:retail/sku      "s567890",
          :retail/color    "silver",
          :retail/type     "dress",
          :retail/brand    "acorn",
          :db/id           -15,
          :retail/material "silk",
          :retail/price    23.69,
          :retail/gender   "female",
          :retail/country  "germany"}
         {:retail/sku      "s678901",
          :retail/color    "white",
          :retail/type     "skirt",
          :retail/brand    "artology",
          :db/id           -16,
          :retail/material "denim",
          :retail/price    19.99,
          :retail/gender   "female",
          :retail/country  "u.s."}
         {:retail/sku      "s123457",
          :retail/color    "red",
          :retail/type     "tshirt",
          :retail/brand    "365life",
          :db/id           -17,
          :retail/material "cotton",
          :retail/price    9.99,
          :retail/gender   "male",
          :retail/country  "japan"}
         {:retail/sku      "f123456",
          :retail/color    "red",
          :retail/type     "tshirt",
          :retail/brand    "365life",
          :db/id           -18,
          :retail/material "cotton",
          :retail/price    9.99,
          :retail/gender   "female",
          :retail/country  "japan"}
         {:retail/sku      "f123457",
          :retail/color    "pink",
          :retail/type     "tshirt",
          :retail/brand    "365life",
          :db/id           -19,
          :retail/material "cotton",
          :retail/price    9.99,
          :retail/gender   "female",
          :retail/country  "japan"}
         {:retail/sku      "f123458",
          :retail/color    "lime green",
          :retail/type     "tshirt",
          :retail/brand    "365life",
          :db/id           -20,
          :retail/material "cotton",
          :retail/price    8.99,
          :retail/gender   "female",
          :retail/country  "u.s."}
         {:retail/sku      "f4567890",
          :retail/color    "silver",
          :retail/type     "tshirt",
          :retail/brand    "acorn",
          :db/id           -21,
          :retail/material "silk",
          :retail/price    19.99,
          :retail/gender   "female",
          :retail/country  "germany"}
         {:retail/sku      "s6789020",
          :retail/color    "white",
          :retail/type     "skirt",
          :retail/brand    "artology",
          :db/id           -22,
          :retail/material "denim",
          :retail/price    49.99,
          :retail/gender   "female",
          :retail/country  "u.s."}]
        r-uri-str  "dtlv://datalevin:datalevin@localhost/xyz"
        l-dir      (u/tmp-dir (str "identity-test-" (UUID/randomUUID)))
        conn       (dc/create-conn r-uri-str schema)
        local-conn (dc/create-conn l-dir schema)]
    (dc/transact! conn txs)
    (dc/transact! local-conn txs)
    (is (= (dc/schema conn) (dc/schema local-conn)))
    (is (= (dc/datoms @conn :eav) (dc/datoms @local-conn :eav)))
    (is (= (dc/q '[:find ?e
                   :in $ ?m
                   :where
                   [?e :retail/material ?m]]
                 @conn "cotton")
           (dc/q '[:find ?e
                   :in $ ?m
                   :where
                   [?e :retail/material ?m]]
                 @local-conn "cotton")))
    (is (= (dc/pull @conn '[*] 22) (dc/pull @local-conn '[*] 22)))
    (is (= (dc/q '[:find [?e ...]
                   :in $ ?ns-in
                   :where
                   [(namespace ?v) ?ns]
                   [(= ?ns ?ns-in)]
                   [?e :juji.data/of-attribute ?v]]
                 @local-conn "retail")
           (dc/q '[:find [?e ...]
                   :in $ ?ns-in
                   :where
                   [(namespace ?v) ?ns]
                   [(= ?ns ?ns-in)]
                   [?e :juji.data/of-attribute ?v]]
                 @conn "retail")))
    (is (= (dc/q '[:find [?e ...]
                   :in $
                   :where
                   [?e :db/ident]] @conn)
           (dc/q '[:find [?e ...]
                   :in $
                   :where
                   [?e :db/ident]] @local-conn)))
    (dc/close conn)
    (dc/close local-conn)))

(deftest update-schema-test
  (let [dir  "dtlv://datalevin:datalevin@localhost/update-schema"
        conn (dc/create-conn dir
                             {:id {:db/unique    :db.unique/identity
                                   :db/valueType :db.type/long}})]
    (dc/transact! conn [{:id 1}])
    (is (= (dc/datoms @conn :eav) [(d/datom 1 :id 1)]))
    (is (thrown-with-msg? Exception #"unique constraint"
                          (dc/transact! conn [[:db/add 2 :id 1]])))

    (dc/update-schema conn {:id {:db/valueType :db.type/long}})
    (dc/transact! conn [[:db/add 2 :id 1]])
    (is (= (count (dc/datoms @conn :eav)) 2))

    (is (thrown-with-msg? Exception #"uniqueness change is inconsistent"
                          (dc/update-schema
                            conn {:id {:db/unique    :db.unique/identity
                                       :db/valueType :db.type/long}})))

    (is (thrown-with-msg? Exception #"Cannot delete attribute with datom"
                          (dc/update-schema conn nil #{:id})))

    (dc/update-schema conn nil nil {:id :identifer})
    (is (= (:identifer (dc/schema conn))
           {:db/valueType :db.type/long :db/aid 3}))
    (is (= (dc/datoms @conn :eav)
           [(d/datom 1 :identifer 1) (d/datom 2 :identifer 1)]))

    (dc/close conn)))

(deftest remote-fulltext-fns-test
  (let [dir      "dtlv://datalevin:datalevin@localhost/remote-fulltext-fns-test"
        analyzer (i/inter-fn
                   [^String text]
                   (map-indexed (fn [i ^String t]
                                  [t i (.indexOf text t)])
                                (str/split text #"\s")))
        db       (-> (dc/empty-db
                       dir
                       {:text {:db/valueType :db.type/string
                               :db/fulltext  true}}
                       {:auto-entity-time? true
                        :search-engine     {:analyzer analyzer}})
                     (dc/db-with
                       [{:db/id 1,
                         :text  "The quick red fox jumped over the lazy red dogs."}
                        {:db/id 2,
                         :text  "Mary had a little lamb whose fleece was red as fire."}
                        {:db/id 3,
                         :text  "Moby Dick is a story of a whale and a man obsessed."}]))]
    (is (= (dc/q '[:find ?e ?a ?v
                   :in $ ?q
                   :where [(fulltext $ ?q) [[?e ?a ?v]]]]
                 db "red fox")
           #{[1 :text "The quick red fox jumped over the lazy red dogs."]
             [2 :text "Mary had a little lamb whose fleece was red as fire."]}))
    (is (empty? (dc/q '[:find ?e ?a ?v
                        :in $ ?q
                        :where [(fulltext $ ?q) [[?e ?a ?v]]]]
                      db "")))
    (is (= (dc/datom-v
             (first (dc/fulltext-datoms db "red fox")))
           "The quick red fox jumped over the lazy red dogs."))
    (dc/close-db db)))

(deftest remote-with-transaction-kv-test
  (let [dir  "dtlv://datalevin:datalevin@localhost/remote-with-tx"
        lmdb (sut/open-kv dir)]
    (dc/open-dbi lmdb "a")

    (testing "new value is invisible to outside readers"
      (dc/with-transaction-kv [db lmdb]
        (is (nil? (dc/get-value db "a" 1 :data :data false)))
        (dc/transact-kv db [[:put "a" 1 2]
                            [:put "a" :counter 0]])
        (is (= [1 2] (dc/get-value db "a" 1 :data :data false)))
        (is (nil? (dc/get-value lmdb "a" 1 :data :data false))))
      (is (= [1 2] (dc/get-value lmdb "a" 1 :data :data false))))

    (testing "abort"
      (dc/with-transaction-kv [db lmdb]
        (dc/transact-kv db [[:put "a" 1 3]])
        (is (= [1 3] (dc/get-value db "a" 1 :data :data false)))
        (dc/abort-transact-kv db))
      (is (= [1 2] (dc/get-value lmdb "a" 1 :data :data false))))

    (testing "concurrent writes from same client do not overwrite each other"
      (let [count-f
            #(dc/with-transaction-kv [db lmdb]
               (let [^long now (dc/get-value db "a" :counter)]
                 (dc/transact-kv db [[:put "a" :counter (inc now)]])
                 (dc/get-value db "a" :counter)))]
        (is (= (set [1 2 3])
               (set (pcalls count-f count-f count-f))))))

    (testing "concurrent writes from diff clients do not overwrite each other"
      (let [count-f
            #(dc/with-transaction-kv [db (dc/open-kv dir)]
               (let [^long now (dc/get-value db "a" :counter)]
                 (dc/transact-kv db [[:put "a" :counter (inc now)]])
                 (dc/get-value db "a" :counter)))
            read-f (fn []
                     (Thread/sleep (rand-int 1000))
                     (dc/get-value lmdb "a" :counter))]
        (is (#{(set [4 5 6]) (set [3 4 5 6])}
              (set (pcalls count-f read-f  read-f
                           count-f count-f read-f))))))

    (dc/close-kv lmdb)))

(deftest remote-with-transaction-test
  (let [dir   "dtlv://datalevin:datalevin@localhost/remote-with-tx"
        conn  (dc/create-conn dir)
        query '[:find ?c .
                :in $ ?e
                :where [?e :counter ?c]]]

    (is (nil? (dc/q query @conn 1)))

    (testing "new value is invisible to outside readers"
      (dc/with-transaction [cn conn]
        (is (nil? (dc/q query @cn 1)))
        (dc/transact! cn [{:db/id 1 :counter 1}])
        (is (= 1 (dc/q query @cn 1)))
        (is (nil? (dc/q query @conn 1))))
      (is (= 1 (dc/q query @conn 1))))

    (testing "abort"
      (dc/with-transaction [cn conn]
        (dc/transact! cn [{:db/id 1 :counter 2}])
        (is (= 2 (dc/q query @cn 1)))
        (dc/abort-transact cn))
      (is (= 1 (dc/q query @conn 1))))

    (testing "concurrent writes do not overwrite each other"
      (let [count-f
            #(dc/with-transaction [cn conn]
               (let [^long now (dc/q query @cn 1)]
                 (dc/transact! cn [{:db/id 1 :counter (inc now)}])
                 (dc/q query @cn 1)))]
        (is (= (set [2 3 4])
               (set (pcalls count-f count-f count-f))))))

    (testing "concurrent writes from diff clients do not overwrite each other"
      (let [count-f
            #(dc/with-transaction [cn (dc/create-conn dir)]
               (let [^long now (dc/q query @cn 1)]
                 (dc/transact! cn [{:db/id 1 :counter (inc now)}])
                 (dc/q query @cn 1)))]
        (is (= (set [5 6 7])
               (set (pcalls count-f count-f count-f))))))

    (dc/close conn)))
