(ns datalevin.core-test
  (:require
   [datalevin.core :as sut]
   [datalevin.datom :as dd]
   [datalevin.util :as u]
   [datalevin.test.core :as tdc :refer [db-fixture]]
   #?(:cljs [cljs.test :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer [is deftest testing use-fixtures]]))
  (:import
   [java.util UUID Arrays]
   [java.lang Thread]))

(use-fixtures :each db-fixture)

(deftest basic-ops-test
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

        dir  (u/tmp-dir (str "datalevin-core-test-" (UUID/randomUUID)))
        conn (sut/create-conn dir schema)
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
    (sut/transact! conn txs)
    (is (= 83 (count (sut/datoms @conn :eavt))))
    (is (= (set (sut/q '[:find [(pull ?e [*]) ...]
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
    (is (= (sut/q '[:find [?v ...]
                    :in $ ?a
                    :where
                    [_ ?a ?v]]
                  @conn
                  :sales/country)
           ["US" "Germany" "U.K."]))
    (is (= (sut/q '[:find ?company ?total
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
    (is (= (sut/q '[:find (pull ?e pattern)
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
    (is (not (:regions/country (sut/schema conn))))
    (sut/update-schema conn schema-update)
    (is (:regions/country (sut/schema conn)))
    (is (:db/created-at (sut/schema conn)))

    (is (sut/conn? conn))
    (sut/clear conn)
    (let [conn1 (sut/create-conn dir)]
      (is (= 0 (count (sut/datoms @conn1 :eavt))))
      (sut/close conn1))
    (u/delete-files dir)))

(deftest instant-update-test
  (let [dir   (u/tmp-dir (str "datalevin-instant-update-test-" (UUID/randomUUID)))
        conn  (sut/create-conn dir {:foo/id   {:db/unique    :db.unique/identity
                                               :db/valueType :db.type/string}
                                    :foo/date {:db/valueType :db.type/instant}})
        query '[:find ?d .
                :where
                [?e :foo/id "foo"]
                [?e :foo/date ?d]]]
    (sut/transact! conn [{:foo/id   "foo"
                          :foo/date (java.util.Date.)}])
    (Thread/sleep 100)
    (let [d1 (sut/q query @conn)]
      (sut/transact! conn [{:foo/id   "foo"
                            :foo/date (java.util.Date.)}])
      (is (not= d1 (sut/q query @conn))))
    (sut/close conn)
    (u/delete-files dir)))

(deftest instant-compare-test
  (let [dir  (u/tmp-dir (str "datalevin-instant-compare-test-" (UUID/randomUUID)))
        conn (sut/create-conn dir {:foo/id   {:db/unique    :db.unique/identity
                                              :db/valueType :db.type/string}
                                   :foo/num  {:db/valueType :db.type/long}
                                   :foo/date {:db/valueType :db.type/instant}})
        now  (java.util.Date.)
        t42  (java.util.Date. 42)
        t50  (java.util.Date. 50)
        t100 (java.util.Date. 100)]
    (sut/transact! conn [{:foo/id   "foo"
                          :foo/num  20
                          :foo/date now}
                         {:foo/id   "bar"
                          :foo/num  30
                          :foo/date t42}
                         {:foo/id   "woo"
                          :foo/num  40
                          :foo/date t100}])
    (is (= (sut/q
             '[:find ?d
               :in $ ?m
               :where
               [?e :foo/date ?d]
               [(.getTime ?d) ?t]
               [(> ?t ?m)]]
             @conn
             50)
           (sut/q
             '[:find ?d
               :in $ ?m
               :where
               [?e :foo/date ?d]
               [(.after ?d ?m)]]
             @conn
             t50)
           #{[t100] [now]}))
    (sut/close conn)
    (u/delete-files dir)))

(deftest other-language-test
  (let [dir  (u/tmp-dir (str "datalevin-other-lang-test-" (UUID/randomUUID)))
        conn (sut/get-conn dir)]
    (sut/transact! conn [{:german "Ümläüt"}])
    (is (= '([{:db/id 1 :german "Ümläüt"}])
           (sut/q '[:find (pull ?e [*])
                    :where
                    [?e :german]]
                  @conn)))
    (sut/transact! conn [{:chinese "您好"}])
    (is (= '([{:db/id 2 :chinese "您好"}])
           (sut/q '[:find (pull ?e [*])
                    :where
                    [?e :chinese]]
                  @conn)))
    (sut/close conn)
    (u/delete-files dir)))

(deftest bytes-test
  (let [dir        (u/tmp-dir (str "datalevin-bytes-test-" (UUID/randomUUID)))
        conn       (sut/create-conn
                     dir
                     {:entity-things {:db/valueType   :db.type/ref
                                      :db/cardinality :db.cardinality/many}
                      :foo-bytes     {:db/valueType :db.type/bytes}})
        ^bytes bs  (.getBytes "foooo")
        ^bytes bs1 (.getBytes "foooo")
        ^bytes bs2 (.getBytes ^String (apply str (range 50000)))]
    (sut/transact! conn [{:foo-bytes bs}])
    (sut/transact! conn [{:entity-things [{:foo-bytes bs1}]}])
    (sut/transact! conn [{:foo-bytes bs2}])
    (sut/transact! conn [{:entity-things
                          [{:foo-bytes bs}
                           {:foo-bytes bs1}]}])
    (let [res (sort-by second
                       (sut/q '[:find ?b ?e
                                :where
                                [?e :foo-bytes ?b]]
                              @conn))]
      (is (= 5 (count res)))
      (is (bytes? (ffirst res)))
      (is (Arrays/equals bs ^bytes (ffirst res)))
      (is (Arrays/equals bs1 ^bytes (first (second res))))
      (is (Arrays/equals bs2 ^bytes (first (nth res 2)))))
    (sut/close conn)
    (u/delete-files dir)))

(deftest id-large-bytes-test
  (let [dir        (u/tmp-dir (str "id-large-bytes-test-" (UUID/randomUUID)))
        ^bytes bs  (.getBytes ^String (apply str (range 1000)))
        ^bytes bs1 (.getBytes ^String (apply str (range 1000)))
        db         (-> (sut/empty-db
                         dir
                         {:id    {:db/valueType :db.type/string
                                  :db/unique    :db.unique/identity}
                          :bytes {:db/valueType :db.type/bytes}})
                       (sut/db-with
                         [{:id    "foo"
                           :bytes bs}])
                       (sut/db-with
                         [{:id    "foo"
                           :bytes bs1}]))]
    (let [res (sort-by second
                       (sut/q '[:find ?b ?e
                                :where
                                [?e :bytes ?b]]
                              db))]
      (is (= 2 (count res)))
      (is (bytes? (ffirst res)))
      (is (Arrays/equals bs ^bytes (ffirst res)))
      (is (Arrays/equals bs1 ^bytes (first (second res)))))
    (sut/close-db db)
    (u/delete-files dir)))

(deftest float-transact-test
  (let [schema {:name   {:db/valueType :db.type/string}
                :height {:db/valueType :db.type/float}}
        dir    (u/tmp-dir (str "datalevin-float-test-" (UUID/randomUUID)))
        conn   (sut/get-conn dir schema)]
    (sut/transact! conn [{:name "John" :height 1.73}
                         {:name "Peter" :height 1.92}])
    (is (= (sut/pull (sut/db conn) '[*] 1)
           {:name "John" :height (float 1.73) :db/id 1}))
    (sut/close conn)
    (u/delete-files dir)))

(deftest copy-test
  (testing "kv db copy"
    (let [src (u/tmp-dir (str "kv-copy-test-" (UUID/randomUUID)))
          dst (u/tmp-dir (str "kv-copy-test-" (UUID/randomUUID)))
          db  (sut/open-kv src)
          dbi "a"]
      (sut/open-dbi db dbi)
      (sut/transact-kv db [[:put dbi "Hello" "Datalevin"]])
      (sut/copy db dst true)
      (is (= (sut/stat db)
             (if (u/apple-silicon?)
               {:psize          16384,
                :depth          1,
                :branch-pages   0,
                :leaf-pages     1,
                :overflow-pages 0,
                :entries        1}
               {:psize          4096,
                :depth          1,
                :branch-pages   0,
                :leaf-pages     1,
                :overflow-pages 0,
                :entries        1})))
      (doseq [i (sut/list-dbis db)]
        (is (= (sut/stat db i)
               (if (u/apple-silicon?)
                 {:psize          16384,
                  :depth          1,
                  :branch-pages   0,
                  :leaf-pages     1,
                  :overflow-pages 0,
                  :entries        1}
                 {:psize          4096,
                  :depth          1,
                  :branch-pages   0,
                  :leaf-pages     1,
                  :overflow-pages 0,
                  :entries        1}))))
      (let [db-copied (sut/open-kv dst)]
        (sut/open-dbi db-copied dbi)
        (is (= (sut/get-value db-copied dbi "Hello") "Datalevin"))
        (sut/close-kv db-copied))
      (sut/close-kv db)
      (u/delete-files src)
      (u/delete-files dst)))
  (testing "dl db copy"
    (let [src  (u/tmp-dir (str "dl-copy-test-" (UUID/randomUUID)))
          dst  (u/tmp-dir (str "dl-copy-test-" (UUID/randomUUID)))
          conn (sut/create-conn src)]
      (sut/transact! conn [{:name "datalevin"}])
      (sut/copy (sut/db conn) dst true)
      (let [conn-copied (sut/create-conn dst)]
        (is (= (sut/q '[:find ?n .
                        :in $
                        :where [_ :name ?n]]
                      (sut/db conn-copied))
               "datalevin"))
        (sut/close conn-copied))
      (sut/close conn)
      (u/delete-files src)
      (u/delete-files dst))))

(deftest kv-test
  (let [dir  (u/tmp-dir (str "datalevin-kv-test-" (UUID/randomUUID)))
        lmdb (sut/open-kv dir)]
    (sut/open-dbi lmdb "misc")

    (sut/transact-kv
      lmdb
      [[:put "misc" :datalevin "Hello, world!"]
       [:put "misc" 42 {:saying "So Long, and thanks for all the fish"
                        :source "The Hitchhiker's Guide to the Galaxy"}]])
    (is (= [[42
             {:saying "So Long, and thanks for all the fish",
              :source "The Hitchhiker's Guide to the Galaxy"}]
            [:datalevin "Hello, world!"]]
           (sut/get-range lmdb "misc" [:all])))

    (sut/visit
      lmdb "misc"
      (fn [kv]
        (let [k (sut/read-buffer (sut/k kv) :data)]
          (when (= k 42)
            (sut/transact-kv
              lmdb
              [[:put "misc" 42 "Don't panic"]]))))

      [:all])
    (is (= [[42 "Don't panic"] [:datalevin "Hello, world!"]]
           (sut/get-range lmdb "misc" [:all])))
    (sut/close-kv lmdb)
    (u/delete-files dir)))

(deftest with-transaction-test
  (let [dir   (u/tmp-dir (str "with-tx-test-" (UUID/randomUUID)))
        conn  (sut/create-conn dir)
        query '[:find ?c .
                :in $ ?e
                :where [?e :counter ?c]]]
    (is (nil? (sut/q query @conn 1)))

    (testing "new value is invisible to outside readers"
      (sut/with-transaction [cn conn]
        (is (nil? (sut/q query @cn 1)))
        (sut/transact! cn [{:db/id 1 :counter 1}])
        (is (= 1 (sut/q query @cn 1)))
        (is (nil? (sut/q query @conn 1))))
      (is (= 1 (sut/q query @conn 1))))

    (testing "abort"
      (sut/with-transaction [cn conn]
        (sut/transact! cn [{:db/id 1 :counter 2}])
        (is (= 2 (sut/q query @cn 1)))
        (sut/abort-transact cn))
      (is (= 1 (sut/q query @conn 1))))

    (testing "concurrent writes do not overwrite each other"
      (let [count-f
            #(sut/with-transaction [cn conn]
               (let [^long now (sut/q query @cn 1)]
                 (sut/transact! cn [{:db/id 1 :counter (inc now)}])
                 (sut/q query @cn 1)))]
        (is (= (set [2 3 4])
               (set (pcalls count-f count-f count-f))))))
    (sut/close conn)
    (u/delete-files dir)))

(deftest with-txn-map-resize-test
  (let [dir   (u/tmp-dir (str "with-tx-resize-test-" (UUID/randomUUID)))
        conn  (sut/create-conn dir nil {:kv-opts {:mapsize 1}})
        query '[:find ?e .
                :in $ ?d
                :where [?e :content ?d]]
        prior "prior data"
        big   "bigger than 1MB"]

    (sut/with-transaction [cn conn]
      (sut/transact! cn [{:content prior :numbers [1 2]}])
      (is (= 1 (sut/q query @cn prior)))
      (sut/transact! cn [{:content big
                          :numbers (range 1000000)}])
      (is (= 2 (sut/q query @cn big))))

    (is (= 1 (sut/q query @conn prior)))
    (is (= 2 (sut/q query @conn big)))

    (is (= 4 (count (sut/datoms @conn :eav))))

    (sut/close conn)
    (u/delete-files dir)))

(deftest simulated-tx-test
  (let [dir  (u/tmp-dir (str "sim-tx-test-" (UUID/randomUUID)))
        conn (sut/create-conn dir
                              {:id {:db/unique    :db.unique/identity
                                    :db/valueType :db.type/long}})]
    (let [rp (sut/transact! conn [{:id 1}])]
      (is (= (:tx-data rp) [(sut/datom 1 :id 1)]))
      (is (= (dd/datom-tx (first (:tx-data rp))) 2)))
    (is (= (sut/datoms @conn :eav) [(sut/datom 1 :id 1)]))
    (is (= (:max-eid @conn) 1))
    (is (= (:max-tx @conn) 2))

    (let [rp (sut/tx-data->simulated-report @conn [{:id 2}])]
      (is (= (:tx-data rp) [(sut/datom 2 :id 2)]))
      (is (= (dd/datom-tx (first (:tx-data rp))) 3))
      (is (= (:max-eid (:db-after rp)) 2))
      (is (= (:max-tx (:db-after rp)) 3)))

    (is (= (sut/datoms @conn :eav) [(sut/datom 1 :id 1)]))
    (is (= (:max-eid @conn) 1))
    (is (= (:max-tx @conn) 2))

    (sut/transact! conn [{:id 1 :a 1}])
    (is (= 2 (count (sut/datoms @conn :eav))))
    (is (= (:max-eid @conn) 1))
    (is (= (:max-tx @conn) 3))

    (let [rp (sut/tx-data->simulated-report @conn [{:id 1 :a 2}])]
      (is (= (:tx-data rp) [(sut/datom 1 :a 1 -4) (sut/datom 1 :a 2 4)]))
      (is (= (:max-eid (:db-after rp)) 1))
      (is (= (:max-tx (:db-after rp)) 4)))

    (is (= 2 (count (sut/datoms @conn :eav))))
    (is (= (:max-eid @conn) 1))
    (is (= (:max-tx @conn) 3))

    (sut/close conn)
    (u/delete-files dir)))
