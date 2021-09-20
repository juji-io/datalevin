(ns datalevin.core-test
  (:require [datalevin.core :as sut]
            [datalevin.server :as s]
            [datalevin.constants :as c]
            [datalevin.util :as u]
            [clojure.test :refer [is deftest]])
  (:import [java.util UUID Arrays]
           [java.nio.charset StandardCharsets]
           [java.lang Thread]))

(deftest basic-ops-test
  (let [schema
        {:db/ident                {:db/unique    :db.unique/identity,
                                   :db/valueType :db.type/keyword,
                                   :db/aid       0},
         :sales/country           {:db/valueType :db.type/string, :db/aid 1},
         :juji.data/display?      {:db/valueType :db.type/boolean, :db/aid 2},
         :juji.data/origin-column {:db/valueType :db.type/long, :db/aid 3},
         :sales/company           {:db/valueType :db.type/string, :db/aid 4},
         :sales/top-product-use   {:db/valueType :db.type/string, :db/aid 5},
         :juji.data/of-attribute  {:db/valueType :db.type/keyword, :db/aid 6},
         :juji.data/references    {:db/valueType :db.type/keyword, :db/aid 7},
         :juji.data/value         {:db/valueType :db.type/string, :db/aid 8},
         :sales/year              {:db/valueType :db.type/long, :db/aid 9},
         :sales/total             {:db/valueType :db.type/long, :db/aid 10},
         :juji.data/synonyms      {:db/valueType   :db.type/string,
                                   :db/cardinality :db.cardinality/many, :db/aid 11}}

        schema-update
        {:regions/region  {:db/valueType :db.type/string :db/aid 12}
         :regions/country {:db/valueType :db.type/string :db/aid 13}}

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
                         :in $ ?ns
                         :where
                         [?e :db/ident ?v]
                         [(namespace ?v) ?ns]]
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
    (sut/update-schema conn schema-update)
    (is (= (sut/schema conn) (merge schema schema-update)))
    (is (sut/conn? conn))
    (sut/close conn)
    (let [conn' (sut/create-conn dir)]
      (is (= 83 (count (sut/datoms @conn' :eavt))))
      (is (= (sut/schema conn') (merge schema schema-update)))
      (sut/close conn'))
    (sut/clear conn)
    (let [conn' (sut/create-conn dir)]
      (is (= 0 (count (sut/datoms @conn' :eavt))))
      (sut/close conn'))
    (u/delete-files dir)))

(deftest remote-basic-ops-test
  (let [server (s/create {:port c/default-port
                          :root (u/tmp-dir
                                  (str "remote-basic-test-"
                                       (UUID/randomUUID)))})
        _      (s/start server)
        schema
        {:db/ident                {:db/unique    :db.unique/identity,
                                   :db/valueType :db.type/keyword,
                                   :db/aid       0},
         :sales/country           {:db/valueType :db.type/string, :db/aid 1},
         :juji.data/display?      {:db/valueType :db.type/boolean, :db/aid 2},
         :juji.data/origin-column {:db/valueType :db.type/long, :db/aid 3},
         :sales/company           {:db/valueType :db.type/string, :db/aid 4},
         :sales/top-product-use   {:db/valueType :db.type/string, :db/aid 5},
         :juji.data/of-attribute  {:db/valueType :db.type/keyword, :db/aid 6},
         :juji.data/references    {:db/valueType :db.type/keyword, :db/aid 7},
         :juji.data/value         {:db/valueType :db.type/string, :db/aid 8},
         :sales/year              {:db/valueType :db.type/long, :db/aid 9},
         :sales/total             {:db/valueType :db.type/long, :db/aid 10},
         :juji.data/synonyms      {:db/valueType   :db.type/string,
                                   :db/cardinality :db.cardinality/many, :db/aid 11}}

        schema-update
        {:regions/region  {:db/valueType :db.type/string :db/aid 12}
         :regions/country {:db/valueType :db.type/string :db/aid 13}}

        dir  "dtlv://datalevin:datalevin@localhost/core"
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
                         :in $ ?ns
                         :where
                         [?e :db/ident ?v]
                         [(namespace ?v) ?ns]]
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
    (sut/update-schema conn schema-update)
    (is (= (sut/schema conn) (merge schema schema-update)))
    (is (sut/conn? conn))
    (sut/close conn)
    (let [conn' (sut/create-conn dir)]
      (is (= 83 (count (sut/datoms @conn' :eavt))))
      (is (= (sut/schema conn') (merge schema schema-update)))
      (sut/close conn'))
    (sut/clear conn)
    (let [conn' (sut/create-conn dir)]
      (is (= 0 (count (sut/datoms @conn' :eavt))))
      (sut/close conn'))
    (s/stop server)))

(deftest remote-conn-schema-update-test
  (let [server (s/create {:port c/default-port
                          :root (u/tmp-dir
                                  (str "remote-schema-test-"
                                       (UUID/randomUUID)))})
        _      (s/start server)
        schema {:user/email {:db/unique    :db.unique/identity
                             :db/valueType :db.type/string}
                :user/notes {:db/valueType   :db.type/ref
                             :db/cardinality :db.cardinality/many}
                :note/text  {:db/valueType :db.type/string}}

        dir  "dtlv://datalevin:datalevin@localhost/schema"
        _    (sut/create-conn dir)
        conn (sut/create-conn dir schema)]
    (sut/transact! conn [{:user/email "eva@example.com"
                          :user/notes [{:note/text "do this do that"}
                                       {:note/text "enough is enough!"}]}])
    (is (= (-> (sut/entity @conn [:user/email "eva@example.com"])
               sut/touch
               :user/email)
           "eva@example.com"))
    (sut/close conn)
    (s/stop server)))

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
  (let [dir        (u/tmp-dir (str "datalevin-bytes-test-" (UUID/randomUUID)))
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
