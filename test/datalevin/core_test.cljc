(ns datalevin.core-test
  (:require [datalevin.core :as sut]
            [datalevin.server :as s]
            [datalevin.client :as cl]
            [datalevin.interpret :as i]
            [datalevin.constants :as c]
            [datalevin.search-utils :as su]
            [datalevin.util :as u]
            [clojure.string :as str]
            #?(:cljs [cljs.test :as t :refer-macros [is are deftest testing]]
               :clj  [clojure.test :as t :refer [is are deftest testing]])
            [datalevin.datom :as d])
  (:import [java.util UUID Arrays]
           [java.nio.charset StandardCharsets]
           [java.lang Thread]
           [datalevin.storage Store]))

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
    (sut/close conn)
    (let [conn1 (sut/create-conn dir)]
      (is (= 83 (count (sut/datoms @conn1 :eavt))))
      (is (:regions/country (sut/schema conn1)))
      (is (:db/created-at (sut/schema conn1)))
      (sut/close conn1))
    (sut/clear conn)
    (let [conn1 (sut/create-conn dir)]
      (is (= 0 (count (sut/datoms @conn1 :eavt))))
      (sut/close conn1))
    (u/delete-files dir)))

(deftest remote-basic-ops-test
  (let [server (s/create {:port c/default-port
                          :root (u/tmp-dir
                                  (str "remote-basic-test-"
                                       (UUID/randomUUID)))})
        _      (s/start server)
        schema
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
    (is (= (count (sut/datoms @conn :eavt)) 83))
    (is (= (sut/q '[:find ?st .
                    :where
                    [?e :sales/company "Unilever"]
                    [?e :sales/year 2018]
                    [?e :sales/total ?st]]
                  @conn)
           23))
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
    (is (thrown? Exception (sut/update-schema conn {} #{:sales/year})))
    (sut/close conn)
    (let [conn1 (sut/create-conn dir)]
      (is (= 83 (count (sut/datoms @conn1 :eavt))))
      (is (:regions/country (sut/schema conn1)))
      (is (:db/created-at (sut/schema conn1)))
      (sut/close conn1))
    (sut/clear conn)
    (let [conn1 (sut/create-conn dir)]
      (is (= 0 (count (sut/datoms @conn1 :eavt))))
      (sut/close conn1))
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
        conn (sut/create-conn dir schema {:auto-entity-time? true})]
    (sut/transact! conn [{:user/email "eva@example.com"
                          :user/notes [{:note/text "do this do that"}
                                       {:note/text "enough is enough!"}]}])
    (is (= (-> (sut/entity @conn [:user/email "eva@example.com"])
               sut/touch
               :user/email)
           "eva@example.com"))
    (sut/close conn)
    (s/stop server)))

(deftest remote-local-identity-test
  (let [server     (s/create {:port c/default-port
                              :root (u/tmp-dir
                                      (str "remote-schema-test-"
                                           (UUID/randomUUID)))})
        _          (s/start server)
        schema
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
        conn       (sut/create-conn r-uri-str schema)
        local-conn (sut/create-conn l-dir schema)]
    (sut/transact! conn txs)
    (sut/transact! local-conn txs)
    (is (= (sut/schema conn) (sut/schema local-conn)))
    (is (= (sut/datoms @conn :eav) (sut/datoms @local-conn :eav)))
    (is (= (sut/q '[:find ?e
                    :in $ ?m
                    :where
                    [?e :retail/material ?m]]
                  @conn "cotton")
           (sut/q '[:find ?e
                    :in $ ?m
                    :where
                    [?e :retail/material ?m]]
                  @local-conn "cotton")))
    (is (= (sut/pull @conn '[*] 22) (sut/pull @local-conn '[*] 22)))
    (is (= (sut/q '[:find [?e ...]
                    :in $ ?ns-in
                    :where
                    [(namespace ?v) ?ns]
                    [(= ?ns ?ns-in)]
                    [?e :juji.data/of-attribute ?v]]
                  @local-conn "retail")
           (sut/q '[:find [?e ...]
                    :in $ ?ns-in
                    :where
                    [(namespace ?v) ?ns]
                    [(= ?ns ?ns-in)]
                    [?e :juji.data/of-attribute ?v]]
                  @conn "retail")))
    (is (= (sut/q '[:find [?e ...]
                    :in $
                    :where
                    [?e :db/ident]] @conn)
           (sut/q '[:find [?e ...]
                    :in $
                    :where
                    [?e :db/ident]] @local-conn)))
    (sut/close conn)
    (sut/close local-conn)
    (s/stop server)))

(deftest restart-server-test
  (let [root    (u/tmp-dir (str "remote-schema-test-" (UUID/randomUUID)))
        server1 (s/create {:port c/default-port
                           :root root})
        _       (s/start server1)
        client  (cl/new-client "dtlv://datalevin:datalevin@localhost"
                               {:time-out 5000})]
    (is (= (cl/list-databases client) []))
    (s/stop server1)
    (is (thrown? Exception (cl/list-databases client)))
    (let [server2 (s/create {:port c/default-port
                             :root root})
          _       (s/start server2)]
      (is (= (cl/list-databases client) []))
      (s/stop server2))))

(deftest entity-fn-test
  (let [server (s/create {:port c/default-port
                          :root (u/tmp-dir
                                  (str "entity-fn-test-"
                                       (UUID/randomUUID)))})
        _      (s/start server)

        f1 (i/inter-fn [db eid] (sut/entity db eid))
        f2 (i/inter-fn [db eid] (sut/touch (sut/entity db eid)))

        end 3
        vs  (range 0 end)
        txs (mapv sut/datom (range c/e0 (+ c/e0 end)) (repeat :value) vs)

        q '[:find ?ent :in $ ent :where [?e _ _] [(ent $ ?e) ?ent]]

        uri    "dtlv://datalevin:datalevin@localhost/entity-fn"
        r-conn (sut/get-conn uri)

        dir    (u/tmp-dir (str "entity-fn-test-" (UUID/randomUUID)))
        l-conn (sut/get-conn dir)]
    (sut/transact! r-conn txs)
    (sut/transact! l-conn txs)

    (is (i/inter-fn? f1))
    (is (i/inter-fn? f2))

    (is (= (sut/q q @r-conn f1)
           (sut/q q @l-conn sut/entity)))
    (is (= (sut/q q @r-conn f2)
           (sut/q q @l-conn (fn [db eid] (sut/touch (sut/entity db eid))))))
    (sut/close r-conn)
    (sut/close l-conn)
    (s/stop server)))

(deftest datalog-larger-tx-test
  (let [server (s/create {:port c/default-port
                          :root (u/tmp-dir
                                  (str "large-tx-test-"
                                       (UUID/randomUUID)))})
        _      (s/start server)
        dir    "dtlv://datalevin:datalevin@localhost/large-tx-test"
        end    100000
        conn   (sut/create-conn dir nil {:auto-entity-time? true})
        vs     (range 0 end)
        txs    (map (fn [a v] {a v}) (repeat :id) vs)]
    (sut/transact! conn txs)
    (is (= (sut/q '[:find (count ?e)
                    :where [?e :id]]
                  @conn)
           [[end]]))
    (let [[c u] (sut/q '[:find [?c ?u]
                         :in $ ?i
                         :where
                         [?e :id ?i]
                         [?e :db/created-at ?c]
                         [?e :db/updated-at ?u]]
                       @conn 1)]
      (is c)
      (is u)
      (is (= c u)))
    (sut/close conn)
    (s/stop server)))

(deftest remote-search-kv-test
  (let [server (s/create {:port c/default-port
                          :root (u/tmp-dir
                                  (str "remote-search-kv-test-"
                                       (UUID/randomUUID)))})
        _      (s/start server)
        dir    "dtlv://datalevin:datalevin@localhost/remote-search-kv-test"
        lmdb   (sut/open-kv dir)
        engine (sut/new-search-engine lmdb)]
    (sut/open-dbi lmdb "raw")
    (sut/transact-kv
      lmdb
      [[:put "raw" 1 "The quick red fox jumped over the lazy red dogs."]
       [:put "raw" 2 "Mary had a little lamb whose fleece was red as fire."]
       [:put "raw" 3 "Moby Dick is a story of a whale and a man obsessed."]])
    (doseq [i [1 2 3]]
      (sut/add-doc engine i (sut/get-value lmdb "raw" i)))

    (is (not (sut/doc-indexed? engine 0)))
    (is (sut/doc-indexed? engine 1))

    (is (= 3 (sut/doc-count engine)))
    (is (= [1 2 3] (sut/doc-refs engine)))

    (is (= (sut/search engine "lazy") [1]))
    (is (= (sut/search engine "red" ) [1 2]))
    (is (= (sut/search engine "red" {:display :offsets})
           [[1 [["red" [10 39]]]] [2 [["red" [40]]]]]))
    (testing "update"
      (sut/add-doc engine 1 "The quick fox jumped over the lazy dogs.")
      (is (= (sut/search engine "red" ) [2])))
    (sut/close-kv lmdb)
    (s/stop server)))

(deftest remote-blank-analyzer-test
  (let [server         (s/create {:port c/default-port
                                  :root (u/tmp-dir
                                          (str "remote-blank-analyzer-test-"
                                               (UUID/randomUUID)))})
        _              (s/start server)
        dir            "dtlv://datalevin:datalevin@localhost/blank-analyzer-test"
        lmdb           (sut/open-kv dir)
        blank-analyzer (i/inter-fn
                         [^String text]
                         (map-indexed (fn [i ^String t]
                                        [t i (.indexOf text t)])
                                      (str/split text #"\s")))
        engine         (sut/new-search-engine lmdb {:analyzer blank-analyzer})]
    (sut/open-dbi lmdb "raw")
    (sut/transact-kv
      lmdb
      [[:put "raw" 1 "The quick red fox jumped over the lazy red dogs."]
       [:put "raw" 2 "Mary had a little lamb whose fleece was red as fire."]
       [:put "raw" 3 "Moby Dick is a story of some dogs and a whale."]])
    (doseq [i [1 2 3]]
      (sut/add-doc engine i (sut/get-value lmdb "raw" i)))
    (is (= [[1 [["dogs." [43]]]]]
           (sut/search engine "dogs." {:display :offsets})))
    (is (= [3] (sut/search engine "dogs")))
    (sut/close-kv lmdb)
    (s/stop server)))

(deftest remote-custom-analyzer-test
  (let [server (s/create {:port c/default-port
                          :root (u/tmp-dir
                                  (str "remote-custom-analyzer-test-"
                                       (UUID/randomUUID)))})
        _      (s/start server)
        dir    "dtlv://datalevin:datalevin@localhost/custom-analyzer-test"
        lmdb   (sut/open-kv dir)
        engine (sut/new-search-engine
                 lmdb
                 {:analyzer
                  (su/create-analyzer
                    {:tokenizer
                     (su/create-regexp-tokenizer
                       #"[\s:/\.;,!=?\"'()\[\]{}|<>&@#^*\\~`]+")
                     :token-filters [su/lower-case-token-filter
                                     su/unaccent-token-filter
                                     su/en-stop-words-token-filter]})})]
    (sut/open-dbi lmdb "raw")
    (sut/transact-kv
      lmdb
      [[:put "raw" 1 "The quick red fox jumped over the lazy red dogs."]
       [:put "raw" 2 "Mary had a little lamb whose fleece was red as fire."]
       [:put "raw" 3 "Moby Dick is a story of some dogs' and a whale."]])
    (doseq [i [1 2 3]]
      (sut/add-doc engine i (sut/get-value lmdb "raw" i)))
    (is (= [[3 [["dogs" [29]]]] [1 [["dogs" [43]]]]]
           (sut/search engine "dogs" {:display :offsets})))
    (sut/close-kv lmdb)
    (s/stop server)))

(deftest fulltext-fns-test
  (let [analyzer (i/inter-fn
                   [^String text]
                   (map-indexed (fn [i ^String t]
                                  [t i (.indexOf text t)])
                                (str/split text #"\s")))
        conn     (sut/create-conn (u/tmp-dir (str "fulltext-fns-" (UUID/randomUUID)))
                                  {:a/string {:db/valueType :db.type/string
                                              :db/fulltext  true}}
                                  {:auto-entity-time? true
                                   :search-opts       {:analyzer analyzer}})
        s        "The quick brown fox jumps over the lazy dog"]
    (sut/transact! conn [{:a/string s}])
    (is (= (sut/q '[:find ?v .
                    :in $ ?q
                    :where [(fulltext $ ?q) [[?e ?a ?v]]]]
                  (sut/db conn) "brown fox") s))
    (sut/close conn)))

(deftest remote-fulltext-fns-test
  (let [server   (s/create {:port c/default-port
                            :root (u/tmp-dir
                                    (str "remote-fulltext-fns-test-"
                                         (UUID/randomUUID)))})
        _        (s/start server)
        dir      "dtlv://datalevin:datalevin@localhost/remote-fulltext-fns-test"
        analyzer (i/inter-fn
                   [^String text]
                   (map-indexed (fn [i ^String t]
                                  [t i (.indexOf text t)])
                                (str/split text #"\s")))
        db       (-> (sut/empty-db
                       dir
                       {:text {:db/valueType :db.type/string
                               :db/fulltext  true}}
                       {:auto-entity-time? true
                        :search-opts       {:analyzer analyzer}})
                     (sut/db-with
                       [{:db/id 1,
                         :text  "The quick red fox jumped over the lazy red dogs."}
                        {:db/id 2,
                         :text  "Mary had a little lamb whose fleece was red as fire."}
                        {:db/id 3,
                         :text  "Moby Dick is a story of a whale and a man obsessed."}]))]
    (is (= (sut/q '[:find ?e ?a ?v
                    :in $ ?q
                    :where [(fulltext $ ?q) [[?e ?a ?v]]]]
                  db "red fox")
           #{[1 :text "The quick red fox jumped over the lazy red dogs."]
             [2 :text "Mary had a little lamb whose fleece was red as fire."]}))
    (sut/close-db db)
    (s/stop server)))

(deftest remote-db-ident-fn
  (let [server (s/create {:port c/default-port
                          :root (u/tmp-dir (str "remote-fn-test-"
                                                (UUID/randomUUID)))})
        _      (s/start server)
        dir    "dtlv://datalevin:datalevin@localhost/remote-fn-test"

        conn    (sut/create-conn dir
                                 {:name {:db/unique :db.unique/identity}})
        inc-age (i/inter-fn
                  [db name]
                  (if-some [ent (sut/entity db [:name name])]
                    [{:db/id (:db/id ent)
                      :age   (inc ^long (:age ent))}
                     [:db/add (:db/id ent) :had-birthday true]]
                    (throw (ex-info (str "No entity with name: " name) {}))))]
    (sut/transact! conn [{:db/id    1
                          :name     "Petr"
                          :age      31
                          :db/ident :Petr}
                         {:db/ident :inc-age
                          :db/fn    inc-age}])
    (is (thrown-with-msg? Exception
                          #"Can’t find entity for transaction fn :unknown-fn"
                          (sut/transact! conn [[:unknown-fn]])))
    (is (thrown-with-msg? Exception
                          #"Entity :Petr expected to have :db/fn attribute"
                          (sut/transact! conn [[:Petr]])))
    (is (thrown-with-msg? Exception
                          #"No entity with name: Bob"
                          (sut/transact! conn [[:inc-age "Bob"]])))
    (sut/transact! conn [[:inc-age "Petr"]])
    (let [e (sut/entity @conn 1)]
      (is (= (:age e) 32))
      (is (:had-birthday e)))
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

(deftest float-transact-test
  (let [schema {:name   {:db/valueType :db.type/string}
                :height {:db/valueType :db.type/float}}
        dir    (u/tmp-dir (str "datalevin-float-test-" (UUID/randomUUID)))
        conn   (sut/get-conn dir schema)]
    (sut/transact! conn [{:name "John" :height 1.73}
                         {:name "Peter" :height 1.92}])
    (is (= (sut/pull (sut/db conn) '[*] 1)
           {:name "John" :height (float 1.73) :db/id 1}))))

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
             {:psize          4096, :depth   1, :branch-pages 0, :leaf-pages 1,
              :overflow-pages 0,    :entries 1}))
      (doseq [i (sut/list-dbis db)]
        (is (= (sut/stat db i)
               {:psize          4096, :depth   1, :branch-pages 0, :leaf-pages 1,
                :overflow-pages 0,    :entries 1})))
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

(deftest auto-entity-time-test
  (let [dir  (u/tmp-dir (str "auto-entity-time-test-" (UUID/randomUUID)))
        conn (sut/create-conn dir
                              {:id {:db/unique    :db.unique/identity
                                    :db/valueType :db.type/long}}
                              {:auto-entity-time? true})]
    (sut/transact! conn [{:id 1}])
    (is (= (count (sut/datoms @conn :eav)) 3))
    (is (:db/created-at (sut/touch (sut/entity @conn [:id 1]))))
    (is (:db/updated-at (sut/touch (sut/entity @conn [:id 1]))))

    (sut/transact! conn [[:db/retractEntity [:id 1]]])
    (is (= (count (sut/datoms @conn :eav)) 0))
    (is (nil? (sut/entity @conn [:id 1])))

    (sut/close conn)
    (u/delete-files dir)))

(deftest update-schema-test
  (let [dir  (u/tmp-dir (str "update-schema-test-" (UUID/randomUUID)))
        conn (sut/create-conn dir
                              {:id {:db/unique    :db.unique/identity
                                    :db/valueType :db.type/long}})]
    (sut/transact! conn [{:id 1}])
    (is (= (sut/datoms @conn :eav) [(d/datom 1 :id 1)]))
    ;; TODO somehow this cannot pass in graal
    ;; (is (thrown-with-msg? Exception #"unique constraint"
    ;;                       (sut/transact! conn [[:db/add 2 :id 1]])))

    (sut/update-schema conn {:id {:db/valueType :db.type/long}})
    (sut/transact! conn [[:db/add 2 :id 1]])
    (is (= (count (sut/datoms @conn :eav)) 2))

    (is (thrown-with-msg? Exception #"uniqueness change is inconsistent"
                          (sut/update-schema
                            conn {:id {:db/unique    :db.unique/identity
                                       :db/valueType :db.type/long}})))

    (sut/close conn)
    (u/delete-files dir)))
