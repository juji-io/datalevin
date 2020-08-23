(ns datalevin.core-test
  (:require [datalevin.core :as sut]
            [clojure.test :refer [is deftest]]))

(deftest basic-ops-test
  (let [schema
        {:db/ident                {:db/unique :db.unique/identity, :db/aid 0},
         :sales/country           {:db/valueType :db.type/string, :db/aid 1},
         :juji.data/display?      {:db/valueType :db.type/boolean, :db/aid 2},
         :juji.data/origin-column {:db/valueType :db.type/long, :db/aid 3},
         :sales/company           {:db/valueType :db.type/string, :db/aid 4},
         :juji.data/attribute     {:db/valueType :db.type/keyword,
                                   :db/unique    :db.unique/identity, :db/aid 5},
         :sales/top-product-use   {:db/valueType :db.type/string, :db/aid 6},
         :juji.data/of-attribute  {:db/valueType :db.type/keyword},
         :juji.data/references    {:db/valueType :db.type/keyword, :db/aid 8},
         :juji.data/value         {:db/valueType :db.type/string, :db/aid 9},
         :sales/year              {:db/valueType :db.type/long, :db/aid 10},
         :sales/total             {:db/valueType :db.type/long, :db/aid 11},
         :juji.data/synonyms      {:db/valueType   :db.type/string,
                                   :db/cardinality :db.cardinality/many, :db/aid 12}}
        conn (sut/create-conn schema)
        txs
        [{:juji.data/synonyms      ["company" "customer"],
          :juji.data/display?      true,
          :juji.data/attribute     :sales/company,
          :juji.data/origin-column 0, :db/id -1}
         {:juji.data/references    :regions/country,
          :juji.data/attribute     :sales/country,
          :juji.data/origin-column 1,
          :db/id                   -2}
         {:sales/year              2019,
          :juji.data/synonyms      ["total" "spending" "payment"],
          :juji.data/display?      true,
          :juji.data/attribute     :sales/total,
          :juji.data/origin-column 2,
          :db/id                   -3}
         {:sales/year              2018,
          :juji.data/attribute     :sales/total,
          :juji.data/origin-column 3,
          :db/id                   -4}
         {:juji.data/attribute     :sales/top-product-use,
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
                                    "united states" "usa"]}]]))))
