(ns datalevin.remote-dl-test
  (:require [datalevin.datom :as d]
            [datalevin.core :as dc]
            [datalevin.util :as u]
            [datalevin.test.core :refer [server-fixture]]
            [clojure.test :refer [is testing deftest use-fixtures]])
  (:import [java.util UUID]
           [datalevin.datom Datom]))

(use-fixtures :each server-fixture)

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
    (is (= (count (dc/datoms @conn :eav)) 83))
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
           ["US" "U.K." "Germany"]))
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
      (is (= 0 (count (dc/datoms @conn1 :eav))))
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
    (dc/close local-conn)
    (u/delete-files l-dir)))

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

(deftest update-opts-test
  (let [dir  "dtlv://datalevin:datalevin@localhost/update-opts"
        conn (dc/create-conn dir)]
    (is (= 100 (dc/datalog-index-cache-limit @conn)))
    (dc/datalog-index-cache-limit @conn 0)
    (is (= 0 (dc/datalog-index-cache-limit @conn)))
    (dc/close conn)))

(deftest re-index-test
  (let [dir  "dtlv://datalevin:datalevin@localhost/re-index-dl"
        conn (dc/get-conn
               dir {:aka  {:db/cardinality :db.cardinality/many}
                    :name {:db/valueType :db.type/string
                           :db/unique    :db.unique/identity}})]
    (let [rp (dc/transact!
               conn
               [{:name "Frege", :db/id -1, :nation "France",
                 :aka  ["foo" "fred"]}
                {:name "Peirce", :db/id -2, :nation "france"}
                {:name "De Morgan", :db/id -3, :nation "English"}])]
      (is (= 8 (count (:tx-data rp))))
      (is (zero? (count (dc/fulltext-datoms @conn "peirce")))))
    (let [conn1 (dc/re-index
                  conn {:name {:db/valueType :db.type/string
                               :db/unique    :db.unique/identity
                               :db/fulltext  true}} {})]
      (is (= #{["France"]}
             (dc/q '[:find ?nation
                     :in $ ?alias
                     :where
                     [?e :aka ?alias]
                     [?e :nation ?nation]]
                   (dc/db conn1)
                   "fred")))
      (dc/transact! conn1 [[:db/retract 1 :name "Frege"]])
      (is (= [[{:db/id 1, :aka ["foo" "fred"], :nation "France"}]]
             (dc/q '[:find (pull ?e [*])
                     :in $ ?alias
                     :where
                     [?e :aka ?alias]]
                   (dc/db conn1)
                   "fred")))
      (is (= 1 (count (dc/fulltext-datoms @conn1 "peirce"))))
      (dc/close conn1))))
