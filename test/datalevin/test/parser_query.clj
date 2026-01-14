(ns datalevin.test.parser-query
  (:require
   [clojure.test :as t :refer [is are deftest testing]]
   [datalevin.parser :as dp]))

(deftest validation
  (are [q msg] (thrown-msg? msg (dp/parse-query q))
    '[:find ?e :where [?x]]
    "Query for unknown vars: [?e]"

    '[:find ?e :with ?f :where [?e]]
    "Query for unknown vars: [?f]"

    '[:find ?e ?x ?t :in ?x :where [?e]]
    "Query for unknown vars: [?t]"

    '[:find ?x ?e :with ?y ?e :where [?x ?e ?y]]
    ":find and :with should not use same variables: [?e]"

    '[:find ?e :in $ $ ?x :where [?e]]
    "Vars used in :in should be distinct"

    '[:find ?e :in ?x $ ?x :where [?e]]
    "Vars used in :in should be distinct"

    '[:find ?e :in $ % ?x % :where [?e]]
    "Vars used in :in should be distinct"

    '[:find ?n :with ?e ?f ?e :where [?e ?f ?n]]
    "Vars used in :with should be distinct"

    '[:find ?x :where [$1 ?x]]
    "Where uses unknown source vars: [$1]"

    '[:find ?x :in $1 :where [$2 ?x]]
    "Where uses unknown source vars: [$2]"

    '[:find ?e :where (rule ?e)]
    "Missing rules var '%' in :in"

    '[:find ?e :where [?e :book] :order-by "some"]
    "Unsupported order-by format"

    '[:find ?e :where [?e :book] :order-by [:asc ?e]]
    "Incorrect order-by format"

    '[:find ?e :where [?e :book] :order-by [?e :book]]
    "Incorrect order-by format"

    '[:find ?e :where [?e :book] :order-by ["some"]]
    "Incorrect order-by format"

    '[:find ?e ?b :where [?e :book ?b] :order-by [?e :asc :desc ?b]]
    "Incorrect order-by format"

    '[:find ?e :where [?e :book ?b] :order-by [?e :desc ?b]]
    "There are :order-by variable that is not in :find spec"

    '[:find ?e ?b :where [?e :book ?b] :order-by [?e :desc ?b ?e]]
    "Repeated :order-by variables"

    '[:find ?e ?b :where [?e :book ?b] :order-by [0 :desc 0]]
    "Repeated :order-by variables"

    '[:find ?e :where [?e :book ?b] :order-by [2]]
    ":order-by column index out of bounds"

    '[:find ?e ?b :where [?e :book ?b] :order-by [5 :desc]]
    ":order-by column index out of bounds"

    '[:find ?e . :where [?e :book ?b] :limit "some"]
    "Unsupported limit format"

    ))
