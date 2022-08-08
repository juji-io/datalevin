(ns datalevin.test.query-deadline
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer        [is are deftest testing]])
   [datalevin.core :as d]
   [datalevin.query :as q]
   [datalevin.db :as db]
   [datalevin.test.core :as tdc]))

(deftest timeout
  (is (thrown-with-msg?
       Exception
       #"Query took too long to run."
       (d/q '[:find  ?e1
              :in    $ ?e1 %
              :where (long-query ?e1)
              :timeout 5000]
            []
            1
            '[[(long-query ?e1) [(inc ?e1) ?e1+1] (long-query ?e1+1)]])
       #{}))
  (is (thrown-with-msg?
       Exception
       #"Query took too long to run."
       (d/q '{:find  [?e1]
              :in    [$ ?e1 %]
              :where [(long-query ?e1)]
              :timeout 5000}
            []
            1
            '[[(long-query ?e1) [(inc ?e1) ?e1+1] (long-query ?e1+1)]])
       #{})))
