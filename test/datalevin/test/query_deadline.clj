(ns datalevin.test.query-deadline
  (:require
   [clojure.test :as t :refer        [is are deftest testing]]
   [datalevin.core :as d]))

(deftest timeout
  (is (thrown-with-msg?
        Exception
        #"Query and/or pull expression took too long to run."
        (d/q '[:find  ?e1
               :in    $ ?e1 %
               :where (long-query ?e1)
               :timeout 1000]
             []
             1
             '[[(long-query ?e1) [(inc ?e1) ?e1+1] (long-query ?e1+1)]])))
  (is (thrown-with-msg?
        Exception
        #"Query and/or pull expression took too long to run."
        (d/q '{:find    [?e1]
               :in      [$ ?e1 %]
               :where   [(long-query ?e1)]
               :timeout 1000}
             []
             1
             '[[(long-query ?e1) [(inc ?e1) ?e1+1] (long-query ?e1+1)]]))))

(defn a-fun
  [t]
  (Thread/sleep t)
  1)

(deftest deadline-no-cache
  (let [q '[:find  ?r .
            :in $ ?t
            :where [(datalevin.test.query-deadline/a-fun ?t) ?r]
            :timeout 1000]]
    (is (thrown-with-msg?
          Exception
          #"Query and/or pull expression took too long to run."
          (d/q q [] 2000)))
    ;; if deadline is cached, this will throw too
    (is (= 1 (d/q q [] 10)))))
