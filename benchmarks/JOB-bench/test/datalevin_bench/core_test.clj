(ns datalevin-bench.core-test
  (:require [datalevin-bench.core :as sut]
            [datalevin.core :as d]
            [clojure.test :as t :refer [are deftest]]))

(deftest correct-results-test
  (let [conn (d/get-conn "db")
        db   (d/db conn)]
    (are [query result] (= (d/q query db) result)
      sut/q-1a [["(A Warner Bros.-First National Picture) (presents)"
                 "A Clockwork Orange" 1934]]
      sut/q-1b [["(Set Decoration Rentals) (uncredited)" "Disaster Movie" 2008]]
      sut/q-1c [["(co-production)" "Intouchables" 2011]]
      sut/q-1d [["(Set Decoration Rentals) (uncredited)" "Disaster Movie" 2004]]
      sut/q-2a [["'Doc'"]]
      sut/q-2b [["'Doc'"]]
      sut/q-2c []
      sut/q-2d [["& Teller"]]
      sut/q-3a [["2 Days in New York"]]
      sut/q-3b [["300: Rise of an Empire"]]
      sut/q-3c [["& Teller 2"]]

      sut/q-6a [["Downey Jr., Robert" "Iron Man 3"]]
      sut/q-6b [["based-on-comic" "Downey Jr., Robert" "The Avengers 2"]]
      sut/q-6c [["Downey Jr., Robert" "The Avengers 2"]]

      sut/q-6e [["Downey Jr., Robert" "Iron Man 3"]]
      sut/q-6f [["based-on-comic" "\"Steff\", Stefanie Oxmann Mcgaha" "& Teller 2"]]
      sut/q-7a [["Antonioni, Michelangelo" "Dressed to Kill"]]
      sut/q-7b [["De Palma, Brian" "Dressed to Kill"]]
      )))
