(ns datalevin.search-test
  (:require [datalevin.search :as sut]
            [clojure.test :refer [is deftest]]))

(deftest english-analyzer-test
  (let [s1 "This is a Datalevin-Analyzers test"
        s2 "This is a Datalevin-Analyzers test. "]
    (is (= (sut/en-analyzer s1)
           (sut/en-analyzer s2)
           [["datalevin-analyzers" 3 10] ["test" 4 30]]))
    (is (= (subs s1 10 (+ 10 (.length "datalevin-analyzers")))
           "Datalevin-Analyzers" ))))

(deftest index-test
  (let [docs ["The quick red fox jumped over the lazy brown dogs.",
              "Mary had a little lamb whose fleece was white as snow.",
              "Moby Dick is a story of a whale and a man obsessed.",
              "The robber wore a black fleece jacket and a baseball cap.",
              "The English Springer Spaniel is the best of all dogs."]]))
