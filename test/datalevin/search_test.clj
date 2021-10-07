(ns datalevin.search-test
  (:require [datalevin.search :as sut]
            [clojure.test :refer [is deftest]]))

(deftest analyzer-test
  (let [s1 "This is a Datalevin-Analyzers test"
        s2 "This is a Datalevin-Analyzers test. "]
    (is (= (sut/analyzer s1)
           (sut/analyzer s2)
           [["datalevin-analyzers" 3 10] ["test" 4 30]]))))

(deftest index-test
  (let [docs ["The quick red fox jumped over the lazy brown dogs.",
              "Mary had a little lamb whose fleece was white as snow.",
              "Moby Dick is a story of a whale and a man obsessed.",
              "The robber wore a black fleece jacket and a baseball cap.",
              "The English Springer Spaniel is the best of all dogs."]]))
