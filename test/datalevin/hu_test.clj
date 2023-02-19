(ns datalevin.hu-test
  (:require
   [datalevin.hu :as sut]
   [clojure.test :refer [deftest testing is are]])
  (:import
   [datalevin.utl OptimalCodeLength]))

(deftest encoding-test
  (testing "level tree construction"
    (are [freqs levels] (= levels
                           (seq (OptimalCodeLength/generate
                                  ^longs (long-array freqs))))
      [8 6 2 3 4 7 11 9 8 1 3]
      [3 3 5 5 4 3 3 3 3 4 4]

      [5 2 7 2 1 1 1 2 4 5]
      [3 3 2 4 5 5 4 4 3 3])))
