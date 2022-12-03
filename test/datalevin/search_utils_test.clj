(ns datalevin.search-utils-test
  (:require [datalevin.search-utils :as sut]
            [datalevin.search :as sc]
            [clojure.test :refer [is deftest testing]]))

(deftest custom-analyzer-test
  (let [s1               "This is a Datalevin-Analyzers test"
        s2               "This is a Datalevin-Analyzers test. "
        cust-analyzer-en (sut/create-analyzer
                           {:tokenizer     (sut/create-regexp-tokenizer
                                             #"[\s:/\.;,!=?\"'()\[\]{}|<>&@#^*\\~`]+")
                            :token-filters [sut/lower-case-token-filter
                                            sut/en-stop-words-token-filter]})]
    (is (fn? cust-analyzer-en))
    (is (= (sc/en-analyzer s1)
           (cust-analyzer-en s1)
           (cust-analyzer-en s2)
           [["datalevin-analyzers" 3 10] ["test" 4 30]]))))

(deftest autocomplete-analyzer-test
  (let [s1               "clock"
        s2               "cloud"
        cust-analyzer-en (sut/create-analyzer
                           {:tokenizer     (sut/create-regexp-tokenizer
                                             #"[\s:/\.;,!=?\"'()\[\]{}|<>&@#^*\\~`]+")
                            :token-filters [sut/lower-case-token-filter
                                            sut/prefix-token-filter]})]
    (is (= (into [] (cust-analyzer-en s1))
           [["c" 0 0] ["cl" 0 0] ["clo" 0 0] ["cloc" 0 0] ["clock" 0 0]]))
    (is (= (into [] (cust-analyzer-en s2))
           [["c" 0 0] ["cl" 0 0] ["clo" 0 0] ["clou" 0 0] ["cloud" 0 0]]))))
