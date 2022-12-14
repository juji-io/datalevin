(ns datalevin.remote-sutil-test
  (:require
   [datalevin.search-utils :as sut]
   [datalevin.core :as d]
   [datalevin.test.core :as tdc :refer [server-fixture]]
   [clojure.test :as t :refer [is deftest testing use-fixtures]]))

(use-fixtures :each server-fixture)

(deftest remote-custom-analyzer-test
  (let [dir    "dtlv://datalevin:datalevin@localhost/custom-analyzer-test"
        lmdb   (d/open-kv dir)
        engine (d/new-search-engine
                 lmdb
                 {:analyzer
                  (sut/create-analyzer
                    {:tokenizer
                     (sut/create-regexp-tokenizer
                       #"[\s:/\.;,!=?\"'()\[\]{}|<>&@#^*\\~`]+")
                     :token-filters [sut/lower-case-token-filter
                                     sut/unaccent-token-filter
                                     sut/en-stop-words-token-filter]})})]
    (d/open-dbi lmdb "raw")
    (d/transact-kv
      lmdb
      [[:put "raw" 1 "The quick red fox jumped over the lazy red dogs."]
       [:put "raw" 2 "Mary had a little lamb whose fleece was red as fire."]
       [:put "raw" 3 "Moby Dick is a story of some dogs' and a whale."]])
    (doseq [i [1 2 3]]
      (d/add-doc engine i (d/get-value lmdb "raw" i)))
    (is (= [[3 [["dogs" [29]]]] [1 [["dogs" [43]]]]]
           (d/search engine "dogs" {:display :offsets})))
    (d/close-kv lmdb)))
