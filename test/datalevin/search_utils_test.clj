(ns datalevin.search-utils-test
  (:require [datalevin.search-utils :as sut]
            [datalevin.search :as sc]
            [datalevin.core :as d]
            [datalevin.server :as sv]
            [datalevin.constants :as c]
            [datalevin.util :as u]
            [clojure.test :refer [is deftest testing]])
  (:import [java.util UUID ]))

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

(deftest remote-custom-analyzer-test
  (let [server (sv/create {:port c/default-port
                           :root (u/tmp-dir
                                   (str "remote-custom-analyzer-test-"
                                        (UUID/randomUUID)))})
        _      (sv/start server)
        dir    "dtlv://datalevin:datalevin@localhost/custom-analyzer-test"
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
    (d/close-kv lmdb)
    (sv/stop server)))
