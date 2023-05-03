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
                                     sut/en-stop-words-token-filter]})
                  :index-position? true})]
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

(deftest stemming-test
  (let [dir      "dtlv://datalevin:datalevin@localhost/stem-test"
        analyzer (sut/create-analyzer
                   {:token-filters [(sut/create-stemming-token-filter
                                      "english")]})
        db       (-> (d/empty-db
                       dir
                       {:text {:db/valueType :db.type/string
                               :db/fulltext  true}}
                       {:search-opts
                        {:query-analyzer analyzer
                         :analyzer       analyzer}})
                     (d/db-with
                       [{:db/id 1,
                         :text  "The quick red fox jumped over the lazy red dogs."}
                        {:db/id 2,
                         :text  "Mary had a little lamb whose fleece was red as fire."}
                        {:db/id 3,
                         :text  "Moby Dick is a story of a whale and a man obsessed."}]))]
    (is (= 1 (count (d/fulltext-datoms db "jump dog"))))
    (is (= 3 (d/q '[:find ?e .
                    :in $ ?q
                    :where
                    [(fulltext $ ?q) [[?e _ _]]]]
                  db "obsess")))
    (d/close-db db)))

(deftest stop-words-test
  (let [dir    "dtlv://datalevin:datalevin@localhost/custom-stop-test"
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
                                     (sut/create-stop-words-token-filter
                                       #{"over" "red" "the" "lazy"})]})
                  :index-position? true})]
    (d/open-dbi lmdb "raw")
    (d/transact-kv
      lmdb
      [[:put "raw" 1 "The quick red fox jumped over the lazy red dogs."]
       [:put "raw" 2 "Mary had a little lamb whose fleece was red as fire."]
       [:put "raw" 3 "Moby Dick is a story of some dogs' and a whale."]])
    (doseq [i [1 2 3]]
      (d/add-doc engine i (d/get-value lmdb "raw" i)))
    (is (= [[1 [["dogs" [43]]]] [3 [["dogs" [29]]]]]
           (d/search engine "dogs" {:display :offsets})))
    (d/close-kv lmdb)))
