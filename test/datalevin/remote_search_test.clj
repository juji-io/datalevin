(ns datalevin.remote-search-test
  (:require [datalevin.interpret :as i]
            [datalevin.core :as d]
            [clojure.string :as s]
            [datalevin.test.core :refer [server-fixture]]
            [clojure.test :refer [is testing deftest use-fixtures]]))

(use-fixtures :each server-fixture)

(deftest remote-search-kv-test
  (let [dir    "dtlv://datalevin:datalevin@localhost/remote-search-kv-test"
        lmdb   (d/open-kv dir)
        engine (d/new-search-engine lmdb)]
    (d/open-dbi lmdb "raw")
    (d/transact-kv
      lmdb
      [[:put "raw" 1 "The quick red fox jumped over the lazy red dogs."]
       [:put "raw" 2 "Mary had a little lamb whose fleece was red as fire."]
       [:put "raw" 3 "Moby Dick is a story of a whale and a man obsessed."]])
    (doseq [i [1 2 3]]
      (d/add-doc engine i (d/get-value lmdb "raw" i)))

    (is (not (d/doc-indexed? engine 0)))
    (is (d/doc-indexed? engine 1))

    (is (= 3 (d/doc-count engine)))
    ;; (is (= [1 2 3] (d/doc-refs engine)))

    (is (= (d/search engine "lazy") [1]))
    (is (= (d/search engine "red" ) [1 2]))
    (is (= (d/search engine "red" {:display :offsets})
           [[1 [["red" [10 39]]]] [2 [["red" [40]]]]]))
    (testing "update"
      (d/add-doc engine 1 "The quick fox jumped over the lazy dogs.")
      (is (= (d/search engine "red" ) [2])))

    (d/remove-doc engine 1)
    (is (= 2 (d/doc-count engine)))

    (d/clear-docs engine)
    (is (= 0 (d/doc-count engine)))

    (d/close-kv lmdb)))

(deftest remote-blank-analyzer-test
  (let [dir            "dtlv://datalevin:datalevin@localhost/blank-analyzer-test"
        lmdb           (d/open-kv dir)
        blank-analyzer (i/inter-fn
                         [^String text]
                         (map-indexed (fn [i ^String t]
                                        [t i (.indexOf text t)])
                                      (s/split text #"\s")))
        engine         (d/new-search-engine lmdb {:analyzer blank-analyzer})]
    (d/open-dbi lmdb "raw")
    (d/transact-kv
      lmdb
      [[:put "raw" 1 "The quick red fox jumped over the lazy red dogs."]
       [:put "raw" 2 "Mary had a little lamb whose fleece was red as fire."]
       [:put "raw" 3 "Moby Dick is a story of some dogs and a whale."]])
    (doseq [i [1 2 3]]
      (d/add-doc engine i (d/get-value lmdb "raw" i)))
    (is (= [[1 [["dogs." [43]]]]]
           (d/search engine "dogs." {:display :offsets})))
    (is (= [3] (d/search engine "dogs")))
    (d/close-kv lmdb)))

(deftest remote-fulltext-fns-test
  (let [dir      "dtlv://datalevin:datalevin@localhost/remote-fulltext-fns-test"
        analyzer (i/inter-fn
                   [^String text]
                   (map-indexed (fn [i ^String t]
                                  [t i (.indexOf text t)])
                                (s/split text #"\s")))
        db       (-> (d/empty-db
                       dir
                       {:text {:db/valueType :db.type/string
                               :db/fulltext  true}}
                       {:auto-entity-time? true
                        :search-engine     {:analyzer analyzer}})
                     (d/db-with
                       [{:db/id 1,
                         :text  "The quick red fox jumped over the lazy red dogs."}
                        {:db/id 2,
                         :text  "Mary had a little lamb whose fleece was red as fire."}
                        {:db/id 3,
                         :text  "Moby Dick is a story of a whale and a man obsessed."}]))]
    (is (= (d/q '[:find ?e ?a ?v
                  :in $ ?q
                  :where [(fulltext $ ?q) [[?e ?a ?v]]]]
                db "red fox")
           #{[1 :text "The quick red fox jumped over the lazy red dogs."]
             [2 :text "Mary had a little lamb whose fleece was red as fire."]}))
    (is (empty? (d/q '[:find ?e ?a ?v
                       :in $ ?q
                       :where [(fulltext $ ?q) [[?e ?a ?v]]]]
                     db "")))
    (is (= (d/datom-v
             (first (d/fulltext-datoms db "red fox")))
           "The quick red fox jumped over the lazy red dogs."))
    (d/close-db db)))
