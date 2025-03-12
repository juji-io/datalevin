(ns datalevin.remote-search-test
  (:require
   [datalevin.interpret :as i]
   [datalevin.search-utils :as su]
   [datalevin.core :as d]
   [clojure.string :as s]
   [datalevin.test.core :refer [server-fixture]]
   [clojure.test :refer [is testing deftest use-fixtures]]))

(use-fixtures :each server-fixture)

(deftest remote-search-kv-test
  (let [dir    "dtlv://datalevin:datalevin@localhost/remote-search-kv-test"
        lmdb   (d/open-kv dir)
        engine (d/new-search-engine lmdb {:index-position? true})]
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
        engine         (d/new-search-engine
                         lmdb {:analyzer        blank-analyzer
                               :index-position? true})]
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
                        :search-opts       {:analyzer analyzer}})
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
    (is (= (peek (first (d/fulltext-datoms db "red fox")))
           "The quick red fox jumped over the lazy red dogs."))
    (d/close-db db)))

(defn- add-docs
  [f engine]
  (f engine :doc0 "")
  (f engine :doc1 "The quick red fox jumped over the lazy red dogs.")
  (f engine :doc2 "Mary had a little lamb whose fleece was red as fire.")
  (f engine :doc3 "Moby Dick is a story of a whale and a man obsessed.")
  (f engine :doc4 "The robber wore a red fleece jacket and a baseball cap.")
  (f engine :doc5
     "The English Springer Spaniel is the best of all red dogs I know.")
  )

(deftest re-index-search-test
  (let [dir    "dtlv://datalevin:datalevin@localhost/re-index-search-test"
        lmdb   (d/open-kv dir)
        opts   {:index-position? true
                :include-text?   true}
        engine (d/new-search-engine lmdb opts)]

    (add-docs d/add-doc engine)

    (is (empty? (d/search engine "dog")))

    (let [engine1 (d/re-index
                    engine (merge opts {:analyzer
                                        (su/create-analyzer
                                          {:token-filters
                                           [(su/create-stemming-token-filter
                                              "english")]})}))]
      (is (= [:doc1 :doc5] (d/search engine1 "dog"))))

    (d/close-kv lmdb)))

(deftest domain-test
  (let [dir      "dtlv://datalevin:datalevin@localhost/domain-test"
        analyzer (su/create-analyzer
                   {:token-filters [(su/create-stemming-token-filter
                                      "english")]})
        conn     (d/create-conn
                   dir
                   {:a/id     {:db/valueType :db.type/long
                               :db/unique    :db.unique/identity}
                    :a/string {:db/valueType        :db.type/string
                               :db/fulltext         true
                               :db.fulltext/domains ["da"]}
                    :b/string {:db/valueType           :db.type/string
                               :db/fulltext            true
                               :db.fulltext/autoDomain true
                               :db.fulltext/domains    ["db"]}}
                   {:search-domains {"da" {:analyzer analyzer}
                                     "db" {}}})
        sa       "The quick brown fox jumps over the lazy dogs"
        sb       "Pack my box with five dozen liquor jugs."
        sc       "How vexingly quick daft zebras jump!"
        sd       "Five dogs jump over my fence."]
    (d/transact! conn [{:a/id 1 :a/string sa :b/string sb}])
    (d/transact! conn [{:a/id 2 :a/string sc :b/string sd}])
    (is (thrown-with-msg? Exception #":db.fulltext/autoDomain"
                          (d/q '[:find [?v ...]
                                 :in $ ?q
                                 :where
                                 [(fulltext $ :a/string ?q) [[?e _ ?v]]]]
                               (d/db conn) "jump")))
    (is (= (d/q '[:find [?v ...]
                  :in $ ?q
                  :where
                  [(fulltext $ :b/string ?q) [[?e _ ?v]]]]
                (d/db conn) "jump")
           [sd]))
    (is (= (set (d/q '[:find [?v ...]
                       :in $ ?q
                       :where
                       [(fulltext $ ?q {:domains ["da"]}) [[?e _ ?v]]]]
                     (d/db conn) "jump"))
           #{sa sc}))
    (is (= (d/q '[:find [?v ...]
                  :in $ ?q
                  :where
                  [(fulltext $ ?q {:domains ["db"]}) [[?e _ ?v]]]]
                (d/db conn) "jump")
           [sd]))
    (is (= (set (d/q '[:find [?v ...]
                       :in $ ?q
                       :where
                       [(fulltext $ ?q {:domains ["da" "db"]}) [[?e _ ?v]]]]
                     (d/db conn) "jump"))
           #{sa sc sd}))
    (is (= (set (d/q '[:find [?v ...]
                       :in $ ?q
                       :where
                       [(fulltext $ ?q) [[?e _ ?v]]]]
                     (d/db conn) "jump"))
           #{sa sc sd}))
    (is (= (set (d/q '[:find [?v ...]
                       :in $ ?q
                       :where
                       [(fulltext $ ?q) [[?e _ ?v]]]]
                     (d/db conn) "dog"))
           #{sa}))
    (is (= (set (d/q '[:find [?v ...]
                       :in $ ?q
                       :where
                       [(fulltext $ ?q) [[?e _ ?v]]]]
                     (d/db conn) "dogs"))
           #{sa sd}))
    (is (empty? (d/q '[:find [?v ...]
                       :in $ ?q
                       :where
                       [(fulltext $ ?q {:domains ["db"]}) [[?e _ ?v]]]]
                     (d/db conn) "dog")))
    (d/close conn)))
