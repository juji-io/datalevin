(ns datalevin.search-test
  (:require
   [datalevin.search :as sut]
   [datalevin.lmdb :as l]
   [datalevin.bits :as b]
   [datalevin.interpret :as i]
   [datalevin.core :as d]
   [datalevin.sparselist :as sl]
   [datalevin.util :as u]
   [clojure.data.csv :as csv]
   [clojure.java.io :as io]
   [clojure.string :as s]
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.clojure-test :as test]
   [clojure.test.check.properties :as prop]
   [clojure.test :refer [deftest testing is use-fixtures]]
   [datalevin.constants :as c])
  (:import
   [java.util UUID ]
   [datalevin.sparselist SparseIntArrayList]
   [datalevin.search SearchEngine]))

(use-fixtures :each db-fixture)

(deftest english-analyzer-test
  (let [s1 "This is a Datalevin-Analyzers test"
        s2 "This is a Datalevin-Analyzers test. "]
    (is (= (sut/en-analyzer s1)
           (sut/en-analyzer s2)
           [["datalevin-analyzers" 3 10] ["test" 4 30]]))
    (is (= (subs s1 10 (+ 10 (.length "datalevin-analyzers")))
           "Datalevin-Analyzers" ))))

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

(deftest blank-analyzer-test
  (let [blank-analyzer (fn [^String text]
                         (map-indexed (fn [i ^String t]
                                        [t i (.indexOf text t)])
                                      (s/split text #"\s")))
        dir            (u/tmp-dir (str "analyzer-" (UUID/randomUUID)))
        lmdb           (l/open-kv dir)
        engine         (sut/new-search-engine
                         lmdb {:analyzer        blank-analyzer
                               :index-position? true})]
    (add-docs sut/add-doc engine)
    (is (= [[:doc1 [["dogs." [43]]]]]
           (sut/search engine "dogs." {:display :offsets})))
    (is (= [:doc5] (sut/search engine "dogs")))
    (l/close-kv lmdb)
    (u/delete-files dir)))

(deftest index-test
  (let [dir           (u/tmp-dir (str "index-" (UUID/randomUUID)))
        lmdb          (l/open-kv dir)
        engine        ^SearchEngine (sut/new-search-engine
                                      lmdb {:index-position? true})
        terms-dbi     (.-terms-dbi engine)
        docs-dbi      (.-docs-dbi engine)
        positions-dbi (.-positions-dbi engine)]

    (is (= (sut/doc-count engine) 0))
    (is (not (sut/doc-indexed? engine :doc4)))

    (add-docs sut/add-doc engine)

    (is (sut/doc-indexed? engine :doc4))
    (is (not (sut/doc-indexed? engine :non-existent)))
    (is (not (sut/doc-indexed? engine "non-existent")))

    (is (= (sut/doc-count engine) 5))

    (let [[tid _ ^SparseIntArrayList sl]
          (l/get-value lmdb terms-dbi "red" :string :term-info true)]
      (is (= (l/range-count lmdb terms-dbi [:all] :string) 32))
      (is (= (l/get-value lmdb terms-dbi "red" :string :int) tid))

      (is (sl/contains-index? sl 1))
      (is (sl/contains-index? sl 5))
      (is (= (sl/size sl) 4))
      (is (= (seq (.-indices sl)) [1 2 4 5]))

      (is (= (l/list-count lmdb positions-dbi [1 tid] :int-int)
             (sl/get sl 1)
             2))
      (is (= (l/list-count lmdb positions-dbi [2 tid] :int-int)
             (sl/get sl 2)
             1))

      (is (= (l/list-count lmdb positions-dbi [3 tid] :int-int)
             0))
      (is (nil? (sl/get sl 3)))

      (is (= (l/list-count lmdb positions-dbi [4 tid] :int-int)
             (sl/get sl 4)
             1))
      (is (= (l/list-count lmdb positions-dbi [5 tid] :int-int)
             (sl/get sl 5)
             1))

      (is (= (l/get-list lmdb positions-dbi [5 tid] :int-int :int-int)
             [[9 48]]))
      (is (= (update (l/get-value lmdb docs-dbi :doc1 :data :doc-info true)
                     2 set)
             [1 7 #{1,2,3,4,5,6,7}]))
      (is (= (update (l/get-value lmdb docs-dbi :doc2 :data :doc-info true)
                     2 set)
             [2 8 #{1, 8, 9, 10, 11, 12, 13, 14}]))
      (is (= (update (l/get-value lmdb docs-dbi :doc3 :data :doc-info true)
                     2 set)
             [3 6 #{15,16,17,18,19,20}]))
      (is (= (update (l/get-value lmdb docs-dbi :doc4 :data :doc-info true)
                     2 set)
             [4 7 #{1,8,21,22,23,24,25}]))
      (is (= (update (l/get-value lmdb docs-dbi :doc5 :data :doc-info true)
                     2 set)
             [5 9 #{1,6,26,27,28,29,30,31,32}]))
      (is (= (l/range-count lmdb docs-dbi [:all]) 5))

      (sut/remove-doc engine :doc1)

      (is (= (sut/doc-count engine) 4))

      (let [[tid _ ^SparseIntArrayList sl]
            (l/get-value lmdb terms-dbi "red" :string :term-info true)]
        (is (not (sut/doc-indexed? engine :doc1)))
        (is (= (l/range-count lmdb docs-dbi [:all]) 4))
        (is (not (sl/contains-index? sl 1)))
        (is (= (sl/size sl) 3))
        (is (= (l/list-count lmdb positions-dbi [1 tid] :int-id) 0))
        (is (nil? (l/get-list lmdb positions-dbi [1 tid] :int-id :int-int))))

      (sut/clear-docs engine)
      (is (= (sut/doc-count engine) 0)))
    (l/close-kv lmdb)
    (u/delete-files dir)))

(deftest search-test
  (let [dir    (u/tmp-dir (str "search-" (UUID/randomUUID)))
        lmdb   (l/open-kv dir)
        engine ^SearchEngine (sut/new-search-engine
                               lmdb {:index-position? true})]
    (add-docs sut/add-doc engine)

    (is (= [:doc3 :doc2] (sut/search engine "mary moby")))
    (is (= [:doc1 :doc4 :doc2 :doc5] (sut/search engine "red cat")))
    (is (= (sut/search engine "cap" {:display :offsets})
           [[:doc4 [["cap" [51]]]]]))
    (is (= (sut/search engine "notaword cap" {:display :offsets})
           [[:doc4 [["cap" [51]]]]]))
    (is (= (sut/search engine "fleece" {:display :offsets})
           [[:doc4 [["fleece" [22]]]] [:doc2 [["fleece" [29]]]]]))
    (is (= (sut/search engine "red fox" {:display :offsets})
           [[:doc1 [["fox" [14]] ["red" [10 39]]]]
            [:doc4 [["red" [18]]]]
            [:doc2 [["red" [40]]]]
            [:doc5 [["red" [48]]]]]))
    (is (= (sut/search engine "red fox" {:doc-filter #(not= % :doc2)})
           [:doc1 :doc4 :doc5]))
    (is (= (sut/search engine "red dogs" {:display :offsets})
           [[:doc1 [["dogs" [43]] ["red" [10 39]]]]
            [:doc5 [["dogs" [52]] ["red" [48]]]]
            [:doc4 [["red" [18]]]]
            [:doc2 [["red" [40]]]]]))
    (is (= (sut/search engine "solar cap" {:display :offsets})
           [[:doc4 [["cap" [51]]]]]))
    (is (empty? (sut/search engine "")))
    (is (empty? (sut/search engine "solar")))
    (is (empty? (sut/search engine "solar wind")))
    (l/close-kv lmdb)
    (u/delete-files dir)))

(deftest search-143-test
  (let [dir           (u/tmp-dir (str "search-143-" (UUID/randomUUID)))
        lmdb          (l/open-kv dir)
        engine        ^SearchEngine (sut/new-search-engine
                                      lmdb {:index-position? true})
        terms-dbi     (.-terms-dbi engine)
        positions-dbi (.-positions-dbi engine)
        docs-dbi      (.-docs-dbi engine)]

    (sut/add-doc engine 1 "a tent")
    (sut/add-doc engine 2 "tent")

    (is (= (sut/doc-count engine) 2))

    (let [[tid mw ^SparseIntArrayList sl]
          (l/get-value lmdb terms-dbi "tent" :string :term-info true)]
      (is (= (l/range-count lmdb terms-dbi [:all] :string) 1))
      (is (= (l/get-value lmdb terms-dbi "tent" :string :int) tid))
      (is (= mw 1.0))

      (is (sl/contains-index? sl 1))
      (is (= (sl/size sl) 2))
      (is (= (seq (.-indices sl)) [1 2]))

      (is (= (l/list-count lmdb positions-dbi [1 tid] :int-int)
             (sl/get sl 1)
             1))
      (is (= (l/list-count lmdb positions-dbi [2 tid] :int-int)
             (sl/get sl 2)
             1))

      (is (= (l/list-count lmdb positions-dbi [3 tid] :int-int)
             0))
      (is (nil? (sl/get sl 3)))

      (is (= (l/get-list lmdb positions-dbi [1 tid] :int-int :int-int)
             [[1 2]]))

      (is (= (update (l/get-value lmdb docs-dbi 1 :data :doc-info true)
                     2 set)
             [1 1 #{1}]))
      (is (= (update (l/get-value lmdb docs-dbi 2 :data :doc-info true)
                     2 set)
             [2 1 #{1}]))
      (is (= (l/range-count lmdb docs-dbi [:all]) 2))
      )

    (is (= (sut/search engine "tent") [2 1]))
    (l/close-kv lmdb)
    (u/delete-files dir)))

(deftest multi-domains-test
  (let [dir     (u/tmp-dir (str "search-multi" (UUID/randomUUID)))
        lmdb    (l/open-kv dir)
        engine1 ^SearchEngine (sut/new-search-engine lmdb)
        engine2 ^SearchEngine (sut/new-search-engine
                                lmdb {:domain "another"})]
    (sut/add-doc engine1 1 "hello world")
    (sut/add-doc engine1 2 "Mars is a red planet")
    (sut/add-doc engine1 3 "Earth is a blue planet")
    (add-docs sut/add-doc engine2)

    (is (empty? (sut/search engine1 "solar")))
    (is (empty? (sut/search engine2 "solar")))
    (is (= (sut/search engine1 "red") [2]))
    (is (= (sut/search engine2 "red") [:doc1 :doc4 :doc2 :doc5]))
    (l/close-kv lmdb)
    (u/delete-files dir)))

(deftest search-kv-test
  (let [dir    (u/tmp-dir (str "search-kv-" (UUID/randomUUID)))
        lmdb   (l/open-kv dir)
        engine (sut/new-search-engine lmdb {:index-position? true})]
    (l/open-dbi lmdb "raw")
    (l/transact-kv
      lmdb
      [[:put "raw" 1 "The quick red fox jumped over the lazy red dogs."]
       [:put "raw" 2 "Mary had a little lamb whose fleece was red as fire."]
       [:put "raw" 3 "Moby Dick is a story of a whale and a man obsessed."]])
    (doseq [i [1 2 3]]
      (sut/add-doc engine i (l/get-value lmdb "raw" i)))

    (is (not (sut/doc-indexed? engine 0)))
    (is (sut/doc-indexed? engine 1))

    (is (= 3 (sut/doc-count engine)))

    (is (= (sut/search engine "lazy") [1]))
    (is (= (sut/search engine "red" ) [1 2]))
    (is (= (sut/search engine "red" {:display :offsets})
           [[1 [["red" [10 39]]]] [2 [["red" [40]]]]]))
    (testing "update"
      (sut/add-doc engine 1 "The quick fox jumped over the lazy dogs.")
      (is (= (sut/search engine "red" ) [2])))
    (testing "parallel update"
      (dorun (pmap #(sut/add-doc engine %1 %2)
                   [2 1 4]
                   ["May has a little lamb."
                    "The quick red fox jumped over the lazy dogs."
                    "do you know the game truth or dare <p>What's your biggest fear? I want to see if you could tell me the truth :-)</p>"]))
      (is (= (sut/search engine "red" ) [1]))
      (is (= (sut/search engine "truth" ) [4])))

    (testing "duplicated docs"
      (sut/add-doc engine 5
                   "Pricing how much is the price for each juji Whats the price of using juji for classes Hello, what is the price of Juji? <p>You can create me or any of my brothers and sisters FREE. You can also chat with us privately FREE, as long as you want.</p><p><br></p><p>If you wish to make me or any of my sisters or brothers public so we can chat with other folks, I'd be happy to help you find the right price package.</p>")
      (sut/add-doc engine 4
                   "do you know the game truth or dare <p>What's your biggest fear? I want to see if you could tell me the truth :-)")
      (is (= (sut/search engine "truth" ) [4])))

    (l/close-kv lmdb)
    (u/delete-files dir)))

(deftest fulltext-fns-test
  (let [analyzer (i/inter-fn
                   [^String text]
                   (map-indexed (fn [i ^String t]
                                  [t i (.indexOf text t)])
                                (s/split text #"\s")))
        dir      (u/tmp-dir (str "fulltext-fns-" (UUID/randomUUID)))
        conn     (d/create-conn dir
                                {:a/id     {:db/valueType :db.type/long
                                            :db/unique    :db.unique/identity
                                            }
                                 :a/string {:db/valueType :db.type/string
                                            :db/fulltext  true}
                                 :b/string {:db/valueType :db.type/string
                                            :db/fulltext  true}}
                                {:auto-entity-time? true
                                 :search-opts       {:analyzer analyzer}})
        s        "The quick brown fox jumps over the lazy dog"]
    (d/transact! conn [{:a/id 1 :a/string s :b/string ""}])
    (d/transact! conn [{:a/id 1 :a/string s :b/string "bar"}])
    (is (= (d/q '[:find ?v .
                  :in $ ?q
                  :where [(fulltext $ ?q) [[?e ?a ?v]]]]
                (d/db conn) "brown fox")
           s))
    (is (empty? (d/q '[:find ?v .
                       :in $ ?q
                       :where [(fulltext $ ?q) [[?e ?a ?v]]]]
                     (d/db conn) "")))
    (is (= (d/datom-v
             (first (d/fulltext-datoms (d/db conn) "brown fox")))
           s))
    (d/close conn)
    (u/delete-files dir)))

(defn- rows->maps [csv]
  (let [headers (map keyword (first csv))
        rows    (rest csv)]
    (map #(zipmap headers %) rows)))

(deftest load-csv-test
  (let [dir   (u/tmp-dir (str "load-csv-test-" (UUID/randomUUID)))
        conn  (d/create-conn
                dir {:id          {:db/valueType :db.type/string
                                   :db/unique    :db.unique/identity}
                     :description {:db/valueType :db.type/string
                                   :db/fulltext  true}})
        data  (rows->maps
                (with-open [reader (io/reader "test/data/data.csv")]
                  (doall (csv/read-csv reader))))
        start (System/currentTimeMillis)]
    (d/transact! conn data)
    (is (< (- (System/currentTimeMillis) start) 5000))
    (is (= 36 (count (d/fulltext-datoms (d/db conn) "Memorex" {:top 100}))))
    (is (= (d/q '[:find (count ?e) .
                  :in $ ?q
                  :where [(fulltext $ ?q {:top 100}) [[?e _ _]]]]
                (d/db conn) "Memorex")
           36))
    (d/close conn)
    (u/delete-files dir)))

(deftest update-doc-test
  (let [dir       (u/tmp-dir (str "update-doc-test-" (UUID/randomUUID)))
        lmdb      (d/open-kv dir)
        engine    ^SearchEngine (d/new-search-engine lmdb)
        terms-dbi (.-terms-dbi engine)
        docs-dbi  (.-docs-dbi engine)]
    (add-docs d/add-doc engine)
    (let [[tid _ sl]
          (l/get-value lmdb terms-dbi "fox" :string :term-info true)]
      (is (= tid 7))
      (is (= sl (sl/sparse-arraylist {1 1}))))
    (let [[tid _ sl]
          (l/get-value lmdb terms-dbi "red" :string :term-info true)]
      (is (= tid 1))
      (is (= sl (sl/sparse-arraylist {1 2 2 1 4 1 5 1}))))
    (is (= (update (l/get-value lmdb docs-dbi :doc1 :data :doc-info true)
                   2 set)
           [1 7 #{1,2,3,4,5,6,7}]))
    (is (= {1 :doc1 2 :doc2 3 :doc3 4 :doc4 5 :doc5} (.-docs engine)))
    (is (= 5 (d/doc-count engine)))

    (is (= [:doc1 :doc4 :doc2 :doc5] (d/search engine "red fox")))

    (d/add-doc engine :doc1
               "The quick brown fox jumped over the lazy black dogs.")
    (let [[tid _ sl]
          (l/get-value lmdb terms-dbi "fox" :string :term-info true)]
      (is (= tid 7))
      (is (= sl (sl/sparse-arraylist {6 1}))))
    (let [[tid _ sl]
          (l/get-value lmdb terms-dbi "red" :string :term-info true)]
      (is (= tid 1))
      (is (= sl (sl/sparse-arraylist {2 1 4 1 5 1}))))
    (is (= {6 :doc1 2 :doc2 3 :doc3 4 :doc4 5 :doc5} (.-docs engine)))
    (is (= 5 (d/doc-count engine)))
    (is (d/doc-indexed? engine :doc1))
    (is (= (update (l/get-value lmdb docs-dbi :doc1 :data :doc-info true)
                   2 set)
           [6 8 #{2,3,4,5,6,7,33,34}]))

    (is (= [:doc1] (d/search engine "black")))
    (is (= [:doc1] (d/search engine "fox")))
    (is (= [:doc1] (d/search engine "black fox")))
    (is (= [:doc4 :doc2 :doc5] (d/search engine "red")))
    (is (= [:doc1 :doc4 :doc2 :doc5] (d/search engine "red fox")))
    (is (= [:doc1] (d/search engine "brown fox")))
    (is (= [:doc1 :doc5] (d/search engine "brown dogs")))
    (is (= [:doc1 :doc2] (d/search engine "brown lamb")))

    (d/add-doc engine :doc2
               "Mary had a little lamb whose fleece was black as coal.")
    (is (= [:doc2 :doc1] (d/search engine "black")))
    (is (= [:doc1 :doc2] (d/search engine "black fox")))
    (is (= [:doc4 :doc5] (d/search engine "red")))
    (is (= [:doc1 :doc4 :doc5] (d/search engine "red fox")))
    (is (= [:doc2 :doc1] (d/search engine "brown lamb")))

    (d/close-kv lmdb)
    (u/delete-files dir)))

(deftest huge-doc-test
  (let [dir    (u/tmp-dir (str "huge-doc-test-" (UUID/randomUUID)))
        lmdb   (d/open-kv dir)
        engine ^SearchEngine (d/new-search-engine
                               lmdb {:index-position? true})]
    (d/add-doc engine "Romeo and Juliet" (slurp "test/data/romeo.txt"))
    (is (= ["Romeo and Juliet"] (d/search engine "romeo")))
    (is (= 4283 (count (.-terms engine))))
    (is (= 299 (l/list-count
                 lmdb (.-positions-dbi engine)
                 [1 (l/get-value lmdb (.-terms-dbi engine)
                                 "romeo" :string :int true)]
                 :int-int)))
    (d/close-kv lmdb)
    (u/delete-files dir)))

;; TODO numerical stability of doubles is not great
;; (def tokens ["b" "c" "d" "e" "f" "g" "h" "i" "j" "k" "l" "m" "n"
;;              "o" "p" "q" "r" "s" "t" "u" "v" "w" "x" "y" "z"])

;; (def doc-num 10)

;; (defn- search-correct
;;   [data query results]
;;   (let [refs (keys data)
;;         docs (vals data)

;;         dfreqs (zipmap refs (map frequencies docs))

;;         norms (zipmap refs (map count (vals dfreqs)))

;;         terms  (s/split query #" ")
;;         qfreqs (frequencies terms)

;;         dfs (reduce (fn [tm term]
;;                       (assoc tm term
;;                              (reduce (fn [dm [ref freqs]]
;;                                        (if-let [c (freqs term)]
;;                                          (assoc dm ref c)
;;                                          dm))
;;                                      {}
;;                                      dfreqs)))
;;                     {}
;;                     terms)
;;         wqs (reduce (fn [m [term freq]]
;;                       (assoc m term
;;                              (* ^double (sut/tf* freq)
;;                                 ^double (sut/idf (count (dfs term))
;;                                                  doc-num))))
;;                     {}
;;                     qfreqs)
;;         rc  (count results)]
;;     (println "results =>" results)
;;     (= results
;;        (->> data
;;             (sort-by
;;               (fn [[ref _]]
;;                 (reduce
;;                   +
;;                   (map (fn [term]
;;                          (/ ^double
;;                             (* (double (or (wqs term) 0.0))
;;                                ^double (sut/tf*
;;                                          (or (get-in dfreqs [ref term])
;;                                              0.0)))
;;                             (double (norms ref))))
;;                        terms)))
;;               >)
;;             (#(do (println "data sorted by query =>" %) %))
;;             (take rc)
;;             (map first)))))

;; (test/defspec search-generative-test
;;   1
;;   (prop/for-all
;;     [refs (gen/vector-distinct gen/nat {:num-elements doc-num})
;;      docs (gen/vector-distinct
;;             (gen/vector (gen/elements tokens) 3 10) ;; doc length
;;             {:num-elements doc-num}) ;; num of docs
;;      qs (gen/vector (gen/vector (gen/elements tokens) 1 3) 1)]
;;     (let [dir     (u/tmp-dir (str "search-test-" (UUID/randomUUID)))
;;           lmdb    (d/open-kv dir)
;;           engine  (d/new-search-engine lmdb)
;;           data    (zipmap refs docs)
;;           _       (println "data =>" data)
;;           jf      #(s/join " " %)
;;           txs     (zipmap refs (map jf docs))
;;           queries (map jf qs)
;;           _       (doseq [[k v] txs] (d/add-doc engine k v))
;;           _       (println "queries =>" queries)
;;           ok      (every? #(search-correct data % (d/search engine %))
;;                           queries)
;;           ]
;;       (l/close-kv lmdb)
;;       (u/delete-files dir)
;;       ok)))
