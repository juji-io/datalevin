(ns datalevin.search-test
  (:require [datalevin.search :as sut]
            [datalevin.lmdb :as l]
            [datalevin.constants :as c]
            [datalevin.util :as u]
            [clojure.test :refer [is deftest]])
  (:import [java.util UUID Map ArrayList]
           [datalevin.sm SymSpell Bigram]
           [datalevin.search SearchEngine]))

(deftest english-analyzer-test
  (let [s1 "This is a Datalevin-Analyzers test"
        s2 "This is a Datalevin-Analyzers test. "]
    (is (= (sut/en-analyzer s1)
           (sut/en-analyzer s2)
           [["datalevin-analyzers" 3 10] ["test" 4 30]]))
    (is (= (subs s1 10 (+ 10 (.length "datalevin-analyzers")))
           "Datalevin-Analyzers" ))))

(deftest index-test
  (let [lmdb   (l/open-kv (u/tmp-dir (str "index-" (UUID/randomUUID))))
        engine ^SearchEngine (sut/new-engine lmdb)]
    (sut/add-doc engine :doc1
                 "The quick red fox jumped over the lazy red dogs.")
    (sut/add-doc engine :doc2
                 "Mary had a little lamb whose fleece was red as fire.")
    (sut/add-doc engine :doc3
                 "Moby Dick is a story of a whale and a man obsessed.")
    (sut/add-doc engine :doc4
                 "The robber wore a red fleece jacket and a baseball cap.")
    (sut/add-doc engine :doc5
                 "The English Springer Spaniel is the best of all red dogs.")
    (is (= (count (.-unigrams engine))
           (l/range-count lmdb c/unigrams [:all] :string)
           (.size ^Map (.getUnigramLexicon ^SymSpell (.-symspell engine)))
           30))
    (let [[tid freq] (l/get-value lmdb c/unigrams "red" :string :double-id true)]
      (is (= freq
             (.get ^Map (.getUnigramLexicon ^SymSpell (.-symspell engine)) "red")
             5))
      (is (= (.get ^Map (.-terms engine) tid) "red"))
      (is (l/in-list? lmdb c/term-docs tid 1 :id :id))
      (is (l/in-list? lmdb c/term-docs tid 5 :id :id))
      (is (= (l/list-count lmdb c/term-docs tid :id) 4))
      (is (= (l/get-list lmdb c/term-docs tid :id :id) [1 2 4 5]))
      (is (= (l/list-count lmdb c/positions [1 tid] :double-id) 2))
      (is (= (l/list-count lmdb c/positions [5 tid] :double-id) 1))
      (is (= (l/get-list lmdb c/positions [5 tid] :double-id :double-int)
             [[9 48]]))
      (is (= (l/range-count lmdb c/positions [:closed [5 0] [5 Long/MAX_VALUE]]
                            :double-id)
             7))
      (let [[tid2 freq2] (l/get-value lmdb c/unigrams "dogs"
                                      :string :double-id true)]
        (is (= freq2 2))
        (is (= (l/get-value lmdb c/bigrams [tid tid2] :double-id :id true)
               (.get ^Map (.getBigramLexicon ^SymSpell (.-symspell engine))
                     (Bigram. "red" "dogs"))
               2)))

      (is (= (l/get-value lmdb c/docs 1 :id :data true) {:ref :doc1 :uniq 7}))
      (is (= (l/get-value lmdb c/docs 4 :id :data true) {:ref :doc4 :uniq 7}))
      (is (= (l/get-value lmdb c/rdocs :doc4 :data :id true) 4))
      (is (= (l/range-count lmdb c/docs [:all]) 5))
      (is (= (l/range-count lmdb c/rdocs [:all]) 5))

      (sut/remove-doc engine :doc5)
      (is (= (l/range-count lmdb c/docs [:all]) 4))
      (is (= (l/range-count lmdb c/rdocs [:all]) 4))
      (is (not (l/in-list? lmdb c/term-docs tid 5 :id :id)))
      (is (= (l/list-count lmdb c/term-docs tid :id) 3))
      (is (= (l/list-count lmdb c/positions [5 tid] :double-id) 0))
      (is (= (l/get-list lmdb c/positions [5 tid] :double-id :double-int) [])))

    (l/close-kv lmdb)
    ))
