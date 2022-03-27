(ns datalevin.search-test
  (:require [datalevin.search :as sut]
            [datalevin.lmdb :as l]
            [datalevin.bits :as b]
            [datalevin.sparselist :as sl]
            [datalevin.constants :as c]
            [datalevin.util :as u]
            [clojure.test :refer [is deftest testing]])
  (:import [java.util UUID Map ArrayList]
           [org.eclipse.collections.impl.map.mutable.primitive IntShortHashMap
            IntObjectHashMap ObjectIntHashMap]
           [org.roaringbitmap RoaringBitmap]
           [datalevin.sparselist SparseIntArrayList]
           [datalevin.search SearchEngine IndexWriter]))

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
  (f engine :doc1
     "The quick red fox jumped over the lazy red dogs.")
  (f engine :doc2
     "Mary had a little lamb whose fleece was red as fire.")
  (f engine :doc3
     "Moby Dick is a story of a whale and a man obsessed.")
  (f engine :doc4
     "The robber wore a red fleece jacket and a baseball cap.")
  (f engine :doc5
     "The English Springer Spaniel is the best of all red dogs I know."))

(deftest index-test
  (let [lmdb   (l/open-kv (u/tmp-dir (str "index-" (UUID/randomUUID))))
        engine ^SearchEngine (sut/new-search-engine lmdb)]

    (is (= (sut/doc-count engine) 0))
    (is (not (sut/doc-indexed? engine :doc4)))
    (is (= (sut/doc-refs engine) []))

    (add-docs sut/add-doc engine)

    (is (sut/doc-indexed? engine :doc4))
    (is (not (sut/doc-indexed? engine :non-existent)))
    (is (not (sut/doc-indexed? engine "non-existent")))

    (is (= (sut/doc-count engine) 5))
    (is (= (sut/doc-refs engine) [:doc1 :doc2 :doc3 :doc4 :doc5]))

    (let [[tid mw ^SparseIntArrayList sl]
          (l/get-value lmdb c/terms "red" :string :term-info true)]
      (is (= (l/range-count lmdb c/terms [:all] :string) 32))
      (is (= (l/get-value lmdb c/terms "red" :string :int) tid))

      (is (sl/contains-index? sl 1))
      (is (sl/contains-index? sl 5))
      (is (= (sl/size sl) 4))
      (is (= (seq (.-indices sl)) [1 2 4 5]))

      (is (= (l/list-count lmdb c/positions [tid 1] :int-int)
             (sl/get sl 1)
             2))
      (is (= (l/list-count lmdb c/positions [tid 2] :int-int)
             (sl/get sl 2)
             1))

      (is (= (l/list-count lmdb c/positions [tid 3] :int-int)
             0))
      (is (nil? (sl/get sl 3)))

      (is (= (l/list-count lmdb c/positions [tid 4] :int-int)
             (sl/get sl 4)
             1))
      (is (= (l/list-count lmdb c/positions [tid 5] :int-int)
             (sl/get sl 5)
             1))

      (is (= (l/get-list lmdb c/positions [tid 5] :int-int :int-int)
             [[9 48]]))

      (is (= (l/get-value lmdb c/docs 1 :int :doc-info true) [7 :doc1]))
      (is (= (l/get-value lmdb c/docs 2 :int :doc-info true) [8 :doc2]))
      (is (= (l/get-value lmdb c/docs 3 :int :doc-info true) [6 :doc3]))
      (is (= (l/get-value lmdb c/docs 4 :int :doc-info true) [7 :doc4]))
      (is (= (l/get-value lmdb c/docs 5 :int :doc-info true) [9 :doc5]))
      (is (= (l/range-count lmdb c/docs [:all]) 5))

      (sut/remove-doc engine :doc1)

      (is (= (sut/doc-count engine) 4))
      (is (= (sut/doc-refs engine) [:doc2 :doc3 :doc4 :doc5]))

      (let [[tid mw ^SparseIntArrayList sl]
            (l/get-value lmdb c/terms "red" :string :term-info true)]
        (is (not (sut/doc-indexed? engine :doc1)))
        (is (= (l/range-count lmdb c/docs [:all]) 4))
        (is (not (sl/contains-index? sl 1)))
        (is (= (sl/size sl) 3))
        (is (= (l/list-count lmdb c/positions [tid 1] :int-id) 0))
        (is (nil? (l/get-list lmdb c/positions [tid 1] :int-id :int-int)))))

    (l/close-kv lmdb)))

(deftest search-test
  (let [lmdb   (l/open-kv (u/tmp-dir (str "search-" (UUID/randomUUID))))
        engine ^SearchEngine (sut/new-search-engine lmdb)]
    (add-docs sut/add-doc engine)

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
    (is (empty? (sut/search engine "solar")))
    (is (empty? (sut/search engine "solar wind")))
    (is (= (sut/search engine "solar cap" {:display :offsets})
           [[:doc4 [["cap" [51]]]]]))
    (l/close-kv lmdb)))

(deftest index-writer-test
  (let [lmdb   (l/open-kv (u/tmp-dir (str "search-" (UUID/randomUUID))))
        writer ^IndexWriter (sut/search-index-writer lmdb)]
    (add-docs sut/write writer)
    (sut/commit writer)

    (let [engine (sut/new-search-engine lmdb)]
      (is (= (sut/search engine "cap" {:display :offsets})
             [[:doc4 [["cap" [51]]]]])))
    (l/close-kv lmdb)))

(deftest search-kv-test
  (let [lmdb   (l/open-kv (u/tmp-dir (str "search-kv-" (UUID/randomUUID))))
        engine (sut/new-search-engine lmdb)]
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
    (is (= [1 2 3] (sut/doc-refs engine)))

    (is (= (sut/search engine "lazy") [1]))
    (is (= (sut/search engine "red" ) [1 2]))
    (is (= (sut/search engine "red" {:display :offsets})
           [[1 [["red" [10 39]]]] [2 [["red" [40]]]]]))
    (testing "update"
      (sut/add-doc engine 1 "The quick fox jumped over the lazy dogs.")
      (is (= (sut/search engine "red" ) [2])))
    (l/close-kv lmdb)))
