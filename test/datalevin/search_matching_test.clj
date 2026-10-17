(ns datalevin.search-matching-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [datalevin.core :as d]
            [datalevin.test.core :refer [db-fixture]]
            [datalevin.util :as u])
  (:import [clojure.lang ExceptionInfo]
           [datalevin.search TermDictionary$ExpansionLimitException
            TermDictionary$WorkLimitException]))

(use-fixtures :each db-fixture)

(deftest fulltext-term-matching-smoke-test
  (let [dir (u/tmp-dir (str "search-matching-" (random-uuid)))
        kv (d/open-kv dir)
        engine (d/new-search-engine kv)]
    (try
      (d/add-doc engine 1 "hello database")
      (d/add-doc engine 2 "help database")
      (d/add-doc engine 3 "world")
      (is (empty? (d/search engine "helo")))
      (is (= #{1 2} (set (d/search engine "helo" {:fuzzy true}))))
      (is (= #{1 2} (set (d/search engine {:prefix "hel"}))))
      (is (= #{1 2} (set (d/search engine
                                   [:and {:fuzzy "helo"} {:term "database"}]))))
      (doseq [leaf [{:fuzzy false} {:fuzzy nil} {:fuzzy {}}]]
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Invalid :fuzzy search leaf"
                             (d/search engine leaf))))
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Invalid :prefix search leaf"
                           (d/search engine {:prefix false})))
      (doseq [opts [{:bogus 1} {:max-edit 2}]]
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Unknown fuzzy option keys:"
                             (d/search engine "helo" {:fuzzy opts})))
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Unknown fuzzy option keys:"
                             (d/search engine (assoc opts :fuzzy "helo")))))
      (is (thrown? TermDictionary$ExpansionLimitException
                   (d/search engine {:prefix "hel"} {:max-prefix-expansions 1})))
      (is (thrown? TermDictionary$ExpansionLimitException
                   (d/search engine "hel" {:prefix true :max-prefix-expansions 1})))
      (is (= #{1 2} (set (d/search engine {:prefix "hel"} {:max-prefix-expansions 2}))))
      (is (= #{1 2 3} (set (d/search engine [:or {:prefix "hel"} {:prefix "wor"}]
                                   {:max-prefix-expansions 2}))))
      (is (= #{1 2} (set (d/search engine "helo" {:fuzzy true :max-prefix-expansions 1}))))
      (let [limited (d/new-search-engine kv {:search-opts {:max-prefix-expansions 1}})]
        (is (thrown? TermDictionary$ExpansionLimitException (d/search limited {:prefix "hel"})))
        (is (= #{1 2} (set (d/search limited {:prefix "hel"} {:max-prefix-expansions nil})))))
      (doseq [cap [0 -1 1.5 false 2147483648]]
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #":max-prefix-expansions must be"
                             (d/search engine {:prefix "hel"} {:max-prefix-expansions cap}))))
      (finally (d/close-kv kv) (u/delete-files dir)))))

(deftest traversal-budget-errors-include-original-query-and-effective-limit
  (let [dir (u/tmp-dir (str "search-budget-context-" (random-uuid)))
        kv (d/open-kv dir)
        engine (d/new-search-engine kv {:search-opts {:max-term-visits 1}})]
    (try
      (d/add-doc engine 1 "hello database")
      (doseq [[query opts] [[{:prefix "hel"} {}]
                            [{:fuzzy "helo"} {:max-term-visits 2}]
                            ["helo" {:fuzzy true}]
                            ["hel" {:prefix true}]
                            [[:and {:fuzzy "helo"} {:term "database"}] {}]]]
        (let [error (try (d/search engine query opts) nil (catch ExceptionInfo e e))]
          (is (instance? ExceptionInfo error))
          (when error
            (is (re-find #"max-term-visits" (ex-message error)))
            (is (= {:error :search/max-term-visits-exceeded
                    :query query :max-term-visits (get opts :max-term-visits 1)}
                   (ex-data error)))
            (is (instance? TermDictionary$WorkLimitException (ex-cause error))))))
      (is (= [1] (vec (d/search engine {:prefix "hel"} {:max-term-visits 10000}))))
      (finally (d/close-kv kv) (u/delete-files dir)))))

(deftest bulk-commit-publishes-to-existing-engine
  (let [dir (u/tmp-dir (str "search-bulk-matching-" (random-uuid)))
        kv (d/open-kv dir)
        engine (d/new-search-engine kv)]
    (try
      (d/add-doc engine 1 "hello database")
      ;; Warm caches before the writer changes existing postings and adds terms.
      (is (= [1] (vec (d/search engine "hello"))))
      (is (= [1] (vec (d/search engine {:prefix "hel"}))))
      (let [writer (d/search-index-writer kv)]
        (d/write writer 2 "hello world")
        (d/write writer 3 "help world")
        (is (= :transacted (d/commit writer))))
      (is (= #{1 2} (set (d/search engine "hello"))))
      (is (= #{1 2 3} (set (d/search engine {:fuzzy "helo"}))))
      (is (= #{1 2 3} (set (d/search engine {:prefix "hel"}))))
      (is (= 3 (d/doc-count engine)))
      ;; Its allocators must advance too, so a later regular write is safe.
      (d/add-doc engine 4 "helmet")
      (is (= #{1 2 3 4} (set (d/search engine {:prefix "hel"}))))
      (finally (d/close-kv kv) (u/delete-files dir)))))
