(ns datalevin.search.query-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.search.query :as query])
  (:import [clojure.lang ExceptionInfo]))

(defn- analyzer [text] [[text 0 0]])

(deftest invalid-expanded-leaves-identify-their-kind-and-value
  (doseq [kind [:fuzzy :prefix]
          value [false nil {} true 42 []]
          :let [leaf {kind value}]
          expression [leaf [:and "hello" leaf]]]
    (let [error (try
                  (query/parse-groups analyzer expression nil nil)
                  nil
                  (catch ExceptionInfo e e))]
      (is (instance? ExceptionInfo error) (pr-str expression))
      (when error
        (is (= (str "Invalid " kind " search leaf: expected a string value")
               (.getMessage error)))
        (is (= {:query leaf :leaf kind :value value} (ex-data error))))))
  (testing "leaf validation precedes fuzzy option validation"
    (is (thrown-with-msg? ExceptionInfo #"Invalid :fuzzy search leaf"
                         (query/parse-groups analyzer
                                             {:fuzzy false :max-edits 3} nil nil)))))

(deftest matching-options-and-string-leaves-keep-their-semantics
  (is (nil? (query/fuzzy-options false)))
  (is (nil? (query/fuzzy-options nil)))
  (is (= query/default-fuzzy-options (query/fuzzy-options {})))
  (doseq [[fuzzy prefix expected-mode] [[true nil :fuzzy] [{} nil :fuzzy]
                                       [false nil :exact] [nil true :prefix]
                                       [nil false :exact]]]
    (is (= [expected-mode]
           (mapv :mode (vals (:matching-groups
                              (query/parse-groups analyzer "hello" fuzzy prefix)))))))
  (doseq [kind [:fuzzy :prefix]]
    (is (= [{:mode kind :term "hello"}]
           (mapv #(select-keys % [:mode :term])
                 (vals (:matching-groups
                         (query/parse-groups analyzer {kind "hello"} nil nil))))))
    (is (nil? (query/parse-groups analyzer {kind ""} nil nil)))))

(deftest fuzzy-options-reject-unknown-keys
  (doseq [opts [{:bogus 1} {:max-edit 2} {:max-edits 2 :bogus 1}
               {:max-edit 1 :bogus 2} {"max-edits" 2} {42 true}]
          :let [unknown (vec (sort-by pr-str (remove (set (keys query/default-fuzzy-options))
                                                    (keys opts))))]
          parse [#(query/fuzzy-options opts)
                 #(query/parse-groups analyzer "helo" opts nil)
                 #(query/parse-groups analyzer (assoc opts :fuzzy "helo") nil nil)
                 #(query/parse-groups analyzer [:and "hello" (assoc opts :fuzzy "helo")]
                                      {:max-edits 2} nil)]]
    (let [error (try (parse) nil (catch ExceptionInfo e e))]
      (is (instance? ExceptionInfo error) (pr-str opts))
      (when error
        (is (re-find #"Unknown fuzzy option keys:" (.getMessage error)))
        (is (= unknown (:unknown-keys (ex-data error))))
        (is (= (set (keys query/default-fuzzy-options))
               (set (:allowed-keys (ex-data error))))))))
  (testing "recognized settings and leaf overrides remain valid"
    (is (= {:max-edits 2 :transpositions? false :max-expansions 12}
           (query/fuzzy-options {:max-edits 2 :transpositions? false :max-expansions 12})))
    (is (= [{:mode :fuzzy :term "helo" :max-edits 1 :transpositions? false :max-expansions 12}]
           (vals (:matching-groups
                   (query/parse-groups analyzer {:fuzzy "helo" :max-edits 1}
                                       {:max-edits 2 :transpositions? false :max-expansions 12}
                                       nil)))))))
