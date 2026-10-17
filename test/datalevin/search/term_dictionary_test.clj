(ns datalevin.search.term-dictionary-test
  (:require [clojure.test :refer [deftest is testing]])
  (:import [datalevin.search TermDictionary TermDictionary$Match TermDictionary$Budget
            TermDictionary$WorkLimitException TermDictionary$ExpansionLimitException]
           [java.util.function IntPredicate]))

(def ^:private ^IntPredicate live (reify IntPredicate (test [_ _] true)))

(defn- matches [xs]
  (mapv (fn [^TermDictionary$Match match]
          [(.-termId match) (.-distance match)]) xs))

(deftest radix-trie-prefix-and-fuzzy-smoke-test
  (let [trie (TermDictionary.)]
    (doseq [[id term] (map-indexed vector ["hello" "help" "helmet" "world" "he" "hell"])]
      (.put trie term id))
    (is (= [[5 0] [0 0] [2 0] [1 0]] (matches (.prefix trie "hel" 10000 live))))
    (is (= [[5 1] [0 1] [1 1]] (matches (.fuzzy trie "helo" 1 true 50 10000 live))))
    (is (= [[0 1]] (matches (.fuzzy trie "hlelo" 1 true 50 10000 live))))
    (is (empty? (.fuzzy trie "hlelo" 1 false 50 10000 live)))
    (testing "dead terms do not consume the expansion limit"
      (let [only-help (reify IntPredicate (test [_ id] (= id 1)))]
        (is (= [[1 1]] (matches (.fuzzy trie "helo" 1 true 1 10000 only-help))))))
    (is (thrown? TermDictionary$WorkLimitException (.prefix trie "hel" 1 live)))
    (.clear trie)
    (is (empty? (.prefix trie "hel" 10000 live)))))

(deftest unicode-codepoint-transitions-across-compressed-edges
  (let [trie (TermDictionary.)]
    (doseq [[id term] (map-indexed vector ["café" "cafe" "caf😀" "caféine"])]
      (.put trie term id))
    (is (= [[0 0] [3 0]] (matches (.prefix trie "café" 10000 live))))
    (is (= #{[0 1] [1 1] [2 1]}
           (set (matches (.fuzzy trie "caf😁" 1 true 50 10000 live)))))))

(deftest prefix-cap-counts-live-terms-and-stops-on-overflow
  (let [trie (TermDictionary.)
        calls (atom 0)
        ^IntPredicate counting-live (reify IntPredicate
                                      (test [_ _] (swap! calls inc) true))]
    (doseq [id (range 100)] (.put trie (str "prefix" id) id))
    (is (thrown-with-msg? TermDictionary$ExpansionLimitException
                          #"max-prefix-expansions: 2"
                          (.prefix trie "prefix" 2 (TermDictionary$Budget. 10000)
                                   counting-live)))
    (is (= 3 @calls))
    (let [^IntPredicate only-one (reify IntPredicate (test [_ id] (= id 99)))]
      (is (= [[99 0]]
             (matches (.prefix trie "prefix" 1 (TermDictionary$Budget. 10000) only-one)))))
    (is (= 100 (count (.prefix trie "prefix" 100 (TermDictionary$Budget. 10000) live))))
    (is (thrown? TermDictionary$WorkLimitException
                 (.prefix trie "prefix" 100 (TermDictionary$Budget. 1) live)))))
