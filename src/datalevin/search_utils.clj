(ns datalevin.search-utils
  "Some useful utility functions that can be passed as options to search
  engine to customize search"
  (:require [clojure.string :as str]
            [datalevin.interpret :as i]
            [datalevin.constants :as c])
  (:import [java.text Normalizer Normalizer$Form]))

(defn create-analyzer
  "Creates an analyzer fn ready for use in search.

  `opts` have the following keys:

  * `:tokenizer` is a tokenizing fn that takes a string and returns a seq of
  [term, position, offset], where term is a word, position is the sequence
  number of the term, and offset is the character offset of this term.
  `create-regexp-tokenizer` produces such fn.

  * `:token-filters` is an ordered list of token filters. A token filter
  receives a token [term, position, offset] and returns a transformed list of
  tokens to replace it with."
  [{:keys [tokenizer token-filters]}]
  (i/inter-fn
    [s]
    (let [tokens     (tokenizer s)
          filters-tx (apply comp (map #(mapcat %) token-filters))]
      (sequence filters-tx tokens))))

(def lower-case-token-filter
  "This token filter converts tokens to lower case."
  (i/inter-fn [t] [(update t 0 (fn [s] (str/lower-case s)))]))

(def unaccent-token-filter
  "This token filter removes accents and diacritics from tokens."
  (i/inter-fn
    [t]
    [(update t 0 (fn [s] (-> (java.text.Normalizer/normalize
                              s java.text.Normalizer$Form/NFD)
                            (str/replace #"[^\p{ASCII}]", ""))))]))

(def en-stop-words-token-filter
  "This token filter removes \"empty\" tokens (for english language)."
  (i/inter-fn [t] (if (c/en-stop-words? (first t)) [] [t])))

(def prefix-token-filter
  "Produces a series of every possible prefixes in a token and replace it with them.

  For example: vault -> v, va, vau, vaul, vault

  This is useful for producing efficient autocomplete engines, provided this
  filter is NOT applied at query time."
  (i/inter-fn [[^String word pos start]]
              (for [idx (range 1 (inc (.length word)))]
                [(subs word 0 idx) pos start])))

(defn create-ngram-token-filter
  "Produces character ngrams between min and max size from the token and returns
  everything as tokens. This is useful for producing efficient fuzzy search."
  ([^long min-gram-size ^long max-gram-size]
   (i/inter-fn
     [[^String word pos start]]
     (let [length (.length word)]
       (loop [idx       0
              gram-size min-gram-size
              ngrams    (transient [])]
         (if (or (= idx length) (< length (+ idx gram-size)))
           (persistent! ngrams)
           (if-not (< gram-size max-gram-size)
             (recur (inc idx) min-gram-size
                    (conj! ngrams
                           [(subs word idx (min (+ idx gram-size) length))
                            pos start]))
             (recur idx (inc gram-size)
                    (conj! ngrams
                           [(subs word idx (min (+ idx gram-size) length))
                            pos start]))))))))
  ([gram-size] (create-ngram-token-filter gram-size gram-size)))

(defn create-min-length-token-filter
  "Filters tokens that are strictly shorter than `min-length`."
  [^long min-length]
  (i/inter-fn
    [[^String word _ _ :as t]]
    (if (< (.length word) min-length) [] [t])))

(defn create-max-length-token-filter
  "Filters tokens that are strictly longer than `max-length`."
  [^long max-length]
  (i/inter-fn
    [[^String word _ _ :as t]]
    (if (> (.length word) max-length) [] [t])))

(defn create-regexp-tokenizer
  "Creates a tokenizer that splits the given text on the pattern given as
  argument, and returns valid tokens."
  [pat]
  (i/inter-fn
    [^String s]
    (let [matcher    (re-matcher pat s)
          res        (volatile! [])
          string-end (.length s)]
      (loop [pos                0
             last-separator-end 0]
        (if (.find matcher)
          (let [match-start (.start matcher)
                match-end   (.end matcher)
                token       (subs s last-separator-end match-start)]
            (vswap! res conj [token pos last-separator-end])
            (recur (inc pos) match-end))
          (when (not= last-separator-end string-end)
            (let [token (subs s last-separator-end string-end)]
              (vswap! res conj [token pos last-separator-end])))))
      @res)))
