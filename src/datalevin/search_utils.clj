(ns datalevin.search-utils
  "Some useful utility functions that can be passed as options to search
  engine to customize search."
  (:require
   [datalevin.interpret :as i :refer [inter-fn]]
   [datalevin.analyzer :as a]))

(defn create-analyzer
  "Creates an analyzer fn ready for use in search.

  `opts` have the following keys:

  * `:tokenizer` is a tokenizing fn that takes a string and returns a seq of
  [term, position, offset], where term is a word, position is the sequence
  number of the term, and offset is the character offset of this term.
  e.g. `create-regexp-tokenizer` produces such fn.

  * `:token-filters` is an ordered list of token filters. A token filter
  receives a [term, position, offset] and returns a transformed list of
  tokens to replace it with."
  [opts]
  (inter-fn [s] ((a/create-analyzer opts) s)))

(def lower-case-token-filter
  "This token filter converts tokens to lower case."
  (inter-fn [t] (a/lower-case-token-filter t)))

(def unaccent-token-filter
  "This token filter removes accents and diacritics from tokens."
  (inter-fn [t] (a/unaccent-token-filter t)))

(defn create-stop-words-token-filter
  "Takes a stop words predicate that returns `true` when the given token is
  a stop word"
  [stop-word-pred]
  (inter-fn [t] ((a/create-stop-words-token-filter stop-word-pred) t)))

(def en-stop-words-token-filter
  "This token filter removes \"empty\" tokens (for english language)."
  (inter-fn [t] (a/en-stop-words-token-filter t)))

(def prefix-token-filter
  "Produces a series of every possible prefixes in a token and replace it with them. For example: vault -> v, va, vau, vaul, vault

  Takes a vector `[word position start-offset]`.

  This is useful for producing efficient autocomplete engines, provided this
  filter is NOT applied at query time."
  (inter-fn [v] (a/prefix-token-filter v)))

(defn create-ngram-token-filter
  "Produces character ngrams between min and max size from the token and returns
  everything as tokens. This is useful for producing efficient fuzzy search."
  ([min-gram-size max-gram-size]
   (inter-fn [v]
     ((a/create-ngram-token-filter min-gram-size max-gram-size) v)))
  ([gram-size] (create-ngram-token-filter gram-size gram-size)))

(defn create-min-length-token-filter
  "Filters tokens that are strictly shorter than `min-length`."
  [min-length]
  (inter-fn [v] ((a/create-min-length-token-filter min-length) v)))

(defn create-max-length-token-filter
  "Filters tokens that are strictly longer than `max-length`."
  [max-length]
  (inter-fn [v] ((a/create-max-length-token-filter max-length) v)))

(defn create-stemming-token-filter
  "Create a token filter that replaces tokens with their stems.

  The stemming algorithm is Snowball https://snowballstem.org/

  `language` is a string, its value can be one of the following:

  arabic, armenian, basque, catalan, danish, dutch, english, french,
  finnish, german, greek, hindi, hungarian, indonesian, irish, italian,
  lithuanian, nepali, norwegian, portuguese, romanian, russian, serbian,
  swedish, tamil, turkish, spanish, yiddish, and porter"
  [^String language]
  (inter-fn [v] ((a/create-stemming-token-filter language) v)))

(defn create-regexp-tokenizer
  "Creates a tokenizer that splits text on the given regular expression
  `pat`."
  [pat]
  (inter-fn [^String s] ((a/create-regexp-tokenizer pat) s)))
