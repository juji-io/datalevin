(ns ^:no-doc datalevin.analyzer
  "See `datalevin.search-utils` for public API"
  (:require
   [clojure.string :as str]
   [datalevin.stem :as s]
   [datalevin.constants :as c])
  (:import
   [java.util HashSet]
   [java.text Normalizer Normalizer$Form]
   [org.eclipse.collections.impl.list.mutable FastList]
   [org.tartarus.snowball SnowballStemmer]))

(defn en-stop-words?
  "return true if the given word is an English stop words"
  [w]
  (.contains ^HashSet c/*en-stop-words-set* w))

(defn en-punctuations?
  "return true if the given character is an English punctuation"
  [c]
  (.contains ^HashSet c/*en-punctuations-set* c))

(defn- non-token-char?
  [^Character c]
  (or (Character/isWhitespace c) (en-punctuations? c)))

(defn en-analyzer
  "English analyzer (tokenizer) does the following:

  - split on white space and punctuation, remove them
  - lower-case all characters
  - remove stop words

  Return a list of [term, position, offset].

  This is the default analyzer of search engine when none is provided."
  [^String x]
  (let [len   (.length x)
        len-1 (dec len)
        res   (FastList.)]
    (loop [i     0
           pos   0
           in?   false
           start 0
           sb    (StringBuilder.)]
      (if (< i len)
        (let [c (.charAt x i)]
          (if (non-token-char? c)
            (if in?
              (let [word (.toString sb)]
                (when-not (en-stop-words? word)
                  (.add res [word pos start]))
                (recur (inc i) (inc pos) false i (StringBuilder.)))
              (recur (inc i) pos false i sb))
            (recur (inc i) pos true (if in? start i)
                   (.append sb (Character/toLowerCase c)))))
        (let [c (.charAt x len-1)]
          (if (non-token-char? c)
            res
            (let [word (.toString sb)]
              (when-not (en-stop-words? word)
                (.add res [word pos start]))
              res)))))))

(defn create-analyzer
  [{:keys [tokenizer token-filters]
    :or   {tokenizer     en-analyzer
           token-filters []}}]
  (fn [s]
    (let [tokens     (tokenizer s)
          filters-tx (apply comp (map #(mapcat %) token-filters))]
      (sequence filters-tx tokens))))

(defn lower-case-token-filter [t] [(update t 0 (fn [s] (str/lower-case s)))])

(defn unaccent-token-filter
  [t]
  [(update t 0 (fn [s]
                 (-> (Normalizer/normalize s Normalizer$Form/NFD)
                     (str/replace #"[^\p{ASCII}]", ""))))])

(defn create-stop-words-token-filter
  [stop-word-pred]
  (fn [t] (if (stop-word-pred (first t)) [] [t])))

(defn en-stop-words-token-filter [t] (if (en-stop-words? (first t)) [] [t]))

(defn prefix-token-filter
  [[^String word pos start]]
  (for [idx (range 1 (inc (.length word)))]
    [(subs word 0 idx) pos start]))

(defn create-ngram-token-filter
  [min-gram-size max-gram-size]
  (fn [[^String word pos start]]
    (let [length (.length word)]
      (loop [idx       0
             gram-size min-gram-size
             ngrams    (transient [])]
        (if (or (= idx length) (< length (+ idx ^long gram-size)))
          (persistent! ngrams)
          (if-not (< ^long gram-size ^long max-gram-size)
            (recur (inc idx) min-gram-size
                   (conj! ngrams
                          [(subs word idx (min (+ idx ^long gram-size) length))
                           pos start]))
            (recur idx (inc ^long gram-size)
                   (conj! ngrams
                          [(subs word idx (min (+ idx ^long gram-size) length))
                           pos start]))))))))

(defn create-min-length-token-filter
  [min-length]
  (fn [[^String word _ _ :as t]]
    (if (< (.length word) ^long min-length) [] [t])))

(defn create-max-length-token-filter
  [max-length]
  (fn [[^String word _ _ :as t]]
    (if (> (.length word) ^long max-length) [] [t])))

(defn create-stemming-token-filter
  [^String language]
  (fn [t]
    (let [^SnowballStemmer stemmer (s/get-stemmer language)]
      [(update t 0 (fn [s]
                     (.setCurrent stemmer s)
                     (.stem stemmer)
                     (.getCurrent stemmer)))])))

(defn create-regexp-tokenizer
  [pat]
  (fn [^String s]
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
