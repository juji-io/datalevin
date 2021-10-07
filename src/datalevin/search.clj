(ns datalevin.search
  "Fuzzy full-text search engine"
  (:require [clojure.string :as s]
            [datalevin.lmdb :as l]
            [datalevin.util :as u]
            [datalevin.constants :as c]
            [datalevin.bits :as b]
            [datalevin.constants :as c])
  (:import [datalevin.sm SymSpell Bigram]
           [datalevin.lmdb ILMDB]
           [java.util HashMap]
           [java.lang.reflect Field]))

(if (u/graal?)
  (require 'datalevin.binding.graal)
  (require 'datalevin.binding.java))

(defn analyzer
  "Datalevin analyzer does the following:
  - lower-case all words
  - split on non-word characters (except `-`) and remove them
  - remove stop words
  Return a vector of [token, position, offset]"
  [^String x]
  (let [x     (s/lower-case x)
        len   (.length x)
        len-1 (dec len)]
    (loop [i     0
           res   (transient [])
           pos   0
           in?   false
           start 0]
      (if (< i len)
        (let [c (.charAt x i)]
          (if (or (Character/isWhitespace c) (c/punctuations c))
            (if in?
              (let [word (subs x start i)]
                (if (c/english-stop-words word)
                  (recur (inc i) res (inc pos) false i)
                  (do
                    (conj! res [word pos start])
                    (recur (inc i) res (inc pos) false i))))
              (recur (inc i) res pos false i))
            (if in?
              (recur (inc i) res pos true start)
              (recur (inc i) res pos true i))))
        (persistent!
          (let [c (.charAt x len-1)]
            (if (or (Character/isWhitespace c) (c/punctuations c))
              res
              (let [word (subs x start i)]
                (if (c/english-stop-words word)
                  res
                  (conj! res [word pos start]))))))))))

(defprotocol ISearchEngine
  (add-doc [this doc-ref doc-text]
    "Add a document to the search engine, `doc-ref` can be arbitrary data that
     uniquely refers to the document in the system, `doc-text` is the content of
     the document as a string. Return `doc-id`, a long.")
  (remove-doc [this doc-id]
    "Remove a document, `doc-id` is a long, as returned by `add-doc`.")
  (search [this query]
    "Issue a `query` to the search engine. `query` is a map.
     Return a lazy sequence of `doc-ref`, ordered by relevance to the query."))

(deftype SearchEngine [lmdb
                       ^SymSpell symspell
                       ^:volatile-mutable max-doc
                       ^:volatile-mutable max-term]
  ISearchEngine
  (add-doc [this doc-ref doc-text]))

(defn new-engine
  [lmdb]
  )

(comment

  (def unigrams {"hello" 49 "world" 30})
  (def bigrams {(Bigram. "hello" "world") 30})

  (def sm (time (SymSpell. unigrams bigrams 2 7)))

  (.getDeletes sm)

  (.addBigrams sm {(Bigram. "hello" "world") 3})

  (.getBigramLexicon sm)

  (.addUnigrams sm {"hello" 3 "datalevin" 1})

  (.getUnigramLexicon sm)


  (.lookupCompound sm "hell" 2 false)

  (def env (l/open-kv "/tmp/search6"))

  (def lst (l/open-inverted-list env "i" c/+id-bytes+))

  (l/put-list-items lst "a" [1 2 3 4] :string :id)
  (l/put-list-items lst "b" [5 6 7] :string :id)

  (l/list-count lst "a" :string)
  (l/list-count lst "b" :string)

  (l/del-list-items lst "a" :string)

  (l/in-list? lst "b" 7 :string :id)

  (l/get-list lst "a" :string :id)

  (l/close-kv env)

  )
