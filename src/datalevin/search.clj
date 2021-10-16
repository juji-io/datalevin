(ns datalevin.search
  "Fuzzy full-text search engine"
  (:require [datalevin.lmdb :as l]
            [datalevin.util :as u]
            [datalevin.constants :as c]
            [datalevin.bits :as b])
  (:import [datalevin.sm SymSpell Bigram SuggestItem]
           [java.util HashMap ArrayList HashSet]))

(if (u/graal?)
  (require 'datalevin.binding.graal)
  (require 'datalevin.binding.java))

(defn en-analyzer
  "English analyzer does the following:
  - split on white space and punctuation, remove them
  - lower-case all characters
  - remove stop words
  Return a list of [term, position, offset]"
  [^String x]
  (let [len   (.length x)
        len-1 (dec len)
        res   (ArrayList. 256)]
    (loop [i     0
           pos   0
           in?   false
           start 0
           sb    (StringBuilder.)]
      (if (< i len)
        (let [c (.charAt x i)]
          (if (or (Character/isWhitespace c) (c/en-punctuations c))
            (if in?
              (let [word (.toString sb)]
                (when-not (c/en-stop-words word)
                  (.add res [word pos start]))
                (recur (inc i) (inc pos) false i (StringBuilder.)))
              (recur (inc i) pos false i sb))
            (recur (inc i) pos true (if in? start i)
                   (.append sb (Character/toLowerCase c)))))
        (let [c (.charAt x len-1)]
          (if (or (Character/isWhitespace c) (c/en-punctuations c))
            res
            (let [word (.toString sb)]
              (when-not (c/en-stop-words word)
                (.add res [word pos start]))
              res)))))))

(defprotocol ISearchEngine
  (add-doc [this doc-ref doc-text]
    "Add a document to the search engine, `doc-ref` can be arbitrary less than
     511 bytes of data that uniquely refers to the document in the system.
     `doc-text` is the content of the document as a string. The search engine
     does not store the original text, and assumes that caller can retrieve them
     by `doc-ref`.")
  (remove-doc [this doc-ref]
    "Remove a document referred to by `doc-ref`. After this function, the
     remaining traces of this document is its effects on the term frequencies.")
  (search [this query]
    "Issue a `query` to the search engine. `query` is a map or a string.

     Return a lazy sequence of `[doc-ref [term offset1 offset2 ...] ...]`,
     ordered by relevance to the query. `term` and `offset` can be used to
     highlight the matched terms and their locations in the documents."))

(defn- collect-terms
  [result]
  (let [terms (HashMap. 256)]
    (doseq [[term position offset] result]
      (.put terms term (if-let [[^long freq ^ArrayList lst] (.get terms term)]
                         [(inc freq) (do (.add lst [position offset]) lst)]
                         [1 (doto (ArrayList. 256) (.add [position offset]))])))
    terms))

(defn- collect-bigrams
  [result]
  (let [bigrams (HashMap. 256)]
    (doseq [[[t1 p1 _] [t2 p2 _]] (partition 2 1 result)]
      (when (= (inc ^long p1) p2)
        (.put bigrams [t1 t2] (if-let [^long freq (.get bigrams [t1 t2])]
                                (inc freq)
                                1))))
    bigrams))

(defn- idf
  "inverse document frequency of a term"
  [lmdb term-id ^long N]
  (Math/log10 (/ N ^long (l/list-count lmdb c/term-docs term-id :id))))

(defn- tf*
  "log-weighted term frequency"
  [^long freq]
  (+ (Math/log10 freq) 1))

(defn- tf
  [lmdb doc-id term-id]
  (let [freq (l/list-count lmdb c/positions [doc-id term-id] :double-id)]
    (if (zero? ^long freq)
      0
      (tf* freq))))

(defn- hydrate-query-terms
  [max-doc ^HashMap unigrams lmdb ^SymSpell symspell tokens]
  (let [sis   (.getSuggestTerms symspell tokens c/dict-max-edit-distance false)
        terms (mapv #(.getSuggestion ^SuggestItem %) sis)
        eds   (zipmap terms (map #(.getEditDistance ^SuggestItem %) sis))]
    (println "terms" terms)
    (into {}
          (map (fn [[term freq]]
                 (println term freq)
                 (let [id  (first (.get unigrams term))
                       idf (idf lmdb id max-doc)]
                   [term
                    {:id  id
                     :ed  (eds term)
                     :idf idf
                     :wq  (* ^double (tf* freq) ^double idf)}]))
               (frequencies terms)))))

(defn- add-candidates
  [lmdb tid ^HashSet taken ^HashMap candid ^HashMap cache]
  (doseq [did (l/get-list lmdb c/term-docs tid :id :id)]
    (when-not (.contains taken did)
      (if-let [seen (.get candid did)]
        (.put candid did (if (.containsKey cache [tid did])
                           seen
                           (inc ^long seen)))
        (.put candid did 1)))))

(defn- check-doc
  [^HashMap cache kid did lmdb ^HashMap candid]
  (when (if (.containsKey cache [kid did])
          (.get cache [kid did])
          (let [in? (l/in-list? lmdb c/term-docs kid did :id :id)]
            (.put cache [kid did] in?)
            in?))
    (.put candid did (inc ^long (.get candid did)))))

(defn- filter-candidates
  [i n term-ids ^HashMap candid cache lmdb
   ^HashSet taken ^HashSet result ^HashMap backup]
  (let [tao (- ^long n ^long i)] ; target number of overlaps
    (if (= tao 1)
      (.addAll result (.keySet candid))
      (doseq [^long k (range (inc ^long i) n)
              :let    [kid (nth term-ids k)]]
        (doseq [did (.keySet candid)]
          (check-doc cache kid did lmdb candid)
          (let [hits ^long (.get candid did)]
            (cond
              (<= tao hits) (do (.add taken did)
                                (.add result did)
                                (.remove candid did)
                                (.remove backup did))
              (< (+ hits ^long (- ^long n k 1)) tao)
              (do (.remove candid did)
                  (.put backup did hits)))))))))

(defn- select-docs
  [n i ^HashMap backup lmdb tid ^HashSet taken term-ids ^HashMap cache]
  (let [candid (HashMap. backup)]
    (add-candidates lmdb tid taken candid cache)
    (let [result (HashSet. 32)]
      (filter-candidates i n term-ids candid cache lmdb
                         taken result backup)
      result)))

(defn- doc-info
  [lmdb doc-id]
  (l/get-value lmdb c/docs doc-id :id :data true))

(defn- score-docs
  [lmdb wqs docs]
  (reduce
    (fn [scores did]
      (let [{:keys [ref uniq]} (doc-info lmdb did)]
        (println "uniq" uniq)
        (conj! scores
               [(let [sum   (reduce
                              +
                              (mapv (fn [[tid wq]]
                                      (println tid "wq" wq)
                                      (let [res (* ^double (tf lmdb did tid) ^double wq)]
                                        (println "res" res)
                                        res))
                                    wqs))
                      _     (println "sum" sum)
                      score (/ ^double sum
                               ^long uniq)]
                  (println did "score" score)
                  score)
                did ref])))
    (transient [])
    docs))

(defn- rank-docs
  "return sorted [score doc-id doc-ref]"
  [lmdb wqs docs]
  (->> docs
       (score-docs lmdb wqs)
       persistent!
       (sort-by first >)))

(deftype SearchEngine [lmdb
                       ^HashMap unigrams ; term -> [term-id,freq]
                       ^HashMap bigrams  ; [term-id,term-id] -> freq
                       ^HashMap terms    ; term-id -> term
                       ^SymSpell symspell
                       ^:volatile-mutable ^long max-doc
                       ^:volatile-mutable ^long max-term]
  ISearchEngine
  (add-doc [this doc-ref doc-text]
    (let [result (en-analyzer doc-text)
          unique (count (set (map first result)))
          txs    (ArrayList. 256)]
      (locking this
        (let [doc-id (inc max-doc)]
          (set! max-doc doc-id)
          (.add txs [:put c/docs doc-id {:ref doc-ref :uniq unique}
                     :id :data [:append]])
          (.add txs [:put c/rdocs doc-ref doc-id :data :id])
          (doseq [[term [^long new-freq new-lst]] (collect-terms result)]
            (let [[term-id freq] (if (.containsKey unigrams term)
                                   (let [[t f] (.get unigrams term)]
                                     [t (+ ^long f new-freq)])
                                   (let [t (inc max-term)]
                                     (set! max-term t)
                                     (.put terms t term)
                                     [t new-freq]))]
              (.addUnigrams symspell {term new-freq})
              (.put unigrams term [term-id freq])
              (.add txs [:put c/unigrams term [term-id freq]
                         :string :double-id])
              (.add txs [:put c/term-docs term-id doc-id :id :id])
              (doseq [po new-lst]
                (.add txs [:put c/positions [doc-id term-id] po
                           :double-id :double-int]))))
          (doseq [[[t1 t2] ^long new-freq] (collect-bigrams result)]
            (let [tids [(first (.get unigrams t1)) (first (.get unigrams t2))]
                  freq (if (.containsKey bigrams tids)
                         (+ ^long (.get bigrams tids) new-freq)
                         new-freq)]
              (.addBigrams symspell {(Bigram. t1 t2) new-freq})
              (.put bigrams tids freq)
              (.add txs [:put c/bigrams tids freq :double-id :id]))))
        (l/transact-kv lmdb txs))))

  (remove-doc [this doc-ref]
    (if-let [doc-id (l/get-value lmdb c/rdocs doc-ref :data :id true)]
      (locking this
        (let [txs (ArrayList. 256)]
          (.add txs [:del c/docs doc-id :id])
          (.add txs [:del c/rdocs doc-ref :data])
          (l/transact-kv lmdb txs))
        (doseq [[[_ term-id] _] (l/get-range
                                  lmdb c/positions
                                  [:closed [doc-id 0] [doc-id Long/MAX_VALUE]]
                                  :double-id :ignore false)]
          (l/del-list-items lmdb c/term-docs term-id [doc-id] :id :id)
          (l/del-list-items lmdb c/positions [doc-id term-id] :double-id)))
      (u/raise "Document does not exist." {:doc-ref doc-ref})))

  (search [this query]
    (let [tokens   (->> (en-analyzer query)
                        (map first)
                        (into-array String))
          terms    (hydrate-query-terms max-doc unigrams lmdb symspell tokens)
          _        (println "terms" terms)
          term-ms  (vals terms)
          wqs      (into {} (map (fn [{:keys [id wq]}] [id wq]) term-ms))
          term-ids (->> term-ms
                        (sort-by :ed)
                        (sort-by :idf >)
                        (mapv :id))
          n        (count term-ids)
          xform
          (comp
            (let [backup (HashMap. 512)
                  taken  (HashSet. 128)
                  cache  (HashMap. 512)]
              (map-indexed
                (fn [^long i tid]
                  (select-docs n i backup lmdb tid taken term-ids cache))))
            (mapcat (fn [docs] (rank-docs lmdb wqs docs))))]
      (sequence
        (map identity)
        (sequence xform term-ids)))))

(defn- init-unigrams
  [lmdb]
  (let [unigrams (HashMap. 1024)
        terms    (HashMap. 1024)
        load     (fn [kv]
                   (let [term          (b/read-buffer (l/k kv) :string)
                         [id _ :as tf] (b/read-buffer (l/v kv) :double-id)]
                     (.put unigrams term tf)
                     (.put terms id term)))]
    (l/visit lmdb c/unigrams load [:all] :string)
    [unigrams terms]))

(defn- init-bigrams
  [lmdb]
  (let [m    (HashMap. 1024)
        load (fn [kv]
               (.put m (b/read-buffer (l/k kv) :double-id)
                     (b/read-buffer (l/v kv) :id)))]
    (l/visit lmdb c/bigrams load [:all])
    m))

(defn- init-max-doc
  [lmdb]
  (or (first (l/get-first lmdb c/docs [:all-back] :id :ignore)) 0))

(defn- init-max-term [lmdb]
  (or (first (l/get-first lmdb c/term-docs [:all-back] :id :ignore)) 0))

(defn new-engine
  [lmdb]
  (assert (not (l/closed-kv? lmdb)) "LMDB env is closed.")
  (l/open-dbi lmdb c/unigrams c/+max-key-size+ (* 2 c/+id-bytes+))
  (l/open-dbi lmdb c/bigrams (* 2 c/+id-bytes+) c/+id-bytes+)
  (l/open-dbi lmdb c/docs c/+id-bytes+)
  (l/open-dbi lmdb c/rdocs c/+max-key-size+ c/+id-bytes+)
  (l/open-inverted-list lmdb c/term-docs c/+id-bytes+ c/+id-bytes+)
  (l/open-inverted-list lmdb c/positions (* 2 c/+id-bytes+) c/+id-bytes+)
  (l/open-dbi lmdb c/search-meta c/+max-key-size+)
  (let [[unigrams ^HashMap terms] (init-unigrams lmdb)
        bigrams                   (init-bigrams lmdb)

        tf (into {} (map (fn [[t [_ f]]] [t f]) unigrams))
        bg (into {} (map (fn [[[t1 t2] f]]
                           [(Bigram. (.get terms t1) (.get terms t2)) f])
                         bigrams))]
    (->SearchEngine lmdb
                    unigrams
                    bigrams
                    terms
                    (SymSpell. tf bg c/dict-max-edit-distance
                               c/dict-prefix-length)
                    (init-max-doc lmdb)
                    (init-max-term lmdb))))

(comment

  (def env (l/open-kv "/tmp/search27"))

  (def engine (new-engine env))

  (search engine "robber fox lamb dogs ")

  (search engine "little lamb")

  (add-doc engine 0 "The quick red fox jumped over the lazy red dogs.")

  (add-doc engine 1 "Mary had a little lamb whose fleece was red as fire.")

  (add-doc engine 2 "The robber wore a red fleece jacket and a baseball cap. ")

  (add-doc engine 3 "Removes the entry for the specified key only if it is currently mapped to the specified value.")


  (search engine "entry value")

  (en-analyzer "what is it like a rad dog fire")

  (.-unigrams engine)
  (.-terms engine)
  (.-bigrams engine)

  (l/get-range env c/unigrams [:all] :string :double-id)
  (l/get-range env c/bigrams [:all] :double-id :id)
  (l/get-range env c/docs [:all] :id)
  (l/get-list env c/term-docs 1 :id :id)
  (l/list-count env c/term-docs 1 :id)
  (l/in-list? env c/term-docs 1 2 :id :id)

  (l/get-list env c/positions [1 1] :double-id :double-int)
  (l/list-count env c/positions [1 1] :double-id)

  (def unigrams {"hello" 49 "world" 30})
  (def bigrams {(Bigram. "hello" "world") 30})

  (def sm (time (SymSpell. unigrams bigrams 2 10)))

  (.getDeletes sm)

  (.addBigrams sm {(Bigram. "hello" "world") 3})

  (.getBigramLexicon sm)

  (.addUnigrams sm {"hello" 3 "datalevin" 1})

  (.getUnigramLexicon sm)


  (.lookupCompound sm "hell" 2 false)

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
