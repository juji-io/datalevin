(ns datalevin.search
  "Fuzzy full-text search engine"
  (:require [datalevin.lmdb :as l]
            [datalevin.util :as u]
            [datalevin.constants :as c]
            [datalevin.bits :as b])
  (:import [datalevin.sm SymSpell Bigram SuggestItem]
           [java.util HashMap ArrayList HashSet]
           [java.util.concurrent Executors]
           [java.io FileInputStream]
           [com.fasterxml.jackson.databind ObjectMapper]
           [com.fasterxml.jackson.core JsonFactory JsonParser JsonToken]))

(if (u/graal?)
  (require 'datalevin.binding.graal)
  (require 'datalevin.binding.java))

(defn- non-token-char?
  [^Character c]
  (or (Character/isWhitespace c) (c/en-punctuations c)))

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
          (if (non-token-char? c)
            (if in?
              (let [word (.toString sb)]
                (when-not (c/en-stop-words word)
                  (.add res [word pos start]))
                (recur (inc i) (inc pos) false i (StringBuilder.)))
              (recur (inc i) pos false i sb))
            (recur (inc i) pos true (if in? start i)
                   (.append sb (Character/toLowerCase c)))))
        (let [c (.charAt x len-1)]
          (if (non-token-char? c)
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

     Return a lazy sequence of
     `[doc-ref [term1 [offset ...]] [term2 [...]] ...]`,
     ordered by relevance to the query. `term` and `offset` can be used to
     highlight the matched terms and their locations in the documents."))

(defn- collect-terms
  [result]
  (let [terms (HashMap. 256)]
    (doseq [[term position offset] result]
      (.put terms term (if-let [[^long freq ^ArrayList lst] (.get terms term)]
                         [(inc freq) (do (.add lst [position offset]) lst)]
                         [1 (doto (ArrayList.) (.add [position offset]))])))
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
  (tf* (l/list-count lmdb c/positions [doc-id term-id] :id-id)))

(defn- hydrate-query-terms
  [max-doc ^HashMap unigrams lmdb ^SymSpell symspell tokens]
  (let [sis   (when symspell
                (.getSuggestTerms symspell tokens c/dict-max-edit-distance false))
        terms (if sis
                (mapv #(.getSuggestion ^SuggestItem %) sis)
                tokens)
        eds   (if sis
                (zipmap terms (mapv #(.getEditDistance ^SuggestItem %) sis))
                (zipmap terms (repeat 0)))]
    (mapv (fn [[term freq]]
            (let [id  (first (.get unigrams term))
                  idf (idf lmdb id max-doc)]
              {:id  id
               :ed  (eds term)
               :idf idf
               :wq  (* ^double (tf* freq) ^double idf)}))
          (frequencies terms))))

(defn- doc-info
  [lmdb doc-id]
  (l/get-value lmdb c/docs doc-id :id :data true))

(defn- inc-score
  [lmdb tid did wqs ^HashMap result]
  (let [dot ^double (* ^double (tf lmdb did tid) ^double (wqs tid))]
    (if-let [res (.get result did)]
      (.put result did (-> res
                           (update :score #(+ ^double % dot))
                           (update :tids conj tid)))
      (let [res (doc-info lmdb did)]
        (.put result did (assoc res :score dot :tids [tid]))))))

(defn- add-candidates
  [lmdb tid ^HashMap candid ^HashMap cache wqs ^HashMap result]
  (doseq [did (l/get-list lmdb c/term-docs tid :id :id)]
    (when-not (.containsKey result did)
      (if-let [seen (.get candid did)]
        (when-not (.containsKey cache [tid did])
          (inc-score lmdb tid did wqs result)
          (.put candid did (inc ^long seen)))
        (do (inc-score lmdb tid did wqs result)
            (.put candid did 1))))))

(defn- check-doc
  [^HashMap cache kid did lmdb ^HashMap candid wqs ^HashMap result]
  (when (if (.containsKey cache [kid did])
          (.get cache [kid did])
          (let [in? (l/in-list? lmdb c/term-docs kid did :id :id)]
            (.put cache [kid did] in?)
            (when in? (inc-score lmdb kid did wqs result))
            in?))
    (.put candid did (inc ^long (.get candid did)))))

(defn- filter-candidates
  [i n term-ids ^HashMap candid cache lmdb
   ^HashSet selected ^HashMap backup wqs ^HashMap result]
  (let [tao (- ^long n ^long i)] ; target number of overlaps
    (if (= tao 1)
      (.addAll selected (.keySet candid))
      (doseq [^long k (range (inc ^long i) n)
              :let    [kid (nth term-ids k)]]
        (doseq [did (.keySet candid)]
          (check-doc cache kid did lmdb candid wqs result)
          (let [hits ^long (.get candid did)]
            (cond
              (<= tao hits) (do (.add selected did)
                                (.remove candid did)
                                (.remove backup did))
              (< (+ hits ^long (- ^long n k 1)) tao)
              (do (.remove candid did)
                  (.put backup did hits)))))))))

(defn- rank-docs
  "return a list of [score doc-id doc-ref [term-ids]] sorted by score"
  [selected ^HashMap result]
  (->> selected
       (map (fn [did]
              (let [{:keys [ref uniq score tids]} (.get result did)]
                [(/ ^double score ^long uniq) did ref tids])))
       (sort-by first >)))

(defn- add-positions
  [lmdb ^HashMap terms [_ did ref tids]]
  [ref (mapv (fn [tid]
               [(.get terms tid)
                (mapv second
                      (l/get-list lmdb c/positions [did tid]
                                  :id-id :int-int))])
             tids)])

(deftype SearchEngine [lmdb
                       ^HashMap unigrams ; term -> [term-id,freq]
                       ^HashMap bigrams  ; [term-id,term-id] -> freq
                       ^HashMap terms    ; term-id -> term
                       ^SymSpell symspell
                       max-doc
                       max-term]
  ISearchEngine
  (add-doc [this doc-ref doc-text]
    (let [result      (en-analyzer doc-text)
          txs         (ArrayList. 512)
          new-terms   (collect-terms result)
          unique      (count new-terms)
          new-bigrams (when symspell (collect-bigrams result))]
      (when symspell
        (.addUnigrams symspell (zipmap (keys new-terms)
                                       (mapv first (vals new-terms))))
        (.addBigrams symspell (zipmap (mapv (fn [[t1 t2]] (Bigram. t1 t2))
                                            (keys new-bigrams))
                                      (vals new-bigrams))))
      (locking this
        (let [doc-id (inc ^long @max-doc)]
          (vreset! max-doc doc-id)
          (.add txs [:put c/docs doc-id {:ref doc-ref :uniq unique}
                     :id :data [:append]])
          (.add txs [:put c/rdocs doc-ref doc-id :data :id])
          (doseq [[term [^long new-freq new-lst]] new-terms]
            (let [[term-id _ :as tf]
                  (if-let [[t f] (.get unigrams term)]
                    [t (+ ^long f new-freq)]
                    (let [t (inc ^long @max-term)]
                      (vreset! max-term t)
                      (.put terms t term)
                      [t new-freq]))]
              (.put unigrams term tf)
              (.add txs [:put c/unigrams term tf :string :id-id])
              (.add txs [:put c/term-docs term-id doc-id :id :id])
              (doseq [po new-lst]
                (.add txs [:put c/positions [doc-id term-id] po
                           :id-id :int-int]))))
          (when symspell
            (doseq [[[t1 t2] ^long new-freq] new-bigrams]
              (let [tids [(first (.get unigrams t1)) (first (.get unigrams t2))]
                    freq (if (.containsKey bigrams tids)
                           (+ ^long (.get bigrams tids) new-freq)
                           new-freq)]
                (.put bigrams tids freq)
                (.add txs [:put c/bigrams tids freq :id-id :id])))))
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
                                  :id-id :ignore false)]
          (l/del-list-items lmdb c/term-docs term-id [doc-id] :id :id)
          (l/del-list-items lmdb c/positions [doc-id term-id] :id-id)))
      (u/raise "Document does not exist." {:doc-ref doc-ref})))

  (search [this query]
    (let [tokens   (->> (en-analyzer query)
                        (map first)
                        (into-array String))
          qterms   (hydrate-query-terms @max-doc unigrams lmdb symspell tokens)
          wqs      (into {} (map (fn [{:keys [id wq]}] [id wq]) qterms))
          term-ids (->> qterms
                        (sort-by :ed)
                        (sort-by :idf >)
                        (mapv :id))
          n        (count term-ids)
          result   (HashMap. 512)
          xform
          (comp
            (let [backup (HashMap. 512)
                  cache  (HashMap. 512)]
              (map-indexed
                (fn [^long i tid]
                  (let [candid (HashMap. backup)]
                    (add-candidates lmdb tid candid cache wqs result)
                    (let [selected (HashSet. 128)]
                      (filter-candidates i n term-ids candid cache lmdb
                                         selected backup wqs result)
                      selected)))))
            (mapcat (fn [selected]
                      (rank-docs selected result)))
            (map (fn [doc-info]
                   (add-positions lmdb terms doc-info))))]
      (sequence xform term-ids))))

(defn- init-unigrams
  [lmdb]
  (let [unigrams (HashMap. 32768)
        terms    (HashMap. 32768)
        load     (fn [kv]
                   (let [term          (b/read-buffer (l/k kv) :string)
                         [id _ :as tf] (b/read-buffer (l/v kv) :id-id)]
                     (.put unigrams term tf)
                     (.put terms id term)))]
    (l/visit lmdb c/unigrams load [:all] :string)
    [unigrams terms]))

(defn- init-bigrams
  [lmdb]
  (let [m    (HashMap. 32768)
        load (fn [kv]
               (.put m (b/read-buffer (l/k kv) :id-id)
                     (b/read-buffer (l/v kv) :id)))]
    (l/visit lmdb c/bigrams load [:all])
    m))

(defn- init-max-doc
  [lmdb]
  (or (first (l/get-first lmdb c/docs [:all-back] :id :ignore)) 0))

(defn- init-max-term [lmdb]
  (or (first (l/get-first lmdb c/term-docs [:all-back] :id :ignore)) 0))

(defn new-engine
  ([lmdb]
   (new-engine lmdb {}))
  ([lmdb {:keys [fuzzy?]
          :or   {fuzzy? false}}]
   (assert (not (l/closed-kv? lmdb)) "LMDB env is closed.")
   (l/open-dbi lmdb c/unigrams c/+max-key-size+ (* 2 c/+id-bytes+))
   (l/open-dbi lmdb c/docs c/+id-bytes+)
   (l/open-dbi lmdb c/rdocs c/+max-key-size+ c/+id-bytes+)
   (l/open-inverted-list lmdb c/term-docs c/+id-bytes+ c/+id-bytes+)
   (l/open-inverted-list lmdb c/positions (* 2 c/+id-bytes+) c/+id-bytes+)
   (when fuzzy?
     (l/open-dbi lmdb c/bigrams (* 2 c/+id-bytes+) c/+id-bytes+))
   (let [[unigrams ^HashMap terms] (init-unigrams lmdb)
         bigrams                   (when fuzzy? (init-bigrams lmdb))

         tf (when fuzzy? (into {} (map (fn [[t [_ f]]] [t f]) unigrams)))
         bg (when fuzzy?
              (into {} (map (fn [[[t1 t2] f]]
                              [(Bigram. (.get terms t1) (.get terms t2)) f])
                            bigrams)))]
     (->SearchEngine lmdb
                     unigrams
                     (when fuzzy? bigrams)
                     terms
                     (when fuzzy? (SymSpell. tf bg c/dict-max-edit-distance
                                             c/dict-prefix-length))
                     (volatile! (init-max-doc lmdb))
                     (volatile! (init-max-term lmdb))))))

(comment

  (def OM (ObjectMapper.))

  (def lst (ArrayList.))

  (with-open [f (FileInputStream. "search-bench/output.json")]
    (let [jf  (JsonFactory.)
          jp  (.createParser jf f)
          cls (Class/forName "java.util.HashMap")]
      (.setCodec jp OM)
      (.nextToken jp)
      (loop []
        (when (.hasCurrentToken jp)
          (let [m (.readValueAs jp cls)]
            (.add ^ArrayList lst m)
            (.nextToken jp)
            (recur))))))

  (.size lst)

  (let [lmdb   (l/open-kv "/tmp/wiki92")
        engine (new-engine lmdb)]
    (time (doseq [^HashMap m lst]
            (add-doc engine (.get m "url") (.get m "text"))))
    (l/close-kv lmdb))

  (let [lmdb   (l/open-kv "/tmp/wiki91")
        engine (time (new-engine lmdb))]
    (l/close-kv lmdb))

  (let [lmdb   (l/open-kv "/tmp/wiki92")
        engine (new-engine lmdb)]
    (time (doall (pmap
                   (fn [^HashMap m]
                     (add-doc engine (.get m "url") (.get m "text")))
                   lst)))
    (l/close-kv lmdb))

  (let [lmdb   (l/open-kv "/tmp/wiki51")
        engine (new-engine lmdb)
        pool   (Executors/newWorkStealingPool)]
    (time (.invokeAll pool
                      (map
                        (fn [^HashMap m]
                          (add-doc engine (.get m "url") (.get m "text")))
                        lst)))
    (l/close-kv lmdb))


  )
