(ns datalevin.search
  "Fuzzy full-text search engine"
  (:require [datalevin.lmdb :as l]
            [datalevin.util :as u]
            [datalevin.constants :as c]
            [datalevin.bits :as b])
  (:import [datalevin.sm SymSpell Bigram SuggestItem]
           [java.util HashMap ArrayList HashSet Iterator Map$Entry]
           [java.util.concurrent Executors ExecutorService ConcurrentHashMap
            TimeUnit CompletionService ExecutorCompletionService Future
            ArrayBlockingQueue ThreadPoolExecutor ThreadPoolExecutor$CallerRunsPolicy]
           [java.util.concurrent.atomic AtomicLong ]
           [java.io FileInputStream]
           [org.roaringbitmap RoaringBitmap RoaringBitmapWriter
            FastAggregation]
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
     by `doc-ref`. This function is for online update of search engine index,
     for off-line indexing of bulk data, use `index-writer`.")
  (index-writer [this] "open an `IndexWriter` for bulk writing of documents")
  (remove-doc [this doc-ref]
    "Remove a document referred to by `doc-ref`.")
  (search [this query] [this query opts]
    "Issue a `query` to the search engine. `query` is a map or a string.
     `opts` map may have these keys:

      * `:algo` can be one of `:smart` (default), `:prune`, or `:bitmap`.

     Return a lazy sequence of
     `[doc-ref [term1 [offset ...]] [term2 [...]] ...]`,
     ordered by relevance to the query. `term` and `offset` can be used to
     highlight the matched terms and their locations in the documents."))

(defn- collect-terms
  [result]
  (let [terms (HashMap. 256)]
    (doseq [[term position offset] result]
      (when (< (count term) 128) ; ignore exceedingly long strings
        (.put terms term (if-let [^ArrayList lst (.get terms term)]
                           (do (.add lst [position offset]) lst)
                           (doto (ArrayList.) (.add [position offset]))))))
    terms))

(defn- idf
  "inverse document frequency of a term"
  [^long freq ^long N]
  (if (zero? freq)
    0
    (Math/log10 (/ N freq))))

(defn- tf*
  "log-weighted term frequency"
  [^long freq]
  (if (zero? freq)
    0
    (+ (Math/log10 freq) 1)))

(defn- tf
  [lmdb doc-id term-id]
  (tf* (l/list-count lmdb c/positions [doc-id term-id] :id-id)))

(defn- hydrate-query
  [max-doc ^HashMap unigrams lmdb ^SymSpell symspell tokens]
  (let [sis (when symspell
              (.getSuggestTerms symspell tokens c/dict-max-edit-distance false))
        tms (if sis
              (mapv #(.getSuggestion ^SuggestItem %) sis)
              tokens)
        eds (if sis
              (zipmap tms (mapv #(.getEditDistance ^SuggestItem %) sis))
              (zipmap tms (repeat 0)))]
    (into []
          (comp
            (map (fn [[term freq]]
                   (let [id (.get unigrams term)
                         df (l/list-count lmdb c/term-docs id :id)]
                     (if (zero? ^long df)
                       :to-remove
                       {:id id
                        :ed (eds term)
                        :df df
                        :wq (* ^double (tf* freq) ^double (idf df max-doc))}))))
            (filter map?))
          (frequencies tms))))

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

(defn- docs-bitmap [lmdb tid]
  (let [writer  (.get (RoaringBitmapWriter/writer))
        visitor (fn [kv]
                  (let [did (b/read-buffer (l/v kv) :id)]
                    (.add ^RoaringBitmapWriter writer (int did))))]
    (l/visit lmdb c/term-docs visitor [:closed tid tid] :id)
    (.get writer)))

(defn- select-docs
  [tao n lmdb term-ids bms wqs ^HashMap result]
  (let [z          (inc (- ^long n ^long tao))
        union-tids (take z term-ids)
        union-bms  (->> (select-keys bms union-tids)
                        vals
                        (into-array RoaringBitmap))
        inter-tids (set (drop z term-ids))
        inter-bms  (->> (select-keys bms inter-tids)
                        vals
                        (into-array RoaringBitmap))
        union-bm   (FastAggregation/or
                     ^"[Lorg.roaringbitmap.RoaringBitmap;" union-bms)
        final-bm   (if (seq inter-tids)
                     (RoaringBitmap/and
                       union-bm
                       (FastAggregation/and
                         ^"[Lorg.roaringbitmap.RoaringBitmap;"
                         inter-bms))
                     union-bm)
        iter       (.iterator ^RoaringBitmap final-bm)
        selected   (ArrayList.)]
    (loop []
      (when (.hasNext iter)
        (let [did (.next iter)]
          (when-not (.get result did)
            (.add selected did)
            (let [res (doc-info lmdb did)
                  up  (fn [r tid]
                        (-> r
                            (update :score (fnil + 0.0)
                                    (* ^double (tf lmdb did tid)
                                       ^double (wqs tid)))
                            (update :tids (fnil conj []) tid)))
                  chk (fn [r tid]
                        (if (inter-tids tid)
                          (up r tid)
                          (if (.contains ^RoaringBitmap (bms tid) (int did))
                            (up r tid)
                            r)))]
              (.put result did (reduce chk res term-ids)))))
        (recur)))
    selected))

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
  [tao n term-ids ^HashMap candid cache lmdb
   ^HashSet selected ^HashMap backup wqs ^HashMap result]
  (if (= tao 1)
    (.addAll selected (.keySet candid))
    (doseq [^long k (range (inc ^long (- ^long n ^long tao)) n)
            :let    [kid (nth term-ids k)]]
      (loop [^Iterator iter (.iterator (.keySet candid))]
        (when (.hasNext iter)
          (let [did (.next iter)]
            (check-doc cache kid did lmdb candid wqs result)
            (let [hits ^long (.get candid did)]
              (cond
                (<= ^long tao hits) (do (.add selected did)
                                        (.remove backup did)
                                        (.remove iter))
                (< (+ hits ^long (- ^long n k 1)) ^long tao)
                (do (.put backup did hits)
                    (.remove iter))))
            (recur iter)))))))

(defn- rank-docs
  "return a list of [score doc-id doc-ref [term-ids]] sorted by score"
  [selected wq-sum ^HashMap result]
  (->> selected
       (map (fn [did]
              (let [{:keys [ref uniq score tids]} (.get result did)]
                [(- ^double score
                    (* ^double wq-sum (Math/log10 uniq)))
                 did ref tids])))
       (sort-by first >)))

(defn- add-positions
  [lmdb ^HashMap terms [_ did ref tids]]
  [ref (mapv (fn [tid]
               [(.get terms tid)
                (mapv second
                      (l/get-list lmdb c/positions [did tid]
                                  :id-id :int-int))])
             tids)])

(defn- prune-candidates
  [term-ids n lmdb wqs result]
  (let [backup (HashMap. 512)
        cache  (HashMap. 512)]
    (fn [tao] ; target # of overlaps between query and doc
      (let [tid    (nth term-ids (- ^long n ^long tao))
            candid (HashMap. backup)]
        (add-candidates lmdb tid candid cache wqs result)
        (let [selected (HashSet. 128)]
          (filter-candidates
            tao n term-ids candid cache lmdb selected
            backup wqs result)
          selected)))))

(defn- intersect-bitmaps
  [term-ids lmdb n wqs result]
  (let [bms (zipmap term-ids (map #(docs-bitmap lmdb %) term-ids))]
    (fn [tao]
      (select-docs tao n lmdb term-ids bms wqs result))))

(defn- select-n-score-docs
  [lmdb n qterms term-ids wqs result algo]
  (case algo
    ;; choose algorithm based on df variation
    :smart  (if (or (= n 1)
                    (> (quot ^long ((peek qterms) :df)
                             ^long ((nth qterms 0) :df))
                       64))
              (prune-candidates term-ids n lmdb wqs result)
              (intersect-bitmaps term-ids lmdb n wqs result))
    :prune  (prune-candidates term-ids n lmdb wqs result)
    :bitmap (intersect-bitmaps term-ids lmdb n wqs result)
    (u/raise "Unknown search algorithm" {:algo algo})))

(deftype SearchEngine [lmdb
                       ^HashMap unigrams ; term -> term-id
                       ^HashMap terms    ; term-id -> term
                       ^SymSpell symspell
                       max-doc
                       max-term]
  ISearchEngine
  (add-doc [this doc-ref doc-text]
    (let [result    (en-analyzer doc-text)
          new-terms (collect-terms result)
          unique    (count new-terms)
          txs       (ArrayList. 512)]
      (locking this
        (let [doc-id (inc ^long @max-doc)]
          (vreset! max-doc doc-id)
          (.add txs [:put c/docs doc-id {:ref doc-ref :uniq unique}
                     :id :data [:append]])
          (.add txs [:put c/rdocs doc-ref doc-id :data :id])
          (doseq [[term new-lst] new-terms]
            (let [term-id (or (.get unigrams term)
                              (let [tid (inc ^long @max-term)]
                                (vreset! max-term tid)
                                (.put unigrams term tid)
                                (.put terms tid term)
                                tid))]
              (.add txs [:put c/unigrams term term-id :string :id])
              (.add txs [:put c/term-docs term-id doc-id :id :id])
              (.add txs [:put-list c/positions [doc-id term-id] new-lst
                         :id-id :int-int]))))
        (l/transact-kv lmdb txs))))

  (remove-doc [this doc-ref]
    (if-let [doc-id (l/get-value lmdb c/rdocs doc-ref :data :id true)]
      (locking this
        (let [txs (ArrayList. 256)]
          (.add txs [:del c/docs doc-id :id])
          (.add txs [:del c/rdocs doc-ref :data])
          (doseq [[[_ term-id] _] (l/get-range
                                    lmdb c/positions
                                    [:closed [doc-id 0] [doc-id Long/MAX_VALUE]]
                                    :id-id :ignore false)]
            (.add txs [:del-list c/term-docs term-id [doc-id] :id :id])
            (.add txs [:del c/positions [doc-id term-id] :id-id]))
          (l/transact-kv lmdb txs)))
      (u/raise "Document does not exist." {:doc-ref doc-ref})))

  (search [this query]
    (.search this query {:algo :smart}))
  (search [this query {:keys [algo]
                       :or   {algo :smart}}]
    (let [tokens   (->> (en-analyzer query)
                        (mapv first)
                        (into-array String))
          qterms   (->> (hydrate-query @max-doc unigrams lmdb symspell tokens)
                        (sort-by :ed)
                        (sort-by :df)
                        vec)
          wqs      (into {} (mapv (fn [{:keys [id wq]}] [id wq]) qterms))
          wq-sum   (reduce + (mapv :wq qterms))
          term-ids (mapv :id qterms)
          n        (count term-ids)
          result   (HashMap. 512)
          xform    (comp
                     (mapcat (fn [selected]
                               (rank-docs selected wq-sum result)))
                     (map (fn [doc-info]
                            (add-positions lmdb terms doc-info))))]
      (if (zero? n)
        []
        (->> (range n 0 -1)
             u/unchunk  ; run one tier at a time
             (map (select-n-score-docs lmdb n qterms term-ids wqs result algo))
             (sequence xform))))))

(defn- init-unigrams-terms
  [lmdb]
  (let [unigrams (HashMap. 32768)
        terms    (HashMap. 32768)
        load     (fn [kv]
                   (let [term (b/read-buffer (l/k kv) :string)
                         id   (b/read-buffer (l/v kv) :id)]
                     (.put unigrams term id)
                     (.put terms id term)))]
    (l/visit lmdb c/unigrams load [:all] :string)
    [unigrams terms]))

(defn- init-unigrams
  [lmdb]
  (let [unigrams (HashMap. 32768)
        load     (fn [kv]
                   (let [term (b/read-buffer (l/k kv) :string)
                         id   (b/read-buffer (l/v kv) :id)]
                     (.put unigrams term id)))]
    (l/visit lmdb c/unigrams load [:all] :string)
    unigrams))

(defn- init-max-doc
  [lmdb]
  (or (first (l/get-first lmdb c/docs [:all-back] :id :ignore)) 0))

(defn- init-max-term
  [lmdb]
  (or (first (l/get-first lmdb c/term-docs [:all-back] :id :ignore)) 0))

(defn- open-dbis
  [lmdb]
  (assert (not (l/closed-kv? lmdb)) "LMDB env is closed.")
  (l/open-dbi lmdb c/unigrams c/+max-key-size+ c/+id-bytes+)
  (l/open-dbi lmdb c/docs c/+id-bytes+)
  (l/open-dbi lmdb c/rdocs c/+max-key-size+ c/+id-bytes+)
  (l/open-inverted-list lmdb c/term-docs c/+id-bytes+ c/+id-bytes+)
  (l/open-inverted-list lmdb c/positions (* 2 c/+id-bytes+) c/+id-bytes+))

(defn new-engine
  ([lmdb]
   (new-engine lmdb {}))
  ([lmdb {:keys [fuzzy?]
          :or   {fuzzy? false}}]
   (open-dbis lmdb)
   (let [[unigrams ^HashMap terms] (init-unigrams-terms lmdb)]
     (->SearchEngine lmdb
                     unigrams
                     terms
                     (when fuzzy? (SymSpell. {} {} c/dict-max-edit-distance
                                             c/dict-prefix-length))
                     (volatile! (init-max-doc lmdb))
                     (volatile! (init-max-term lmdb))
                     ))))

(defprotocol IIndexWriter
  (write [this doc-ref doc-text] "Write a document")
  (commit [this] "Commit the index write"))

(deftype IndexWriter [lmdb
                      ^ConcurrentHashMap unigrams
                      max-doc
                      max-term
                      ^ExecutorService threadpool
                      ^ArrayList tasks]
  IIndexWriter
  (write [this doc-ref doc-text]
    (let [task (fn []
                 (let [result    (en-analyzer doc-text)
                       new-terms ^HashMap (collect-terms result)
                       unique    (count new-terms)
                       txs       ^ArrayList (ArrayList. 512)
                       doc-id    (locking max-doc (vswap! max-doc inc))]
                   (.add txs [:put c/docs doc-id {:ref doc-ref :uniq unique}
                              :id :data [:append]])
                   (.add txs [:put c/rdocs doc-ref doc-id :data :id])
                   (doseq [^Map$Entry kv (.entrySet new-terms)]
                     (let [term    (.getKey kv)
                           new-lst (.getValue kv)
                           term-id (locking this
                                     (or (.get unigrams term)
                                         (let [tid (vswap! max-term inc)]
                                           (.put unigrams term tid)
                                           tid)))]
                       (.add txs [:put c/unigrams term term-id :string :id])
                       (.add txs [:put c/term-docs term-id doc-id :id :id])
                       (.add txs [:put-list c/positions [doc-id term-id] new-lst
                                  :id-id :int-int])))
                   (l/transact-kv lmdb txs)))]
      (.add tasks task)
      ;; (when (< 1000000 (.size tasks))
      ;;   (locking tasks
      ;;     (let  (.invokeAll threadpool tasks))
      ;;     (.clear tasks)))
      ))
  (commit [this]
    (println "size" (.size tasks))
    (doseq [^Future f (.invokeAll threadpool tasks)]
      (.get f))

    ;; (.shutdown threadpool)
    ;; (.awaitTermination threadpool 10 TimeUnit/MINUTES)
    ))

(defn index-writer
  [lmdb]
  (open-dbis lmdb)
  (->IndexWriter lmdb
                 (ConcurrentHashMap. ^HashMap (init-unigrams lmdb))
                 (volatile! (init-max-doc lmdb))
                 (volatile! (init-max-term lmdb))
                 (Executors/newWorkStealingPool)
                 (ArrayList. 8192)))

(comment

  (def lmdb  (l/open-kv "/tmp/wiki1005"))

  (def engine (time (new-engine lmdb)))

  (with-open [f (FileInputStream. "search-bench/wiki.json")]
    (let [jf  (JsonFactory.)
          jp  (.createParser jf f)
          cls (Class/forName "java.util.HashMap")]
      (.setCodec jp (ObjectMapper.))
      (.nextToken jp)
      (time
        (loop []
          (when (.hasCurrentToken jp)
            (let [^HashMap m (.readValueAs jp cls)]
              (add-doc engine (.get m "url") (.get m "text"))
              (.nextToken jp)
              (recur)))))))

  (time (take 10 (search engine "french lick resort and casino")))
  (time (take 10 (search engine "rv solar panels")))
  (time (take 10 (search engine "f1")))
  (time (take 10 (search engine "solar system")))
  (time (take 10 (search engine "community service projects for children")))
  (time (take 10 (search engine "libraries michigan")))
  (time (take 10 (search engine "roadrunner email")))
  (time (take 10 (search engine "josephine baker")))
  (time (take 10 (search engine "tel")))
  (time (take 10 (search engine "novi expo center")))
  (time (take 10 (search engine "ocean logos")))
  (time (take 10 (search engine "can i deduct credit card interest on my taxes")))
  (time (take 10 (search engine "what california district am i in")))
  (time (take 10 (search engine "jokes about women turning 40")))

  )
