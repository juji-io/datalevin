(ns datalevin.search
  "Full-text search engine"
  (:require [datalevin.lmdb :as l]
            [datalevin.util :as u]
            [datalevin.constants :as c]
            [datalevin.bits :as b])
  (:import [datalevin.sm SymSpell SuggestItem]
           [datalevin.utl PriorityQueue]
           [java.util HashMap ArrayList Iterator Map$Entry]
           [java.util.concurrent.atomic AtomicInteger]
           [org.eclipse.collections.impl.map.mutable.primitive IntShortHashMap
            IntObjectHashMap]
           [org.eclipse.collections.impl.list.mutable FastList]
           [org.eclipse.collections.impl.list.mutable.primitive ShortArrayList]
           [org.roaringbitmap RoaringBitmap FastAggregation]))

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
  (add-doc [this doc-ref doc-text] [this doc-ref doc-text positions?]
    "Add a document to the search engine, `doc-ref` can be arbitrary Clojure data
     that uniquely refers to the document in the system.
     `doc-text` is the content of the document as a string. The search engine
     does not store the original text, and assumes that caller can retrieve them
     by `doc-ref`. Term positions will not be stored if `positions?` is `false`
     (default). This function is for online update of search engine index,
     for index creation of bulk data, use `search-index-writer`.")
  (remove-doc [this doc-ref]
    "Remove a document referred to by `doc-ref`. A slow operation.")
  (search [this query] [this query opts]
    "Issue a `query` to the search engine. `query` is a map or a string.
     `opts` map may have these keys:

      * `:algo` can be one of `:smart` (default), `:prune`, or `:bitmap`.
      * `:display` can be one of `:refs` (default), `:offsets`.
      * `:top` is an integer (default 10), the number of results desired.

     Return a lazy sequence of
     `[doc-ref [term1 [offset ...]] [term2 [...]] ...]`,
     ordered by relevance to the query. `term` and `offset` can be used to
     highlight the matched terms and their locations in the documents."))

(defn- collect-terms
  [result]
  (let [terms (HashMap.)]
    (doseq [[term position offset] result]
      (when (< (count term) c/+max-term-length+)
        (.put terms term (if-let [^ArrayList lst (.get terms term)]
                           (do (.add lst [position offset]) lst)
                           (doto (ArrayList.) (.add [position offset]))))))
    terms))

(defn- idf
  "inverse document frequency of a term"
  [^long freq N]
  (if (zero? freq) 0 (Math/log10 (/ ^int N freq))))

(defn- tf*
  "log-weighted term frequency"
  [freq]
  (if (zero? ^short freq) 0 (+ (Math/log10 ^short freq) 1)))

(defn- tf
  [lmdb doc-id term-id]
  (tf* (l/list-count lmdb c/positions [term-id doc-id] :int-int)))

(defn- get-term-info
  [lmdb term]
  (l/get-value lmdb c/terms term :string :term-info))

(defn- hydrate-query
  [lmdb ^AtomicInteger max-doc ^SymSpell symspell tokens]
  (let [sis (when symspell
              (.getSuggestTerms symspell tokens c/dict-max-edit-distance false))
        tms (if sis
              (mapv #(.getSuggestion ^SuggestItem %) sis)
              tokens)
        eds (zipmap tms (if sis
                          (mapv #(.getEditDistance ^SuggestItem %) sis)
                          (repeat 0)))]
    (into []
          (comp
            (map (fn [[term freq]]
                   (when-let [[id mw bm] (get-term-info lmdb term)]
                     (let [df (.getCardinality ^RoaringBitmap bm)]
                       {:bm  bm
                        :df  df
                        :ed  (eds term)
                        :id  id
                        :idf (idf df (.get max-doc))
                        :mw  mw
                        :tm  term
                        :wq  (tf* freq)}))))
            (filter map?))
          (frequencies tms))))

(defn- inc-score
  [lmdb tid did idfs ^HashMap result]
  (let [tf-idf ^double (* ^double (tf lmdb did tid) ^double (idfs tid))]
    (if-let [res (.get result did)]
      (.put result did (+ ^double res tf-idf))
      (.put result did tf-idf))))

(defn- add-candidates
  [tid ^HashMap candid ^HashMap cache ^HashMap result bms]
  (doseq [did (bms tid)]
    (when-not (.containsKey result did)
      (if-let [seen (.get candid did)]
        (when-not (.containsKey cache [tid did])
          (.put candid did (inc ^long seen)))
        (.put candid did 1)))))

(defn- check-doc
  [^HashMap cache kid did tfs ^HashMap candid wqs ^HashMap result bms]
  (when (if (.containsKey cache [kid did])
          (.get cache [kid did])
          (let [in? (.contains ^RoaringBitmap (bms kid) ^int did)]
            (.put cache [kid did] in?)
            (when in? (inc-score tfs kid did wqs result))
            in?))
    (.put candid did (inc ^long (.get candid did)))))

(defn- filter-candidates
  [tao n term-ids ^HashMap candid cache tfs
   ^FastList selected ^HashMap backup wqs ^HashMap result bms]
  (if (= tao 1)
    (.addAll selected (.keySet candid))
    (doseq [^long k (range (inc ^long (- ^long n ^long tao)) n)
            :let    [kid (nth term-ids k)]]
      (loop [^Iterator iter (.iterator (.keySet candid))]
        (when (.hasNext iter)
          (let [did (.next iter)]
            (check-doc cache kid did tfs candid wqs result bms)
            (let [hits ^long (.get candid did)]
              (cond
                (<= ^long tao hits) (do (.add selected did)
                                        (.remove backup did)
                                        (.remove iter))
                (< (+ hits ^long (- ^long n k 1)) ^long tao)
                (do (.put backup did hits)
                    (.remove iter))))
            (recur iter)))))))

(defn- priority-queue
  [top]
  (proxy [PriorityQueue] [top]
    (lessThan [a b]
      (< ^double (nth a 0) ^double (nth b 0)))))

(defn- rank-docs
  "return a list of [score doc-id] sorted by normalized score"
  [selected ^IntShortHashMap norms ^HashMap result size]
  (let [^PriorityQueue pq (priority-queue size)]
    (doseq [did selected]
      (let [score (.get result did)
            uniq  (.get norms did)]
        (.insertWithOverflow pq [(/ ^double score uniq) did])))
    (let [lst (ArrayList.)]
      (dotimes [_ (.size pq)]
        (.add lst 0 (.pop pq)))
      lst)))

(defn- get-doc-ref
  [lmdb [_ doc-id]]
  (nth (l/get-value lmdb c/docs doc-id :int :doc-info) 1))

(defn- add-offsets
  [lmdb terms [_ doc-id :as info]]
  [(get-doc-ref lmdb info)
   (sequence
     (comp (map (fn [tid]
               (let [lst (l/get-list lmdb c/positions [doc-id tid]
                                     :int-int :int-int)]
                 (when (seq lst)
                   [(terms tid) (mapv #(nth % 1) lst)]))))
        (remove nil? ))
     (keys terms))])

(defn- prune-candidates
  [term-ids n tfs wqs result bms]
  (let [backup (HashMap.)
        cache  (HashMap.)]
    (fn [tao] ; target # of overlaps between query and doc
      (let [tid    (nth term-ids (- ^long n ^long tao))
            candid (HashMap. backup)]
        (add-candidates tid candid cache result bms)
        (let [selected (FastList.)]
          (filter-candidates
            tao n term-ids candid cache tfs selected
            backup wqs result bms)
          (doseq [did selected] (inc-score tfs tid did wqs result))
          selected)))))

(defn- intersect-bitmaps
  [term-ids tfs n wqs ^HashMap result bms]
  (fn [tao]
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
          selected   (FastList.)]
      (loop []
        (when (.hasNext iter)
          (let [did (.next iter)]
            (when-not (.get result did)
              (.add selected did)
              (let [res 0.0
                    up  (fn [^double r tid]
                          (+ r (* ^double (tf tfs did tid)
                                  ^double (wqs tid))))
                    chk (fn [r tid]
                          (if (inter-tids tid)
                            (up r tid)
                            (if (.contains ^RoaringBitmap (bms tid) (int did))
                              (up r tid)
                              r)))]
                (.put result did (reduce chk res term-ids)))))
          (recur)))
      selected)))

(defn- score-docs
  [tfs n term-ids bms wqs dfs result algo]
  (case algo
    :smart  (if (or (= n 1)
                    (> (quot ^long (peek dfs) ^long (nth dfs 0)) 128))
              (prune-candidates term-ids n tfs wqs result bms)
              (intersect-bitmaps term-ids tfs n wqs result bms))
    :prune  (prune-candidates term-ids n tfs wqs result bms)
    :bitmap (intersect-bitmaps term-ids tfs n wqs result bms)
    (u/raise "Unknown search algorithm" {:algo algo})))

(defn- display-xf
  [display lmdb tms]
  (case display
    :offsets (map #(add-offsets lmdb tms %))
    :refs    (map #(get-doc-ref lmdb %))))

(defn- add-doc-txs
  [lmdb doc-text ^AtomicInteger max-doc ^FastList txs doc-ref ^HashMap pre-terms
   ^IntObjectHashMap pre-freqs ^IntShortHashMap norms ^AtomicInteger max-term
   positions? ^HashMap hit-terms ^HashMap hit-freqs]
  (let [result    (en-analyzer doc-text)
        new-terms ^HashMap (collect-terms result)
        unique    (count new-terms)
        doc-id    (.incrementAndGet max-doc)]
    (.add txs [:put c/docs doc-id [unique doc-ref] :int :doc-info])
    (when norms (.put norms doc-id unique))
    (doseq [^Map$Entry kv (.entrySet new-terms)]
      (let [term         (.getKey kv)
            new-lst      (.getValue kv)
            [term-id bm] (or (.get hit-terms term)
                             (when pre-terms (.get pre-terms term))
                             (get-term-info lmdb term)
                             [(.incrementAndGet max-term) (b/bitmap)])
            term-info    [term-id (b/bitmap-add bm doc-id)]
            term-freqs   (or (.get hit-freqs term-id)
                             (when pre-freqs (.get pre-freqs term-id))
                             (get-tfs lmdb term-id)
                             (IntShortHashMap.))
            term-freqs   (doto ^IntShortHashMap term-freqs
                           (.put doc-id (count new-lst)))
            ]
        (.put hit-terms term term-info)
        (.put hit-freqs term-id term-freqs)
        (when positions?
          (.add txs [:put-list c/positions [doc-id term-id] new-lst
                     :int-int :int-int]))))))

(defn- doc-ref->id
  [lmdb doc-ref]
  (let [is-ref? (fn [kv]
                  (= doc-ref (nth (b/read-buffer (l/v kv) :doc-info) 1)))]
    (nth (l/get-some lmdb c/docs is-ref? [:all] :int :doc-info) 0)))

(defn- term-id->info
  [lmdb term-id]
  (let [is-id? (fn [kv] (= term-id (b/read-buffer (l/v kv) :int)))]
    (l/get-some lmdb c/terms is-id? [:all] :string :term-info false)))

(defn- term-id->freq
  [lmdb term-id doc-id]
  (let [tfs (get-tfs lmdb term-id)]
    [doc-id (tfs doc-id)]))

(deftype SearchEngine [lmdb
                       ^IntShortHashMap norms ; doc-id -> norm
                       ^SymSpell symspell
                       ^AtomicInteger max-doc
                       ^AtomicInteger max-term]
  ISearchEngine
  (add-doc [this doc-ref doc-text]
    (.add-doc this doc-ref doc-text false))
  (add-doc [this doc-ref doc-text positions?]
    (let [txs       (FastList.)
          hit-terms (HashMap.)
          hit-freqs (HashMap.)]
      (add-doc-txs lmdb doc-text max-doc txs doc-ref nil nil norms max-term
                   positions? hit-terms hit-freqs)
      (doseq [^Map$Entry kv (.entrySet hit-terms)]
        (let [term (.getKey kv)
              info (.getValue kv)]
          (.add txs [:put c/terms term info :string :term-info])))
      (doseq [^Map$Entry kv (.entrySet hit-freqs)]
        (let [term-id (.getKey kv)
              freqs   (.getValue kv)]
          (.add txs [:put c/term-freq term-id freqs :int :int-short-map])))
      (l/transact-kv lmdb txs)))

  (remove-doc [this doc-ref]
    (if-let [doc-id (doc-ref->id lmdb doc-ref)]
      (let [txs (FastList.)]
        (.remove norms doc-id)
        (.add txs [:del c/docs doc-id :int])
        (doseq [[[_ term-id] _] (l/get-range
                                  lmdb c/positions
                                  [:closed [doc-id 0] [doc-id Long/MAX_VALUE]]
                                  :int-int :ignore false)]
          (let [[term [_ bm]] (term-id->info lmdb term-id)]
            (.add txs [:put c/terms term [term-id (b/bitmap-del bm doc-id)]
                       :string :term-info]))
          (let [tf (term-id->freq lmdb term-id doc-id)]
            (.add txs [:del-list c/term-freq term-id [tf] :int :int-short]))
          (.add txs [:del c/positions [doc-id term-id] :int-int]))
        (l/transact-kv lmdb txs))
      (u/raise "Document does not exist." {:doc-ref doc-ref})))

  (search [this query]
    (.search this query {:algo :smart :display :refs :top 10}))
  (search [this query {:keys [algo display ^long top]
                       :or   {algo :smart display :refs top 10}}]
    (let [tokens (->> (en-analyzer query)
                      (mapv first)
                      (into-array String))
          qterms (->> (hydrate-query lmdb max-doc symspell tokens)
                      (sort-by :ed)
                      (sort-by :df)
                      vec)
          n      (count qterms)]
      (when-not (zero? n)
        (let [term-ids  (mapv :id qterms)
              dfs       (mapv :df qterms)
              bms       (zipmap term-ids (mapv :bm qterms))
              wqs       (zipmap term-ids (mapv :wq qterms))
              tms       (zipmap term-ids (mapv :tm qterms))
              tfs       (zipmap term-ids (mapv :tf qterms))
              result    (HashMap.)
              select-fn (score-docs tfs n term-ids bms wqs dfs result algo)]
          (sequence
            (display-xf display lmdb tms)
            (persistent!
              (reduce
                (fn [coll tao]
                  (let [sofar    (count coll)
                        selected ^FastList (select-fn tao)
                        ssize    (.size selected)
                        rank     (fn [size]
                                   (rank-docs selected norms result size))]
                    (if (<= top (+ sofar ssize))
                      (reduced
                        (reduce conj! coll (rank (- top sofar))))
                      (reduce conj! coll (rank ssize)))))
                (transient[])
                (range n 0 -1)))))))))

(defn- init-pre-terms
  [lmdb]
  (let [pre-terms (HashMap.)
        load      (fn [kv]
                    (let [term (b/read-buffer (l/k kv) :string)
                          info (b/read-buffer (l/v kv) :term-info)]
                      (.put pre-terms term info)))]
    (l/visit lmdb c/terms load [:all] :int)
    pre-terms))

(defn- init-pre-freqs
  [lmdb]
  (let [pre-freqs (IntObjectHashMap.)
        load      (fn [kv]
                    (let [term (b/read-buffer (l/k kv) :int)
                          m    (b/read-buffer (l/v kv) :int-short-map)]
                      (.put pre-freqs term m)))]
    (l/visit lmdb c/term-freq load [:all] :int)
    pre-freqs))

(defn- init-norms
  [lmdb]
  (let [norms (IntShortHashMap.)
        load  (fn [kv]
                (let [doc-id (b/read-buffer (l/k kv) :int)
                      norm   (b/read-buffer (l/v kv) :short)]
                  (.put norms doc-id norm)))]
    (l/visit lmdb c/docs load [:all] :data)
    norms))

(defn- init-max-doc
  [lmdb]
  (or (first (l/get-first lmdb c/docs [:all-back] :int :ignore)) 0))

(defn- init-max-term
  [lmdb]
  (or (first (l/get-first lmdb c/term-freq [:all-back] :int :ignore)) 0))

(defn- open-dbis
  [lmdb]
  (assert (not (l/closed-kv? lmdb)) "LMDB env is closed.")
  (l/open-dbi lmdb c/terms c/+max-key-size+)
  (l/open-dbi lmdb c/docs Integer/BYTES)
  (l/open-inverted-list lmdb c/positions (* 2 Integer/BYTES)
                        (* 2 Integer/BYTES)))

(defn new-engine
  ([lmdb]
   (new-engine lmdb {}))
  ([lmdb {:keys [fuzzy?]
          :or   {fuzzy? false}}]
   (open-dbis lmdb)
   (->SearchEngine lmdb
                   (init-norms lmdb)
                   (when fuzzy? (SymSpell. {} {} c/dict-max-edit-distance
                                           c/dict-prefix-length))
                   (AtomicInteger. (init-max-doc lmdb))
                   (AtomicInteger. (init-max-term lmdb)))))

(defprotocol IIndexWriter
  (write [this doc-ref doc-text] [this doc-ref doc-text positions?]
    "Write a document")
  (commit [this] "Commit writes"))

(deftype IndexWriter [lmdb
                      ^HashMap pre-terms
                      ^IntObjectHashMap pre-freqs
                      ^AtomicInteger max-doc
                      ^AtomicInteger max-term
                      ^FastList txs
                      ^HashMap hit-terms
                      ^HashMap hit-freqs]
  IIndexWriter
  (write [this doc-ref doc-text]
    (.write this doc-ref doc-text false))
  (write [this doc-ref doc-text positions?]
    (add-doc-txs lmdb doc-text max-doc txs doc-ref pre-terms pre-freqs nil
                 max-term positions? hit-terms hit-freqs)
    (when (< 250000 (.size txs))
      (.commit this)))

  (commit [this]
    (doseq [^Map$Entry kv (.entrySet hit-terms)]
      (let [term (.getKey kv)
            info (.getValue kv)]
        (.add txs [:put c/terms term info :string :term-info])))
    (doseq [^Map$Entry kv (.entrySet hit-freqs)]
      (let [term-id (.getKey kv)
            freqs   (.getValue kv)]
        (.add txs [:put c/term-freq term-id freqs :int :int-short-map])))
    (l/transact-kv lmdb txs)
    (.clear txs)
    (.clear pre-terms)
    (.clear pre-freqs)
    (.clear hit-terms)
    (.clear hit-freqs)))

(defn search-index-writer
  [lmdb]
  (open-dbis lmdb)
  (->IndexWriter lmdb
                 (init-pre-terms lmdb)
                 (init-pre-freqs lmdb)
                 (AtomicInteger. (init-max-doc lmdb))
                 (AtomicInteger. (init-max-term lmdb))
                 (FastList.)
                 (HashMap.)
                 (HashMap.)))

(comment

  (def lmdb  (l/open-kv "search-bench/data/wiki-datalevin-5"))

  (def engine (time (new-engine lmdb)))

  (l/get-first lmdb c/term-freq [:all-back] :int :int-short-map false)

  (let [m (volatile! 0)
        f (fn [kv]
            (let [[p _] (b/read-buffer (l/v kv) :int-int)]
              (when (< @m p)
                (vreset! m p))))]
    (l/visit-list lmdb c/positions f [1 1] :int-int)
    @m)

  (time (l/list-count lmdb c/positions [11 17655 ] :int-int ))

  (time (search engine "french lick resort and casino"))
  (time (search engine "rv solar panels"))
  (time (search engine "f1"))
  (time (search engine "solar system"))
  (time (search engine "community service projects for children"))
  (time (search engine "libraries michigan"))
  (time (search engine "roadrunner email"))
  (time (search engine "josephine baker"))
  (time (search engine "tel"))
  (time (search engine "novi expo center"))
  (time (search engine "ocean logos"))
  (time (search engine "can i deduct credit card interest on my taxes"))
  (time (search engine "what california district am i in"))
  (time (search engine "jokes about women turning 40"))
  (time (search engine "free inmigration forms"))
  (time (search engine "bartender license indiana"))



  )
