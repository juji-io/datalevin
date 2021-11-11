(ns datalevin.search
  "Full-text search engine"
  (:require [datalevin.lmdb :as l]
            [datalevin.util :as u]
            [datalevin.constants :as c]
            [datalevin.bits :as b])
  (:import [datalevin.sm SymSpell SuggestItem]
           [datalevin.utl PriorityQueue]
           [java.util HashSet HashMap ArrayList Map$Entry]
           [java.util.concurrent.atomic AtomicInteger]
           [org.eclipse.collections.impl.map.mutable.primitive IntShortHashMap]
           [org.eclipse.collections.impl.list.mutable FastList]
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

(defn- collect-terms
  [result]
  (let [terms (HashMap.)]
    (doseq [[term position offset] result]
      (when (< (count term) c/+max-term-length+)
        (.put terms term (if-let [^ArrayList lst (.get terms term)]
                           (do (.add lst [position offset]) lst)
                           (doto (ArrayList.) (.add [position offset]))))))
    terms))

(defn- get-term-info
  [lmdb term]
  (l/get-value lmdb c/terms term :string :term-info))

(defn- get-tf
  [lmdb doc-id term-id]
  (l/list-count lmdb c/positions [term-id doc-id] :int-int))

(defn- idf
  "inverse document frequency of a term"
  [^long freq N]
  (float (if (zero? freq) 0 (Math/log10 (/ ^int N freq)))))

(defn- tf*
  "log-weighted term frequency"
  [freq]
  (float (if (zero? ^short freq) 0 (+ (Math/log10 ^short freq) 1))))

(defn- tf
  [lmdb doc-id term-id]
  (tf* (get-tf lmdb doc-id term-id)))

(defn- add-max-weight
  [mw tf norm]
  (let [w (float (/ ^float (tf* tf) ^short norm))]
    (if (< ^float mw w) w mw)))

(defn- del-max-weight
  [lmdb ^RoaringBitmap bm doc-id term-id mw tf norm]
  (let [w (float (/ ^float (tf* tf) ^short norm))]
    (if (= mw w)
      (apply max (map #(float (/ ^float (tf* (get-tf lmdb % term-id))
                                 ^short norm))
                      (doto bm (.remove doc-id))))
      mw)))

(defn- add-doc-txs
  [lmdb doc-text ^AtomicInteger max-doc ^FastList txs doc-ref ^HashMap pre-terms
   ^IntShortHashMap norms ^AtomicInteger max-term ^HashMap hit-terms]
  (let [result    (en-analyzer doc-text)
        new-terms ^HashMap (collect-terms result)
        unique    (count new-terms)
        doc-id    (.incrementAndGet max-doc)]
    (.add txs [:put c/docs doc-id [unique doc-ref] :int :doc-info])
    (when norms (.put norms doc-id unique))
    (doseq [^Map$Entry kv (.entrySet new-terms)]
      (let [term        (.getKey kv)
            new-lst     ^ArrayList (.getValue kv)
            tf          (.size new-lst)
            [tid mw bm] (or (.get hit-terms term)
                            (when pre-terms (.get pre-terms term))
                            (get-term-info lmdb term)
                            [(.incrementAndGet max-term) (float 0.0) (b/bitmap)])]
        (.put hit-terms term [tid (add-max-weight mw tf unique)
                              (b/bitmap-add bm doc-id)])
        (.add txs [:put-list c/positions [tid doc-id] new-lst
                   :int-int :int-int])))))

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
                       {:bm bm
                        :df df
                        :ed (eds term)
                        :id id
                        :mw mw
                        :tm term
                        :wq (* ^float (tf* freq)
                               ^float (idf df (.get max-doc)))}))))
            (filter map?))
          (frequencies tms))))

(defn- priority-queue
  [top]
  (proxy [PriorityQueue] [top]
    (lessThan [a b]
      (< ^float (nth a 0) ^float (nth b 0)))))

(defn- pour
  [coll ^PriorityQueue pq ^HashSet result]
  (let [lst (ArrayList.)]
    (dotimes [_ (.size pq)]
      (let [[_ did :as res] (.pop pq)]
        (.add lst 0 res)
        (.add result did)))
    (println "lst" lst)
    (reduce conj! coll lst)))

(defn- real-score
  [lmdb tid did wqs norms]
  (/ (* ^float (wqs tid) ^float (tf lmdb did tid))
     ^short (.get ^IntShortHashMap norms did)))

(defn- max-score [wqs mws tid] (* ^float (wqs tid) ^float (mws tid)))

(defn- find-pivot
  [mxs tao-1 minimal-score candidates]
  (let [n-1 (dec (count candidates))]
    (loop [s (float 0.0) p 0 cs candidates]
      (let [[tid did] (nth cs 0)]
        (if (< p n-1)
          (let [s (+ s ^float (mxs tid))]
            (if (and (<= ^long tao-1 p) (< ^float minimal-score s))
              [s p did]
              (recur s (inc p) (rest cs))))
          [s p did])))))

(defn- score-pivot
  [wqs mxs lmdb norms pivot-did minimal-score mxscore tao n candidates]
  (let [res (reduce-kv
              (fn [[score ^long hits] ^long k [tid did]]
                (println "score" score "hits" hits)
                (if (= did pivot-did)
                  (let [hits (inc hits)]
                    (if (< ^long (+ hits ^long (- ^long n k 1)) ^long tao)
                      (reduced :prune)
                      (let [s (+ (- ^float score ^float (mxs tid))
                                 ^float (real-score lmdb tid did wqs norms))]
                        (if (< s ^float minimal-score)
                          (reduced :prune)
                          [s hits]))))
                  [score hits]))
              [mxscore 0]
              candidates)]
    (if (= res :prune) res (nth res 0))))

(defn- fresh-did
  [^RoaringBitmap bm ^HashSet result init-did]
  (loop [did ^int init-did]
    (if (.contains result did)
      (recur (.nextValue bm (inc did)))
      did)))

(defn- first-candidates
  [bms term-ids result]
  (persistent!
    (reduce (fn [cs tid]
              (let [bm   ^RoaringBitmap (bms tid)
                    ndid (fresh-did bm result (.first bm))]
                (if (= ndid -1)
                  cs
                  (conj! cs [tid ndid]))))
            (transient [])
            term-ids)))

(defn- next-candidates
  [bms did term-ids result]
  (println "everyone moves beyond" did)
  (persistent!
    (reduce (fn [cs tid]
              (let [bm   ^RoaringBitmap (bms tid)
                    ndid (fresh-did bm result (.nextValue bm (inc ^int did)))]
                (if (= ndid -1)
                  cs
                  (conj! cs [tid ndid]))))
            (transient [])
            term-ids)))

(defn- skip-candidates
  [pivot bms pivot-did candidates result]
  (println "some skips to" pivot-did)
  (persistent!
    (reduce-kv
      (fn [cs t [tid _ :as ct]]
        (if (< ^long t ^long pivot)
          (let [bm   ^RoaringBitmap (bms tid)
                ndid (fresh-did bm result (.nextValue bm pivot-did))]
            (if (= ndid -1)
              cs
              (conj! cs [tid ndid])))
          (conj! cs ct)))
      (transient [])
      candidates)))

(defn- current-threshold
  [^PriorityQueue pq]
  (if (< (.size pq) (.maxSize pq))
    (float 0.0)
    (nth (.top pq) 0)))

(defn- prune-candidates
  [lmdb n term-ids bms mxs wqs ^IntShortHashMap norms ^HashSet result]
  (fn [^PriorityQueue pq ^long tao] ; target # of overlaps between query and doc
    (println "working on tao" tao)
    (loop [candidates (first-candidates bms term-ids result)]
      (println "candidates" candidates)
      (when (seq candidates)
        (let [candidates          (vec (sort-by peek candidates))
              _                   (println "sorted candidates" candidates)
              minimal-score       ^float (current-threshold pq)
              _                   (println "minimal-score" minimal-score)
              [mxscore pivot did] (find-pivot mxs (dec tao) minimal-score
                                              candidates)]
          (println "found pivot" did)
          (if (= ^int did (peek (nth candidates 0)))
            (let [score (score-pivot wqs mxs lmdb norms did minimal-score
                                     mxscore tao n candidates)]
              (println "real score" score)
              (when-not (= score :prune)
                (.insertWithOverflow pq [score did]))
              (recur (next-candidates bms did term-ids result)))
            (recur (skip-candidates pivot bms did candidates result))))))))

#_(defn- prune-candidates
    [lmdb n term-ids bms mtfs wqs ^IntShortHashMap norms ^HashMap result
     ^HashMap backup]
    (fn [^PriorityQueue pq ^long tao] ; target # of overlaps between query and doc
      (let [n          ^long n
            base       (- n tao)
            base-tid   (nth term-ids base)
            candidates (HashMap. backup)]
        (doseq [did (bms base-tid)]
          (when-not (.containsKey result did)
            (if-let [{:keys [^long reach] :as seen} (.get candidates did)]
              (when (< reach base)
                (.put candidates did (-> seen
                                         (assoc :reach base)
                                         (update :hits inc)
                                         (update :tids conj base-tid))))
              (.put candidates did {:reach base :hits 1 :tids #{base-tid}}))))
        (when-not (= tao 1)
          (doseq [^long k (range (inc base) n)
                  :let    [ktid (nth term-ids k)]]
            (loop [^Iterator iter (.iterator (.entrySet candidates))]
              (when (.hasNext iter)
                (let [^Map$Entry entry              (.next iter)
                      ^int did                      (.getKey entry)
                      {:keys [^long reach] :as res} (.getValue entry)]
                  (when (and (< reach k)
                             (.contains ^RoaringBitmap (bms ktid) did))
                    (.setValue entry (-> res
                                         (assoc :reach k)
                                         (update :hits inc)
                                         (update :tids conj ktid))))
                  (let [{:keys [hits] :as res} (.getValue entry)]
                    (when (< (+ ^long hits ^long (- n k 1)) tao)
                      (.put backup did res)
                      (.remove iter)))
                  (recur iter))))))
        (loop [^Iterator iter (.iterator (.entrySet candidates))]
          (when (.hasNext iter)
            (let [^Map$Entry entry (.next iter)
                  ^int did         (.getKey entry)
                  norm             (.get norms did)
                  {:keys [tids computed score]
                   :or   {computed 0 score 0.0}
                   :as   res}      (.getValue entry)
                  minimal-score    (if (< (.size pq) (.maxSize pq))
                                     0.0
                                     (nth (.top pq) 0))
                  prune?           (volatile! false)]
              (loop [^long i computed score score]
                (if (< i n)
                  (if (< ^double
                         (opt-score score tids n i did term-ids mtfs wqs norm)
                         ^double minimal-score)
                    (do (vreset! prune? true)
                        (println "pruned" did)
                        (.setValue entry (assoc res :computed i :score score)))
                    (recur (inc i)
                           (real-score lmdb score tids i did term-ids wqs norm)))
                  (.setValue entry (assoc res :computed i :score score))))
              (let [{:keys [score] :as res} (.getValue entry)]
                (.put backup did res)
                (when-not @prune?
                  (.insertWithOverflow pq [score did])))
              (recur iter)))))))

(defn- intersect-bitmaps
  [term-ids tfs n wqs ^HashMap result bms]
  (fn [^PriorityQueue pq tao]
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
  [lmdb n term-ids dfs bms mxs wqs norms result algo]
  (case algo
    :smart
    (if (or (= n 1)
            (> (quot ^long (peek dfs) ^long (nth dfs 0)) 128))
      (prune-candidates lmdb n term-ids bms mxs wqs norms result)
      (intersect-bitmaps lmdb n term-ids bms mxs wqs norms result))
    :prune
    (prune-candidates lmdb n term-ids bms mxs wqs norms result)
    :bitmap
    (intersect-bitmaps lmdb n term-ids bms mxs wqs norms result)
    (u/raise "Unknown search algorithm" {:algo algo})))

(defn- get-doc-ref
  [lmdb [_ doc-id]]
  (nth (l/get-value lmdb c/docs doc-id :int :doc-info) 1))

(defn- add-offsets
  [lmdb terms [_ doc-id :as result]]
  [(get-doc-ref lmdb result)
   (sequence
     (comp (map (fn [tid]
               (let [lst (l/get-list lmdb c/positions [tid doc-id]
                                     :int-int :int-int)]
                 (when (seq lst)
                   [(terms tid) (mapv #(nth % 1) lst)]))))
        (remove nil? ))
     (keys terms))])

(defn- display-xf
  [display lmdb tms]
  (case display
    :offsets (map #(add-offsets lmdb tms %))
    :refs    (map #(get-doc-ref lmdb %))))

(defn- doc-ref->id
  [lmdb doc-ref]
  (let [is-ref? (fn [kv]
                  (= doc-ref (nth (b/read-buffer (l/v kv) :doc-info) 1)))]
    (nth (l/get-some lmdb c/docs is-ref? [:all] :int :doc-info) 0)))

(defn- doc-id->term-ids
  [lmdb doc-id]
  (dedupe
    (map ffirst
         (l/range-filter lmdb c/positions
                         (fn [kv]
                           (let [[_ did] (b/read-buffer (l/k kv) :int-int)]
                             (= did doc-id)))
                         [:all] :int-int :ignore false))))

(defn- term-id->info
  [lmdb term-id]
  (let [is-id? (fn [kv] (= term-id (b/read-buffer (l/v kv) :int)))]
    (l/get-some lmdb c/terms is-id? [:all] :string :term-info false)))

(defprotocol ISearchEngine
  (add-doc [this doc-ref doc-text]
    "Add a document to the search engine, `doc-ref` can be arbitrary Clojure data
     that uniquely refers to the document in the system.
     `doc-text` is the content of the document as a string. The search engine
     does not store the original text, and assumes that caller can retrieve them
     by `doc-ref`. This function is for online update of search engine index,
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

(deftype SearchEngine [lmdb
                       ^IntShortHashMap norms ; doc-id -> norm
                       ^SymSpell symspell
                       ^AtomicInteger max-doc
                       ^AtomicInteger max-term]
  ISearchEngine
  (add-doc [this doc-ref doc-text]
    (let [txs       (FastList.)
          hit-terms (HashMap.)]
      (add-doc-txs lmdb doc-text max-doc txs doc-ref nil norms max-term
                   hit-terms)
      (doseq [^Map$Entry kv (.entrySet hit-terms)]
        (let [term (.getKey kv)
              info (.getValue kv)]
          (.add txs [:put c/terms term info :string :term-info])))
      (l/transact-kv lmdb txs)))

  (remove-doc [this doc-ref]
    (if-let [doc-id (doc-ref->id lmdb doc-ref)]
      (let [txs  (FastList.)
            norm (.get norms doc-id)]
        (.remove norms doc-id)
        (.add txs [:del c/docs doc-id :int])
        (doseq [term-id (doc-id->term-ids lmdb doc-id)]
          (let [[term [_ mw bm]] (term-id->info lmdb term-id)
                tf               (get-tf lmdb doc-id term-id)]
            (.add txs [:put c/terms term
                       [term-id
                        (del-max-weight lmdb bm doc-id term-id mw tf norm)
                        (b/bitmap-del bm doc-id)]
                       :string :term-info]))
          (.add txs [:del c/positions [term-id doc-id] :int-int]))
        (l/transact-kv lmdb txs))
      (u/raise "Document does not exist." {:doc-ref doc-ref})))

  (search [this query]
    (.search this query {:algo :prune :display :refs :top 10}))
  (search [this query {:keys [algo display ^long top]
                       :or   {algo :prune display :refs top 10}}]
    (let [tokens (->> (en-analyzer query)
                      (mapv first)
                      (into-array String))
          qterms (->> (hydrate-query lmdb max-doc symspell tokens)
                      (sort-by :ed)
                      (sort-by :df)
                      vec)
          n      (count qterms)]
      (when-not (zero? n)
        (let [term-ids (mapv :id qterms)
              dfs      (mapv :df qterms)
              bms      (zipmap term-ids (mapv :bm qterms))
              wqs      (zipmap term-ids (mapv :wq qterms))
              tms      (zipmap term-ids (mapv :tm qterms))
              mws      (zipmap term-ids (mapv :mw qterms))
              mxs      (zipmap term-ids (map #(max-score wqs mws %) term-ids))
              result   (HashSet.)
              score-fn (score-docs lmdb n term-ids dfs bms mxs wqs norms
                                   result algo)]
          (sequence
            (display-xf display lmdb tms)
            (persistent!
              (reduce
                (fn [coll tao]
                  (let [so-far (count coll)
                        to-get (- top so-far)]
                    (if (< 0 to-get)
                      (let [^PriorityQueue pq (priority-queue to-get)]
                        (score-fn pq tao)
                        (pour coll pq result))
                      (reduced coll))))
                (transient[])
                (range n 0 -1)))))))))

(defn- init-pre-terms
  [lmdb]
  (let [pre-terms (HashMap.)
        load      (fn [kv]
                    (let [term (b/read-buffer (l/k kv) :string)
                          info (b/read-buffer (l/v kv) :term-info)]
                      (.put pre-terms term info)))]
    (l/visit lmdb c/terms load [:all] :string)
    pre-terms))

(defn- init-norms
  [lmdb]
  (let [norms (IntShortHashMap.)
        load  (fn [kv]
                (let [doc-id (b/read-buffer (l/k kv) :int)
                      norm   (b/read-buffer (l/v kv) :short)]
                  (.put norms doc-id norm)))]
    (l/visit lmdb c/docs load [:all] :int)
    norms))

(defn- init-max-doc
  [lmdb]
  (or (first (l/get-first lmdb c/docs [:all-back] :int :ignore)) 0))

(defn- init-max-term
  [lmdb]
  (or (first (l/get-first lmdb c/positions [:all-back] :int :ignore)) 0))

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
  (write [this doc-ref doc-text] "Write a document")
  (commit [this] "Commit writes"))

(deftype IndexWriter [lmdb
                      ^HashMap pre-terms
                      ^AtomicInteger max-doc
                      ^AtomicInteger max-term
                      ^FastList txs
                      ^HashMap hit-terms]
  IIndexWriter
  (write [this doc-ref doc-text]
    (add-doc-txs lmdb doc-text max-doc txs doc-ref pre-terms nil max-term
                 hit-terms)
    (when (< 1000000 (.size txs))
      (.commit this)))

  (commit [this]
    (doseq [^Map$Entry kv (.entrySet hit-terms)]
      (let [term (.getKey kv)
            info (.getValue kv)]
        (.add txs [:put c/terms term info :string :term-info])))
    (l/transact-kv lmdb txs)
    (.clear txs)
    (.clear pre-terms)
    (.clear hit-terms)))

(defn search-index-writer
  [lmdb]
  (open-dbis lmdb)
  (->IndexWriter lmdb
                 (init-pre-terms lmdb)
                 (AtomicInteger. (init-max-doc lmdb))
                 (AtomicInteger. (init-max-term lmdb))
                 (FastList.)
                 (HashMap.)))

(comment

  (def lmdb  (l/open-kv "search-bench/data/wiki-datalevin-1"))

  (def engine (time (new-engine lmdb)))

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
