(ns datalevin.search
  "Fuzzy full-text search engine"
  (:require [datalevin.lmdb :as l]
            [datalevin.util :as u]
            [datalevin.constants :as c]
            [datalevin.bits :as b])
  (:import [datalevin.sm SymSpell SuggestItem]
           [datalevin.utl PriorityQueue]
           [java.util HashMap ArrayList Iterator Map$Entry]
           [java.util.concurrent.atomic AtomicInteger]
           [org.eclipse.collections.impl.map.mutable.primitive IntShortHashMap
            IntObjectHashMap ObjectIntHashMap]
           [org.eclipse.collections.impl.list.mutable FastList]
           [org.eclipse.collections.impl.list.mutable.primitive IntArrayList]
           [org.roaringbitmap RoaringBitmap RoaringBitmapWriter
            FastAggregation]))

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
     for off-line indexing of bulk data, use `index-writer`.")
  (remove-doc [this doc-ref]
    "Remove a document referred to by `doc-ref`.")
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
  (if-let [freq (l/get-value lmdb c/term-freq [term-id doc-id]
                             :int-int :short true)]
    (tf* freq)
    0))

#_(defn- docs-bitmap [lmdb tid]
   (l/get-value lmdb c/term-docs tid :int :bitmap true)
   #_(let [writer  (.get (RoaringBitmapWriter/writer))
           visitor (fn [kv]
                     (let [did (b/read-buffer (l/v kv) :int)]
                       (.add ^RoaringBitmapWriter writer (int did))))]
       (l/visit-list lmdb c/term-docs visitor tid :int)
       (.get writer)))

(defn- hydrate-query
  [^AtomicInteger max-doc ^ObjectIntHashMap unigrams
   ^SymSpell symspell tokens ^IntObjectHashMap td-bms]
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
                   (let [id (.getIfAbsent unigrams term -1)]
                     (when-not (= id -1)
                       (let [df (.getCardinality
                                  ^RoaringBitmap (.get td-bms id))]
                         {:tm term
                          :id id
                          :ed (eds term)
                          :df df
                          :wq (* ^double (tf* freq)
                                 ^double (idf df (.get max-doc)))})))))
            (filter map?))
          (frequencies tms))))

(defn- inc-score
  [lmdb tid did wqs ^HashMap result]
  (let [dot ^double (* ^double (tf lmdb did tid) ^double (wqs tid))]
    (if-let [res (.get result did)]
      (.put result did (+ ^double res dot))
      (.put result did dot))))

(defn- add-candidates
  [tid ^HashMap candid ^HashMap cache ^HashMap result ^IntObjectHashMap td-bms]
  (doseq [did (.get td-bms tid)]
    (when-not (.containsKey result did)
      (if-let [seen (.get candid did)]
        (when-not (.containsKey cache [tid did])
          (.put candid did (inc ^long seen)))
        (.put candid did 1)))))

(defn- check-doc
  [^HashMap cache kid did lmdb ^HashMap candid wqs ^HashMap result
   ^IntObjectHashMap td-bms]
  (when (if (.containsKey cache [kid did])
          (.get cache [kid did])
          (let [in? (.contains ^RoaringBitmap (.get td-bms kid))]
            (.put cache [kid did] in?)
            (when in? (inc-score lmdb kid did wqs result))
            in?))
    (.put candid did (inc ^long (.get candid did)))))

(defn- filter-candidates
  [tao n term-ids ^HashMap candid cache lmdb
   ^FastList selected ^HashMap backup wqs ^HashMap result]
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

(defn- priority-queue
  [top]
  (proxy [PriorityQueue] [top]
    (lessThan [a b]
      (< ^double (nth a 0) ^double (nth b 0)))))

(defn- rank-docs
  "return a list of [score doc-id doc-ref [term-ids]] sorted by
  normalized score"
  [selected ^IntObjectHashMap rdocs ^IntShortHashMap norms ^HashMap result size]
  (let [^PriorityQueue pq (priority-queue size)]
    (doseq [did selected]
      (let [score   (.get result did)
            doc-ref (.get rdocs did)
            uniq    (.get norms did)]
        (.insertWithOverflow pq [(/ ^double score uniq) did doc-ref])))
    (let [lst (ArrayList.)]
      (dotimes [_ (.size pq)]
        (.add lst 0 (.pop pq)))
      lst)))

(defn- add-offsets
  [lmdb terms [_ did doc-ref]]
  [doc-ref (sequence
             (comp (map (fn [tid]
                       (let [lst (l/get-list lmdb c/positions [did tid]
                                             :int-int :int-int)]
                         (when (seq lst)
                           [(terms tid) (mapv #(nth % 1) lst)]))))
                (remove nil? ))
             (keys terms))])

(defn- get-doc-ref [doc-info] (nth doc-info 2))

(defn- prune-candidates
  [term-ids n lmdb wqs result td-bms]
  (let [backup (HashMap.)
        cache  (HashMap.)]
    (fn [tao] ; target # of overlaps between query and doc
      (let [tid    (nth term-ids (- ^long n ^long tao))
            candid (HashMap. backup)]
        (add-candidates tid candid cache result td-bms)
        (let [selected (FastList.)]
          (filter-candidates
            tao n term-ids candid cache lmdb selected
            backup wqs result td-bms)
          (doseq [did selected] (inc-score lmdb tid did wqs result))
          selected)))))

(defn- intersect-bitmaps
  [term-ids lmdb n wqs ^HashMap result]
  (let [bms (zipmap term-ids (map #(docs-bitmap lmdb %) term-ids))]
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
                            (+ r (* ^double (tf lmdb did tid)
                                    ^double (wqs tid))))
                      chk (fn [r tid]
                            (if (inter-tids tid)
                              (up r tid)
                              (if (.contains ^RoaringBitmap (bms tid) (int did))
                                (up r tid)
                                r)))]
                  (.put result did (reduce chk res term-ids)))))
            (recur)))
        selected))))

(defn- score-docs
  [lmdb n qterms term-ids wqs result algo td-bms]
  (case algo
    ;; choose algorithm based on df variation
    :smart  (if (or (= n 1)
                    (> (quot ^long ((peek qterms) :df)
                             ^long ((nth qterms 0) :df))
                       16))
              (prune-candidates term-ids n lmdb wqs result td-bms)
              (intersect-bitmaps term-ids lmdb n wqs result td-bms))
    :prune  (prune-candidates term-ids n lmdb wqs result td-bms)
    :bitmap (intersect-bitmaps term-ids lmdb n wqs result td-bms)
    (u/raise "Unknown search algorithm" {:algo algo})))

(defn- display-xf
  [display lmdb term-ids qterms]
  (case display
    :offsets (map #(add-offsets
                     lmdb
                     (zipmap term-ids (mapv :tm qterms))
                     %))
    :refs    (map #(get-doc-ref %))))

(defn- add-doc-txs
  [doc-text ^AtomicInteger max-doc ^FastList txs doc-ref
   ^IntObjectHashMap rdocs ^IntShortHashMap norms ^ObjectIntHashMap unigrams
   ^AtomicInteger max-term positions? ^IntObjectHashMap td-bms]
  (let [result    (en-analyzer doc-text)
        new-terms ^HashMap (collect-terms result)
        unique    (count new-terms)
        doc-id    (.incrementAndGet max-doc)
        term-ids  (ArrayList.)]
    (.add txs [:put c/docs doc-ref [doc-id unique] :data :int-short])
    (when rdocs (.put rdocs doc-id doc-ref))
    (when norms (.put norms doc-id unique))
    (doseq [^Map$Entry kv (.entrySet new-terms)]
      (let [term    (.getKey kv)
            new-lst (.getValue kv)
            term-id (.getIfAbsentPut unigrams term
                                     (.incrementAndGet max-term))]
        (.add term-ids term-id)
        (if (.containsKey td-bms term-id)
          (.put td-bms term-id
                (b/bitmap-add (.get td-bms term-id) doc-id))
          (.put td-bms term-id (b/bitmap-add (b/bitmap) doc-id)))
        (.add txs [:put c/terms term-id term :int :string])
        (.add txs [:put c/term-freq [term-id doc-id] (count new-lst)
                   :int-int :short])
        (when positions?
          (.add txs [:put-list c/positions [doc-id term-id] new-lst
                     :int-int :int-int]))))
    term-ids))

(deftype SearchEngine [lmdb
                       ^ObjectIntHashMap unigrams ; term -> term-id
                       ^IntObjectHashMap td-bms   ; term-id -> doc-ids
                       ^IntShortHashMap norms     ; doc-id -> norm
                       ^IntObjectHashMap rdocs    ; doc-id -> doc-ref
                       ^SymSpell symspell
                       ^AtomicInteger max-doc
                       ^AtomicInteger max-term]
  ISearchEngine
  (add-doc [this doc-ref doc-text]
    (.add-doc this doc-ref doc-text false))
  (add-doc [this doc-ref doc-text positions?]
    (let [txs (FastList.)]
      (doseq [tid (add-doc-txs doc-text max-doc txs doc-ref rdocs norms
                               unigrams max-term positions? td-bms)]
        (.add txs [:put c/term-docs tid (.get td-bms tid) :int :bitmap]))
      (l/transact-kv lmdb txs)))

  (remove-doc [this doc-ref]
    (if-let [doc-id (l/get-value lmdb c/docs doc-ref :data :int true)]
      (let [txs (FastList.)]
        (.remove rdocs doc-id)
        (.remove norms doc-id)
        (.add txs [:del c/docs doc-ref :data])
        (doseq [[[_ term-id] _] (l/get-range
                                  lmdb c/positions
                                  [:closed [doc-id 0] [doc-id Long/MAX_VALUE]]
                                  :int-int :ignore false)]
          (.add txs [:del-list c/term-docs term-id [doc-id] :int :int])
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
          qterms (->> (hydrate-query max-doc unigrams symspell tokens td-bms)
                      (sort-by :ed)
                      (sort-by :df)
                      vec)
          n      (count qterms)]
      (when-not (zero? n)
        (let [term-ids  (mapv :id qterms)
              wqs       (into {} (mapv (fn [{:keys [id wq]}] [id wq]) qterms))
              result    (HashMap.)
              select-fn (score-docs lmdb n qterms term-ids wqs result
                                    algo td-bms)]
          (sequence
            (display-xf display lmdb term-ids qterms)
            (persistent!
              (reduce
                (fn [coll tao]
                  (let [sofar    (count coll)
                        selected ^FastList (select-fn tao)
                        ssize    (.size selected)
                        rank     (fn [size]
                                   (rank-docs selected rdocs norms result size))]
                    (if (<= top (+ sofar ssize))
                      (reduced
                        (reduce conj! coll (rank (- top sofar))))
                      (reduce conj! coll (rank ssize)))))
                (transient[])
                (range n 0 -1)))))))))

(defn- init-unigrams
  [lmdb]
  (let [unigrams (ObjectIntHashMap.)
        load     (fn [kv]
                   (let [id   (b/read-buffer (l/k kv) :int)
                         term (b/read-buffer (l/v kv) :string)]
                     (.put unigrams term id)))]
    (l/visit lmdb c/terms load [:all] :int)
    unigrams))

(defn- init-docs
  [lmdb]
  (let [rdocs (IntObjectHashMap.)
        norms (IntShortHashMap.)
        load  (fn [kv]
                (let [doc-ref    (b/read-buffer (l/k kv) :data)
                      [did norm] (b/read-buffer (l/v kv) :int-short)]
                  (.put rdocs did doc-ref)
                  (.put norms did norm)))]
    (l/visit lmdb c/docs load [:all] :data)
    [rdocs norms]))

(defn- init-td-bms
  [lmdb]
  (let [td-bms (IntObjectHashMap.)
        load   (fn [kv]
                 (let [term-id (b/read-buffer (l/k kv) :int)
                       bm      (b/read-buffer (l/v kv) :bitmap)]
                   (.put td-bms term-id bm)))]
    (l/visit lmdb c/term-docs load [:all] :int)
    td-bms))

(defn- init-max-doc
  [lmdb]
  (if-let [doc-id (ffirst (l/get-first lmdb c/positions [:all-back]
                                       :int-int :ignore))]
    doc-id
    0))

(defn- init-max-term
  [lmdb]
  (or (first (l/get-first lmdb c/terms [:all-back] :int :ignore)) 0))

(defn- open-dbis
  [lmdb]
  (assert (not (l/closed-kv? lmdb)) "LMDB env is closed.")
  (l/open-dbi lmdb c/terms Integer/BYTES c/+max-term-length+)
  (l/open-dbi lmdb c/docs c/+max-key-size+ (+ Integer/BYTES Short/BYTES))
  (l/open-dbi lmdb c/term-freq (* 2 Integer/BYTES) Short/BYTES)
  (l/open-dbi lmdb c/term-docs Integer/BYTES)
  (l/open-inverted-list lmdb c/positions (* 2 Integer/BYTES)
                        (* 2 Integer/BYTES)))

(defn new-engine
  ([lmdb]
   (new-engine lmdb {}))
  ([lmdb {:keys [fuzzy?]
          :or   {fuzzy? false}}]
   (open-dbis lmdb)
   (let [[rdocs norms] (init-docs lmdb)]
     (->SearchEngine lmdb
                     (init-unigrams lmdb)
                     (init-td-bms lmdb)
                     norms
                     rdocs
                     (when fuzzy? (SymSpell. {} {} c/dict-max-edit-distance
                                             c/dict-prefix-length))
                     (AtomicInteger. (init-max-doc lmdb))
                     (AtomicInteger. (init-max-term lmdb))))))

(defprotocol IIndexWriter
  (write [this doc-ref doc-text] [this doc-ref doc-text positions?]
    "Write a document")
  (commit [this] "Commit writes"))

(deftype IndexWriter [lmdb
                      ^ObjectIntHashMap unigrams
                      ^IntObjectHashMap td-bms
                      ^AtomicInteger max-doc
                      ^AtomicInteger max-term
                      ^FastList txs]
  IIndexWriter
  (write [this doc-ref doc-text]
    (.write this doc-ref doc-text false))
  (write [this doc-ref doc-text positions?]
    (doseq [tid (add-doc-txs doc-text max-doc txs doc-ref nil nil
                             unigrams max-term positions? td-bms)]
      (.add txs [:put c/term-docs tid (.get td-bms tid) :int :bitmap]))
    (when (< 1000000 (.size txs))
      (.commit this)
      (.clear txs)))

  (commit [this]
    (l/transact-kv lmdb txs)))

(defn index-writer
  [lmdb]
  (open-dbis lmdb)
  (->IndexWriter lmdb
                 (init-unigrams lmdb)
                 (init-td-bms lmdb)
                 (AtomicInteger. (init-max-doc lmdb))
                 (AtomicInteger. (init-max-term lmdb))
                 (FastList.)))

(comment

  (def lmdb  (l/open-kv "search-bench/data/wiki-datalevin-2"))

  (l/open-dbi lmdb c/docs)

  (def engine (time (new-engine lmdb)))
  (l/get-range lmdb c/term-docs [:all] :int :bitmap false)

  (let [m (volatile! 0)
        f (fn [kv]
            (let [[p _] (b/read-buffer (l/v kv) :int-int)]
              (when (< @m p)
                (vreset! m p))))]
    (l/visit-list lmdb c/positions f [1 1] :int-int)
    @m)

  (time (.getCardinality (l/get-value lmdb c/term-docs 1 :int :bitmap)))
  (time (l/list-count lmdb c/positions [11 17655 ] :int-int ))

  (l/get-range lmdb c/positions [:all] :int-int :int-int)

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
