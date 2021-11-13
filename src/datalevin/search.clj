(ns datalevin.search
  "Full-text search engine"
  (:require [datalevin.lmdb :as l]
            [datalevin.util :as u]
            [datalevin.sparselist :as sl]
            [datalevin.constants :as c]
            [datalevin.bits :as b])
  (:import [datalevin.sm SymSpell SuggestItem]
           [datalevin.utl PriorityQueue]
           [datalevin.sparselist SparseIntArrayList]
           [java.util HashMap ArrayList Map$Entry]
           [java.util.concurrent.atomic AtomicInteger]
           [java.io Writer]
           [org.eclipse.collections.impl.map.mutable.primitive IntShortHashMap]
           [org.eclipse.collections.impl.list.mutable FastList]
           [org.roaringbitmap RoaringBitmap FastAggregation
            FastRankRoaringBitmap PeekableIntIterator]))

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

(defn- get-tf [sls doc-id term-id] (sl/get (sls term-id) doc-id))

(defn- idf
  "inverse document frequency of a term"
  [^long freq N]
  (float (if (zero? freq) 0 (Math/log10 (/ ^int N freq)))))

(defn- tf*
  "log-weighted term frequency"
  [freq]
  (float (if (zero? ^short freq) 0 (+ (Math/log10 ^short freq) 1))))

(defn- tf
  [sls doc-id term-id]
  (tf* (get-tf sls doc-id term-id)))

(defn- add-max-weight
  [mw tf norm]
  (let [w (float (/ ^float (tf* tf) ^short norm))]
    (if (< ^float mw w) w mw)))

(defn- del-max-weight
  [^SparseIntArrayList sl doc-id mw tf norm]
  (let [w (float (/ ^float (tf* tf) ^short norm))]
    (if (= mw w)
      (if (= (sl/size sl) 1)
        (float 0.0)
        (apply max (map #(/ (float (if (= doc-id %)
                                     0.0
                                     (tf* (sl/get sl %))))
                            ^short norm)
                        (.-indices sl))))
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
            [tid mw sl] (or (.get hit-terms term)
                            (when pre-terms (.get pre-terms term))
                            (get-term-info lmdb term)
                            [(.incrementAndGet max-term) (float 0.0)
                             (sl/sparse-arraylist)])]
        (.put hit-terms term [tid (add-max-weight mw tf unique)
                              (sl/set sl doc-id tf)])
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
                   (when-let [[id mw sl] (get-term-info lmdb term)]
                     (let [df (sl/size sl)]
                       {:df df
                        :ed (eds term)
                        :id id
                        :mw mw
                        :sl sl
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
  [coll ^PriorityQueue pq ^RoaringBitmap result]
  (let [lst (ArrayList.)]
    (dotimes [_ (.size pq)]
      (let [[_ did :as res] (.pop pq)]
        (.add lst 0 res)
        (.add result (int did))))
    ;; (println "lst" lst)
    (reduce conj! coll lst)))

(defn- real-score
  [sls tid did wqs norms]
  (let [res (/ (* ^float (wqs tid) ^float (tf sls did tid))
               ^short (.get ^IntShortHashMap norms did))]
    (println "compute real score for tid" tid "did" did "norm" (.get ^IntShortHashMap norms did) "tf" (get-tf sls did tid) "got" res)
    res))

(defn- max-score [wqs mws tid] (* ^float (wqs tid) ^float (mws tid)))

(defprotocol ICandidate
  (skip-before [this limit] "move the iterator to just before the limit")
  (advance [this] "move the iterator to the next position, return its value")
  (has-next? [this] "return true if there's next in iterator")
  (get-did [this] "return the current did the iterator points to"))

(deftype Candidate [tid
                    ^:volatile-mutable did
                    ^PeekableIntIterator iter]
  ICandidate
  (skip-before [_ limit]
    (.advanceIfNeeded iter limit))
  (has-next? [_]
    (.hasNext iter))
  (advance [_]
    (set! did (.next iter))
    did)
  (get-did [_]
    did))

(defmethod print-method Candidate [^Candidate c, ^Writer w]
  (.write w
          (pr-str {:tid       (.-tid c)
                   :did       (get-did c)
                   :has-next? (has-next? c)})))

(defn- find-pivot
  [mxs tao-1 minimal-score candidates]
  (let [res (reduce-kv
              (fn [score ^long p ^Candidate candidate]
                (let [s (+ ^float score ^float (mxs (.-tid candidate)))]
                  (if (and (<= ^long tao-1 p) (< ^float minimal-score s))
                    (reduced [s p (get-did candidate)])
                    s)))
              (float 0.0)
              candidates)]
    (if (vector? res)
      res
      [res (dec (count candidates)) (get-did (peek candidates))])))

(defn- score-pivot
  [wqs mxs sls norms pivot-did minimal-score mxscore tao n candidates]
  (let [res (reduce-kv
              (fn [[score ^long hits] ^long k ^Candidate candidate]
                (let [tid (.-tid candidate)
                      did (get-did candidate)]
                  (if (= did pivot-did)
                    (let [h (inc hits)]
                      (if (< ^long (+ h ^long (- ^long n k 1)) ^long tao)
                        (reduced :prune)
                        (let [s (+ (- ^float score ^float (mxs tid))
                                   ^float (real-score sls tid did wqs norms))]
                          (if (< s ^float minimal-score)
                            (reduced :prune)
                            [s h]))))
                    (reduced [score hits]))))
              [mxscore 0]
              candidates)]
    (if (= res :prune) res (nth res 0))))

(def candidate-comp
  (comparator
    (fn [^Candidate a ^Candidate b]
      (compare (get-did a) (get-did b)))))

(defn- first-candidates
  [bms tids ^RoaringBitmap result tao n]
  (let [z          (inc (- ^long n ^long tao))
        union-tids (set (take z tids))
        union-bms  (->> (select-keys bms union-tids)
                        vals
                        (into-array RoaringBitmap))
        union-bm   (FastAggregation/or
                     ^"[Lorg.roaringbitmap.RoaringBitmap;" union-bms)
        cnd-bms    (into {} (for [tid tids]
                              [tid (doto (FastRankRoaringBitmap.)
                                     (.or ^RoaringBitmap (bms tid)))]))]
    (reduce
      (fn [cs tid]
        (let [bm   (cnd-bms tid)
              bm   (if (or (union-tids tid) (= tao 1))
                     bm
                     (doto ^FastRankRoaringBitmap bm
                       (.and ^RoaringBitmap union-bm)))
              bm   (doto ^FastRankRoaringBitmap bm (.andNot result))
              iter (.getIntIterator ^FastRankRoaringBitmap bm)]
          (when (.hasNext ^PeekableIntIterator iter)
            (let [candidate (->Candidate tid -1 iter)]
              (advance candidate)
              (conj cs candidate)))))
      []
      tids)))

(defn- next-candidates
  [did candidates]
  ;; (println "everyone moves beyond" did)
  (doseq [candidate candidates]
    (skip-before candidate (inc ^int did)))
  (let [live-cs (vec (filter #(has-next? %) candidates))]
    (doseq [candidate live-cs] (advance candidate))
    live-cs))

(defn- skip-candidates
  [pivot pivot-did candidates]
  ;; (println pivot "iterators skip to" pivot-did)
  (let [prev-cs (take pivot candidates)]
    (doseq [candidate prev-cs]
      (skip-before candidate pivot-did))
    (let [prev-live-cs (vec (filter #(has-next? %) prev-cs))]
      (doseq [candidate prev-live-cs] (advance candidate))
      (into prev-live-cs (drop pivot candidates)))))

(defn- current-threshold
  [^PriorityQueue pq]
  (if (< (.size pq) (.maxSize pq))
    (float 0.0)
    (nth (.top pq) 0)))

(defn- score-term
  [^Candidate candidate sls mxs wqs norms minimal-score pq]
  (let [tid     (.-tid candidate)
        mxscore (mxs tid)]
    (println "score for term" tid)
    (when (< ^float minimal-score ^float mxscore)
      (loop [did (get-did candidate) minscore minimal-score]
        (let [score (real-score sls tid did wqs norms)]
          (when (< ^float minscore ^float score)
            (.insertWithOverflow ^PriorityQueue pq [score did])))
        (when (has-next? candidate)
          (recur (advance candidate) (current-threshold pq)))))))

(defn- score-docs
  [n tids sls bms mxs wqs ^IntShortHashMap norms ^RoaringBitmap result]
  (fn [^PriorityQueue pq ^long tao] ; target # of overlaps between query and doc
    (println "working on tao" tao "with result" result)
    (loop [candidates (first-candidates bms tids result tao n)]
      (let [nc            (count candidates)
            minimal-score ^float (current-threshold pq)]
        (println "tao" tao "nc" nc )
        (cond
          (or (= nc 0) (< nc tao)) :finish
          (= nc 1)
          (score-term (nth candidates 0) sls mxs wqs norms minimal-score pq)
          :else
          (let [candidates          (vec (sort-by get-did candidates))
                [mxscore pivot did] (find-pivot mxs (dec tao) minimal-score
                                                candidates)
                did0                (get-did (nth candidates 0))]
            (println "found pivot" did "when did0 is" did0)
            (if (= ^int did ^int did0)
              (let [score (score-pivot wqs mxs sls norms did minimal-score
                                       mxscore tao n candidates)]
                (println "real score" score)
                (when-not (= score :prune) (.insertWithOverflow pq [score did]))
                (recur (next-candidates did candidates)))
              (recur (skip-candidates pivot did candidates)))))))))

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

#_(defn- intersect-bitmaps
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

#_(defn- score-docs
   [n term-ids dfs sls mxs wqs norms result]
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
          (let [[term [_ mw sl]] (term-id->info lmdb term-id)
                tf               (sl/get sl doc-id)]
            (.add txs [:put c/terms term
                       [term-id
                        (del-max-weight sl doc-id mw tf norm)
                        (sl/remove sl doc-id)]
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
        (let [tids     (mapv :id qterms)
              sls      (mapv :sl qterms)
              bms      (zipmap tids (mapv #(.-indices ^SparseIntArrayList %)
                                          sls))
              sls      (zipmap tids sls)
              wqs      (zipmap tids (mapv :wq qterms))
              tms      (zipmap tids (mapv :tm qterms))
              mws      (zipmap tids (mapv :mw qterms))
              mxs      (zipmap tids (map #(max-score wqs mws %) tids))
              result   (RoaringBitmap.)
              score-fn (score-docs n tids sls bms mxs wqs norms result)]
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
  (l/open-inverted-list lmdb c/positions (* 2 Integer/BYTES) c/+max-key-size+))

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
    (when (< 10000000 (.size txs))
      (l/transact-kv lmdb txs)
      (.clear txs)))

  (commit [this]
    (doseq [^Map$Entry kv (.entrySet hit-terms)]
      (let [term (.getKey kv)
            info (.getValue kv)]
        (.add txs [:put c/terms term info :string :term-info])))
    (l/transact-kv lmdb txs)))

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

  (def lmdb  (l/open-kv "search-bench/data/wiki-datalevin-odd"))

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
