(ns ^:no-doc datalevin.search
  "Full-text search engine"
  (:require [datalevin.lmdb :as l]
            [datalevin.util :as u]
            [datalevin.sparselist :as sl]
            [datalevin.constants :as c]
            [datalevin.bits :as b]
            [clojure.string :as s])
  (:import [datalevin.utl PriorityQueue GrowingIntArray]
           [datalevin.sparselist SparseIntArrayList]
           [java.util HashMap ArrayList Map$Entry Arrays]
           [java.util.concurrent.atomic AtomicInteger]
           [java.io Writer]
           [org.eclipse.collections.impl.map.mutable.primitive IntShortHashMap
            IntDoubleHashMap]
           [org.eclipse.collections.impl.list.mutable FastList]
           [org.eclipse.collections.impl.map.mutable UnifiedMap]
           [org.roaringbitmap RoaringBitmap FastAggregation
            FastRankRoaringBitmap PeekableIntIterator]))

(if (u/graal?)
  (require 'datalevin.binding.graal)
  (require 'datalevin.binding.java))

(defn- non-token-char?
  [^Character c]
  (or (Character/isWhitespace c) (c/en-punctuations? c)))

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
                (when-not (c/en-stop-words? word)
                  (.add res [word pos start]))
                (recur (inc i) (inc pos) false i (StringBuilder.)))
              (recur (inc i) pos false i sb))
            (recur (inc i) pos true (if in? start i)
                   (.append sb (Character/toLowerCase c)))))
        (let [c (.charAt x len-1)]
          (if (non-token-char? c)
            res
            (let [word (.toString sb)]
              (when-not (c/en-stop-words? word)
                (.add res [word pos start]))
              res)))))))

(defn- collect-terms
  [result]
  (let [terms (HashMap.)]
    (doseq [[term position offset] result]
      (when (< (count term) c/+max-term-length+)
        (.put terms term (if-let [^FastList lst (.get terms term)]
                           (do (.add lst [position offset]) lst)
                           (doto (FastList.) (.add [position offset]))))))
    terms))

(defn- idf
  "inverse document frequency of a term"
  [^long freq N]
  (if (zero? freq) 0 (Math/log10 (/ ^long N freq))))

(defn- tf*
  "log-weighted term frequency"
  [freq]
  (if (zero? ^short freq) 0 (+ (Math/log10 ^short freq) 1)))

(defn- add-max-weight
  [mw tf norm]
  (let [w (/ ^double (tf* tf) ^short norm)]
    (if (< ^double mw w) w mw)))

(defn- del-max-weight
  [^SparseIntArrayList sl doc-id mw tf norm]
  (let [w (/ ^double (tf* tf) ^short norm)]
    (if (= mw w)
      (if (= (sl/size sl) 1)
        0.0
        (apply max (map #(/ (if (= doc-id %)
                              0.0
                              ^double (tf* (sl/get sl %)))
                            ^short norm)
                        (.-indices sl))))
      mw)))

(defn- priority-queue
  [top]
  (proxy [PriorityQueue] [top]
    (lessThan [a b]
      (< ^double (nth a 0) ^double (nth b 0)))))

(defn- pouring
  [coll ^PriorityQueue pq ^RoaringBitmap result]
  (let [lst (ArrayList.)]
    (dotimes [_ (.size pq)]
      (let [[_ did :as res] (.pop pq)]
        (.add lst 0 res)
        (.add result (int did))))
    (reduce conj! coll lst)))

(defn- real-score
  [tid did tf ^IntDoubleHashMap wqs ^IntShortHashMap norms]
  (/ (* ^double (.get wqs tid) ^double (tf* tf))
     (double (.get norms did))))

(defn- max-score
  [^IntDoubleHashMap wqs ^IntDoubleHashMap mws tid]
  (* ^double (.get wqs tid) ^double (.get mws tid)))

(defn- get-ws
  [tids qterms k]
  (let [m (IntDoubleHashMap.)]
    (dorun (map (fn [tid qterm] (.put m tid (qterm k))) tids qterms))
    m))

(defn- get-mxs [tids wqs mws]
  (let [m (IntDoubleHashMap.)]
    (doseq [tid tids] (.put m tid (max-score wqs mws tid)))
    m))

(defprotocol ^:no-doc ICandidate
  (skip-before [this limit] "move the iterator to just before the limit")
  (advance [this] "move the iterator to the next position")
  (has-next? [this] "return true if there's next in iterator")
  (get-did [this] "return the current did the iterator points to")
  (get-tf [this] "return tf of the current did"))

(deftype ^:no-doc Candidate [tid
                             ^:volatile-mutable did
                             ^SparseIntArrayList sl
                             ^PeekableIntIterator iter]
  ICandidate
  (skip-before [this limit]
    (.advanceIfNeeded iter limit)
    this)

  (has-next? [_]
    (.hasNext iter))

  (advance [this]
    (set! did (.next iter))
    this)

  (get-did [_]
    did)

  (get-tf [_]
    (.get ^GrowingIntArray (.-items sl)
          (dec (.rank ^FastRankRoaringBitmap (.-indices sl) did)))))

(def ^:no-doc candidate-comp
  (comparator (fn [^Candidate a ^Candidate b]
                (- ^int (get-did a) ^int (get-did b)))))

(defmethod print-method Candidate [^Candidate c, ^Writer w]
  (.write w (pr-str {:tid       (.-tid c)
                     :did       (get-did c)
                     :tf        (get-tf c)
                     :sl        (.-sl c)
                     :has-next? (has-next? c)})))

(defn- find-pivot
  [^IntDoubleHashMap mxs tao-1 minimal-score
   ^"[Ldatalevin.search.Candidate;" candidates]
  (let [n (alength candidates)]
    (loop [score 0.0 p 0]
      (if (< p n)
        (let [candidate ^Candidate (aget candidates p)
              s         (+ score ^double (.get mxs (.-tid candidate)))]
          (if (and (<= ^long tao-1 p) (< ^double minimal-score s))
            [s p (get-did candidate)]
            (recur s (inc p))))
        (let [n-1 (dec n)]
          [score n-1 (get-did (aget candidates n-1))])))))

(defn- score-pivot
  [wqs ^IntDoubleHashMap mxs norms pivot-did minimal-score mxscore tao n
   ^"[Ldatalevin.search.Candidate;" candidates]
  (let [c (alength candidates)]
    (loop [score mxscore hits 0 k 0]
      (if (< k c)
        (let [candidate ^Candidate (aget candidates k)
              did       (get-did candidate)]
          (if (= ^int did ^int pivot-did)
            (let [h (inc hits)]
              (if (< ^long (+ h ^long (- ^long n k 1)) ^long tao)
                :prune
                (let [tid (.-tid candidate)
                      s   (+ (- ^double score ^double (.get mxs tid))
                             ^double (real-score tid did (get-tf candidate)
                                                 wqs norms))]
                  (if (< s ^double minimal-score)
                    :prune
                    (recur s h (inc k))))))
            score))
        score))))

(defn- first-candidates
  [sls bms tids ^RoaringBitmap result tao n]
  (let [z          (inc (- ^long n ^long tao))
        union-tids (set (take z tids))
        union-bms  (->> (select-keys bms union-tids)
                        vals
                        (into-array RoaringBitmap))
        union-bm   (FastAggregation/or
                     ^"[Lorg.roaringbitmap.RoaringBitmap;" union-bms)
        lst        (ArrayList.)]
    (doseq [tid tids]
      (let [sl   (sls tid)
            bm   ^RoaringBitmap (bms tid)
            bm'  (let [iter-bm (.clone bm)]
                   (if (or (union-tids tid) (= tao 1))
                     iter-bm
                     (doto ^RoaringBitmap iter-bm
                       (.and ^RoaringBitmap union-bm))))
            bm'  (doto ^RoaringBitmap bm' (.andNot result))
            iter (.getIntIterator ^RoaringBitmap bm')]
        (when (.hasNext ^PeekableIntIterator iter)
          (let [did       (.peekNext iter)
                candidate (->Candidate tid did sl iter)]
            (.add lst (advance candidate))))))
    (.toArray lst ^"[Ldatalevin.search.Candidate;" (make-array Candidate 0))))

(defn- next-candidates
  [did ^"[Ldatalevin.search.Candidate;" candidates]
  (let [lst (FastList.)]
    (dotimes [i (alength candidates)]
      (let [candidate (aget candidates i)]
        (skip-before candidate (inc ^int did))
        (when (has-next? candidate) (.add lst (advance candidate)))))
    (.toArray lst ^"[Ldatalevin.search.Candidate;" (make-array Candidate 0))))

(defn- skip-candidates
  [pivot pivot-did ^"[Ldatalevin.search.Candidate;" candidates]
  (let [lst (FastList.)]
    (dotimes [i (alength candidates)]
      (let [candidate (aget candidates i)]
        (if (< i ^long pivot)
          (do (skip-before candidate pivot-did)
              (when (has-next? candidate) (.add lst (advance candidate))))
          (.add lst candidate))))
    (.toArray lst ^"[Ldatalevin.search.Candidate;" (make-array Candidate 0))))

(defn- current-threshold
  [^PriorityQueue pq]
  (if (< (.size pq) (.maxSize pq))
    -0.1
    (nth (.top pq) 0)))

(defn- score-term
  [^Candidate candidate ^IntDoubleHashMap mxs wqs norms minimal-score pq]
  (let [tid (.-tid candidate)]
    (when (< ^double minimal-score (.get mxs tid))
      (loop [did (get-did candidate) minscore minimal-score]
        (let [score (real-score tid did (get-tf candidate) wqs norms)]
          (when (< ^double minscore ^double score)
            (.insertWithOverflow ^PriorityQueue pq [score did])))
        (when (has-next? candidate)
          (recur (get-did (advance candidate)) (current-threshold pq)))))))

(defn- score-docs
  [n tids sls bms mxs wqs ^IntShortHashMap norms ^RoaringBitmap result]
  (fn [^PriorityQueue pq ^long tao] ; target # of overlaps between query and doc
    (loop [^"[Ldatalevin.search.Candidate;" candidates
           (first-candidates sls bms tids result tao n)]
      (let [nc            (alength candidates)
            minimal-score ^double (current-threshold pq)]
        (cond
          (or (= nc 0) (< nc tao)) :finish
          (= nc 1)
          (score-term (aget candidates 0) mxs wqs norms minimal-score pq)
          :else
          (let [_                   (Arrays/sort candidates candidate-comp)
                [mxscore pivot did] (find-pivot mxs (dec tao) minimal-score
                                                candidates)]
            (if (= ^int did ^int (get-did (aget candidates 0)))
              (let [score (score-pivot wqs mxs norms did minimal-score
                                       mxscore tao n candidates)]
                (when-not (= score :prune) (.insertWithOverflow pq [score did]))
                (recur (next-candidates did candidates)))
              (recur (skip-candidates pivot did candidates)))))))))

(defprotocol ISearchEngine
  (add-doc [this doc-ref doc-text])
  (remove-doc [this doc-ref])
  (clear-docs [this])
  (doc-indexed? [this doc-ref])
  (doc-count [this])
  (doc-refs [this])
  (search [this query] [this query opts])
  (analyzer [this])
  (terms-dbi [this])
  (docs-dbi [this])
  (positions-dbi [this])
  (max-doc [this])
  (max-term [this])
  (lmdb [this]))

(declare doc-ref->id remove-doc* add-doc-txs hydrate-query display-xf)

(deftype ^:no-doc SearchEngine [lmdb
                                analyzer
                                query-analyzer
                                terms-dbi
                                docs-dbi
                                positions-dbi
                                ^IntShortHashMap norms ; doc-id -> norm
                                ^AtomicInteger max-doc
                                ^AtomicInteger max-term]
  ISearchEngine
  (add-doc [this doc-ref doc-text]
    (locking this
      (try
        (when-not (s/blank? doc-text)
          (when-let [doc-id (doc-ref->id this doc-ref)]
            (remove-doc* this norms doc-id))
          (let [txs       (FastList.)
                hit-terms (UnifiedMap.)]
            (add-doc-txs this norms doc-text txs doc-ref hit-terms)
            (doseq [^Map$Entry kv (.entrySet hit-terms)]
              (let [term (.getKey kv)
                    info (.getValue kv)]
                (.add txs [:put terms-dbi term info :string :term-info])))
            (l/transact-kv lmdb txs)))
        (catch Exception e
          (u/raise "Error indexing document:" (ex-message e)
                   {:doc-ref doc-ref :doc-text doc-text})))))

  (remove-doc [this doc-ref]
    (if-let [doc-id (doc-ref->id this doc-ref)]
      (remove-doc* this norms doc-id)
      (u/raise "Document does not exist." {:doc-ref doc-ref})))

  (clear-docs [this]
    (l/clear-dbi lmdb terms-dbi)
    (l/clear-dbi lmdb docs-dbi)
    (l/clear-dbi lmdb positions-dbi))

  (doc-indexed? [this doc-ref] (doc-ref->id this doc-ref))

  (doc-count [_] (l/entries lmdb docs-dbi))

  (doc-refs [_]
    (map second (l/get-range lmdb docs-dbi [:all] :int :doc-info true)))

  (search [this query]
    (.search this query {}))
  (search [this query {:keys [display ^long top doc-filter]
                       :or   {display    :refs
                              top        10
                              doc-filter (constantly true)}}]
    (when-not (s/blank? query)
      (let [tokens (->> (query-analyzer query)
                        (mapv first)
                        (into-array String))
            qterms (->> (hydrate-query this max-doc tokens)
                        (sort-by :df)
                        vec)
            n      (count qterms)]
        (when-not (zero? n)
          (let [tids    (mapv :id qterms)
                sls     (mapv :sl qterms)
                bms     (zipmap tids (mapv #(.-indices ^SparseIntArrayList %)
                                           sls))
                sls     (zipmap tids sls)
                tms     (zipmap tids (mapv :tm qterms))
                mws     (get-ws tids qterms :mw)
                wqs     (get-ws tids qterms :wq)
                mxs     (get-mxs tids wqs mws)
                result  (RoaringBitmap.)
                scoring (score-docs n tids sls bms mxs wqs norms result)]
            (sequence
              (display-xf this doc-filter display tms)
              (persistent!
                (reduce
                  (fn [coll tao]
                    (let [so-far (count coll)
                          to-get (- top so-far)]
                      (if (< 0 to-get)
                        (let [^PriorityQueue pq (priority-queue to-get)]
                          (scoring pq tao)
                          (pouring coll pq result))
                        (reduced coll))))
                  (transient [])
                  (range n 0 -1)))))))))

  (analyzer [_]
    analyzer)

  (terms-dbi [_]
    terms-dbi)

  (docs-dbi [_]
    docs-dbi)

  (positions-dbi [_]
    positions-dbi)

  (max-doc [_]
    max-doc)

  (max-term [_]
    max-term)

  (lmdb [_]
    lmdb))

(defn- get-term-info
  [engine term]
  (l/get-value (lmdb engine) (terms-dbi engine) term :string :term-info))

(defn- doc-ref->id
  [engine doc-ref]
  (let [is-ref? (fn [kv]
                  (= doc-ref (nth (b/read-buffer (l/v kv) :doc-info) 1)))]
    (nth (l/get-some (lmdb engine) (docs-dbi engine) is-ref? [:all]
                     :int :doc-info) 0)))

(defn- doc-id->term-ids
  [engine doc-id]
  (dedupe
    (map ffirst
         (l/range-filter (lmdb engine) (positions-dbi engine)
                         (fn [kv]
                           (let [[_ did] (b/read-buffer (l/k kv) :int-int)]
                             (= did doc-id)))
                         [:all] :int-int :ignore false))))

(defn- term-id->info
  [engine term-id]
  (let [is-id? (fn [kv] (= term-id (b/read-buffer (l/v kv) :int)))]
    (l/get-some (lmdb engine) (terms-dbi engine) is-id? [:all]
                :string :term-info false)))

(defn- remove-doc*
  [engine ^IntShortHashMap norms doc-id]
  (let [txs  (FastList.)
        norm (.get norms doc-id)]
    (.remove norms doc-id)
    (.add txs [:del (docs-dbi engine) doc-id :int])
    (doseq [term-id (doc-id->term-ids engine doc-id)]
      (let [[term [_ mw sl]] (term-id->info engine term-id)]
        (when-let [tf (sl/get sl doc-id)]
          (.add txs [:put (terms-dbi engine) term
                     [term-id
                      (del-max-weight sl doc-id mw tf norm)
                      (sl/remove sl doc-id)]
                     :string :term-info])))
      (.add txs [:del (positions-dbi engine) [term-id doc-id] :int-int]))
    (l/transact-kv (lmdb engine) txs)))

(defn- add-doc-txs
  [engine ^IntShortHashMap norms doc-text  ^FastList txs doc-ref
   ^UnifiedMap hit-terms]
  (let [result    ((analyzer engine) doc-text)
        new-terms ^HashMap (collect-terms result)
        unique    (.size new-terms)
        doc-id    (.incrementAndGet ^AtomicInteger (max-doc engine))]
    (.add txs [:put (docs-dbi engine) doc-id [unique doc-ref] :int :doc-info])
    (when norms (.put norms doc-id unique))
    (doseq [^Map$Entry kv (.entrySet new-terms)]
      (let [term    (.getKey kv)
            new-lst ^FastList (.getValue kv)
            tf      (.size new-lst)
            [tid mw sl]
            (or (.get hit-terms term)
                (get-term-info engine term)
                [(.incrementAndGet ^AtomicInteger (max-term engine))
                 0.0
                 (sl/sparse-arraylist)])]
        (.put hit-terms term [tid (add-max-weight mw tf unique)
                              (sl/set sl doc-id tf)])
        (.add txs [:put-list (positions-dbi engine) [tid doc-id] new-lst
                   :int-int :int-int])))))

(defn- hydrate-query
  [engine ^AtomicInteger max-doc tokens]
  (into []
        (comp
          (map (fn [[term freq]]
                 (when-let [[id mw ^SparseIntArrayList sl]
                            (get-term-info engine term)]
                   (let [df (sl/size sl)
                         sl (sl/->SparseIntArrayList
                              (doto (FastRankRoaringBitmap.)
                                (.or ^RoaringBitmap (.-indices sl)))
                              (.-items sl))]
                     {:df df
                      :id id
                      :mw mw
                      :sl sl
                      :tm term
                      :wq (* ^double (tf* freq)
                             ^double (idf df (.get max-doc)))}))))
          (filter map?))
        (frequencies tokens)))

(defn- get-doc-ref
  [engine doc-filter [_ doc-id]]
  (when-let [doc-ref (nth (l/get-value (lmdb engine) (docs-dbi engine)
                                       doc-id :int :doc-info) 1)]
    (when (doc-filter doc-ref) doc-ref)))

(defn- add-offsets
  [engine doc-filter terms [_ doc-id :as result]]
  (when-let [doc-ref (get-doc-ref engine doc-filter result)]
    [doc-ref
     (sequence
       (comp (map (fn [tid]
                 (let [lst (l/get-list (lmdb engine) (positions-dbi engine)
                                       [tid doc-id] :int-int :int-int)]
                   (when (seq lst)
                     [(terms tid) (mapv #(nth % 1) lst)]))))
          (remove nil? ))
       (keys terms))]))

(defn- display-xf
  [^SearchEngine engine doc-filter display tms]
  (case display
    :offsets (comp (map #(add-offsets engine doc-filter tms %))
                (remove nil?))
    :refs    (comp (map #(get-doc-ref engine doc-filter %))
                (remove nil?))))

(defn- init-norms
  [lmdb docs-dbi]
  (let [norms (IntShortHashMap.)
        load  (fn [kv]
                (let [doc-id (b/read-buffer (l/k kv) :int)
                      norm   (b/read-buffer (l/v kv) :short)]
                  (.put norms doc-id norm)))]
    (l/visit lmdb docs-dbi load [:all] :int)
    norms))

(defn- init-max-doc
  [lmdb docs-dbi]
  (or (first (l/get-first lmdb docs-dbi [:all-back] :int :ignore)) 0))

(defn- init-max-term
  [lmdb positions-dbi]
  (or (first (l/get-first lmdb positions-dbi [:all-back] :int :ignore)) 0))

(defn- open-dbis
  [lmdb terms-dbi docs-dbi positions-dbi]
  (assert (not (l/closed-kv? lmdb)) "LMDB env is closed.")
  (l/open-dbi lmdb terms-dbi {:key-size c/+max-key-size+})
  (l/open-dbi lmdb docs-dbi {:key-size Integer/BYTES})
  (l/open-inverted-list lmdb positions-dbi {:key-size (* 2 Integer/BYTES)
                                            :val-size c/+max-key-size+}))

(defn new-search-engine
  ([lmdb]
   (new-search-engine lmdb nil))
  ([lmdb {:keys [domain analyzer query-analyzer]
          :or   {analyzer en-analyzer domain "datalevin"}}]
   (let [terms-dbi     (str domain "/" c/terms)
         docs-dbi      (str domain "/" c/docs)
         positions-dbi (str domain "/" c/positions)]
     (open-dbis lmdb terms-dbi docs-dbi positions-dbi)
     (->SearchEngine lmdb
                     analyzer
                     (or query-analyzer analyzer)
                     terms-dbi
                     docs-dbi
                     positions-dbi
                     (init-norms lmdb docs-dbi)
                     (AtomicInteger. (init-max-doc lmdb docs-dbi))
                     (AtomicInteger. (init-max-term lmdb positions-dbi))))))

(defn transfer
  "transfer state of an existing engine to an new engine that has a
  different LMDB instance"
  [^SearchEngine old lmdb]
  (locking old
    (->SearchEngine lmdb
                    (.-analyzer old)
                    (.-query-analyzer old)
                    (.-terms-dbi old)
                    (.-docs-dbi old)
                    (.-positions-dbi old)
                    (.-norms old)
                    (.-max-doc old)
                    (.-max-term old))))

(defprotocol IIndexWriter
  (write [this doc-ref doc-text])
  (commit [this]))

(deftype ^:no=doc IndexWriter [lmdb
                               analyzer
                               terms-dbi
                               docs-dbi
                               positions-dbi
                               ^AtomicInteger max-doc
                               ^AtomicInteger max-term
                               ^FastList txs
                               ^UnifiedMap hit-terms]
  IIndexWriter
  (write [this doc-ref doc-text]
    (when-not (s/blank? doc-text)
      (add-doc-txs this nil doc-text txs doc-ref hit-terms)
      (when (< 10000000 (.size txs))
        (.commit this))))

  (commit [this]
    (l/transact-kv lmdb txs)
    (.clear txs)
    (loop [iter (.iterator (.entrySet hit-terms))]
      (when (.hasNext iter)
        (let [^Map$Entry kv (.next iter)]
          (.add txs [:put terms-dbi (.getKey kv) (.getValue kv)
                     :string :term-info])
          (.remove iter)
          (recur iter))))
    (l/transact-kv lmdb txs)
    (.clear txs))

  ISearchEngine
  (analyzer [_]
    analyzer)

  (terms-dbi [_]
    terms-dbi)

  (docs-dbi [_]
    docs-dbi)

  (positions-dbi [_]
    positions-dbi)

  (max-doc [_]
    max-doc)

  (max-term [_]
    max-term)

  (lmdb [_]
    lmdb))

(defn search-index-writer
  ([lmdb]
   (search-index-writer lmdb nil))
  ([lmdb {:keys [domain analyzer]
          :or   {domain "datalevin" analyzer en-analyzer}}]
   (let [terms-dbi     (str domain "/" c/terms)
         docs-dbi      (str domain "/" c/docs)
         positions-dbi (str domain "/" c/positions)]
     (open-dbis lmdb terms-dbi docs-dbi positions-dbi)
     (->IndexWriter lmdb
                    analyzer
                    terms-dbi
                    docs-dbi
                    positions-dbi
                    (AtomicInteger. (init-max-doc lmdb docs-dbi))
                    (AtomicInteger. (init-max-term lmdb positions-dbi))
                    (FastList.)
                    (UnifiedMap.)))))

(comment

  (def lmdb  (l/open-kv "search-bench/data/wiki-datalevin-all"))

  (time (search-index-writer lmdb))
  (def engine (time (new-search-engine lmdb)))
  (.size (peek (l/get-value lmdb "datalevin/terms"
                            "s" :string :term-info))) ; over 3 mil.

  (time (search engine "s"))
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
