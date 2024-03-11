(ns ^:no-doc datalevin.search
  "Full-text search engine"
  (:require
   [datalevin.lmdb :as l]
   [datalevin.util :as u :refer [cond+]]
   [datalevin.spill :as sp]
   [datalevin.sparselist :as sl]
   [datalevin.analyzer :as a]
   [datalevin.constants :as c]
   [datalevin.bits :as b]
   [datalevin.lru :as lru]
   [clojure.string :as s])
  (:import
   [datalevin.utl PriorityQueue GrowingIntArray]
   [datalevin.sparselist SparseIntArrayList]
   [datalevin.spill SpillableMap]
   [datalevin.lmdb IAdmin]
   [java.util ArrayList Map$Entry Arrays]
   [java.util.concurrent.atomic AtomicInteger]
   [java.io Writer]
   [org.eclipse.collections.impl.map.mutable UnifiedMap]
   [org.eclipse.collections.impl.map.mutable.primitive IntShortHashMap
    IntDoubleHashMap]
   [org.eclipse.collections.impl.set.mutable.primitive IntHashSet]
   [org.eclipse.collections.impl.list.mutable FastList]
   [org.eclipse.collections.impl.list.mutable.primitive IntArrayList]
   [org.roaringbitmap RoaringBitmap FastAggregation FastRankRoaringBitmap
    PeekableIntIterator]))

(if (u/graal?)
  (require 'datalevin.binding.graal)
  (require 'datalevin.binding.java))

(defn- collect-terms
  [result]
  (let [terms (UnifiedMap.)]
    (doseq [[term position offset] result]
      (when (< (count term) c/+max-term-length+)
        (.put terms term
              (if-let [[positions offsets] (.get terms term)]
                (do (.add ^IntArrayList positions position)
                    (.add ^IntArrayList offsets offset)
                    [positions offsets])
                [(doto (IntArrayList.) (.add (int position)))
                 (doto (IntArrayList.) (.add (int offset)))]))))
    terms))

(defn idf
  "inverse document frequency of a term"
  [^long freq N]
  (if (zero? freq) 0 (Math/log10 (/ ^long N freq))))

(defn tf*
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
        (apply max (sequence (map #(/ (if (= doc-id %)
                                        0.0
                                        ^double (tf* (sl/get sl %)))
                                      ^short norm))
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

(defprotocol ICandidate
  (skip-before [this limit] "move the iterator to just before the limit")
  (advance [this] "move the iterator to the next position")
  (has-next? [this] "return true if there's next in iterator")
  (get-did [this] "return the current did the iterator points to")
  (get-tf [this did] "return tf of the given did"))

(deftype Candidate [^int tid
                    ^SparseIntArrayList sl
                    ^PeekableIntIterator iter]
  ICandidate
  (skip-before [this limit] (.advanceIfNeeded iter limit) this)

  (advance [this] (.next iter) this)

  (has-next? [_] (.hasNext iter))

  (get-did [_] (.peekNext iter))

  (get-tf [_ did]
    (.get ^GrowingIntArray (.-items sl)
          (dec (.rank ^FastRankRoaringBitmap (.-indices sl) did)))))

(defn- candidate-comp
  [^Candidate a ^Candidate b]
  (- ^int (get-did a) ^int (get-did b)))

(defmethod print-method Candidate [^Candidate c, ^Writer w]
  (.write w (pr-str
              (let [hn? (has-next? c)]
                (cond-> {:tid (.-tid c)
                         :sl  (.-sl c)}
                  hn?  ((fn [m]
                          (let [did (get-did c)]
                            (merge m {:did did
                                      :tf  (get-tf c did)}))))
                  true (merge {:has-next? hn?}))))))

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
                             ^double (real-score tid did
                                                 (get-tf candidate did)
                                                 wqs norms))]
                  (if (< s ^double minimal-score)
                    :prune
                    (recur s h (inc k))))))
            score))
        score))))

(defmacro candidate-array
  [& body]
  `(let [~'lst (FastList.)]
     ~@body
     (.toArray ~'lst ^"[Ldatalevin.search.Candidate;"
               (make-array Candidate (.size ~'lst)))))

(defn- first-candidates
  [sls bms tids ^RoaringBitmap result tao n]
  (let [z          (inc (- ^long n ^long tao))
        union-tids (set (take z tids))
        union-bms  (->> (select-keys bms union-tids)
                        vals
                        (into-array RoaringBitmap))
        union-bm   (FastAggregation/or
                     ^"[Lorg.roaringbitmap.RoaringBitmap;" union-bms)]
    (candidate-array
      (doseq [tid tids]
        (let [bm   ^RoaringBitmap (bms tid)
              bm'  (let [iter-bm (.clone bm)]
                     (if (or (union-tids tid) (= tao 1))
                       iter-bm
                       (doto ^RoaringBitmap iter-bm
                         (.and ^RoaringBitmap union-bm))))
              bm'  (doto ^RoaringBitmap bm' (.andNot result))
              iter (.getIntIterator ^RoaringBitmap bm')]
          (when (.hasNext ^PeekableIntIterator iter)
            (.add lst (Candidate. tid (sls tid) iter))))))))

(defn- next-candidates
  [did ^"[Ldatalevin.search.Candidate;" candidates]
  (let [did+1 (inc ^int did)]
    (candidate-array
      (dotimes [i (alength candidates)]
        (let [candidate (aget candidates i)]
          (skip-before candidate did+1)
          (when (has-next? candidate) (.add lst candidate)))))))

(defn- skip-candidates
  [pivot pivot-did ^"[Ldatalevin.search.Candidate;" candidates]
  (candidate-array
    (dotimes [i (alength candidates)]
      (let [candidate (aget candidates i)]
        (if (< i ^long pivot)
          (do (skip-before candidate pivot-did)
              (when (has-next? candidate) (.add lst candidate)))
          (.add lst candidate))))))

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
        (let [score (real-score tid did (get-tf candidate did) wqs norms)]
          (when (< ^double minscore ^double score)
            (.insertWithOverflow ^PriorityQueue pq [score did])))
        (when (has-next? (advance candidate))
          (recur (get-did candidate) (current-threshold pq)))))))

(defn- tf-idf-scoring
  [sls bms tids result tao n pq mxs wqs norms]
  (loop [^"[Ldatalevin.search.Candidate;" candidates
         (first-candidates sls bms tids result tao n)]
    (let [nc            (alength candidates)
          minimal-score ^double (current-threshold pq)]
      (cond
        (or (= nc 0) (< nc ^long tao)) :finish
        (= nc 1)
        (score-term (aget candidates 0) mxs wqs norms minimal-score pq)
        :else
        (let [_                   (Arrays/sort candidates candidate-comp)
              [mxscore pivot did] (find-pivot mxs (dec ^long tao)
                                              minimal-score
                                              candidates)]
          (if (= ^int did ^int (get-did (aget candidates 0)))
            (let [score (score-pivot wqs mxs norms did minimal-score
                                     mxscore tao n candidates)]
              (when-not (= score :prune)
                (.insertWithOverflow ^PriorityQueue pq [score did]))
              (recur (next-candidates did candidates)))
            (recur (skip-candidates pivot did candidates))))))))

(defprotocol ISearchEngine
  (add-doc [this doc-ref doc-text] [this doc-ref doc-text check-exist?])
  (remove-doc [this doc-ref])
  (clear-docs [this])
  (doc-indexed? [this doc-ref])
  (doc-count [this])
  (search [this query] [this query opts]))

(declare doc-ref->id remove-doc* add-doc* hydrate-query display-xf score-docs
         get-rawtext new-search-engine)

(def default-search-opts {:display             :refs
                          :top                 10
                          :proximity-expansion 2
                          :proximity-max-dist  45
                          :doc-filter          (constantly true)})

(deftype SearchEngine [lmdb
                       analyzer
                       query-analyzer
                       terms-dbi
                       docs-dbi
                       positions-dbi
                       rawtext-dbi
                       ^SpillableMap terms       ; term-id -> term
                       ^SpillableMap docs        ; doc-id -> doc-ref
                       ^IntShortHashMap norms    ; doc-id -> norm
                       cache
                       ^AtomicInteger max-doc
                       ^AtomicInteger max-term
                       index-position?
                       include-text?
                       search-opts]
  ISearchEngine
  (add-doc [this doc-ref doc-text check-exist?]
    (locking docs
      (when-not (s/blank? doc-text)
        (when check-exist?
          (when-let [doc-id (doc-ref->id this doc-ref)]
            (remove-doc* this doc-id doc-ref)))
        (add-doc* this doc-ref doc-text))))
  (add-doc [this doc-ref doc-text]
    (.add-doc this doc-ref doc-text true))

  (remove-doc [this doc-ref]
    (if-let [doc-id (doc-ref->id this doc-ref)]
      (remove-doc* this doc-id doc-ref)
      (u/raise "Document does not exist." {:doc-ref doc-ref})))

  (clear-docs [_]
    (.empty docs)
    (.empty terms)
    (.clear norms)
    (l/clear-dbi lmdb terms-dbi)
    (l/clear-dbi lmdb docs-dbi)
    (l/clear-dbi lmdb positions-dbi)
    (l/clear-dbi lmdb rawtext-dbi))

  (doc-indexed? [this doc-ref] (doc-ref->id this doc-ref))

  (doc-count [_] (count docs))

  (search [this query]
    (.search this query {}))
  (search [this query {:keys [display top proximity-expansion
                              proximity-max-dist doc-filter]
                       :or   {display (:display search-opts)
                              top     (:top search-opts)
                              proximity-expansion
                              (:proximity-expansion search-opts)
                              proximity-max-dist
                              (:proximity-max-dist search-opts)
                              doc-filter
                              (:doc-filter search-opts)}}]
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
                    (let [to-get (- ^long top (count coll))]
                      (if (< 0 to-get)
                        (let [^PriorityQueue pq (priority-queue to-get)]
                          (scoring this pq tao to-get proximity-expansion
                                   proximity-max-dist)
                          (pouring coll pq result))
                        (reduced coll))))
                  (transient [])
                  (range n 0 -1)))))))))

  IAdmin
  (re-index [this opts]
    (if include-text?
      (try
        (let [d    (l/dir lmdb)
              coll (UnifiedMap.)]
          (doseq [[doc-id doc-ref] docs
                  :let             [doc-text (get-rawtext this doc-id)]]
            (.put coll doc-ref doc-text))
          (.clear-docs this)
          (let [^SearchEngine engine (new-search-engine lmdb opts)]
            (doseq [^Map$Entry kv (.entrySet coll)]
              (.add-doc engine (.getKey kv) (.getValue kv) false))
            engine))
        (catch Exception e
          (u/raise "Unable to re-index search. " e {:dir (l/dir lmdb)})))
      (u/raise "Can only re-index search when :include-text? is true" {}))))

(defn- get-pos-info
  [^SearchEngine engine doc-id term-id]
  (lru/-get
    (.-cache engine) [:get-pos-info doc-id term-id]
    #(l/get-value (.-lmdb engine) (.-positions-dbi engine)
                  [doc-id term-id] :int-int :pos-info)))

(defn- get-offsets
  [^SearchEngine engine doc-id term-id]
  (peek (get-pos-info engine doc-id term-id)))

(defprotocol IPositions
  (cur-pos [this] "return the current position, or nil if there is no more")
  (go-next [this] "move cursor to the next position")
  (get-tid [this] "return the term id"))

(deftype Positions [^int tid
                    ^ints positions
                    ^:unsynchronized-mutable cur]
  IPositions
  (cur-pos [_] (when (< ^long cur (alength positions)) (aget positions cur)))
  (go-next [_] (set! cur (u/long-inc cur)))
  (get-tid [_] tid))

(defn- get-positions
  [engine doc-id term-id]
  (when-let [pos-info (get-pos-info engine doc-id term-id)]
    (Positions. term-id (first pos-info) 0)))

(defprotocol ISpan
  (get-n [this] "return the number of terms in the span")
  (get-width [this max-dist] "return the width of the span")
  (add-term [this tid position] "add a term to the span")
  (find-term [this tid] "return the index of a term, or return nil")
  (get-ith-pos [this i] "return the ith position")
  (get-next-pos [this i] "return the i+1'th position")
  (split [this i] "split span into two after ith term"))

(deftype Span [^FastList hits]
  ISpan
  (get-n [_] (.size hits))
  (get-width [_ max-dist]
    (if (= 1 (.size hits))
      max-dist
      (inc (- ^long (peek (.getLast hits)) ^long (peek (.getFirst hits))))))
  (add-term [_ tid position] (.add hits [tid position]))
  (find-term [_ tid] (u/index-of #(= tid (first %)) hits))
  (get-ith-pos [_ i] (peek (.get hits i)))
  (get-next-pos [_ i] (peek (.get hits (inc ^long i))))
  (split [_ i] [(Span. (.take hits ^int i)) (Span. (.drop hits ^int i))]))

(defmethod print-method Span [^Span s, ^Writer w]
  (.write w "[")
  (.write w (str (with-out-str
                   (let [^FastList hits (.-hits s)]
                     (dotimes [i (.size hits)]
                       (print (.get hits i))
                       (print " "))))))
  (.write w "]"))

(defn- segment-doc
  [engine did tids ^long max-dist]
  (let [pos-lst (FastList.)
        spans   (FastList.)]
    (doseq [tid tids]
      (when-let [poss (get-positions engine did tid)]
        (.add pos-lst poss)))
    (when (seq pos-lst)
      (loop [cur-poss (apply min-key cur-pos pos-lst)
             cur-span (Span. (FastList.))]
        (let [^long cpos (cur-pos cur-poss)
              ctid       (get-tid cur-poss)]
          (add-term cur-span ctid cpos)
          (go-next cur-poss)
          (when-not (cur-pos cur-poss) (.remove pos-lst cur-poss))
          (if (.isEmpty pos-lst)
            (.add spans cur-span)
            (let [next-poss  (apply min-key cur-pos pos-lst)
                  ^long npos (cur-pos next-poss)
                  ntid       (get-tid next-poss)
                  ndist      (- npos cpos)]
              (cond+
                (or (< max-dist ndist) (= ctid ntid))
                (let [next-span (Span. (FastList.))]
                  (.add spans cur-span)
                  (recur next-poss next-span))
                :let [found (find-term cur-span ntid)]
                found
                (let [[cur-span next-span]
                      (if (< ndist (- ^long (get-next-pos cur-span found)
                                      ^long (get-ith-pos cur-span found)))
                        (split cur-span found)
                        [cur-span (Span. (FastList.))])]
                  (.add spans cur-span)
                  (recur next-poss next-span))
                :else
                (recur next-poss cur-span)))))))
    spans))

(defn- proximity-score*
  [max-dist did spans ^IntDoubleHashMap wqs ^IntShortHashMap norms tid]
  (let [^double rc (reduce
                     (fn [^double score span]
                       (if (find-term span tid)
                         (+ score
                            (let [^long n (get-n span)
                                  ^long w (get-width span max-dist)]
                              (* (Math/pow (/ n w) 0.25)
                                 (Math/pow n 0.3))))
                         score))
                     0.0 spans)]
    (/ (* ^double (.get wqs tid) rc) (double (.get norms did)))))

(defn- proximity-score
  [engine max-dist tids did wqs norms]
  (when-let [spans (segment-doc engine did tids max-dist)]
    (->> tids
         (map #(proximity-score* max-dist did spans wqs norms %))
         (reduce + 0.0))))

(defn- proximity-scoring
  [engine max-dist tids wqs norms pq0 pq]
  (dotimes [_ (.size ^PriorityQueue pq0)]
    (let [[tscore did] (.pop ^PriorityQueue pq0)]
      (if-let [pscore (proximity-score engine max-dist tids did wqs norms)]
        (.insertWithOverflow ^PriorityQueue pq [pscore did])
        (.insertWithOverflow ^PriorityQueue pq [tscore did])))))

(defn- score-docs
  [n tids sls bms mxs wqs norms result]
  (fn [engine pq tao to-get expansion max-dist]
    (if (.-index-position? ^SearchEngine engine)
      (let [pq0 (priority-queue (* ^long to-get ^long expansion))]
        (tf-idf-scoring sls bms tids result tao n pq0 mxs wqs norms)
        (proximity-scoring engine max-dist tids wqs norms pq0 pq))
      (tf-idf-scoring sls bms tids result tao n pq mxs wqs norms))))

(defn- get-term-info
  [^SearchEngine engine term]
  (lru/-get (.-cache engine)
            [:get-term-info term]
            #(l/get-value (.-lmdb engine) (.-terms-dbi engine) term
                          :string :term-info)))

(defn- term-id->term-info
  [^SearchEngine engine term-id]
  (when-let [term ((.-terms engine) term-id)]
    [term (get-term-info engine term)]))

(defn- doc-ref->id
  [^SearchEngine engine doc-ref]
  (lru/-get (.-cache engine)
            [:doc-ref->id doc-ref]
            #(l/get-value (.-lmdb engine) (.-docs-dbi engine)
                          doc-ref :data :int)))

(defn- term-ids-via-positions-dbi
  [^SearchEngine engine doc-ref]
  (let [doc-id (doc-ref->id engine doc-ref)
        lst    (IntArrayList.)
        load   (fn [kv]
                 (let [[_ tid] (b/read-buffer (l/k kv) :int-int)]
                   (.add lst tid)))]
    (l/visit (.-lmdb engine) (.-positions-dbi engine) load
             [:closed [doc-id 1]
              [doc-id (.get ^AtomicInteger (.-max-term engine))]]
             :int-int)
    (.toArray lst)))

(defn- doc-ref->term-ids
  [^SearchEngine engine doc-ref]
  (lru/-get (.-cache engine)
            [:doc-ref->term-ids doc-ref]
            #(let [ar (peek (l/get-value (.-lmdb engine)
                                         (.-docs-dbi engine)
                                         doc-ref :data :doc-info))]
               (if (< 0 (alength ^ints ar))
                 ar
                 (term-ids-via-positions-dbi engine doc-ref)))))

(defn- remove-doc*
  [^SearchEngine engine doc-id doc-ref]
  (let [txs           (FastList.)
        norms         ^IntShortHashMap (.-norms engine)
        norm          (.get norms doc-id)
        terms-dbi     (.-terms-dbi engine)
        positions-dbi (.-positions-dbi engine)
        rawtext-dbi   (.-rawtext-dbi engine)
        cache         (.-cache engine)]
    (.add txs (l/kv-tx :del rawtext-dbi doc-id :int))
    (doseq [term-id (doc-ref->term-ids engine doc-ref)]
      (let [[term [_ mw sl]] (term-id->term-info engine term-id)]
        (when-let [tf (sl/get sl doc-id)]
          (.add txs (l/kv-tx :put terms-dbi term
                             [term-id
                              (del-max-weight sl doc-id mw tf norm)
                              (sl/remove sl doc-id)]
                             :string :term-info))
          (-> cache
              (lru/-del [:get-term-info term])
              (lru/-del [:get-pos-info doc-id term-id]))))
      (.add txs (l/kv-tx :del positions-dbi [doc-id term-id] :int-int)))
    (.add txs (l/kv-tx :del (.-docs-dbi engine) doc-ref :data))
    (.remove ^SpillableMap (.-docs engine) doc-id)
    (.remove norms doc-id)
    (l/transact-kv (.-lmdb engine) txs)
    (-> cache
        (lru/-del [:doc-ref->id doc-ref])
        (lru/-del [:doc-ref->term-ids doc-ref])))
  :doc-removed)

(defn- add-doc*
  [^SearchEngine engine doc-ref doc-text]
  (let [terms-dbi       (.-terms-dbi engine)
        positions-dbi   (.-positions-dbi engine)
        terms           ^SpillableMap (.-terms engine)
        max-term        (.-max-term engine)
        index-position? (.-index-position? engine)
        include-text?   (.-include-text? engine)
        result          ((.-analyzer engine) doc-text)
        new-terms       ^UnifiedMap (collect-terms result)
        unique          (.size new-terms)
        doc-id          (.incrementAndGet ^AtomicInteger (.-max-doc engine))
        term-set        (IntHashSet.)
        txs             (FastList.)]
    (when include-text? (.add txs (l/kv-tx :put (.-rawtext-dbi engine) doc-id
                                           doc-text :int :string)))
    (.put ^SpillableMap (.-docs engine) doc-id doc-ref)
    (.put ^IntShortHashMap (.-norms engine) doc-id unique)
    (doseq [^Map$Entry kv (.entrySet new-terms)]
      (let [term                                            (.getKey kv)
            [^IntArrayList positions ^IntArrayList offsets] (.getValue kv)
            tf                                              (.size positions)

            [tid mw sl]
            (or (get-term-info engine term)
                [(let [new-tid (.incrementAndGet ^AtomicInteger max-term)]
                   (.put terms new-tid term)
                   new-tid)
                 0.0
                 (sl/sparse-arraylist)])

            term-info
            [tid (add-max-weight mw tf unique) (sl/set sl doc-id tf)]]
        (.add txs (l/kv-tx :put terms-dbi term term-info :string :term-info))
        (if index-position?
          (let [pos-info [(.toArray positions) (.toArray offsets)]]
            (.add txs (l/kv-tx :put positions-dbi [doc-id tid]
                               pos-info :int-int :pos-info)))
          (.add ^IntHashSet term-set (int tid)))))
    (let [term-ar  (.toArray ^IntHashSet term-set)
          doc-info [doc-id unique term-ar]]
      (.add txs (l/kv-tx :put (.-docs-dbi engine) doc-ref doc-info
                         :data :doc-info))
      (l/transact-kv (.-lmdb engine) txs)))
  :doc-added)

(defn- hydrate-query
  [^SearchEngine engine ^AtomicInteger max-doc tokens]
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
  [^SearchEngine engine doc-filter [_ doc-id]]
  (when-let [doc-ref ((.-docs engine) doc-id)]
    (when (doc-filter doc-ref) doc-ref)))

(defn- add-offsets
  [^SearchEngine engine doc-filter terms [_ doc-id :as result]]
  (when-let [doc-ref (get-doc-ref engine doc-filter result)]
    [doc-ref
     (sequence
       (comp (map (fn [tid]
                    (when-let [offsets (get-offsets engine doc-id tid)]
                      [(terms tid) (apply vector offsets)])))
             (remove nil?))
       (keys terms))]))

(defn- get-rawtext
  [^SearchEngine engine doc-id]
  (l/get-value (.-lmdb engine) (.-rawtext-dbi engine) doc-id
               :int :string))

(defn- add-rawtext
  [^SearchEngine engine doc-filter [_ doc-id :as result]]
  (when-let [doc-ref (get-doc-ref engine doc-filter result)]
    [doc-ref (get-rawtext engine doc-id)]))

(defn- add-text+offset
  [^SearchEngine engine doc-filter terms [_ doc-id :as result]]
  (when-let [doc-ref (get-doc-ref engine doc-filter result)]
    [doc-ref
     (l/get-value (.-lmdb engine) (.-rawtext-dbi engine)
                  doc-id :int :string)
     (sequence
       (comp (map (fn [tid]
                 (when-let [offsets (get-offsets engine doc-id tid)]
                   [(terms tid) (apply vector offsets)])))
          (remove nil?))
       (keys terms))]))

(defn- display-xf
  [^SearchEngine engine doc-filter display tms]
  (case display
    :texts+offsets (comp (map #(add-text+offset engine doc-filter tms %))
                         (remove nil?))
    :texts         (comp (map #(add-rawtext engine doc-filter %))
                         (remove nil?))
    :offsets       (comp (map #(add-offsets engine doc-filter tms %))
                         (remove nil?))
    :refs          (comp (map #(get-doc-ref engine doc-filter %))
                         (remove nil?))))

(defn- open-dbis
  [lmdb terms-dbi docs-dbi positions-dbi rawtext-dbi]
  (assert (not (l/closed-kv? lmdb)) "LMDB env is closed.")

  ;; term -> term-id,max-weight,doc-freq
  (l/open-dbi lmdb terms-dbi {:key-size c/+max-key-size+})

  ;; doc-ref -> doc-id,norm,term-set
  (l/open-dbi lmdb docs-dbi {:key-size c/+max-key-size+})

  ;; doc-id,term-id -> positions,offsets
  (l/open-dbi lmdb positions-dbi {:key-size (* 2 Integer/BYTES)})

  ;; doc-id -> raw-text
  (l/open-dbi lmdb rawtext-dbi {:key-size Integer/BYTES}))

(defn- init-terms
  [lmdb terms-dbi]
  (let [terms  (sp/new-spillable-map)
        max-id (volatile! 0)
        load   (fn [kv]
                 (let [term (b/read-buffer (l/k kv) :string)
                       id   (b/read-buffer (l/v kv) :int)]
                   (when (< ^int @max-id ^int id) (vreset! max-id id))
                   (.put ^SpillableMap terms id term)))]
    (l/visit lmdb terms-dbi load [:all-back])
    [@max-id terms]))

(defn- init-docs
  [lmdb docs-dbi]
  (let [norms  (IntShortHashMap.)
        docs   (sp/new-spillable-map)
        max-id (volatile! 0)
        load   (fn [kv]
                 (let [ref  (b/read-buffer (l/k kv) :data)
                       vb   (l/v kv)
                       id   (b/read-buffer vb :int)
                       norm (b/read-buffer vb :short)]
                   (when (< ^int @max-id ^int id) (vreset! max-id id))
                   (.put ^SpillableMap docs id ref)
                   (.put norms id norm)))]
    (l/visit lmdb docs-dbi load [:all-back])
    [@max-id norms docs]))

(def default-opts {:analyzer        a/en-analyzer
                   :index-position? false
                   :include-text?   false
                   :search-opts     default-search-opts})

(defn new-search-engine
  ([lmdb]
   (new-search-engine lmdb nil))
  ([lmdb {:keys [domain analyzer query-analyzer index-position? include-text?
                 search-opts]
          :or   {analyzer        (default-opts :analyzer)
                 index-position? (default-opts :index-position?)
                 include-text?   (default-opts :include-text?)
                 search-opts     (default-opts :search-opts)
                 domain          c/default-domain}}]
   (let [terms-dbi     (str domain "/" c/terms)
         docs-dbi      (str domain "/" c/docs)
         positions-dbi (str domain "/" c/positions)
         rawtext-dbi   (str domain "/" c/rawtext)]
     (open-dbis lmdb terms-dbi docs-dbi positions-dbi rawtext-dbi)
     (let [[max-doc norms docs] (init-docs lmdb docs-dbi)
           [max-term terms]     (init-terms lmdb terms-dbi)]
       (->SearchEngine lmdb
                       analyzer
                       (or query-analyzer analyzer)
                       terms-dbi
                       docs-dbi
                       positions-dbi
                       rawtext-dbi
                       terms     ;; term-id -> term
                       docs      ;; doc-id -> doc-ref
                       norms     ;; doc-id -> norm
                       (lru/cache 100000 :constant)
                       (AtomicInteger. max-doc)
                       (AtomicInteger. max-term)
                       index-position?
                       include-text?
                       search-opts)))))

(defn transfer
  "transfer state of an existing engine to an new engine that has a
  different LMDB instance, e.g. result of `mark-write`"
  [^SearchEngine old lmdb]
  (->SearchEngine lmdb
                  (.-analyzer old)
                  (.-query-analyzer old)
                  (.-terms-dbi old)
                  (.-docs-dbi old)
                  (.-positions-dbi old)
                  (.-rawtext-dbi old)
                  (.-terms old)
                  (.-docs old)
                  (.-norms old)
                  (.-cache old)
                  (.-max-doc old)
                  (.-max-term old)
                  (.-index-position? old)
                  (.-include-text? old)
                  (.-search-opts old)))

(defprotocol IIndexWriter
  (write [this doc-ref doc-text])
  (commit [this]))

(deftype IndexWriter [lmdb
                      analyzer
                      terms-dbi
                      docs-dbi
                      positions-dbi
                      rawtext-dbi
                      ^AtomicInteger max-doc
                      ^AtomicInteger max-term
                      index-position?
                      include-text?
                      ^FastList txs
                      ^UnifiedMap hit-terms]
  IIndexWriter
  (write [_ doc-ref doc-text]
    (when-not (s/blank? doc-text)
      (let [result    (analyzer doc-text)
            new-terms ^UnifiedMap (collect-terms result)
            unique    (.size new-terms)
            doc-id    (.incrementAndGet ^AtomicInteger max-doc)
            term-set  (IntHashSet.)
            batch     (if index-position? 250000 500)]
        (when include-text?
          (.add txs (l/kv-tx :put rawtext-dbi doc-id doc-text :int :string)))
        (doseq [^Map$Entry kv (.entrySet new-terms)]
          (let [term                                            (.getKey kv)
                [^IntArrayList positions ^IntArrayList offsets] (.getValue kv)
                tf                                              (.size positions)

                [tid mw sl]
                (or (.get hit-terms term)
                    (l/get-value lmdb terms-dbi term :string :term-info)
                    [(.incrementAndGet ^AtomicInteger max-term)
                     0.0
                     (sl/sparse-arraylist)])]
            (.put hit-terms term
                  [tid (add-max-weight mw tf unique) (sl/set sl doc-id tf)])
            (if index-position?
              (.add txs (l/kv-tx :put positions-dbi [doc-id tid]
                                 [(.toArray positions) (.toArray offsets)]
                                 :int-int :pos-info))
              (.add ^IntHashSet term-set (int tid)))))
        (.add txs (l/kv-tx :put docs-dbi doc-ref
                           [doc-id unique (.toArray ^IntHashSet term-set)]
                           :data :doc-info))
        (when (< batch (.size txs))
          (l/transact-kv lmdb txs)
          (.clear txs)))))

  (commit [_]
    (l/transact-kv lmdb txs)
    (.clear txs)
    (let [iter (.iterator (.entrySet hit-terms))]
      (loop []
        (when (.hasNext iter)
          (let [^Map$Entry kv (.next iter)]
            (.remove iter)
            (.add txs (l/kv-tx :put terms-dbi (.getKey kv)
                               (.getValue kv) :string :term-info))
            (recur)))))
    (l/transact-kv lmdb txs)))

(defn- init-max-id [lmdb dbi]
  (let [max-id (volatile! 0)
        load   (fn [kv]
                 (let [id (b/read-buffer (l/v kv) :int)]
                   (when (< ^int @max-id ^int id) (vreset! max-id id))))]
    (l/visit lmdb dbi load [:all-back])
    @max-id))

(defn search-index-writer
  ([lmdb]
   (search-index-writer lmdb nil))
  ([lmdb {:keys [domain analyzer index-position? include-text?]
          :or   {domain          "datalevin"
                 analyzer        a/en-analyzer
                 index-position? false
                 include-text?   false}}]
   (let [terms-dbi     (str domain "/" c/terms)
         docs-dbi      (str domain "/" c/docs)
         positions-dbi (str domain "/" c/positions)
         rawtext-dbi   (str domain "/" c/rawtext)]
     (open-dbis lmdb terms-dbi docs-dbi positions-dbi rawtext-dbi)
     (->IndexWriter lmdb
                    analyzer
                    terms-dbi
                    docs-dbi
                    positions-dbi
                    rawtext-dbi
                    (AtomicInteger. (init-max-id lmdb docs-dbi))
                    (AtomicInteger. (init-max-id lmdb terms-dbi))
                    index-position?
                    include-text?
                    (FastList.)
                    (UnifiedMap.)))))

(comment
  (def lmdb (time (l/open-kv "search-bench/data/wiki-datalevin-all")))
  (def engine (time (new-search-engine lmdb)))
  (doc-count engine)
  (search engine "debian linux")

  )
