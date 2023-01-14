(ns ^:no-doc datalevin.search
  "Full-text search engine"
  (:require
   [datalevin.lmdb :as l]
   [datalevin.util :as u]
   [datalevin.spill :as sp]
   [datalevin.sparselist :as sl]
   [datalevin.constants :as c]
   [datalevin.bits :as b]
   [datalevin.lru :as lru]
   [clojure.stacktrace :as st]
   [clojure.string :as s])
  (:import
   [datalevin.utl PriorityQueue GrowingIntArray]
   [datalevin.sparselist SparseIntArrayList]
   [datalevin.spill SpillableIntObjMap]
   [java.util HashMap ArrayList Map$Entry Arrays]
   [java.util.concurrent.atomic AtomicInteger]
   [java.io Writer]
   [org.eclipse.collections.impl.map.mutable.primitive IntShortHashMap
    IntDoubleHashMap]
   [org.eclipse.collections.impl.list.mutable FastList]
   [org.roaringbitmap RoaringBitmap FastAggregation FastRankRoaringBitmap
    PeekableIntIterator]))

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

  (has-next? [_] (.hasNext iter))

  (advance [this]
    (set! did (.next iter))
    this)

  (get-did [_] did)

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
  (add-doc [this doc-ref doc-text] [this doc-ref doc-text check-exist?])
  (remove-doc [this doc-ref])
  (clear-docs [this])
  (doc-indexed? [this doc-ref])
  (doc-count [this])
  (search [this query] [this query opts]))

(declare doc-ref->id remove-doc* add-doc* hydrate-query display-xf)

(deftype SearchEngine [lmdb
                       analyzer
                       query-analyzer
                       terms-dbi
                       docs-dbi
                       positions-dbi
                       ^SpillableIntObjMap terms ; term-id -> term
                       ^SpillableIntObjMap docs  ; doc-id -> doc-ref
                       ^IntShortHashMap norms    ; doc-id -> norm
                       cache
                       ^AtomicInteger max-doc
                       ^AtomicInteger max-term
                       index-position?]
  ISearchEngine
  (add-doc [this doc-ref doc-text check-exist?]
    (locking docs
      (try
        (when-not (s/blank? doc-text)
          (when check-exist?
            (when-let [doc-id (doc-ref->id this doc-ref)]
              (remove-doc* this doc-id doc-ref)))
          (add-doc* this doc-ref doc-text))
        (catch Exception e
          (st/print-stack-trace e)
          (u/raise "Error indexing document:" (ex-message e)
                   {:doc-ref doc-ref :doc-text doc-text})))))
  (add-doc [this doc-ref doc-text]
    (.add-doc this doc-ref doc-text true))

  (remove-doc [this doc-ref]
    (if-let [doc-id (doc-ref->id this doc-ref)]
      (remove-doc* this doc-id doc-ref)
      (u/raise "Document does not exist." {:doc-ref doc-ref})))

  (clear-docs [_]
    (.empty docs)
    (.empty terms)
    (l/clear-dbi lmdb terms-dbi)
    (l/clear-dbi lmdb docs-dbi)
    (l/clear-dbi lmdb positions-dbi))

  (doc-indexed? [this doc-ref] (doc-ref->id this doc-ref))

  (doc-count [_] (count docs))

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
                  (range n 0 -1))))))))))

(defn- get-term-info
  [^SearchEngine engine term]
  (lru/-get (.-cache engine)
            [:get-term-info term]
            #(l/get-value (.-lmdb engine) (.-terms-dbi engine) term
                          :string :term-info)))

(defn- term-id->term-info
  [^SearchEngine engine term-id]
  (when-let [term (get (.-terms engine) term-id)]
    [term (get-term-info engine term)]))

(defn- doc-ref->id
  [^SearchEngine engine doc-ref]
  (lru/-get (.-cache engine)
            [:doc-ref->id doc-ref]
            #(l/get-value (.-lmdb engine) (.-docs-dbi engine)
                          doc-ref :data :int)))

(defn- doc-ref->term-ids
  [^SearchEngine engine doc-ref]
  (let [bm (lru/-get (.-cache engine)
                     [:doc-ref->term-ids doc-ref]
                     #(peek (l/get-value (.-lmdb engine)
                                         (.-docs-dbi engine)
                                         doc-ref :data :doc-info true)))]
    (iterator-seq (.iterator ^RoaringBitmap bm))))

(defn- remove-doc*
  [^SearchEngine engine doc-id doc-ref]
  (let [txs           (FastList.)
        norms         ^IntShortHashMap (.-norms engine)
        norm          (.get norms doc-id)
        terms-dbi     (.-terms-dbi engine)
        positions-dbi (.-positions-dbi engine)
        cache         (.-cache engine)]
    (.remove ^SpillableIntObjMap (.-docs engine) doc-id)
    (.remove norms doc-id)
    (.add txs [:del (.-docs-dbi engine) doc-ref :data])
    (doseq [term-id (doc-ref->term-ids engine doc-ref)]
      (let [[term [_ mw sl]] (term-id->term-info engine term-id)]
        (when-let [tf (sl/get sl doc-id)]
          (let [new-info [term-id
                          (del-max-weight sl doc-id mw tf norm)
                          (sl/remove sl doc-id)]]
            ;; (println "new-info ==> " new-info)
            (.add txs [:put terms-dbi term new-info :string :term-info])
            (lru/-del cache [:get-term-info term]))))
      (.add txs [:del positions-dbi [doc-id term-id] :int-int]))
    (l/transact-kv (.-lmdb engine) txs)
    (-> cache
        (lru/-del [:doc-ref->id doc-ref])
        (lru/-del [:doc-ref->term-ids doc-ref])))
  :doc-removed)

(defn- add-doc*
  [^SearchEngine engine doc-ref doc-text]
  (let [result          ((.-analyzer engine) doc-text)
        new-terms       ^HashMap (collect-terms result)
        unique          (.size new-terms)
        doc-id          (.incrementAndGet ^AtomicInteger (.-max-doc engine))
        term-bm         (RoaringBitmap.)
        txs             (FastList.)
        terms-dbi       (.-terms-dbi engine)
        positions-dbi   (.-positions-dbi engine)
        terms           ^SpillableIntObjMap (.-terms engine)
        max-term        (.-max-term engine)
        index-position? (.-index-position? engine)
        cache           (.-cache engine)]
    (.put ^SpillableIntObjMap (.-docs engine) doc-id doc-ref)
    (.put ^IntShortHashMap (.-norms engine) doc-id unique)
    (doseq [^Map$Entry kv (.entrySet new-terms)]
      (let [term    (.getKey kv)
            new-lst ^FastList (.getValue kv)
            tf      (.size new-lst)

            [tid mw sl]
            (or (get-term-info engine term)
                [(let [new-tid (.incrementAndGet ^AtomicInteger max-term)]
                   (.put terms new-tid term)
                   new-tid)
                 0.0
                 (sl/sparse-arraylist)])

            term-info
            [tid (add-max-weight mw tf unique) (sl/set sl doc-id tf)]]
        (lru/-put cache [:get-term-info term] term-info)
        (.add txs [:put terms-dbi term term-info :string :term-info])
        (.add ^RoaringBitmap term-bm ^int tid)
        (when index-position?
          (.add txs [:put-list positions-dbi [doc-id tid]
                     new-lst :int-int :int-int]))))
    (let [doc-info [doc-id unique term-bm]]
      (.add txs [:put (.-docs-dbi engine) doc-ref doc-info
                 :data :doc-info])
      (l/transact-kv (.-lmdb engine) txs)
      (-> cache
          (lru/-put [:doc-ref->id doc-ref] doc-id)
          (lru/-put [:doc-ref->term-ids doc-ref] term-bm))))
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
    (when (doc-filter doc-ref) doc-ref))
  #_(when-let [doc-ref (nth (l/get-value (.-lmdb engine) (.-docs-dbi engine)
                                         doc-id :int :doc-info) 1)]
      (when (doc-filter doc-ref) doc-ref)))

(defn- add-offsets
  [^SearchEngine engine doc-filter terms [_ doc-id :as result]]
  (when-let [doc-ref (get-doc-ref engine doc-filter result)]
    [doc-ref
     (sequence
       (comp (map (fn [tid]
                 (let [lst (l/get-list (.-lmdb engine) (.-positions-dbi engine)
                                       [doc-id tid] :int-int :int-int)]
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

(defn- open-dbis
  [lmdb terms-dbi docs-dbi positions-dbi]
  (assert (not (l/closed-kv? lmdb)) "LMDB env is closed.")

  ;; term -> term-id,max-weight,doc-freq
  (l/open-dbi lmdb terms-dbi {:key-size c/+max-term-length+})

  ;; doc-ref -> doc-id,norm,term-bm
  (l/open-dbi lmdb docs-dbi {:key-size c/+max-key-size+})

  ;; doc-id,term-id -> position,offset (list)
  (l/open-list-dbi lmdb positions-dbi {:key-size (* 2 Integer/BYTES)
                                       :val-size (* 2 Integer/BYTES)}))

(defn- init-terms
  [lmdb terms-dbi]
  (let [terms  (sp/new-spillable-intobj-map)
        max-id (volatile! 0)]
    (doseq [[term id] (l/get-range lmdb terms-dbi [:all] :string :int)]
      (when (< ^int @max-id ^int id) (vreset! max-id id))
      (.put ^SpillableIntObjMap terms id term))
    [@max-id terms]))

(defn- init-docs
  [lmdb docs-dbi]
  (let [norms  (IntShortHashMap.)
        docs   (sp/new-spillable-intobj-map)
        max-id (volatile! 0)
        load   (fn [kv]
                 (let [vb   (l/v kv)
                       ref  (b/read-buffer (l/k kv) :data)
                       id   (b/read-buffer vb :int)
                       norm (b/read-buffer vb :short)]
                   (when (< ^int @max-id ^int id) (vreset! max-id id))
                   (.put ^SpillableIntObjMap docs id ref)
                   (.put norms id norm)))]
    (l/visit lmdb docs-dbi load [:all] :data)
    [@max-id norms docs]))

(defn new-search-engine
  ([lmdb]
   (new-search-engine lmdb nil))
  ([lmdb {:keys [domain analyzer query-analyzer index-position?]
          :or   {domain          "datalevin"
                 analyzer        en-analyzer
                 index-position? false}}]
   (let [terms-dbi     (str domain "/" c/terms)
         docs-dbi      (str domain "/" c/docs)
         positions-dbi (str domain "/" c/positions)]
     (open-dbis lmdb terms-dbi docs-dbi positions-dbi)
     (let [[max-doc norms docs] (init-docs lmdb docs-dbi)
           [max-term terms]     (init-terms lmdb terms-dbi)]
       (->SearchEngine lmdb
                       analyzer
                       (or query-analyzer analyzer)
                       terms-dbi
                       docs-dbi
                       positions-dbi
                       terms
                       docs
                       norms
                       (lru/cache 100000 :constant)
                       (AtomicInteger. max-doc)
                       (AtomicInteger. max-term)
                       index-position?)))))

(defn transfer
  "transfer state of an existing engine to an new engine that has a
  different LMDB instance"
  [^SearchEngine old lmdb]
  (->SearchEngine lmdb
                  (.-analyzer old)
                  (.-query-analyzer old)
                  (.-terms-dbi old)
                  (.-docs-dbi old)
                  (.-positions-dbi old)
                  (.-terms old)
                  (.-docs old)
                  (.-norms old)
                  (.-cache old)
                  (.-max-doc old)
                  (.-max-term old)
                  (.-index-position? old)))
