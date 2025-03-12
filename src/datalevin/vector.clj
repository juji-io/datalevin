(ns ^:no-doc datalevin.vector
  "Vector indexing and search"
  (:require
   [datalevin.lmdb :as l]
   [datalevin.util :as u :refer [raise]]
   [datalevin.spill :as sp]
   [datalevin.constants :as c]
   [datalevin.async :as a]
   [datalevin.bits :as b]
   [taoensso.nippy :as nippy])
  (:import
   [datalevin.dtlvnative DTLV DTLV$usearch_index_t]
   [datalevin.cpp VecIdx VecIdx$SearchResult VecIdx$IndexInfo]
   [datalevin.spill SpillableMap]
   [datalevin.async IAsyncWork]
   [datalevin.lmdb IAdmin]
   [java.io FileOutputStream FileInputStream DataOutputStream DataInputStream]
   [java.util Map]
   [java.util.concurrent.atomic AtomicLong]))

(defn- metric-key->type
  [k]
  (int (case k
         :custom      DTLV/usearch_metric_unknown_k
         :cosine      DTLV/usearch_metric_cos_k
         :dot-product DTLV/usearch_metric_ip_k
         :euclidean   DTLV/usearch_metric_l2sq_k
         :haversine   DTLV/usearch_metric_haversine_k
         :divergence  DTLV/usearch_metric_divergence_k
         :pearson     DTLV/usearch_metric_pearson_k
         :jaccard     DTLV/usearch_metric_jaccard_k
         :hamming     DTLV/usearch_metric_hamming_k
         :tanimoto    DTLV/usearch_metric_tanimoto_k
         :sorensen    DTLV/usearch_metric_sorensen_k)))

(defn- scalar-kind
  [k]
  (int (case k
         :custom  DTLV/usearch_scalar_unknown_k
         :double  DTLV/usearch_scalar_f64_k
         :float   DTLV/usearch_scalar_f32_k
         :float16 DTLV/usearch_scalar_f16_k
         :int8    DTLV/usearch_scalar_i8_k
         :byte    DTLV/usearch_scalar_b1_k)))

(defn index-fname
  [lmdb domain]
  (str (l/dir lmdb) u/+separator+ domain c/vector-index-suffix))

(defn- init-index
  [^String fname dimensions metric-key quantization connectivity expansion-add
   expansion-search]
  (let [^DTLV$usearch_index_t index (VecIdx/create
                                      ^long dimensions
                                      ^int (metric-key->type metric-key)
                                      ^int (scalar-kind quantization)
                                      ^long connectivity
                                      ^long expansion-add
                                      ^long expansion-search)]
    (when (u/file-exists fname) (VecIdx/load index fname))
    index))

(defn- ->array
  [quantization vec-data]
  (case quantization
    :double       (double-array vec-data)
    :float        (float-array vec-data)
    :float16      (short-array vec-data)
    (:int8 :byte) (byte-array vec-data)))

(defn- arr-len
  [quantization arr]
  (case quantization
    :double       (alength ^doubles arr)
    :float        (alength ^floats arr)
    :float16      (alength ^shorts arr)
    (:int8 :byte) (alength ^bytes arr)))

(defn- validate-arr
  [^long dimensions quantization arr]
  (if (== dimensions ^long (arr-len quantization arr))
    arr
    (raise "Expect a " dimensions " dimensions vector" {})))

(defn- vec->arr
  [^long dimensions quantization vec-data]
  (if (u/array? vec-data)
    (validate-arr dimensions quantization vec-data)
    (if (== dimensions (count vec-data))
      (->array quantization vec-data)
      (raise "Expect a " dimensions " dimensions vector" {}))))

(defn- add
  [index quantization ^long k arr]
  (case quantization
    :double  (VecIdx/addDouble index k ^doubles arr)
    :float   (VecIdx/addFloat index k ^floats arr)
    :float16 (VecIdx/addShort index k ^shorts arr)
    :int8    (VecIdx/addInt8 index k ^bytes arr)
    :byte    (VecIdx/addByte index k ^bytes arr)))

(defn- search
  [index query quantization top]
  (case quantization
    :double  (VecIdx/searchDouble index query top)
    :float   (VecIdx/searchFloat index query top)
    :float16 (VecIdx/searchShort index query top)
    :int8    (VecIdx/searchInt8 index query top)
    :byte    (VecIdx/searchByte index query top)))

(defn- get-vec*
  [index id quantization dimensions]
  (case quantization
    :double  (VecIdx/getDouble index id (int dimensions))
    :float   (VecIdx/getFloat index id (int dimensions))
    :float16 (VecIdx/getShort index id (int dimensions))
    :int8    (VecIdx/getInt8 index id (int dimensions))
    :byte    (VecIdx/getByte index id (int dimensions))))

(defn- open-dbi
  [lmdb vecs-dbi]
  (assert (not (l/closed-kv? lmdb)) "LMDB env is closed.")

  ;; vec-ref -> vec-ids
  (l/open-list-dbi lmdb vecs-dbi {:key-size c/+max-key-size+
                                  :val-size c/+id-bytes+}))

(defn- init-vecs
  [lmdb vecs-dbi]
  (let [vecs   (sp/new-spillable-map)
        max-id (volatile! 0)
        load   (fn [kv]
                 (let [ref (b/read-buffer (l/k kv) :data)
                       id  (b/read-buffer (l/v kv) :id)]
                   (when (< ^long @max-id ^long id) (vreset! max-id id))
                   (.put ^SpillableMap vecs id ref)))]
    (l/visit-list-range lmdb vecs-dbi load [:all] :data [:all] :id)
    [@max-id vecs]))

(def default-search-opts {:display    :refs
                          :top        10
                          :vec-filter (constantly true)})

(defprotocol IVectorIndex
  (add-vec [this vec-ref vec-data] "add vector to in memory index")
  (remove-vec [this vec-ref] "remove vector from in memory index")
  (get-vec [this vec-ref] "retrieve the vectors of a vec-ref")
  (persist-vecs [this] "persistent index on disk")
  (close-vecs [this] "free the in memory index")
  (closed? [this] "return true if this index is closed")
  (clear-vecs [this] "close and remove this index")
  (vecs-info [this] "return a map of info about this index")
  (vec-indexed? [this vec-ref] "test if a rec-ref is in the index")
  (search-vec [this query-vec] [this query-vec opts]
    "search vector, return found vec-refs"))

(defn- vec-save-key* [fname] (->> fname hash (str "vec-save-") keyword))

(def vec-save-key (memoize vec-save-key*))

(deftype AsyncVecSave [vec-index fname]
  IAsyncWork
  (work-key [_] (vec-save-key fname))
  (do-work [_] (when-not (closed? vec-index) (persist-vecs vec-index)))
  (combine [_] first)
  (callback [_] nil))

(declare display-xf new-vector-index)

(deftype VectorIndex [lmdb
                      closed?
                      ^DTLV$usearch_index_t index
                      ^String fname
                      ^long dimensions
                      ^clojure.lang.Keyword metric-type
                      ^clojure.lang.Keyword quantization
                      ^long connectivity
                      ^long expansion-add
                      ^long expansion-search
                      ^String vecs-dbi
                      ^SpillableMap vecs     ; vec-id -> vec-ref
                      ^AtomicLong max-vec
                      ^Map search-opts]
  IVectorIndex
  (add-vec [this vec-ref vec-data]
    (let [vec-id  (.incrementAndGet max-vec)
          vec-arr (vec->arr dimensions quantization vec-data)]
      (add index quantization vec-id vec-arr)
      (a/exec (a/get-executor) (AsyncVecSave. this fname))
      (.put vecs vec-id vec-ref)
      (l/transact-kv
        lmdb [(l/kv-tx :put vecs-dbi vec-ref vec-id :data :id)])
      vec-id))

  (get-vec [_ vec-ref]
    (let [ids (l/get-list lmdb vecs-dbi vec-ref :data :id)]
      (for [^long id ids]
        (get-vec* index id quantization dimensions))))

  (remove-vec [this vec-ref]
    (let [ids (l/get-list lmdb vecs-dbi vec-ref :data :id)]
      (doseq [^long id ids]
        (VecIdx/remove index id)
        (.remove vecs id))
      (a/exec (a/get-executor) (AsyncVecSave. this fname))
      (l/transact-kv lmdb [(l/kv-tx :del vecs-dbi vec-ref)])))

  (persist-vecs [_] (VecIdx/save index fname))

  (close-vecs [this]
    (when-not (.closed? this)
      (.persist_vecs this)
      (swap! l/vector-indices dissoc fname)
      (vreset! closed? true)
      (VecIdx/free index)))

  (closed? [_] @closed?)

  (clear-vecs [this]
    (.close-vecs this)
    (.empty vecs)
    (l/clear-dbi lmdb vecs-dbi)
    (u/delete-files fname))

  (vecs-info [_]
    (let [^VecIdx$IndexInfo info (VecIdx/info index)]
      {:size             (.getSize info)
       :memory           (.getMemory info)
       :capacity         (.getCapacity info)
       :hardware         (.getHardware info)
       :filename         fname
       :dimensions       dimensions
       :metric-type      metric-type
       :quantization     quantization
       :connectivity     connectivity
       :expansion-add    expansion-add
       :expansion-search expansion-search}))

  (vec-indexed? [_ vec-ref] (l/get-value lmdb vecs-dbi vec-ref))

  (search-vec [this query-vec]
    (.search-vec this query-vec {}))
  (search-vec [this query-vec {:keys [display top vec-filter]
                               :or   {display    (:display search-opts)
                                      top        (:top search-opts)
                                      vec-filter (:vec-filter search-opts)}}]
    (let [query                    (vec->arr dimensions quantization query-vec)
          ^VecIdx$SearchResult res (search index query quantization (int top))]
      (doall (sequence
               (display-xf this vec-filter display)
               (.getKeys res) (.getDists res)))))

  IAdmin
  (re-index [this opts]
    (try
      (let [dfname (str fname ".dump")
            dos    (DataOutputStream. (FileOutputStream. ^String dfname))]
        (nippy/freeze-to-out!
          dos (for [[vec-id vec-ref] vecs]
                [vec-ref (get-vec* index vec-id quantization dimensions)]))
        (.flush dos)
        (.close dos)
        (.clear-vecs this)
        (let [new (new-vector-index lmdb opts)
              dis (DataInputStream. (FileInputStream. ^String dfname))]
          (doseq [[vec-ref vec-data] (nippy/thaw-from-in! dis)]
            (add-vec new vec-ref vec-data))
          (.close dis)
          (u/delete-files dfname)
          new))
      (catch Exception e
        (u/raise "Unable to re-index vectors. " e {:dir (l/dir lmdb)})))))

(defn- get-ref
  [^VectorIndex index vec-filter vec-id _]
  (when-let [vec-ref ((.-vecs index) vec-id)]
    (when (vec-filter vec-ref) vec-ref)))

(defn- get-ref-dist
  [^VectorIndex index vec-filter vec-id dist]
  (when-let [vec-ref ((.-vecs index) vec-id)]
    (when (vec-filter vec-ref) [vec-ref dist])))

(defn- display-xf
  [index vec-filter display]
  (case display
    :refs       (comp (map #(get-ref index vec-filter %1 %2))
                   (remove nil?))
    :refs+dists (comp (map #(get-ref-dist index vec-filter %1 %2))
                   (remove nil?))))

(def default-opts {:metric-type      c/default-metric-type
                   :quantization     c/default-quantization
                   :connectivity     c/default-connectivity
                   :expansion-add    c/default-expansion-add
                   :expansion-search c/default-expansion-search
                   :search-opts      default-search-opts})

(defn new-vector-index
  [lmdb {:keys [domain metric-type quantization dimensions connectivity
                expansion-add expansion-search search-opts]
         :or   {metric-type      (default-opts :metric-type)
                quantization     (default-opts :quantization)
                connectivity     (default-opts :connectivity)
                expansion-add    (default-opts :expansion-add)
                expansion-search (default-opts :expansion-search)
                search-opts      (default-opts :search-opts)
                domain           c/default-domain}}]
  (assert dimensions ":dimensions is required")
  (let [vecs-dbi (str domain "/" c/vec-refs)]
    (open-dbi lmdb vecs-dbi)
    (let [[max-vec-id vecs] (init-vecs lmdb vecs-dbi)
          fname             (index-fname lmdb domain)
          index             (init-index fname dimensions metric-type
                                        quantization connectivity
                                        expansion-add expansion-search)]
      (swap! l/vector-indices assoc fname index)
      (->VectorIndex lmdb
                     (volatile! false)
                     index
                     fname
                     dimensions
                     metric-type
                     quantization
                     connectivity
                     expansion-add
                     expansion-search
                     vecs-dbi
                     vecs
                     (AtomicLong. max-vec-id)
                     search-opts))))

(defn transfer
  [^VectorIndex old lmdb]
  (->VectorIndex lmdb
                 (.-closed? old)
                 (.-index old)
                 (.-fname old)
                 (.-dimensions old)
                 (.-metric-type old)
                 (.-quantization old)
                 (.-connectivity old)
                 (.-expansion-add old)
                 (.-expansion-search old)
                 (.-vecs-dbi old)
                 (.-vecs old)
                 (.-max-vec old)
                 (.-search-opts old)))
