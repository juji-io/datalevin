(ns datalevin.vector
  "Vector indexing and search"
  (:require
   [datalevin.lmdb :as l]
   [datalevin.util :as u :refer [raise]]
   [datalevin.spill :as sp]
   [datalevin.constants :as c]
   [datalevin.bits :as b])
  (:import
   [datalevin.dtlvnative DTLV DTLV$usearch_init_options_t DTLV$usearch_index_t
    DTLV$usearch_metric_t]
   [datalevin.cpp VecIdx]
   [datalevin.spill SpillableMap]
   [org.bytedeco.javacpp BytePointer ShortPointer FloatPointer DoublePointer
    PointerPointer]
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

(defmacro wrap-error
  [call]
  `(let [~'error  (.put (PointerPointer. 1) 0 nil)
         res#     ~call
         err-out# (.get ~'error BytePointer)]
     (.close ~'error)
     (if err-out#
       (raise "Failed usearch call:" (.getString ^BytePointer err-out#) {})
       res#)))

(defn- index-fname [lmdb domain] (str (l/dir lmdb) u/+separator+ domain ".vi"))

(defn- init-index
  [fname dimensions metric-key quantization connectivity expansion-add
   expansion-search]
  (let [^VecIdx index (VecIdx. ^long dimensions
                               ^int (metric-key->type metric-key)
                               ^int (scalar-kind quantization)
                               ^long connectivity
                               ^long expansion-add
                               ^long expansion-search)]
    #_(when (u/file-exists fname)
        (wrap-error
          (DTLV/usearch_load ^DTLV$usearch_index_t index ^String fname error)))
    index))

(defn- valid-scalar?
  [quantization scalar]
  (case quantization
    :double       (double? scalar)
    :float        (float? scalar)
    :float16      (instance? Short scalar)
    (:int8 :byte) (instance? Byte scalar)))

(defn- ->array
  [quantization vec-data]
  (case quantization
    :double       (double-array vec-data)
    :float        (float-array vec-data)
    :float16      (short-array vec-data)
    (:int8 :byte) (byte-array vec-data)))

(defn- ->ptr
  [quantization arr]
  (case quantization
    :double       (DoublePointer. ^doubles arr)
    :float        (FloatPointer. ^floats arr)
    :float16      (ShortPointer. ^shorts arr)
    (:int8 :byte) (BytePointer. ^bytes arr)))

(defn- vec->ptr
  [^long dimensions quantization vec-data]
  (if (u/array? vec-data)
    (if (== dimensions ^long (alength vec-data))
      (if (valid-scalar? quantization (aget vec-data 0))
        (->ptr quantization vec-data)
        (raise "Invalid scalar, expect" quantization {}))
      (raise "Expect a " dimensions " dimensions vector" {}))
    (if (== dimensions (count vec-data))
      (->>  vec-data (->array quantization) (->ptr quantization))
      (raise "Expect a " dimensions " dimensions vector" {}))))

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
  (persist-vecs [this] "persistent index on disk")
  (close-vecs [this] "close the index")
  (clear-vecs [this])
  (vec-indexed? [this vec-ref])
  (vec-count [this])
  (search-vec [this query-vec] [this query-vec opts]))

(declare display-xf)

(deftype VectorIndex [lmdb
                      ^VecIdx index
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
  (add-vec [_ vec-ref vec-data]
    (locking vecs
      (let [vec-id (.incrementAndGet max-vec)
            ;; vec-ptr (vec->ptr dimensions quantization vec-data)
            ]
        (println "vec-id ->" vec-id)
        (println "vec-data ->" (vec vec-data))
        (.addFloat index vec-id vec-data)
        #_(wrap-error
            (DTLV/usearch_add index vec-id vec-ptr
                              (scalar-kind quantization) error))
        (.put vecs vec-id vec-ref)
        (l/transact-kv
          lmdb [(l/kv-tx :put vecs-dbi vec-ref vec-id :data :id)]))))

  (remove-vec [_ vec-ref]
    (let [ids (l/get-list lmdb vecs-dbi vec-ref :data :id)]
      (doseq [id ids]
        (wrap-error (DTLV/usearch_remove index id error))
        (.remove vecs id))
      (l/transact-kv lmdb [(l/kv-tx :del vecs-dbi vec-ref)])))

  (persist-vecs [_] (wrap-error (DTLV/usearch_save index fname error)))

  (close-vecs [_] (wrap-error (DTLV/usearch_free index error)))

  (clear-vecs [this]
    (.close-vecs this)
    (.empty vecs)
    (l/clear-dbi lmdb vecs-dbi)
    (u/delete-files fname))

  (vec-indexed? [_ vec-ref] (l/get-value lmdb vecs-dbi vec-ref))

  (vec-count [_] (count vecs))

  (search-vec [this query-vec]
    (.search-vec this query-vec {}))
  (search-vec [this query-vec {:keys [display top vec-filter]
                               :or   {display    (:display search-opts)
                                      top        (:top search-opts)
                                      vec-filter (:vec-filter search-opts)}}]
    (let [vec-ids (long-array top)
          dists   (float-array top)
          query   (vec->ptr dimensions quantization query-vec)]
      (wrap-error
        (DTLV/usearch_search
          index query (scalar-kind quantization) top vec-ids dists error))
      (sequence
        (display-xf this vec-filter display)
        vec-ids dists))))

(defn- get-ref
  [^VectorIndex index vec-filter vec-id _]
  (let [vec-ref ((.-vecs index) vec-id)]
    (when (vec-filter vec-ref) vec-ref)))

(defn- get-ref-dist
  [^VectorIndex index vec-filter vec-id dist]
  (let [vec-ref ((.-vecs index) vec-id)]
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
      (->VectorIndex lmdb
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
