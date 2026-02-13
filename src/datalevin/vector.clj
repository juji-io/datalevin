;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.vector
  "Vector indexing and search"
  (:require
   [datalevin.lmdb :as l]
   [datalevin.util :as u :refer [raise]]
   [datalevin.spill :as sp]
   [datalevin.constants :as c]
   [datalevin.async :as a]
   [datalevin.remote :as r]
   [datalevin.bits :as b]
   [datalevin.interface :as i]
   [clojure.string :as s]
   [taoensso.nippy :as nippy])
  (:import
   [datalevin.dtlvnative DTLV DTLV$usearch_index_t]
   [datalevin.cpp VecIdx VecIdx$SearchResult VecIdx$IndexInfo]
   [datalevin.spill SpillableMap]
   [datalevin.async IAsyncWork]
   [datalevin.remote KVStore]
   [datalevin.interface IAdmin IVectorIndex]
   [java.io File FileOutputStream FileInputStream DataOutputStream
    DataInputStream]
   [java.util Arrays Map]
   [java.util.concurrent.atomic AtomicLong]
   [java.util.concurrent.locks ReentrantReadWriteLock]
   [org.bytedeco.javacpp BytePointer]))

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
  (str (i/env-dir lmdb) u/+separator+ domain c/vector-index-suffix))

(defn- create-index
  "Create a fresh native HNSW index without loading any data."
  [dimensions metric-key quantization connectivity expansion-add
   expansion-search]
  (VecIdx/create
    ^long dimensions
    ^int (metric-key->type metric-key)
    ^int (scalar-kind quantization)
    ^long connectivity
    ^long expansion-add
    ^long expansion-search))

(defn- init-index
  "Create a native HNSW index, loading from file if it exists."
  [^String fname dimensions metric-key quantization connectivity expansion-add
   expansion-search]
  (let [^DTLV$usearch_index_t index (create-index
                                      dimensions metric-key quantization
                                      connectivity expansion-add
                                      expansion-search)]
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
  (assert (not (i/closed-kv? lmdb)) "LMDB env is closed.")

  ;; vec-ref -> vec-ids
  (i/open-list-dbi lmdb vecs-dbi {:key-size c/+max-key-size+
                                  :val-size c/+id-bytes+}))

(defn- open-vec-blob-dbis
  "Open the vec-index-dbi and vec-meta-dbi for LMDB blob storage."
  [lmdb]
  (i/open-dbi lmdb c/vec-index-dbi)
  (i/open-dbi lmdb c/vec-meta-dbi))

(defn- init-vecs
  [lmdb vecs-dbi]
  (let [vecs   (sp/new-spillable-map)
        max-id (volatile! 0)
        load   (fn [kv]
                 (let [ref (b/read-buffer (l/k kv) :data)
                       id  (b/read-buffer (l/v kv) :id)]
                   (when (< ^long @max-id ^long id) (vreset! max-id id))
                   (.put ^SpillableMap vecs id ref)))]
    (i/visit-list-range lmdb vecs-dbi load [:all] :data [:all] :id)
    [@max-id vecs]))

;;; ---------------------------------------------------------------
;;; LMDB Blob Checkpoint / Load for WAL Mode
;;; ---------------------------------------------------------------

(defn- checkpoint-to-lmdb
  "Serialize the native HNSW index and store as chunked blobs in LMDB.
   Bypasses WAL since this is a bulk snapshot write to LMDB base."
  [lmdb ^DTLV$usearch_index_t index ^String domain]
  (let [total-bytes (VecIdx/serializedLength index)]
    (when (pos? total-bytes)
      (let [chunk-size  (long c/*wal-vec-chunk-bytes*)
            chunk-count (long (Math/ceil (/ (double total-bytes)
                                           (double chunk-size))))
            txs         (java.util.ArrayList.)]
        (if (<= total-bytes (long c/*wal-vec-max-buffer-bytes*))
          ;; Buffer mode: serialize to native buffer, copy to JVM, write chunks
          (let [buf (BytePointer. (long total-bytes))]
            (try
              (VecIdx/saveBuffer index buf total-bytes)
              (let [ba (byte-array total-bytes)]
                (.get buf ba)
                (dotimes [i chunk-count]
                  (let [offset (int (* i chunk-size))
                        end    (int (min (+ offset chunk-size) total-bytes))
                        chunk  (Arrays/copyOfRange ba offset end)]
                    (.add txs (l/kv-tx :put c/vec-index-dbi
                                       [domain i] chunk :data :bytes)))))
              (finally (.close buf))))
          ;; File-spool mode: save to temp file, stream into chunks
          (let [tmp-dir  (File. (str (i/env-dir lmdb) u/+separator+ "tmp"))
                _        (.mkdirs tmp-dir)
                tmp-file (File/createTempFile "vec-checkpoint-" ".tmp" tmp-dir)]
            (try
              (VecIdx/save index (.getAbsolutePath tmp-file))
              (with-open [fis (FileInputStream. tmp-file)]
                (let [read-buf (byte-array (int (min chunk-size
                                                     Integer/MAX_VALUE)))]
                  (loop [chunk-id 0]
                    (let [n (.read fis read-buf)]
                      (when (pos? n)
                        (let [chunk (Arrays/copyOf read-buf n)]
                          (.add txs (l/kv-tx :put c/vec-index-dbi
                                             [domain chunk-id] chunk
                                             :data :bytes))
                          (recur (inc chunk-id))))))))
              (finally (.delete tmp-file)))))
        ;; Write metadata
        (.add txs (l/kv-tx :put c/vec-meta-dbi domain
                           {:chunk-count chunk-count :total-bytes total-bytes}
                           :string :data))
        ;; Bypass WAL for blob checkpoint (bulk snapshot write)
        (binding [c/*bypass-wal*    true
                  c/*trusted-apply* true]
          (i/transact-kv lmdb txs))))))

(defn- load-from-lmdb
  "Read chunked blobs from LMDB and deserialize into the native index.
   Returns true if data was loaded, false if no blob found."
  [lmdb ^DTLV$usearch_index_t index ^String domain]
  (let [meta-val (i/get-value lmdb c/vec-meta-dbi domain :string :data)]
    (if (nil? meta-val)
      false
      (let [total-bytes (long (:total-bytes meta-val))
            chunk-count (long (:chunk-count meta-val))]
        (if (<= total-bytes (long c/*wal-vec-max-buffer-bytes*))
          ;; Buffer mode: read chunks into byte array, copy to native, load
          (let [ba (byte-array total-bytes)]
            (loop [i 0, offset 0]
              (when (< i chunk-count)
                (let [chunk ^bytes (i/get-value lmdb c/vec-index-dbi
                                               [domain i] :data :bytes)
                      len   (alength chunk)]
                  (System/arraycopy chunk 0 ba (int offset) len)
                  (recur (inc i) (+ offset (long len))))))
            (let [buf (BytePointer. ba)]
              (try
                (VecIdx/loadBuffer index buf total-bytes)
                (finally (.close buf)))))
          ;; File-spool mode: write chunks to temp file, load from file
          (let [tmp-dir  (File. (str (i/env-dir lmdb) u/+separator+ "tmp"))
                _        (.mkdirs tmp-dir)
                tmp-file (File/createTempFile "vec-load-" ".tmp" tmp-dir)]
            (try
              (with-open [fos (FileOutputStream. tmp-file)]
                (dotimes [i chunk-count]
                  (let [chunk ^bytes (i/get-value lmdb c/vec-index-dbi
                                                 [domain i] :data :bytes)]
                    (.write fos chunk))))
              (VecIdx/load index (.getAbsolutePath tmp-file))
              (finally (.delete tmp-file)))))
        true))))

(defn- clear-vec-blobs
  "Delete all chunks for domain from vec-index-dbi and metadata from
   vec-meta-dbi."
  [lmdb ^String domain]
  (let [meta-val (i/get-value lmdb c/vec-meta-dbi domain :string :data)]
    (when meta-val
      (let [chunk-count (long (:chunk-count meta-val))
            txs         (java.util.ArrayList.)]
        (dotimes [i chunk-count]
          (.add txs (l/kv-tx :del c/vec-index-dbi [domain i] nil :data)))
        (.add txs (l/kv-tx :del c/vec-meta-dbi domain nil :string))
        (binding [c/*bypass-wal*    true
                  c/*trusted-apply* true]
          (i/transact-kv lmdb txs))))))

;;; ---------------------------------------------------------------

(def default-search-opts {:display    :refs
                          :top        10
                          :vec-filter (constantly true)})

(defn- vec-save-key* [fname] (->> fname hash (str "vec-save-") keyword))

(def vec-save-key (memoize vec-save-key*))

(deftype AsyncVecSave [vec-index fname ^ReentrantReadWriteLock vec-lock]
  IAsyncWork
  (work-key [_] (vec-save-key fname))
  (do-work [_]
    (let [rlock (.readLock vec-lock)]
      (when (.tryLock rlock)
        (try
          (when-not (i/vec-closed? vec-index)
            (i/persist-vecs vec-index))
          (catch Throwable _)
          (finally
            (.unlock rlock))))))
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
                      ^Map search-opts
                      ^ReentrantReadWriteLock vec-lock
                      ^String domain
                      wal-mode?]
  IVectorIndex
  (add-vec [this vec-ref vec-data]
    (let [vec-id  (.incrementAndGet max-vec)
          vec-arr (vec->arr dimensions quantization vec-data)]
      (add index quantization vec-id vec-arr)
      (when-not wal-mode?
        (a/exec (a/get-executor) (AsyncVecSave. this fname vec-lock)))
      (.put vecs vec-id vec-ref)
      ;; In WAL mode, bypass WAL for vecs-dbi writes so list DBI data
      ;; goes directly to LMDB base (list DBIs don't support WAL overlay).
      (if wal-mode?
        (binding [c/*bypass-wal*    true
                  c/*trusted-apply* true]
          (i/transact-kv
            lmdb [(l/kv-tx :put vecs-dbi vec-ref vec-id :data :id)]))
        (i/transact-kv
          lmdb [(l/kv-tx :put vecs-dbi vec-ref vec-id :data :id)]))
      vec-id))

  (get-vec [_ vec-ref]
    (let [ids (i/get-list lmdb vecs-dbi vec-ref :data :id)]
      (for [^long id ids]
        (get-vec* index id quantization dimensions))))

  (remove-vec [this vec-ref]
    (let [ids (i/get-list lmdb vecs-dbi vec-ref :data :id)]
      (doseq [^long id ids]
        (VecIdx/remove index id)
        (.remove vecs id))
      (when-not wal-mode?
        (a/exec (a/get-executor) (AsyncVecSave. this fname vec-lock)))
      (if wal-mode?
        (binding [c/*bypass-wal*    true
                  c/*trusted-apply* true]
          (i/transact-kv lmdb [(l/kv-tx :del vecs-dbi vec-ref)]))
        (i/transact-kv lmdb [(l/kv-tx :del vecs-dbi vec-ref)]))))

  (persist-vecs [_]
    (when-not @closed?
      (if wal-mode?
        (checkpoint-to-lmdb lmdb index domain)
        (VecIdx/save index fname))))

  (close-vecs [this]
    (let [wlock (.writeLock vec-lock)]
      (.lock wlock)
      (try
        (when-not (.vec-closed? this)
          (.persist_vecs this)
          (vreset! closed? true)
          (swap! l/vector-indices dissoc fname)
          (VecIdx/free index))
        (finally
          (.unlock wlock)))))

  (vec-closed? [_] @closed?)

  (clear-vecs [this]
    (.close-vecs this)
    (.empty vecs)
    (i/clear-dbi lmdb vecs-dbi)
    (if wal-mode?
      (clear-vec-blobs lmdb domain)
      (u/delete-files fname)))

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

  (vec-indexed? [_ vec-ref] (i/get-value lmdb vecs-dbi vec-ref))

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
            (i/add-vec new vec-ref vec-data))
          (.close dis)
          (u/delete-files dfname)
          new))
      (catch Exception e
        (u/raise "Unable to re-index vectors. " e {:dir (i/env-dir lmdb)})))))

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

(defn new-vector-index*
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
  (let [vecs-dbi  (str domain "/" c/vec-refs)
        wal-mode? (boolean (:kv-wal? (i/env-opts lmdb)))]
    (open-dbi lmdb vecs-dbi)
    (when wal-mode?
      (open-vec-blob-dbis lmdb))
    (let [[max-vec-id vecs] (init-vecs lmdb vecs-dbi)
          fname             (index-fname lmdb domain)
          index             (create-index dimensions metric-type
                                          quantization connectivity
                                          expansion-add expansion-search)]
      ;; Load index data
      (if wal-mode?
        (let [loaded? (load-from-lmdb lmdb index domain)]
          (when-not loaded?
            (when (u/file-exists fname)
              ;; Migration from file-based to LMDB blob
              (VecIdx/load index fname)
              (checkpoint-to-lmdb lmdb index domain)
              (u/delete-files fname))))
        (when (u/file-exists fname)
          (VecIdx/load index fname)))
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
                     search-opts
                     (ReentrantReadWriteLock.)
                     domain
                     wal-mode?))))

(defn new-vector-index
  [lmdb opts]
  (if (instance? KVStore lmdb)
    (r/new-vector-index lmdb opts)
    (new-vector-index* lmdb opts)))

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
                 (.-search-opts old)
                 (ReentrantReadWriteLock.)
                 (.-domain old)
                 (.-wal-mode? old)))

(defn attr-domain [attr] (s/replace (u/keyword->string attr) "/" "_"))
