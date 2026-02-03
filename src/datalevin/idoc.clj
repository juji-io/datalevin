;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.idoc
  "Indexed document parsing and validation utilities."
  (:require
   [clojure.set :as set]
   [clojure.string :as str]
   [datalevin.buffer :as bf]
   [datalevin.bits :as b]
   [datalevin.constants :as c]
   [datalevin.datom :as d]
   [datalevin.interface :as i
    :refer [open-dbi open-list-dbi get-value visit transact-kv]]
   [datalevin.lmdb :as l]
   [datalevin.util :as u :refer [raise]]
   [cybermonday.ir :as cm-ir]
   [jsonista.core :as json])
  (:import
   [java.util ArrayList HashMap IdentityHashMap Arrays]
   [java.util.concurrent.atomic AtomicInteger AtomicLong]
   [org.eclipse.collections.impl.list.mutable FastList]
   [java.math BigDecimal BigInteger]))

(def ^:private default-format :edn)

(def ^:private allowed-formats #{:edn :json :markdown})

(def ^:private json-mapper
  (json/object-mapper {:decode-key-fn identity}))

(defn- resolve-format
  [attr props]
  (let [fmt (or (:db/idocFormat props) default-format)]
    (when-not (allowed-formats fmt)
      (raise "Bad attribute specification for " attr
             ": {:db/idocFormat " fmt "} should be one of " allowed-formats
             {:error     :schema/validation
              :attribute attr
              :key       :db/idocFormat
              :value     fmt}))
    fmt))

(defn- normalize-header
  [s]
  (let [s (-> s str str/trim str/lower-case)
        s (str/replace s #"^[0-9]+[.\)\s-]*" "")
        s (str/replace s #"[^\p{L}\p{Nd}\s-]" "")
        s (str/replace s #"\s+" "-")
        s (str/replace s #"-+" "-")
        s (str/replace s #"(^-+|-+$)" "")]
    (when (str/blank? s)
      (raise "Markdown header normalizes to empty string" {}))
    (keyword s)))

(defn- ensure-map
  [m path ctx]
  (if (empty? path)
    m
    (let [v (get-in m path)]
      (cond
        (nil? v) (assoc-in m path {})
        (map? v) m
        :else    (raise "Markdown header has both content and subheaders at "
                        ctx {:path path})))))

(defn- set-content
  [m path lines]
  (let [content (str/join "\n" lines)]
    (cond
      (empty? path)
      (do
        (when-not (str/blank? content)
          (raise "Markdown content appears before any header" {}))
        m)

      (str/blank? content)
      m

      :else
      (let [existing (get-in m path)]
        (cond
          (nil? existing) (assoc-in m path content)
          (map? existing) (raise "Markdown header has both content and subheaders"
                                 {:path path})
          :else           (assoc-in m path content))))))

(defn- hiccup-node?
  [x]
  (and (vector? x) (keyword? (first x))))

(defn- node-children
  [node]
  (let [[_ a & more] node]
    (if (map? a) more (cons a more))))

(defn- node-text
  [node]
  (cond
    (string? node) node
    (vector? node)
    (let [tag (first node)
          children (node-children node)]
      (case tag
        (:markdown/soft-line-break :markdown/hard-line-break :br) "\n"
        (apply str (map node-text children))))
    (sequential? node) (apply str (map node-text node))
    :else ""))

(defn- header-node?
  [node]
  (and (vector? node)
       (let [tag (first node)]
         (or (= tag :markdown/heading)
             (and (keyword? tag)
                  (re-matches #"h[1-6]" (name tag)))))))

(defn- header-level
  [node]
  (let [tag (first node)]
    (cond
      (= tag :markdown/heading) (:level (second node))
      (keyword? tag) (Long/parseLong (subs (name tag) 1))
      :else nil)))

(defn- header-text
  [node]
  (let [tag (first node)
        children (node-children node)]
    (case tag
      :markdown/heading (node-text (if (map? (second node)) (drop 2 node) children))
      (node-text children))))

(defn- top-level-nodes
  [ir]
  (cond
    (and (vector? ir)
         (keyword? (first ir)))
    (let [tag (first ir)
          children (node-children ir)]
      (if (or (= tag :markdown/document) (= tag :div))
        children
        [ir]))
    (sequential? ir) ir
    :else [ir]))

(defn- parse-markdown
  [s]
  (let [ir      (cm-ir/md-to-ir (str s))
        nodes   (top-level-nodes ir)
        result  (volatile! {})
        levels  (volatile! [])
        path    (volatile! [])
        content (volatile! [])]
    (doseq [node nodes]
      (if (header-node? node)
        (let [level (long (header-level node))
              title (header-text node)
              key   (normalize-header title)]
          (vswap! result set-content @path @content)
          (vreset! content [])
          (loop []
            (when (and (seq @levels)
                       (>= (long (peek @levels)) level))
              (vswap! levels pop)
              (vswap! path pop)
              (recur)))
          (vswap! result ensure-map @path {:title title})
          (when (contains? (get-in @result @path) key)
            (raise "Markdown header collision after normalization: " title
                   {:path @path :header title}))
          (vswap! path conj key)
          (vswap! levels conj level))
        (let [text (node-text node)]
          (when (and (empty? @path) (not (str/blank? text)))
            (raise "Markdown content appears before any header" {}))
          (when-not (str/blank? text)
            (vswap! content conj text)))))
    (vswap! result set-content @path @content)
    @result))

(defn- parse-json
  [s]
  (try
    (json/read-value s json-mapper)
    (catch Exception e
      (raise "Invalid JSON string for idoc" {:error e}))))

(defn- normalize-doc
  [doc {:keys [idoc/max-depth]}]
  (let [seen      (IdentityHashMap.)
        max-depth (when max-depth (long max-depth))]
    (letfn [(walk-coll [x depth f]
              (when (.containsKey seen x)
                (raise "Circular reference in idoc document" {}))
              (.put seen x Boolean/TRUE)
              (try
                (f depth)
                (finally
                  (.remove seen x))))
            (walk [x depth]
              (cond
                (map? x)
                (walk-coll
                  x depth
                  (fn [d]
                    (let [next-depth (unchecked-inc ^long d)]
                      (when (and max-depth (> next-depth max-depth))
                        (raise "Idoc exceeds max depth"
                               {:max-depth max-depth :depth next-depth}))
                      (reduce-kv
                        (fn [m k v]
                          (when-not (or (keyword? k) (string? k))
                            (raise "Idoc keys must be keywords or strings"
                                   {:key k}))
                          (when (identical? v :json/null)
                            (raise "Literal :json/null is reserved" {:key k}))
                          (when (and (sequential? v) (not (vector? v)))
                            (raise "Lists are not valid idoc values; use vectors"
                                   {:key k}))
                          (assoc m k (walk v next-depth)))
                        {} x))))

                (vector? x)
                (walk-coll
                  x depth
                  (fn [d]
                    (mapv #(walk % d) x)))

                (and (sequential? x) (not (vector? x)))
                (raise "Lists are not valid idoc values; use vectors" {})

                (identical? x :json/null)
                (raise "Literal :json/null is reserved" {})

                (nil? x)
                :json/null

                :else
                x))]
      (walk doc 0))))

(defn parse-value
  [attr props opts v]
  (let [fmt (resolve-format attr props)
        doc (cond
              (string? v)
              (case fmt
                :json     (parse-json v)
                :markdown (parse-markdown v)
                :edn      (raise "String input is not allowed for :edn idoc"
                                 {:attribute attr}))

              (map? v) v

              :else
              (raise "Idoc root must be a map" {:attribute attr :value v}))]
    (when-not (map? doc)
      (raise "Idoc root must be a map" {:attribute attr :value doc}))
    (normalize-doc doc opts)))

;; path encoding

(defn- encode-string-seg
  [s]
  (let [s (str s)
        s (str/replace s "%" "%25")
        s (str/replace s "/" "%2F")]
    (if (str/starts-with? s ":")
      (str "%3A" (subs s 1))
      s)))

(defn encode-path
  [segments]
  (reduce
    (fn [acc seg]
      (if (keyword? seg)
        (str acc "/:" (subs (str seg) 1))
        (str acc "/" (encode-string-seg seg))))
    "" segments))

(defn- decode-string-seg
  [s]
  (let [s (if (str/starts-with? s "%3A")
            (str ":" (subs s 3))
            s)
        s (str/replace s "%2F" "/")
        s (str/replace s "%25" "%")]
    s))

(defn decode-path
  [path]
  (when-not (str/starts-with? path "/")
    (raise "Idoc path must start with '/'" {:path path}))
  (let [len (count path)]
    (loop [idx 0
           out []]
      (if (>= idx len)
        out
        (let [idx1 (unchecked-inc idx)]
          (if (and (= (nth path (int idx)) \/)
                   (< idx1 len)
                   (= (nth path (int idx1)) \:))
            (let [start (+ idx 2)
                  next  (long (or (str/index-of path "/:" start) len))
                  seg   (subs path (int start) (int next))]
              (recur next (conj out (keyword seg))))
            (let [start idx1
                  next  (long (or (str/index-of path "/" start) len))
                  seg   (subs path (int start) (int next))]
              (recur next (conj out (decode-string-seg seg))))))))))

;; value typing for index encoding

(defn- value-type
  [v]
  (cond
    (instance? BigInteger v) [:db.type/bigint v]
    (instance? clojure.lang.BigInt v) [:db.type/bigint (biginteger v)]
    (integer? v) [:db.type/long (long v)]
    (instance? BigDecimal v) [:db.type/bigdec v]
    (ratio? v) [:db.type/bigdec (bigdec v)]
    (float? v) [:db.type/float (float v)]
    (double? v) [:db.type/double (double v)]
    (number? v) [:db.type/double (double v)]
    (string? v) [:db.type/string v]
    (keyword? v) [:db.type/keyword v]
    (symbol? v) [:db.type/symbol v]
    (boolean? v) [:db.type/boolean v]
    (uuid? v) [:db.type/uuid v]
    (inst? v) [:db.type/instant v]
    (bytes? v) [:db.type/bytes v]
    :else [:data v]))

(defn- indexable-key
  [^long path-id v]
  (let [[vt v'] (value-type v)
        idx     (b/indexable 0 (int path-id) v' vt c/g0)]
    (when (b/giant? idx)
      (raise "Idoc value too large to index" {:value v}))
    idx))

(defn- indexable->bytes
  [idx]
  (let [^java.nio.ByteBuffer bf (bf/get-array-buffer)]
    (b/put-buffer bf idx :avg)
    (let [res (Arrays/copyOfRange (.array bf) 0 (.position bf))]
      (bf/return-array-buffer bf)
      res)))

(defn- doc->path-values
  [doc]
  (let [acc (transient {})]
    (letfn [(add-leaf [path v]
              (let [p (encode-path path)]
                (assoc! acc p (conj (get acc p #{}) v))))
            (walk [node path]
              (cond
                (map? node) (doseq [[k v] node] (walk v (conj path k)))
                (vector? node) (doseq [v node] (walk v path))
                :else (add-leaf path node)))]
      (walk doc [])
      (persistent! acc))))

(defn- ensure-path-capacity
  [^ArrayList id->path ^long path-id]
  (while (<= (.size id->path) path-id)
    (.add id->path nil)))

(defn- ensure-path-id
  [^HashMap path->id ^ArrayList id->path ^AtomicInteger max-path
   path-dict-dbi path ^FastList txs]
  (if-let [pid (.get path->id path)]
    pid
    (let [pid (.incrementAndGet max-path)]
      (.put path->id path pid)
      (ensure-path-capacity id->path pid)
      (.set id->path pid path)
      (.add txs (l/kv-tx :put path-dict-dbi path pid :string :int))
      pid)))

(defn- init-paths
  [lmdb path-dict-dbi]
  (let [path->id (HashMap.)
        id->path (ArrayList.)
        max-id   (volatile! 0)
        load     (fn [kv]
                   (let [path (b/read-buffer (l/k kv) :string)
                         pid  (b/read-buffer (l/v kv) :int)]
                     (when (< ^long @max-id ^long pid) (vreset! max-id pid))
                     (.put path->id path pid)
                     (ensure-path-capacity id->path pid)
                     (.set id->path pid path)))]
    (visit lmdb path-dict-dbi load [:all-back])
    [@max-id path->id id->path]))

(defn- init-doc-refs
  [lmdb doc-ref-dbi]
  (let [doc-refs (HashMap.)
        max-id   (volatile! 0)
        load     (fn [kv]
                   (let [ref (b/read-buffer (l/k kv) :data)
                         did (b/read-buffer (l/v kv) :id)]
                     (when (< ^long @max-id ^long did) (vreset! max-id did))
                     (.put doc-refs (long did) ref)))]
    (visit lmdb doc-ref-dbi load [:all-back])
    [@max-id doc-refs]))

(defn- open-dbis
  [lmdb domain]
  (let [doc-ref-dbi   (str domain "/" c/idoc-doc-ref)
        doc-index-dbi (str domain "/" c/idoc-doc-index)
        path-dict-dbi (str domain "/" c/idoc-path-dict)]
    (open-dbi lmdb doc-ref-dbi {:key-size c/+max-key-size+})
    (open-list-dbi lmdb doc-index-dbi {:key-size c/+max-key-size+
                                       :val-size c/+id-bytes+})
    (open-dbi lmdb path-dict-dbi {:key-size c/+max-key-size+})
    [doc-ref-dbi doc-index-dbi path-dict-dbi]))

(deftype IdocIndex [lmdb
                    domain
                    format
                    doc-ref-dbi
                    doc-index-dbi
                    path-dict-dbi
                    path->id
                    id->path
                    doc-refs
                    ^AtomicLong max-doc
                    ^AtomicInteger max-path])

(defn new-idoc-index
  [lmdb {:keys [domain format] :as _opts}]
  (let [[doc-ref-dbi doc-index-dbi path-dict-dbi] (open-dbis lmdb domain)
        [max-path path->id id->path] (init-paths lmdb path-dict-dbi)
        [max-doc doc-refs]           (init-doc-refs lmdb doc-ref-dbi)]
    (->IdocIndex lmdb
                 domain
                 format
                 doc-ref-dbi
                 doc-index-dbi
                 path-dict-dbi
                 path->id
                 id->path
                 doc-refs
                 (AtomicLong. max-doc)
                 (AtomicInteger. max-path))))

(defn transfer
  [^IdocIndex old lmdb]
  (->IdocIndex lmdb
               (.-domain old)
               (.-format old)
               (.-doc-ref-dbi old)
               (.-doc-index-dbi old)
               (.-path-dict-dbi old)
               (.-path->id old)
               (.-id->path old)
               (.-doc-refs old)
               (.-max-doc old)
               (.-max-path old)))

(defn add-doc
  ([index doc-ref doc] (add-doc index doc-ref doc true))
  ([^IdocIndex index doc-ref doc check-exist?]
   (if (and check-exist?
            (get-value (.-lmdb index) (.-doc-ref-dbi index)
                       doc-ref :data :id))
     :doc-exists
     (let [txs         (FastList.)
           lmdb        (.-lmdb index)
           doc-id      (.incrementAndGet ^AtomicLong (.-max-doc index))
           path->id    (.-path->id index)
           id->path    (.-id->path index)
           max-path    (.-max-path index)
           path-dbi    (.-path-dict-dbi index)
           index-dbi   (.-doc-index-dbi index)
           doc-ref-dbi (.-doc-ref-dbi index)
           doc-refs    (.-doc-refs index)]
       (.add txs (l/kv-tx :put doc-ref-dbi doc-ref doc-id :data :id))
       (.put ^HashMap doc-refs (long doc-id) doc-ref)
       (doseq [[path values] (doc->path-values doc)
               :let [pid (ensure-path-id path->id id->path max-path
                                         path-dbi path txs)]]
         (doseq [v values
                 :let [idx (indexable-key pid v)
                       key (indexable->bytes idx)]]
           (.add txs (l/kv-tx :put index-dbi key doc-id :raw :id))))
       (transact-kv lmdb txs)
       :doc-added))))

(defn remove-doc
  [^IdocIndex index doc-ref doc]
  (when-let [doc-id (get-value (.-lmdb index) (.-doc-ref-dbi index)
                               doc-ref :data :id)]
    (let [txs       (FastList.)
          index-dbi (.-doc-index-dbi index)
          doc-refs  (.-doc-refs index)
          lmdb      (.-lmdb index)]
      (doseq [[path values] (doc->path-values doc)
              :let [pid (.get ^HashMap (.-path->id index) path)]]
        (when pid
          (doseq [v values
                  :let [idx (indexable-key pid v)
                        key (indexable->bytes idx)]]
            (.add txs (l/kv-tx :del-list index-dbi key [doc-id] :raw :id)))))
      (.add txs (l/kv-tx :del (.-doc-ref-dbi index) doc-ref :data))
      (.remove ^HashMap doc-refs (long doc-id))
      (transact-kv lmdb txs)
      :doc-removed)))

;; query evaluation

(defn- normalize-seg
  [format seg]
  (if (= format :markdown)
    (cond
      (and (keyword? seg) (#{:? :*} seg)) seg
      (keyword? seg) (normalize-header (subs (str seg) 1))
      (string? seg) (normalize-header seg)
      :else seg)
    seg))

(defn- normalize-path
  [format segments]
  (if (= format :markdown)
    (mapv #(normalize-seg format %) segments)
    segments))

(defn- path-wildcards?
  [segments]
  (some #(and (keyword? %) (#{:? :*} %)) segments))

(defn- match-path?
  [pattern path]
  (let [p    (vec pattern)
        t    (vec path)
        lp   (count p)
        lt   (count t)
        memo (atom {})]
    (letfn [(step [^long i ^long j]
              (if-let [res (get @memo [i j])]
                res
                (let [res (cond
                            (= i lp) (= j lt)
                            :else
                            (let [seg (nth p (int i))]
                              (cond
                                (= seg :*)
                                (or (step (unchecked-inc i) j)
                                    (and (< j lt) (step i (unchecked-inc j))))

                                (= seg :?)
                                (and (< j lt)
                                     (step (unchecked-inc i) (unchecked-inc j)))

                                :else
                                (and (< j lt)
                                     (= seg (nth t (int j)))
                                     (step (unchecked-inc i)
                                           (unchecked-inc j))))))]
                  (swap! memo assoc [i j] res)
                  res)))]
      (step 0 0))))

(defn- path-expr?
  [x]
  (or (keyword? x) (string? x) (vector? x)))

(defn- path-expr->segments
  [x]
  (cond
    (keyword? x) [x]
    (string? x) [x]
    (vector? x) x
    :else (raise "Idoc path must be keyword, string, or vector" {:path x})))

(defn- strict-eq?
  [a b]
  (let [[ta a'] (value-type a)
        [tb b'] (value-type b)]
    (and (= ta tb) (= a' b'))))

(defn- pred-match?
  [op doc-val args]
  (let [[t v] (value-type doc-val)
        cmp-val (fn [x]
                  (let [[t2 v2] (value-type x)]
                    (when (= t t2) v2)))]
    (case op
      :nil? (= doc-val :json/null)
      :between (when-let [lo (cmp-val (first args))]
                 (when-let [hi (cmp-val (second args))]
                   (and (not (nil? v))
                        (<= (compare lo v) 0)
                        (<= (compare v hi) 0))))
      (:> :>= :< :<=)
      (when-let [rhs (cmp-val (first args))]
        (and (not (nil? v))
             (case op
               :>  (pos? (compare v rhs))
               :>= (not (neg? (compare v rhs)))
               :<  (neg? (compare v rhs))
               :<= (not (pos? (compare v rhs)))))))))

(declare parse-predicate get-path values-for-path)

(defn- doc-matches*
  [format doc expr ctx-path]
  (cond
    (map? expr)
    (cond
      (vector? doc)
      (boolean (some #(doc-matches* format % expr ctx-path) doc))

      (map? doc)
      (every?
        (fn [[k v]]
          (let [k' (normalize-seg format k)]
            (cond
              (= k' :?)
              (boolean
                (some (fn [[ck cv]]
                        (doc-matches* format cv v
                                      (conj (or ctx-path [])
                                            (normalize-seg format ck))))
                      doc))

              (= k' :*)
              (letfn [(match-depth [node path]
                        (or (doc-matches* format node v path)
                            (cond
                              (map? node)
                              (boolean
                                (some (fn [[ck cv]]
                                        (match-depth cv
                                                     (conj path
                                                           (normalize-seg format ck))))
                                      node))

                              (vector? node)
                              (boolean (some #(match-depth % path) node))

                              :else false)))]
                (match-depth doc (or ctx-path [])))

              :else
              (doc-matches* format (get doc k') v (conj (or ctx-path []) k')))))
        expr)

      :else false)

    (vector? expr)
    (let [[op & rest] expr]
      (case op
        :and (every? #(doc-matches* format doc % nil) rest)
        :or  (boolean (some #(doc-matches* format doc % nil) rest))
        :not (not (doc-matches* format doc (first rest) nil))
        (raise "Unknown idoc logical operator" {:op op})))

    (and (sequential? expr) (not (vector? expr)))
    (let [op (keyword (first expr))
          args (rest expr)]
      (if (and (nil? ctx-path) (seq args) (path-expr? (first args)))
        (let [{:keys [path args]} (parse-predicate expr nil)
              vals (values-for-path format doc path)]
          (boolean (some #(pred-match? op % args) vals)))
        (cond
          (and (= op :nil?) (vector? doc))
          (boolean (some #(pred-match? :nil? % []) doc))

          (vector? doc)
          (boolean (some #(pred-match? op % args) doc))

          :else
          (pred-match? op doc args))))

    :else
    (cond
      (vector? doc) (boolean (some #(strict-eq? % expr) doc))
      (nil? doc) false
      :else (strict-eq? doc expr))))

(defn doc-ref->doc
  [lmdb doc-ref]
  (if (and (vector? doc-ref) (= :g (first doc-ref)))
    (let [datom (get-value lmdb c/giants (second doc-ref) :id :data)]
      (when datom (d/datom-v datom)))
    (peek doc-ref)))

(defn get-path
  [doc segments]
  (letfn [(step [node segs]
            (if (empty? segs)
              node
              (let [seg  (first segs)
                    rest (rest segs)]
                (cond
                  (nil? node) nil

                  (vector? node)
                  (if (integer? seg)
                    (if (and (<= 0 (long seg)) (< (long seg) (count node)))
                      (step (nth node (long seg)) rest)
                      nil)
                    (let [vals (keep #(step % segs) node)]
                      (when (seq vals) (vec vals))))

                  (map? node)
                  (step (get node seg) rest)

                  :else nil))))]
    (step doc segments)))

(defn- values-for-path
  [format doc path]
  (let [path (normalize-path format path)]
    (if (path-wildcards? path)
      (let [path-values (doc->path-values doc)]
        (seq
          (mapcat
            identity
            (for [[p vals] path-values
                  :let [segs (decode-path p)]
                  :when (match-path? path segs)]
              vals))))
      (let [v (get-path doc path)]
        (cond
          (nil? v) nil
          (vector? v) v
          :else [v])))))

(defn- indexable-key*
  [^long path-id vt v]
  (let [idx (b/indexable 0 (int path-id) v vt c/g0)]
    (when (b/giant? idx)
      (raise "Idoc value too large to index" {:value v}))
    idx))

(defn- ids-for-eq-path-id
  [^IdocIndex index ^long pid value]
  (let [idx (indexable-key pid value)
        key (indexable->bytes idx)
        res (i/get-list (.-lmdb index) (.-doc-index-dbi index)
                        key :raw :id)]
    (if res (set res) #{})))

(defn- matching-path-ids
  [^IdocIndex index path]
  (let [path->id (.-path->id index)]
    (reduce
      (fn [acc [p pid]]
        (if (match-path? path (decode-path p))
          (conj acc pid)
          acc))
      [] path->id)))

(defn- ids-for-eq
  [^IdocIndex index path value]
  (if (path-wildcards? path)
    (reduce set/union #{} (map #(ids-for-eq-path-id index % value)
                               (matching-path-ids index path)))
    (if-let [pid (.get ^HashMap (.-path->id index) (encode-path path))]
      (ids-for-eq-path-id index pid value)
      #{})))

(defn- ids-for-range-path-id
  [^IdocIndex index ^long pid lo hi]
  (let [[lo-t lo-v] (when lo (value-type lo))
        [hi-t hi-v] (when hi (value-type hi))
        vt          (or lo-t hi-t)]
    (when (and lo-t hi-t (not= lo-t hi-t))
      (raise "Range bounds must have the same type" {:lo lo :hi hi}))
    (when (= vt :data)
      (raise "Range predicates do not support :data values" {:value (or lo hi)}))
    (let [min-key (indexable->bytes (indexable-key* pid vt c/v0))
          max-key (indexable->bytes (indexable-key* pid vt c/vmax))
          low     (if lo (indexable->bytes (indexable-key* pid vt lo-v)) min-key)
          high    (if hi (indexable->bytes (indexable-key* pid vt hi-v)) max-key)
          res     (i/list-range (.-lmdb index) (.-doc-index-dbi index)
                                [:closed low high] :raw [:all] :id)]
      (if res (set (map second res)) #{}))))

(defn- ids-for-range
  [^IdocIndex index path lo hi]
  (if (path-wildcards? path)
    (reduce set/union #{} (map #(ids-for-range-path-id index % lo hi)
                               (matching-path-ids index path)))
    (if-let [pid (.get ^HashMap (.-path->id index) (encode-path path))]
      (ids-for-range-path-id index pid lo hi)
      #{})))

(defn- parse-predicate
  [expr ctx-path]
  (let [op   (keyword (first expr))
        args (rest expr)
        has-path? (and (seq args) (path-expr? (first args)))
        _ (when (and ctx-path has-path?)
            (raise "Map value predicates cannot specify a path" {:expr expr}))
        [path args]
        (if has-path?
          [(path-expr->segments (first args)) (rest args)]
          [ctx-path args])]
    (when-not (seq path)
      (raise "Predicate requires a path" {:expr expr}))
    {:op op :path path :args args}))

(defn- ids-for-predicate
  [^IdocIndex index format expr ctx-path]
  (let [{:keys [op path args]} (parse-predicate expr ctx-path)
        path (normalize-path format path)]
    (case op
      :nil? (ids-for-eq index path :json/null)
      :between (ids-for-range index path (first args) (second args))
      :> (ids-for-range index path (first args) nil)
      :>= (ids-for-range index path (first args) nil)
      :< (ids-for-range index path nil (first args))
      :<= (ids-for-range index path nil (first args))
      (raise "Unknown idoc predicate" {:op op}))))

(defn- intersect-ids
  [a b]
  (cond
    (nil? a) b
    (nil? b) a
    :else (set/intersection a b)))

(defn- union-ids
  [a b]
  (cond
    (nil? a) nil
    (nil? b) nil
    :else (set/union a b)))

(defn- all-doc-ids
  [^IdocIndex index]
  (let [m (.-doc-refs index)]
    (set (.keySet ^HashMap m))))

(defn- ids-for-expr
  [^IdocIndex index format expr ctx-path]
  (cond
    (map? expr)
    (reduce
      (fn [acc [k v]]
        (let [k'   (normalize-seg format k)
              path (conj (or ctx-path []) k')
              ids  (ids-for-expr index format v path)]
          (intersect-ids acc ids)))
      nil expr)

    (vector? expr)
    (let [[op & rest] expr]
      (case op
        :and (reduce (fn [acc e]
                       (intersect-ids acc (ids-for-expr index format e ctx-path)))
                     nil rest)
        :or (reduce (fn [acc e]
                      (union-ids acc (ids-for-expr index format e ctx-path)))
                    #{} rest)
        :not nil
        (raise "Unknown idoc logical operator" {:op op})))

    (and (sequential? expr) (not (vector? expr)))
    (ids-for-predicate index format expr ctx-path)

    :else
    (if (seq ctx-path)
      (do
        (when (nil? expr)
          (raise "Use (nil? :field) to match null values" {:path ctx-path}))
        (ids-for-eq index (normalize-path format ctx-path) expr))
      (raise "Idoc scalar query must be inside a map" {:expr expr}))))

(defn candidate-ids
  [^IdocIndex index expr]
  (let [format (.-format index)
        ids    (ids-for-expr index format expr nil)]
    (if (nil? ids) (all-doc-ids index) ids)))

(defn matches-doc?
  [^IdocIndex index doc expr]
  (doc-matches* (.-format index) doc expr nil))
