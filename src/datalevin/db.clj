;;
;; Copyright (c) Nikita Prokopov, Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.db
  "Datalog DB abstraction"
  (:refer-clojure :exclude [update assoc sync])
  (:require
   [clojure.walk]
   [clojure.data]
   [clojure.set]
   [datalevin.constants :as c :refer [e0 tx0 emax txmax v0 vmax]]
   [datalevin.datom :as d :refer [datom datom-added datom?]]
   [datalevin.util :as u
    :refer [case-tree raise defrecord-updatable conjv conjs concatv cond+]]
   [datalevin.storage :as s]
   [datalevin.bits :as b]
   [datalevin.remote :as r]
   [datalevin.inline :refer [update assoc]]
   [datalevin.interface :as i
    :refer [last-modified dir opts schema rschema ave-tuples ave-tuples-list
            sample-ave-tuples sample-ave-tuples-list e-sample eav-scan-v
            eav-scan-v-list val-eq-scan-e val-eq-scan-e-list val-eq-filter-e
            val-eq-filter-e-list fetch slice slice-filter e-datoms av-datoms
            ea-first-datom head-filter e-first-datom av-first-datom head
            size size-filter e-size av-size actual-a-size a-size v-size
            datom-count populated? rslice cardinality
            av-range-size init-max-eid db-name start-sampling load-datoms
            stop-sampling close av-first-e ea-first-v v-datoms assoc-opt
            max-tx get-env-flags set-env-flags sync abort-transact-kv]])
  (:import
   [datalevin.datom Datom]
   [datalevin.interface IStore]
   [datalevin.storage Store]
   [datalevin.remote DatalogStore]
   [datalevin.utl LRUCache]
   [java.util SortedSet Comparator Date]
   [java.util.concurrent ConcurrentHashMap]
   [java.io Writer]
   [org.eclipse.collections.impl.set.sorted.mutable TreeSortedSet]))

;;;;;;;;;; Protocols

(defprotocol ISearch
  (-search [db pattern])
  (-count [db pattern] [data pattern cap] [data pattern cap actual?])
  (-first [db pattern]))

(defprotocol IIndexAccess
  (-populated? [db index c1 c2 c3])
  (-datoms [db index] [db index c1] [db index c1 c2] [db index c1 c2 c3]
    [db index c1 c2 c3 n])
  (-e-datoms [db e])
  (-av-datoms [db attr v])
  (-range-datoms [db index start-datom end-datom])
  (-seek-datoms [db index c1 c2 c3] [db index c1 c2 c3 n])
  (-rseek-datoms [db index c1 c2 c3] [db index c1 c2 c3 n])
  (-cardinality [db attr])
  (-index-range [db attr start end])
  (-index-range-size [db attr start end] [db attr start end cap]))

(defprotocol IDB
  (-schema [db])
  (-rschema [db])
  (-attrs-by [db property])
  (-is-attr? [db attr property])
  (-clear-tx-cache [db]))

(defprotocol ISearchable (-searchable? [_]))

(extend-type Object ISearchable (-searchable? [_] false))
(extend-type nil ISearchable (-searchable? [_] false))

(defprotocol ITuples
  (-init-tuples [db out a v-range pred get-v?])
  (-init-tuples-list [db a v-range pred get-v?])
  (-sample-init-tuples [db out a mcount v-range pred get-v?])
  (-sample-init-tuples-list [db a mcount v-range pred get-v?])
  (-e-sample [db a])
  (-eav-scan-v [db in out eid-idx attrs-v])
  (-eav-scan-v-list [db in eid-idx attrs-v])
  (-val-eq-scan-e [db in out v-idx attr] [db in out v-idx attr bound])
  (-val-eq-scan-e-list [db in v-idx attr] [db in v-idx attr bound])
  (-val-eq-filter-e [db in out v-idx attr f-idx])
  (-val-eq-filter-e-list [db in v-idx attr f-idx]))

;; ----------------------------------------------------------------------------

(declare empty-db resolve-datom validate-attr components->pattern
         components->end-datom)

(defrecord TxReport [db-before db-after tx-data tempids tx-meta])

;; (defmethod print-method TxReport [^TxReport rp, ^java.io.Writer w]
;;   (binding [*out* w]
;;     (pr {:datoms-transacted (count (:tx-data rp))})))

(defn- sf [^SortedSet s] (when-not (.isEmpty s) (.first s)))

(defonce dbs (atom {}))

;; read caches
(defonce ^:private caches (ConcurrentHashMap.))

(defn refresh-cache
  ([store]
   (refresh-cache store (last-modified store)))
  ([store target]
   (.put ^ConcurrentHashMap caches (dir store)
         (LRUCache. (:cache-limit (opts store)) target))))

(defn cache-disabled?
  [store]
  (.isDisabled ^LRUCache (.get ^ConcurrentHashMap caches (dir store))))

(defn disable-cache
  [store]
  (.disable ^LRUCache (.get ^ConcurrentHashMap caches (dir store))))

(defn enable-cache
  [store]
  (.enable ^LRUCache (.get ^ConcurrentHashMap caches (dir store))))

(defn cache-get
  [store k]
  (.get ^LRUCache (.get ^ConcurrentHashMap caches (dir store)) k))

(defn cache-put
  [store k v]
  (.put ^LRUCache (.get ^ConcurrentHashMap caches (dir store)) k v))

(defmacro wrap-cache
  [store pattern body]
  `(let [cache# (.get ^ConcurrentHashMap caches (dir ~store))]
     (if-some [cached# (.get ^LRUCache cache# ~pattern)]
       cached#
       (let [res# ~body]
         (.put ^LRUCache cache# ~pattern res#)
         res#))))

(defn vpred
  [v]
  (cond
    (string? v)  (fn [x] (if (string? x) (.equals ^String v x) false))
    (int? v)     (fn [x] (if (int? x) (= (long v) (long x)) false))
    (keyword? v) (fn [x] (.equals ^Object v x))
    (nil? v)     (fn [x] (nil? x))
    :else        (fn [x] (= v x))))

(defrecord-updatable DB [^IStore store
                         ^long max-eid
                         ^long max-tx
                         ^TreeSortedSet eavt
                         ^TreeSortedSet avet
                         pull-patterns]

  ISearchable
  (-searchable? [_] true)

  IDB
  (-schema [_] (schema store))
  (-rschema [_] (rschema store))
  (-attrs-by [db property] ((-rschema db) property))
  (-is-attr? [db attr property] (contains? (-attrs-by db property) attr))
  (-clear-tx-cache
    [db]
    (let [clear #(.clear ^TreeSortedSet %)]
      (clear eavt)
      (clear avet)
      db))

  ITuples
  (-init-tuples
    [db out a v-ranges pred get-v?]
    (ave-tuples store out a v-ranges pred get-v?))

  (-init-tuples-list
    [db a v-ranges pred get-v?]
    (wrap-cache
        store [:init-tuples a v-ranges pred get-v?]
      (ave-tuples-list store a v-ranges pred get-v?)))

  (-sample-init-tuples
    [db out a mcount v-ranges pred get-v?]
    (sample-ave-tuples store out a mcount v-ranges pred get-v?))

  (-sample-init-tuples-list
    [db a mcount v-ranges pred get-v?]
    (wrap-cache
        store [:sample-init-tuples a mcount v-ranges pred get-v?]
      (sample-ave-tuples-list store a mcount v-ranges pred get-v?)))

  (-e-sample
    [db a]
    (wrap-cache
        store [:e-sample a]
      (e-sample store a)))

  (-eav-scan-v
    [db in out eid-idx attrs-v]
    (eav-scan-v store in out eid-idx attrs-v))

  (-eav-scan-v-list
    [db in eid-idx attrs-v]
    (wrap-cache
        store [:eav-scan-v in eid-idx attrs-v]
      (eav-scan-v-list store in eid-idx attrs-v)))

  (-val-eq-scan-e
    [db in out v-idx attr]
    (val-eq-scan-e store in out v-idx attr))

  (-val-eq-scan-e-list
    [db in v-idx attr]
    (wrap-cache
        store [:val-eq-scan-e in v-idx attr]
      (val-eq-scan-e-list store in v-idx attr)))

  (-val-eq-scan-e
    [db in out v-idx attr bound]
    (val-eq-scan-e store in out v-idx attr bound))

  (-val-eq-scan-e-list
    [db in v-idx attr bound]
    (wrap-cache
        store [:val-eq-scan-e in v-idx attr bound]
      (val-eq-scan-e-list store in v-idx attr bound)))

  (-val-eq-filter-e
    [db in out v-idx attr f-idx]
    (val-eq-filter-e store in out v-idx attr f-idx))

  (-val-eq-filter-e-list
    [db in v-idx attr f-idx]
    (wrap-cache
        store [:val-eq-filter-e in v-idx attr f-idx]
      (val-eq-filter-e-list store in v-idx attr f-idx)))

  ISearch
  (-search
    [db pattern]
    (let [[e a v _] pattern]
      (wrap-cache
          store [:search e a v]
        (case-tree
          [e a (some? v)]
          [(fetch store (datom e a v)) ; e a v
           (slice store :eav (datom e a c/v0) (datom e a c/vmax)) ; e a _
           (slice-filter store :eav
                         (fn [^Datom d] (when ((vpred v) (.-v d)) d))
                         (datom e nil nil)
                         (datom e nil nil))  ; e _ v
           (e-datoms store e) ; e _ _
           (av-datoms store a v) ; _ a v
           (mapv #(datom (aget ^objects % 0) a (aget ^objects % 1))
                 (ave-tuples-list
                   store a [[[:closed c/v0] [:closed c/vmax]]] nil true)) ; _ a _
           (slice-filter store :eav
                         (fn [^Datom d] (when ((vpred v) (.-v d)) d))
                         (datom e0 nil nil)
                         (datom emax nil nil)) ; _ _ v
           (slice store :eav (datom e0 nil nil) (datom emax nil nil))])))) ; _ _ _

  (-first
    [db pattern]
    (let [[e a v _] pattern]
      (wrap-cache
          store [:first e a v]
        (case-tree
          [e a (some? v)]
          [(first (fetch store (datom e a v))) ; e a v
           (ea-first-datom store e a) ; e a _
           (head-filter store :eav
                        (fn [^Datom d]
                          (when ((vpred v) (.-v d)) d))
                        (datom e nil nil)
                        (datom e nil nil))  ; e _ v
           (e-first-datom store e) ; e _ _
           (av-first-datom store a v) ; _ a v
           (head store :ave (datom e0 a nil) (datom emax a nil)) ; _ a _
           (head-filter store :eav
                        (fn [^Datom d]
                          (when ((vpred v) (.-v d)) d))
                        (datom e0 nil nil)
                        (datom emax nil nil)) ; _ _ v
           (head store :eav (datom e0 nil nil) (datom emax nil nil))])))) ; _ _ _

  (-count
    [db pattern]
    (.-count db pattern nil))
  (-count
    [db pattern cap]
    (.-count db pattern cap false))
  (-count
    [db pattern cap actual?]
    (let [[e a v] pattern]
      (wrap-cache
          store [:count e a v cap]
        (case-tree
          [e a (some? v)]
          [(size store :eav (datom e a v) (datom e a v) cap) ; e a v
           (size store :eav (datom e a c/v0) (datom e a c/vmax) cap) ; e a _
           (size-filter store :eav
                        (fn [^Datom d] ((vpred v) (.-v d)))
                        (datom e nil nil) (datom e nil nil) cap)  ; e _ v
           (e-size store e) ; e _ _
           (av-size store a v) ; _ a v
           (if actual? (actual-a-size store a) (a-size store a)) ; _ a _
           (v-size store v) ; _ _ v, for ref only
           (datom-count store :eav)])))) ; _ _ _

  IIndexAccess
  (-populated?
    [db index c1 c2 c3]
    (wrap-cache
        store [:populated? index c1 c2 c3]
      (populated? store index
                  (components->pattern db index c1 c2 c3 e0 v0)
                  (components->pattern db index c1 c2 c3 emax vmax))))

  (-datoms
    [db index]
    (-datoms db index nil nil nil))
  (-datoms
    [db index c1]
    (-datoms db index c1 nil nil))
  (-datoms
    [db index c1 c2]
    (-datoms db index c1 c2 nil))
  (-datoms
    [db index c1 c2 c3]
    (wrap-cache
        store [:datoms index c1 c2 c3]
      (slice store index
             (components->pattern db index c1 c2 c3 e0 v0)
             (components->pattern db index c1 c2 c3 emax vmax))))
  (-datoms
    [db index c1 c2 c3 n]
    (wrap-cache
        store [:datoms index c1 c2 c3 n]
      (slice store index
             (components->pattern db index c1 c2 c3 e0 v0)
             (components->pattern db index c1 c2 c3 emax vmax)
             n)))

  (-e-datoms [db e] (wrap-cache store [:e-datoms e] (e-datoms store e)))

  (-av-datoms
    [db attr v]
    (wrap-cache store [:av-datoms attr v] (av-datoms store attr v)))

  (-range-datoms
    [db index start-datom end-datom]
    (wrap-cache
        store [:range-datoms index start-datom end-datom]
      (slice store index start-datom end-datom)))

  (-seek-datoms
    [db index c1 c2 c3]
    (wrap-cache
        store [:seek index c1 c2 c3]
      (slice store index
             (components->pattern db index c1 c2 c3 e0 v0)
             (components->end-datom db index c1 c2 c3 emax vmax))))
  (-seek-datoms
    [db index c1 c2 c3 n]
    (wrap-cache
        store [:seek index c1 c2 c3 n]
      (slice store index
             (components->pattern db index c1 c2 c3 e0 v0)
             (components->end-datom db index c1 c2 c3 emax vmax)
             n)))

  (-rseek-datoms
    [db index c1 c2 c3]
    (wrap-cache
        store [:rseek index c1 c2 c3]
      (rslice store index
              (components->pattern db index c1 c2 c3 emax vmax)
              (components->end-datom db index c1 c2 c3 e0 v0))))
  (-rseek-datoms
    [db index c1 c2 c3 n]
    (wrap-cache
        store [:rseek index c1 c2 c3 n]
      (rslice store index
              (components->pattern db index c1 c2 c3 emax vmax)
              (components->end-datom db index c1 c2 c3 e0 v0)
              n)))

  (-cardinality
    [db attr]
    (wrap-cache store [:cardinality attr]
      (cardinality store attr)))

  (-index-range
    [db attr start end]
    (wrap-cache
        store [:index-range attr start end]
      (do (validate-attr attr (list '-index-range 'db attr start end))
          (slice store :ave (resolve-datom db nil attr start e0 v0)
                 (resolve-datom db nil attr end emax vmax)))))

  (-index-range-size
    [db attr start end]
    (.-index-range-size db attr start end nil))
  (-index-range-size
    [db attr start end cap]
    (wrap-cache
        store [:index-range-size attr start end]
      (av-range-size store attr start end cap))))

;; (defmethod print-method DB [^DB db, ^java.io.Writer w]
;;   (binding [*out* w]
;;     (let [{:keys [store eavt max-eid max-tx]} db]
;;       (pr {:db-name       (s/db-name store)
;;            :last-modified (s/last-modified store)
;;            :datom-count   (count eavt)
;;            :max-eid       max-eid
;;            :max-tx        max-tx}))))

(defn db?
  "Check if x is an instance of DB, also refresh its cache if it's stale.
  Often used in the :pre condition of a DB access function"
  [x]
  (when (-searchable? x)
    (let [store  (.-store ^DB x)
          target (last-modified store)
          cache  (.get ^ConcurrentHashMap caches (dir store))]
      (when (< ^long (.target ^LRUCache cache) ^long target)
        (refresh-cache store target)))
    true))

(defn search-datoms [db e a v] (-search db [e a v]))

(defn count-datoms [db e a v] (-count db [e a v] nil true))

(defn seek-datoms
  ([db index]
   (-seek-datoms db index nil nil nil))
  ([db index c1]
   (-seek-datoms db index c1 nil nil))
  ([db index c1 c2]
   (-seek-datoms db index c1 c2 nil))
  ([db index c1 c2 c3]
   (-seek-datoms db index c1 c2 c3))
  ([db index c1 c2 c3 n]
   (-seek-datoms db index c1 c2 c3 n)))

(defn rseek-datoms
  ([db index]
   (-rseek-datoms db index nil nil nil))
  ([db index c1]
   (-rseek-datoms db index c1 nil nil))
  ([db index c1 c2]
   (-rseek-datoms db index c1 c2 nil))
  ([db index c1 c2 c3]
   (-rseek-datoms db index c1 c2 c3))
  ([db index c1 c2 c3 n]
   (-rseek-datoms db index c1 c2 c3 n)))

(defn max-eid [db] (init-max-eid (:store db)))

(defn analyze
  ([db] {:pre [(db? db)]}
   (i/analyze (:store db) nil))
  ([db attr] {:pre [(db? db)]}
   (i/analyze (:store db) attr)))

;; ----------------------------------------------------------------------------

(defn- validate-schema-key [a k v expected]
  (when-not (or (nil? v) (contains? expected v))
    (throw (ex-info (str "Bad attribute specification for "
                         (pr-str {a {k v}}) ", expected one of " expected)
                    {:error     :schema/validation
                     :attribute a
                     :key       k
                     :value     v}))))

(def tuple-props #{:db/tupleAttrs :db/tupleTypes :db/tupleType})

(defn- validate-schema [schema]
  (doseq [[a kv] schema]
    (let [comp? (:db/isComponent kv false)]
      (validate-schema-key a :db/isComponent (:db/isComponent kv) #{true false})
      (when (and comp? (not (identical? (:db/valueType kv) :db.type/ref)))
        (raise "Bad attribute specification for " a
               ": {:db/isComponent true} should also have {:db/valueType :db.type/ref}"
               {:error     :schema/validation
                :attribute a
                :key       :db/isComponent})))
    (validate-schema-key a :db/unique (:db/unique kv)
                         #{:db.unique/value :db.unique/identity})
    (validate-schema-key a :db/valueType (:db/valueType kv)
                         c/datalog-value-types)
    (validate-schema-key a :db/cardinality (:db/cardinality kv)
                         #{:db.cardinality/one :db.cardinality/many})
    (validate-schema-key a :db/fulltext (:db/fulltext kv)
                         #{true false})

    ;; tuple should have one of tuple-props
    (when (and (identical? :db.type/tuple (:db/valueType kv))
               (not (some tuple-props (keys kv))))
      (raise "Bad attribute specification for " a ": {:db/valueType :db.type/tuple} should also have :db/tupleAttrs, :db/tupleTypes, or :db/tupleType"
             {:error     :schema/validation
              :attribute a
              :key       :db/valueType}))

    ;; :db/tupleAttrs is a non-empty sequential coll
    (when (contains? kv :db/tupleAttrs)
      (let [ex-data {:error     :schema/validation
                     :attribute a
                     :key       :db/tupleAttrs}]
        (when (identical? :db.cardinality/many (:db/cardinality kv))
          (raise a " has :db/tupleAttrs, must be :db.cardinality/one" ex-data))

        (let [attrs (:db/tupleAttrs kv)]
          (when-not (sequential? attrs)
            (raise a " :db/tupleAttrs must be a sequential collection, got: " attrs ex-data))

          (when (empty? attrs)
            (raise a " :db/tupleAttrs can’t be empty" ex-data))

          (doseq [attr attrs
                  :let [ex-data (assoc ex-data :value attr)]]
            (when (contains? (schema attr) :db/tupleAttrs)
              (raise a " :db/tupleAttrs can’t depend on another tuple attribute: " attr ex-data))

            (when (identical? :db.cardinality/many (:db/cardinality (schema attr)))
              (raise a " :db/tupleAttrs can’t depend on :db.cardinality/many attribute: " attr ex-data))))))

    (when (contains? kv :db/tupleType)
      (let [ex-data {:error     :schema/validation
                     :attribute a
                     :key       :db/tupleType}
            attr    (:db/tupleType kv)]
        (when-not (c/datalog-value-types attr)
          (raise a " :db/tupleType must be a single value type, got: " attr ex-data))
        (when (identical? attr :db.type/tuple)
          (raise a " :db/tupleType cannot be :db.type/tuple" ex-data))))

    (when (contains? kv :db/tupleTypes)
      (let [ex-data {:error     :schema/validation
                     :attribute a
                     :key       :db/tupleTypes}
            attrs   (:db/tupleTypes kv)]
        (when-not (and (sequential? attrs) (< 1 (count attrs))
                       (every? c/datalog-value-types attrs)
                       (not (some #(identical? :db.type/tuple %) attrs)))
          (raise a " :db/tupleTypes must be a sequential collection of more than one value types, got: " attrs ex-data))))))

(defn- open-store
  [dir schema opts]
  (if (r/dtlv-uri? dir)
    (r/open dir schema opts)
    (s/open dir schema opts)))

(defn new-db
  [^IStore store]
  (let [db (map->DB
             {:store         store
              :max-eid       (init-max-eid store)
              :max-tx        (max-tx store)
              :eavt          (TreeSortedSet. ^Comparator d/cmp-datoms-eavt)
              :avet          (TreeSortedSet. ^Comparator d/cmp-datoms-avet)
              :pull-patterns (LRUCache. 64)})]
    (swap! dbs assoc (db-name store) db)
    (refresh-cache store (System/currentTimeMillis))
    (start-sampling store)
    db))

(defn transfer
  [^DB old store]
  (DB. store (.-max-eid old) (.-max-tx old) (.-eavt old) (.-avet old)
       (.-pull-patterns old)))

(defn ^DB empty-db
  ([] (empty-db nil nil))
  ([dir] (empty-db dir nil))
  ([dir schema] (empty-db dir schema nil))
  ([dir schema opts]
   {:pre [(or (nil? schema) (map? schema))]}
   (validate-schema schema)
   (new-db (open-store dir schema opts))))

(defn- validate-type
  [store a v]
  (let [opts   (opts store)
        schema (schema store)
        vt     (s/value-type (schema a))]
    (or (not (opts :validate-data?))
        (b/valid-data? v vt)
        (raise "Invalid data, expecting" vt " got " v {:input v}))
    vt))

(defn coerce-inst
  [v]
  (cond
    (inst? v)    v
    (integer? v) (Date. (long v))
    :else        (raise "Expect java.util.Date" {:input v})))

(defn coerce-uuid
  [v]
  (cond
    (uuid? v)   v
    (string? v) (if-let [u (parse-uuid v)]
                  u
                  (raise "Unable to parse string to UUID" {:input v}))
    :else       (raise "Expect java.util.UUID" {:input v})))

(defn- type-coercion
  [vt v]
  (case vt
    :db.type/string              (str v)
    :db.type/bigint              (biginteger v)
    :db.type/bigdec              (bigdec v)
    (:db.type/long :db.type/ref) (long v)
    :db.type/float               (float v)
    :db.type/double              (double v)
    (:db.type/bytes :bytes)      (if (bytes? v) v (byte-array v))
    (:db.type/keyword :keyword)  (keyword v)
    (:db.type/symbol :symbol)    (symbol v)
    (:db.type/boolean :boolean)  (boolean v)
    (:db.type/instant :instant)  (coerce-inst v)
    (:db.type/uuid :uuid)        (coerce-uuid v)
    :db.type/tuple               (vec v)
    v))

(defn- correct-datom*
  [^Datom datom vt]
  (d/datom (.-e datom) (.-a datom) (type-coercion vt (.-v datom))
           (d/datom-tx datom) (d/datom-added datom)))

(defn- correct-datom
  [store ^Datom datom]
  (correct-datom* datom (validate-type store (.-a datom) (.-v datom))))

(defn- correct-value
  [store a v]
  (type-coercion (validate-type store a v) v))

(defn- pour
  [store datoms]
  (doseq [batch (sequence (comp
                            (map #(correct-datom store %))
                            (partition-all c/*fill-db-batch-size*))
                          datoms)]
    (load-datoms store batch)))

(defn close-db [^DB db]
  (let [store ^IStore (.-store db)]
    (stop-sampling store)
    (.remove ^ConcurrentHashMap caches (dir store))
    (swap! dbs dissoc (db-name store))
    (close store)
    nil))

(defn- quick-fill
  [^Store store datoms]
  (let [lmdb    (.-lmdb store)
        flags   (get-env-flags lmdb)
        nosync? (:nosync flags)]
    (set-env-flags lmdb #{:nosync} true)
    (pour store datoms)
    (when-not nosync? (set-env-flags lmdb #{:nosync} false))
    (sync lmdb)))

(defn ^DB init-db
  ([datoms] (init-db datoms nil nil nil))
  ([datoms dir] (init-db datoms dir nil nil))
  ([datoms dir schema] (init-db datoms dir schema nil))
  ([datoms dir schema opts]
   {:pre [(or (nil? schema) (map? schema))]}
   (when-some [not-datom (first (drop-while datom? datoms))]
     (raise "init-db expects list of Datoms, got " (type not-datom)
            {:error :init-db}))
   (validate-schema schema)
   (let [^Store store (open-store dir schema opts)]
     (quick-fill store datoms)
     (new-db store))))

(defn fill-db [db datoms] (quick-fill (.-store ^DB db) datoms) db)

;; ----------------------------------------------------------------------------

(declare entid-strict entid-some ref?)

(defn- resolve-datom
  [db e a v default-e default-v]
  (when a (validate-attr a (list 'resolve-datom 'db e a v default-e default-v)))
  (let [v? (some? v)]
    (datom
      (or (entid-some db e) default-e)  ;; e
      a                                 ;; a
      (if (and v? (ref? db a))          ;; v
        (entid-strict db v)
        (if v? v default-v)))))

(defn- components->pattern
  [db index c0 c1 c2 default-e default-v]
  (case index
    :eav (resolve-datom db c0 c1 c2 default-e default-v)
    :ave (resolve-datom db c2 c0 c1 default-e default-v)))

(defn- components->end-datom
  [_ index c0 c1 _ default-e default-v]
  (datom default-e
         (case index
           :eav c1
           :ave c0)
         default-v))

;; ----------------------------------------------------------------------------

(defn multival? ^Boolean [db attr] (-is-attr? db attr :db.cardinality/many))

(defn ^Boolean multi-value?
  ^Boolean [db attr value]
  (and
    (-is-attr? db attr :db.cardinality/many)
    (or
      (u/array? value)
      (and (coll? value) (not (map? value))))))

(defn ref? ^Boolean [db attr] (-is-attr? db attr :db.type/ref))

(defn component? ^Boolean [db attr] (-is-attr? db attr :db/isComponent))

(defn tuple-attr? ^Boolean [db attr] (-is-attr? db attr :db/tupleAttrs))

(defn tuple-type? ^Boolean [db attr] (-is-attr? db attr :db/tupleType))

(defn tuple-types? ^Boolean [db attr] (-is-attr? db attr :db/tupleTypes))

(defn tuple-source? ^Boolean [db attr] (-is-attr? db attr :db/attrTuples))

(defn entid [db eid]
  (cond
    (and (integer? eid) (not (neg? ^long eid)))
    eid

    (sequential? eid)
    (let [[attr value] eid]
      (cond
        (not= (count eid) 2)
        (raise "Lookup ref should contain 2 elements: " eid
               {:error :lookup-ref/syntax, :entity-id eid})
        (not (-is-attr? db attr :db/unique))
        (raise "Lookup ref attribute should be marked as :db/unique: " eid
               {:error :lookup-ref/unique, :entity-id eid})
        (nil? value)
        nil
        :else
        (or (:e (sf (.subSet ^TreeSortedSet (:avet db)
                             (datom e0 attr value tx0)
                             (datom emax attr value txmax))))
            (av-first-e (:store db) attr value))))

    (keyword? eid)
    (or (:e (sf (.subSet ^TreeSortedSet (:avet db)
                         (datom e0 :db/ident eid tx0)
                         (datom emax :db/ident eid txmax))))
        (av-first-e (:store db) :db/ident eid))

    :else
    (raise "Expected number or lookup ref for entity id, got " eid
           {:error :entity-id/syntax, :entity-id eid})))

(defn entid-strict [db eid]
  (or (entid db eid)
      (raise "Nothing found for entity id " eid
             {:error     :entity-id/missing
              :entity-id eid})))

(defn entid-some [db eid]
  (when eid
    (entid-strict db eid)))

(defn reverse-ref?
  ^Boolean [attr]
  (if (keyword? attr)
    (= \_ (nth (name attr) 0))
    (raise "Bad entity attribute: " attr ", expected keyword"
           {:error :transact/syntax, :attribute attr})))

(defn reverse-ref [attr]
  (if (reverse-ref? attr)
    (keyword (namespace attr) (subs (name attr) 1))
    (keyword (namespace attr) (str "_" (name attr)))))

;;;;;;;;;; Transacting

(def *last-auto-tempid (volatile! 0))

(deftype AutoTempid [id]
  Object
  (toString [d] (str "#datalevin/AutoTempid [" id "]")))

(defmethod print-method AutoTempid [^AutoTempid id, ^Writer w]
  (.write w (str "#datalevin/AutoTempid "))
  (binding [*out* w]
    (pr [(.-id id)])))

(defn- auto-tempid [] (AutoTempid. (vswap! *last-auto-tempid u/long-inc)))

(defn- ^Boolean auto-tempid? [x] (instance? AutoTempid x))

(declare assoc-auto-tempid)

(defn- assoc-auto-tempids
  [db entities]
  (mapv #(assoc-auto-tempid db %) entities))

(defn- assoc-auto-tempid
  [db entity]
  (cond+
    (map? entity)
    (persistent!
      (reduce-kv
        (fn [entity a v]
          (cond
            (and (ref? db a) (multi-value? db a v))
            (assoc! entity a (assoc-auto-tempids db v))

            (ref? db a)
            (assoc! entity a (assoc-auto-tempid db v))

            (and (reverse-ref? a) (sequential? v))
            (assoc! entity a (assoc-auto-tempids db v))

            (reverse-ref? a)
            (assoc! entity a (assoc-auto-tempid db v))

            :else
            (assoc! entity a v)))
        (transient {})
        (if (contains? entity :db/id)
          entity
          (assoc entity :db/id (auto-tempid)))))

    (not (sequential? entity))
    entity

    :let [[op e a v] entity]

    (and (= :db/add op) (ref? db a))
    (if (multi-value? db a v)
      [op e a (assoc-auto-tempids db v)]
      [op e a (assoc-auto-tempid db v)])

    :else
    entity))

(defn- validate-datom [^DB db ^Datom datom]
  (let [store (.-store db)
        a     (.-a datom)
        v     (.-v datom)
        vt    (validate-type store a v)
        v     (correct-value store a v)]
    (when (and (-is-attr? db a :db/unique) (datom-added datom))
      (when-some [found (or (not (.isEmpty
                                   (.subSet ^TreeSortedSet (:avet db)
                                            (d/datom e0 a v tx0)
                                            (d/datom emax a v txmax))))
                            (-populated? db :ave a v nil))]
        (raise "Cannot add " datom " because of unique constraint: " found
               {:error     :transact/unique
                :attribute a
                :datom     datom})))
    vt))

(defn- validate-attr [attr at]
  (when-not (keyword? attr)
    (raise "Bad entity attribute " attr " at " at ", expected keyword"
           {:error :transact/syntax, :attribute attr, :context at})))

(defn- validate-val [v at]
  (when (nil? v)
    (raise "Cannot store nil as a value at " at
           {:error :transact/syntax, :value v, :context at})))

(defn- current-tx
  {:inline (fn [report] `(-> ~report :db-before :max-tx long inc))}
  ^long [report]
  (-> report :db-before :max-tx long inc))

(defn- next-eid
  {:inline (fn [db] `(inc (long (:max-eid ~db))))}
  ^long [db]
  (inc (long (:max-eid db))))

(defn- tx-id? ^Boolean [e] (identical? :db/current-tx e))

(defn- tempid?
  ^Boolean [x]
  (or (and (number? x) (neg? ^long x))
      (string? x)
      (auto-tempid? x)))

(defn- new-eid? [db ^long eid] (> eid ^long (:max-eid db)))

(defn- advance-max-eid [db eid]
  (cond-> db
    (new-eid? db eid)
    (assoc :max-eid eid)))

(defn- allocate-eid
  ([_ report eid]
   (update report :db-after advance-max-eid eid))
  ([tx-time report e eid]
   (let [db   (:db-after report)
         new? (new-eid? db eid)
         opts (opts (.-store ^DB db))]
     (cond-> report
       (tx-id? e)
       (->
         (update :tempids assoc e eid)
         (update ::reverse-tempids update eid conjs e))

       (tempid? e)
       (->
         (update :tempids assoc e eid)
         (update ::reverse-tempids update eid conjs e))

       (and new? (not (tempid? e)))
       (update :tempids assoc eid eid)

       (and new? (opts :auto-entity-time?))
       (update :tx-data conj (d/datom eid :db/created-at tx-time))

       true
       (update :db-after advance-max-eid eid)))))

(defn- with-datom [db datom]
  (let [^Datom datom (correct-datom* datom (validate-datom db datom))
        add          #(do (.add ^TreeSortedSet % datom) %)
        del          #(do (.remove ^TreeSortedSet % datom) %)]
    (if (datom-added datom)
      (-> db
          (update :eavt add)
          (update :avet add)
          (advance-max-eid (.-e datom)))
      (if (.isEmpty
            (.subSet ^TreeSortedSet (:eavt db)
                     (d/datom (.-e datom) (.-a datom) (.-v datom) tx0)
                     (d/datom (.-e datom) (.-a datom) (.-v datom) txmax)))
        db
        (-> db
            (update :eavt del)
            (update :avet del))))))

(defn- queue-tuple [queue tuple idx db e _ v]
  (let [tuple-value  (or (get queue tuple)
                         (:v (sf
                               (.subSet ^TreeSortedSet (:eavt db)
                                        (d/datom e tuple nil tx0)
                                        (d/datom e tuple nil txmax))))
                         (ea-first-v (:store db) e tuple)
                         (vec (repeat (-> db -schema (get tuple)
                                          :db/tupleAttrs count)
                                      nil)))
        tuple-value' (assoc tuple-value idx v)]
    (assoc queue tuple tuple-value')))

(defn- queue-tuples [queue tuples db e a v]
  (reduce-kv
    (fn [queue tuple idx]
      (queue-tuple queue tuple idx db e a v))
    queue
    tuples))

(defn- transact-report [report datom]
  (let [db      (:db-after report)
        a       (:a datom)
        report' (-> report
                    (assoc :db-after (with-datom db datom))
                    (update :tx-data conj datom))]
    (if (tuple-source? db a)
      (let [e      (:e datom)
            v      (if (datom-added datom) (:v datom) nil)
            queue  (or (-> report' ::queued-tuples (get e)) {})
            tuples (get (-attrs-by db :db/attrTuples) a)
            queue' (queue-tuples queue tuples db e a v)]
        (update report' ::queued-tuples assoc e queue'))
      report')))

(defn- resolve-upserts
  "Returns [entity' upserts]. Upsert attributes that resolve to existing entities
   are removed from entity, rest are kept in entity for insertion. No validation is performed.

   upserts :: {:name  {\"Ivan\"  1}
               :email {\"ivan@\" 2}
               :alias {\"abc\"   3
                       \"def\"   4}}}"
  [db entity]
  (if-some [idents (not-empty (-attrs-by db :db.unique/identity))]
    (let [store   (:store db)
          resolve (fn [a v]
                    (cond
                      (not (ref? db a))
                      (or (:e (sf (.subSet ^TreeSortedSet (:avet db)
                                           (d/datom e0 a v tx0)
                                           (d/datom emax a v txmax))))
                          (av-first-e store a v))
                      (not (tempid? v))
                      (let [rv (entid db v)]
                        (or (:e (sf (.subSet ^TreeSortedSet (:avet db)
                                             (d/datom e0 a rv tx0)
                                             (d/datom emax a rv txmax))))
                            (av-first-e store a rv)))))

          split (fn [a vs]
                  (reduce
                    (fn [acc v]
                      (if-some [e (resolve a v)]
                        (update acc 1 assoc v e)
                        (update acc 0 conj v)))
                    [[] {}] vs))]
      (reduce-kv
        (fn [[entity' upserts] a v]
          (validate-attr a entity)
          (validate-val v entity)
          (cond
            (not (contains? idents a))
            [(assoc entity' a v) upserts]

            (multi-value? db a v)
            (let [[insert upsert] (split a v)]
              [(cond-> entity'
                 (seq insert) (assoc a insert))
               (cond-> upserts
                 (seq upsert) (assoc a upsert))])

            :else
            (if-some [e (resolve a (correct-value (.-store ^DB db) a v))]
              [entity' (assoc upserts a {v e})]
              [(assoc entity' a v) upserts])))
        [{} {}]
        entity))
    [entity nil]))

(defn validate-upserts
  "Throws if not all upserts point to the same entity.
   Returns single eid that all upserts point to, or null."
  [entity upserts]
  (let [upsert-ids (reduce-kv
                     (fn [m a v->e]
                       (reduce-kv
                         (fn [m v e]
                           (assoc m e [a v]))
                         m v->e))
                     {} upserts)]
    (if (<= 2 (count upsert-ids))
      (let [[e1 [a1 v1]] (first upsert-ids)
            [e2 [a2 v2]] (second upsert-ids)]
        (raise "Conflicting upserts: " [a1 v1] " resolves to " e1 ", but " [a2 v2] " resolves to " e2
               {:error     :transact/upsert
                :assertion [e1 a1 v1]
                :conflict  [e2 a2 v2]}))
      (let [[upsert-id [a v]] (first upsert-ids)
            eid               (:db/id entity)]
        (when (and
                (some? upsert-id)
                (some? eid)
                (not (tempid? eid))
                (not= upsert-id eid))
          (raise "Conflicting upsert: " [a v] " resolves to " upsert-id ", but entity already has :db/id " eid
                 {:error     :transact/upsert
                  :assertion [upsert-id a v]
                  :conflict  {:db/id eid}}))
        upsert-id))))

;; multivals/reverse can be specified as coll or as a single value, trying to guess
(defn- maybe-wrap-multival [db a vs]
  (cond
    ;; not a multival context
    (not (or (reverse-ref? a) (multival? db a)))
    [vs]

    ;; not a collection at all, so definitely a single value
    (not (and (coll? vs) (not (map? vs))))
    [vs]

    ;; probably lookup ref
    (and (= (count vs) 2) (-is-attr? db (first vs) :db.unique/identity))
    [vs]

    :else vs))

(defn- explode [db entity]
  (let [eid  (:db/id entity)
        ;; sort tuple attrs after non-tuple
        a+vs (into []
                   cat
                   (reduce
                     (fn [acc [a vs]]
                       (update acc (if (tuple-attr? db a) 1 0) conj [a vs]))
                     [[] []] entity))]
    (for [[a vs] a+vs
          :when  (not (identical? a :db/id))
          :let   [reverse?   (reverse-ref? a)
                  straight-a (if reverse? (reverse-ref a) a)
                  _          (when (and reverse? (not (ref? db straight-a)))
                               (raise "Bad attribute " a ": reverse attribute name requires {:db/valueType :db.type/ref} in schema"
                                      {:error   :transact/syntax, :attribute a,
                                       :context {:db/id eid, a vs}}))]
          v      (maybe-wrap-multival db a vs)]
      (if (and (ref? db straight-a) (map? v)) ;; another entity specified as nested map
        (assoc v (reverse-ref a) eid)
        (if reverse?
          [:db/add v   straight-a eid]
          [:db/add eid straight-a v])))))

(defn- transact-add [report [_ e a v tx :as ent]]
  (validate-attr a ent)
  (validate-val  v ent)
  (let [tx        (or tx (current-tx report))
        db        (:db-after report)
        e         (entid-strict db e)
        v         (if (ref? db a) (entid-strict db v) v)
        new-datom (datom e a v tx)
        multival? (multival? db a)
        ^Datom old-datom
        (if multival?
          (or (sf (.subSet ^TreeSortedSet (:eavt db)
                           (datom e a v tx0)
                           (datom e a v txmax)))
              (first (fetch (:store db) (datom e a v))))
          (or (sf (.subSet ^TreeSortedSet (:eavt db)
                           (datom e a nil tx0)
                           (datom e a nil txmax)))
              (ea-first-datom (:store db) e a)))]
    (cond
      (nil? old-datom)
      (transact-report report new-datom)

      (= (.-v old-datom) v)
      (if (some #(and (not (datom-added %)) (= % new-datom))
                (:tx-data report))
        ;; special case: retract then transact the same datom
        (transact-report report new-datom)
        (update report ::tx-redundant conjv new-datom))

      :else
      (-> report
          (transact-report (datom e a (.-v old-datom) tx false))
          (transact-report new-datom)))))

(defn- transact-retract-datom [report ^Datom d]
  (let [tx (current-tx report)]
    (transact-report report (datom (.-e d) (.-a d) (.-v d) tx false))))

(defn- retract-components [db datoms]
  (into #{} (comp
              (filter (fn [^Datom d] (component? db (.-a d))))
              (map (fn [^Datom d] [:db.fn/retractEntity (.-v d)]))) datoms))

(defn- check-value-tempids [report]
  (let [tx-data (:tx-data report)]
    (if-let [tempids (::value-tempids report)]
      (let [all-tempids (transient tempids)
            reduce-fn   (fn [tempids datom]
                          (if (datom-added datom)
                            (dissoc! tempids (:e datom))
                            tempids))
            unused      (reduce reduce-fn all-tempids tx-data)
            unused      (reduce reduce-fn unused (::tx-redundant report))]
        (if (zero? (count unused))
          (-> report
              (dissoc ::value-tempids ::tx-redundant)
              (assoc :tx-data tx-data))
          (raise "Tempids used only as value in transaction: " (sort (vals (persistent! unused)))
                 {:error :transact/syntax, :tempids unused})))
      (-> report
          (dissoc ::value-tempids ::tx-redundant)
          (assoc :tx-data tx-data)))))

(declare local-transact-tx-data)

(defn- retry-with-tempid
  [initial-report report es tempid upserted-eid tx-time]
  (if-some [eid (get (::upserted-tempids initial-report) tempid)]
    (raise "Conflicting upsert: " tempid " resolves"
           " both to " upserted-eid " and " eid
           { :error :transact/upsert })
    ;; try to re-run from the beginning
    ;; but remembering that `tempid` will resolve to `upserted-eid`
    (let [tempids' (-> (:tempids report)
                       (assoc tempid upserted-eid))
          report'  (-> initial-report
                       (assoc :tempids tempids')
                       (update ::upserted-tempids assoc tempid upserted-eid))]
      (local-transact-tx-data report' es tx-time))))

(def builtin-fn?
  #{:db.fn/call
    :db.fn/cas
    :db/cas
    :db/add
    :db/retract
    :db.fn/retractAttribute
    :db.fn/retractEntity
    :db/retractEntity})

(defn flush-tuples [report]
  (let [db    (:db-after report)
        store (:store db)]
    (reduce-kv
      (fn [entities eid tuples+values]
        (persistent!
          (reduce-kv
            (fn [entities tuple value]
              (let [value   (if (every? nil? value) nil value)
                    current (or (:v (sf (.subSet ^TreeSortedSet (:eavt db)
                                                 (d/datom eid tuple nil tx0)
                                                 (d/datom eid tuple nil txmax))))
                                (ea-first-v store eid tuple))]
                (cond
                  (= value current) entities
                  (nil? value)
                  (conj! entities ^::internal [:db/retract eid tuple current])
                  :else
                  (conj! entities ^::internal [:db/add eid tuple value]))))
            (transient entities) tuples+values)))
      [] (::queued-tuples report))))

(defn- local-transact-tx-data
  ([initial-report initial-es tx-time]
   (local-transact-tx-data initial-report initial-es tx-time false))
  ([initial-report initial-es tx-time simulated?]
   (let [initial-report' (-> initial-report
                             (update :db-after -clear-tx-cache))
         db              ^DB (:db-before initial-report)
         initial-es'     (if (seq (-attrs-by db :db.type/tuple))
                           (sequence
                             (mapcat vector)
                             initial-es (repeat ::flush-tuples))
                           initial-es)
         store           (.-store db)
         schema          (schema store)
         rp
         (loop [report initial-report'
                es     initial-es']
           (cond+
             (empty? es)
             (-> report
                 check-value-tempids
                 (dissoc ::upserted-tempids)
                 (dissoc ::reverse-tempids)
                 (update :tempids #(u/removem auto-tempid? %))
                 (update :tempids assoc :db/current-tx (current-tx report))
                 (update :db-after update :max-tx u/long-inc))

             :let [[entity & entities] es]

             (identical? ::flush-tuples entity)
             (if (contains? report ::queued-tuples)
               (recur (dissoc report ::queued-tuples)
                      (concat (flush-tuples report) entities))
               (recur report entities))

             :let [^DB db      (:db-after report)
                   tempids (:tempids report)]

             (map? entity)
             (let [old-eid (:db/id entity)]
               (cond+
                 ;; :db/current-tx  => tx
                 (tx-id? old-eid)
                 (let [id (current-tx report)]
                   (recur (allocate-eid tx-time report old-eid id)
                          (cons (assoc entity :db/id id) entities)))

                 ;; lookup-ref => resolved | error
                 (sequential? old-eid)
                 (let [id (entid-strict db old-eid)]
                   (recur report (cons (assoc entity :db/id id) entities)))

                 ;; upserted => explode | error
                 :let [[entity' upserts] (resolve-upserts db entity)
                       upserted-eid      (validate-upserts entity' upserts)]

                 (some? upserted-eid)
                 (if (and (tempid? old-eid)
                          (contains? tempids old-eid)
                          (not= upserted-eid (get tempids old-eid)))
                   (retry-with-tempid initial-report report initial-es old-eid
                                      upserted-eid tx-time)
                   (recur (-> (allocate-eid tx-time report old-eid upserted-eid)
                              (update ::tx-redundant conjv
                                      (datom upserted-eid nil nil tx0)))
                          (concat (explode db (assoc entity' :db/id upserted-eid))
                                  entities)))

                 ;; resolved | allocated-tempid | tempid | nil => explode
                 (or (number? old-eid)
                     (nil?    old-eid)
                     (string? old-eid)
                     (auto-tempid? old-eid))
                 (recur report (concat (explode db entity) entities))

                 ;; trash => error
                 :else
                 (raise "Expected number, string or lookup ref for :db/id, got " old-eid
                        { :error :entity-id/syntax, :entity entity })))

             (sequential? entity)
             (let [[op e a v] entity]
               (cond+
                 (identical? op :db.fn/call)
                 (let [[_ f & args] entity]
                   (recur report (concat (apply f db args) entities)))

                 (and (keyword? op) (not (builtin-fn? op)))
                 (if-some [ident (or (:e (sf (.subSet
                                               ^TreeSortedSet (:avet db)
                                               (d/datom e0 op nil tx0)
                                               (d/datom emax op nil txmax))))
                                     (entid db op))]
                   (let [fun  (or (:v (sf (.subSet
                                            ^TreeSortedSet (:eavt db)
                                            (d/datom ident :db/fn nil tx0)
                                            (d/datom ident :db/fn nil txmax))))
                                  (ea-first-v store ident :db/fn))
                         args (next entity)]
                     (if (fn? fun)
                       (recur report (concat (apply fun db args) entities))
                       (raise "Entity " op " expected to have :db/fn attribute with fn? value"
                              {:error :transact/syntal, :operation :db.fn/call, :tx-data entity})))
                   (raise "Can’t find entity for transaction fn " op
                          {:error :transact/syntax, :operation :db.fn/call, :tx-data entity}))

                 (and (tempid? e) (not (identical? op :db/add)))
                 (raise "Can't use tempid in '" entity "'. Tempids are allowed in :db/add only"
                        { :error :transact/syntax, :op entity })

                 (or (identical? op :db.fn/cas) (identical? op :db/cas))
                 (let [[_ e a ov nv] entity
                       e             (entid-strict db e)
                       _             (validate-attr a entity)
                       ov            (if (ref? db a) (entid-strict db ov) ov)
                       nv            (if (ref? db a) (entid-strict db nv) nv)
                       _             (validate-val nv entity)
                       datoms        (clojure.set/union
                                       (.subSet ^TreeSortedSet (:eavt db)
                                                (datom e a nil tx0)
                                                (datom e a nil txmax))
                                       (slice (:store db) :eav
                                              (datom e a c/v0)
                                              (datom e a c/vmax)))]
                   (if (multival? db a)
                     (if (some (fn [^Datom d] (= (.-v d) ov)) datoms)
                       (recur (transact-add report [:db/add e a nv]) entities)
                       (raise ":db.fn/cas failed on datom [" e " " a " " (map :v datoms) "], expected " ov
                              {:error :transact/cas, :old datoms, :expected ov, :new nv}))
                     (let [v (:v (first datoms))]
                       (if (= v ov)
                         (recur (transact-add report [:db/add e a nv]) entities)
                         (raise ":db.fn/cas failed on datom [" e " " a " " v "], expected " ov
                                {:error :transact/cas, :old (first datoms), :expected ov, :new nv })))))

                 (tx-id? e)
                 (recur (allocate-eid tx-time report e (current-tx report))
                        (cons [op (current-tx report) a v] entities))

                 (and (ref? db a) (tx-id? v))
                 (recur (allocate-eid tx-time report v (current-tx report))
                        (cons [op e a (current-tx report)] entities))

                 (and (ref? db a) (tempid? v))
                 (if-some [resolved (get tempids v)]
                   (recur (update report ::value-tempids assoc resolved v)
                          (cons [op e a resolved] entities))
                   (let [resolved (next-eid db)]
                     (recur (-> (allocate-eid tx-time report v resolved)
                                (update ::value-tempids assoc resolved v))
                            es)))

                 (tempid? e)
                 (let [upserted-eid  (when (-is-attr? db a :db.unique/identity)
                                       (or (:e (sf (.subSet
                                                     ^TreeSortedSet (:avet db)
                                                     (d/datom e0 a v tx0)
                                                     (d/datom emax a v txmax))))
                                           (av-first-e store a v)))
                       allocated-eid (get tempids e)]
                   (if (and upserted-eid allocated-eid (not= upserted-eid allocated-eid))
                     (retry-with-tempid initial-report report initial-es e upserted-eid
                                        tx-time)
                     (let [eid (or upserted-eid allocated-eid (next-eid db))]
                       (recur (allocate-eid tx-time report e eid)
                              (cons [op eid a v] entities)))))

                 :let [upserted-eid (when (and (-is-attr? db a :db.unique/identity)
                                               (contains? (::reverse-tempids report) e)
                                               e)
                                      (av-first-e store a v))]

                 (and upserted-eid (not= e upserted-eid))
                 (let [tempids (get (::reverse-tempids report) e)
                       tempid  (u/find #(not (contains? (::upserted-tempids report) %))
                                       tempids)]
                   (if tempid
                     (retry-with-tempid initial-report report initial-es tempid
                                        upserted-eid tx-time)
                     (raise "Conflicting upsert: " e " resolves to " upserted-eid
                            " via " entity {:error :transact/upsert})))

                 (and (tuple-attr? db a) (not (::internal (meta entity))))
                 ;; allow transacting in tuples if they fully match already existing values
                 (let [tuple-attrs (get-in schema [a :db/tupleAttrs])]
                   (if (and
                         (every? some? v)
                         (= (count tuple-attrs) (count v))
                         (every?
                           (fn [[tuple-attr tuple-value]]
                             (let [db-value
                                   (or (:v (sf
                                             (.subSet
                                               ^TreeSortedSet (:eavt db)
                                               (d/datom e tuple-attr nil tx0)
                                               (d/datom e tuple-attr nil txmax))))
                                       (ea-first-v store e tuple-attr))]
                               (= tuple-value db-value)))
                           (mapv vector tuple-attrs v)))
                     (recur report entities)
                     (raise "Can’t modify tuple attrs directly: " entity
                            {:error :transact/syntax, :tx-data entity})))

                 :let [tuple-types (when (and (or (tuple-type? db a)
                                                  (tuple-types? db a))
                                              (not (::internal (meta entity))))
                                     (or (get-in schema [a :db/tupleTypes])
                                         (repeat (get-in schema [a :db/tupleType]))))
                       vs (when tuple-types
                            (partition 2 (interleave tuple-types v)))]

                 (some #(and (identical? (first %) :db.type/ref)
                             (tempid? (second %))) vs)
                 (let [[report' v']
                       (loop [report' report
                              vs      vs
                              v'      []]
                         (if-let [[[tuple-type v] & vs] vs]
                           (if (and (identical? tuple-type :db.type/ref)
                                    (tempid? v))
                             (if-some [resolved (get tempids v)]
                               (recur report' vs (conj v' resolved))
                               (let [resolved (next-eid db)
                                     report'  (-> (allocate-eid tx-time report' v resolved)
                                                  (update ::value-tempids assoc resolved v))]
                                 (recur report' vs (conj v' resolved))))
                             (recur report' vs (conj v' v)))
                           [report' v']))]
                   (recur report' (cons [op e a v'] entities)))

                 (identical? op :db/add)
                 (recur (transact-add report entity) entities)

                 (and (identical? op :db/retract) (some? v))
                 (if-some [e (entid db e)]
                   (let [v (if (ref? db a) (entid-strict db v) v)]
                     (validate-attr a entity)
                     (validate-val v entity)
                     (if-some [old-datom (or (sf (.subSet
                                                   ^TreeSortedSet (:eavt db)
                                                   (datom e a v tx0)
                                                   (datom e a v txmax)))
                                             (-first db [e a v]))]
                       (recur (transact-retract-datom report old-datom) entities)
                       (recur report entities)))
                   (recur report entities))

                 (or (identical? op :db.fn/retractAttribute)
                     (identical? op :db/retract))
                 (if-some [e (entid db e)]
                   (let [_      (validate-attr a entity)
                         datoms (concatv
                                  (slice (:store db) :eav
                                         (datom e a c/v0)
                                         (datom e a c/vmax))
                                  (.subSet ^TreeSortedSet (:eavt db)
                                           (datom e a nil tx0)
                                           (datom e a nil txmax)))]
                     (recur (reduce transact-retract-datom report datoms)
                            (concat (retract-components db datoms) entities)))
                   (recur report entities))

                 (or (identical? op :db.fn/retractEntity)
                     (identical? op :db/retractEntity))
                 (if-some [e (entid db e)]
                   (let [e-datoms (concatv
                                    (e-datoms (:store db) e)
                                    (.subSet ^TreeSortedSet (:eavt db)
                                             (datom e nil nil tx0)
                                             (datom e nil nil txmax)))
                         v-datoms (v-datoms (:store db) e)]
                     (recur (reduce transact-retract-datom report
                                    (concat e-datoms v-datoms))
                            (concat (retract-components db e-datoms) entities)))
                   (recur report entities))

                 :else
                 (raise "Unknown operation at " entity ", expected :db/add, :db/retract, :db.fn/call, :db.fn/retractAttribute, :db.fn/retractEntity or an ident corresponding to an installed transaction function (e.g. {:db/ident <keyword> :db/fn <Ifn>}, usage of :db/ident requires {:db/unique :db.unique/identity} in schema)" {:error :transact/syntax, :operation op, :tx-data entity})))

             (datom? entity)
             (let [[e a v tx added] entity]
               (if added
                 (recur (transact-add report [:db/add e a v tx]) entities)
                 (recur report (cons [:db/retract e a v] entities))))

             (nil? entity)
             (recur report entities)

             :else
             (raise "Bad entity type at " entity ", expected map, vector, or datom"
                    {:error :transact/syntax, :tx-data entity})
             ))
         pstore (.-store ^DB (:db-after rp))]
     (when-not simulated?
       (load-datoms pstore (:tx-data rp))
       (refresh-cache pstore (System/currentTimeMillis)))
     rp)))

(defn- remote-tx-result
  [res]
  (if (map? res)
    (let [{:keys [tx-data tempids]} res]
      [tx-data (dissoc tempids :max-eid) (tempids :max-eid)])
    (let [[tx-data tempids] (split-with datom? res)
          max-eid           (-> tempids last second)
          tempids           (into {} (butlast tempids))]
      [tx-data tempids max-eid])))

;; HACK to avoid circular dependency
(def de-entity? (delay (resolve 'datalevin.entity/entity?)))
(def de-entity->txs (delay (resolve 'datalevin.entity/->txs)))

(defn- expand-transactable-entity
  [entity]
  (if (@de-entity? entity)
    (@de-entity->txs entity)
    [entity]))

(defn- update-entity-time
  [entity tx-time]
  (cond
    (map? entity)
    [(assoc entity :db/updated-at tx-time)]

    (sequential? entity)
    (let [[op e _ _] entity]
      (if (or (identical? op :db/retractEntity)
              (identical? op :db.fn/retractEntity))
        [entity]
        [entity [:db/add e :db/updated-at tx-time]]))

    (datom? entity)
    (let [e (d/datom-e entity)]
      [entity [:db/add e :db/updated-at tx-time]])

    (nil? entity)
    []

    :else
    (raise "Bad entity at " entity ", expected map, vector, datom or entity"
           {:error :transact/syntax, :tx-data entity})))

(defn- prepare-entities
  [^DB db entities tx-time]
  (let [aat #(assoc-auto-tempid db %)
        uet #(update-entity-time % tx-time)]
    (sequence
      (if (:auto-entity-time? (opts (.-store db)))
        (comp (mapcat expand-transactable-entity)
           (map aat)
           (mapcat uet))
        (comp (mapcat expand-transactable-entity)
           (map aat)))
      entities)))

(defn transact-tx-data
  [initial-report initial-es simulated?]
  (let [^DB db  (:db-before initial-report)
        store   (.-store db)
        tx-time (System/currentTimeMillis)]
    (if (instance? datalevin.remote.DatalogStore store)
      (try
        (let [res                       (r/tx-data store initial-es simulated?)
              [tx-data tempids max-eid] (remote-tx-result res)]
          (assoc initial-report
                 :db-after (-> (new-db store)
                               (assoc :max-eid max-eid)
                               (#(if simulated?
                                   (update % :max-tx u/long-inc)
                                   %)))
                 :tx-data tx-data
                 :tempids tempids))
        (catch Exception e
          (if (:resized (ex-data e))
            (throw e)
            (let [entities (prepare-entities db initial-es tx-time)]
              (local-transact-tx-data initial-report entities tx-time
                                      simulated?)))))
      (let [entities (prepare-entities db initial-es tx-time)]
        (local-transact-tx-data initial-report entities tx-time simulated?)))))

(defn tx-data->simulated-report
  [db tx-data]
  {:pre [(db? db)]}
  (when-not (or (nil? tx-data)
                (sequential? tx-data))
    (raise "Bad transaction data " tx-data ", expected sequential collection"
           {:error :transact/syntax, :tx-data tx-data}))
  (let [initial-report (map->TxReport
                         {:db-before db
                          :db-after  db
                          :tx-data   []
                          :tempids   {}
                          :tx-meta   nil})]
    (transact-tx-data initial-report tx-data true)))

(defn abort-transact
  [conn]
  (let [s (.-store ^DB (deref conn))]
    (if (instance? DatalogStore s)
      (r/abort-transact s)
      (abort-transact-kv (.-lmdb ^Store s)))))

(defn datalog-index-cache-limit
  ([^DB db]
   (let [^Store store (.-store db)]
     (:cache-limit (opts store))))
  ([^DB db ^long n]
   (let [^Store store (.-store db)]
     (assoc-opt store :cache-limit n)
     (refresh-cache store (System/currentTimeMillis)))))
