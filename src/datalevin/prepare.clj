;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.prepare
  "Prepare phase of transaction processing."
  (:require
   [datalevin.interface :as i
    :refer [schema opts populated?]]
   [datalevin.constants :as c]
   [datalevin.index :as idx]
   [datalevin.datom :as d]
   [datalevin.vector :as v]
   [datalevin.util :as u]
   [datalevin.validate :as vld])
  (:import
   [datalevin.datom Datom]
   [java.util Date]))

;; ---- Records ----

(defrecord PrepareCtx
  [schema        ; map: attr keyword -> props map
   rschema       ; reverse schema (property -> attrs)
   opts          ; store options map (:closed-schema?, :auto-entity-time?, etc.)
   store         ; the Store (for point lookups during resolve phase)
   db            ; DB snapshot (db-before, for index lookups)
   lmdb          ; the LMDB instance (for index lookups)
   attrs         ; aid -> attr mapping
   attr-cache])  ; precomputed per-attr flags cache (ref?, multival?, tuple?, etc.)

(defrecord PreparedTx
    [tx-data        ; vector of resolved Datom objects (canonical order)
     db-after       ; DB snapshot after applying all datoms (with updated indexes)
     tempids        ; map: tempid -> resolved eid
     new-attributes ; vector of attrs added in this tx
     tx-redundant   ; vector of redundant (no-op) datoms
     side-index-ops ; map: {:ft [...] :vec [...] :idoc [...] :gt [...]}
     touch-summary  ; set of touched attr keywords (for cache invalidation)
     stats])        ; optional: per-stage timing/counters map

;; ---- Constructor ----

(defn make-prepare-ctx
  "Build a PrepareCtx from a DB snapshot."
  [db lmdb]
  (let [store (:store db)]
    (->PrepareCtx (schema store)
                  (i/rschema store)
                  (opts store)
                  store
                  db
                  lmdb
                  (i/attrs store)
                  nil)))

;; ---- Stage boundary function shells ----
;; Each stage accepts a context map and returns it unchanged (pass-through).
;; Stages will be filled in subsequent steps.

(defn normalize
  "Expand/normalize entities and tx forms.
   Currently a pass-through shell."
  [ctx entities]
  entities)

(defn resolve-ids
  "Resolve tempids/upserts/refs to fixed point.
   Currently a pass-through shell."
  [ctx entities]
  entities)

(defn apply-op-semantics
  "Apply :db/add, retract, CAS, patchIdoc, etc.
   Currently a pass-through shell."
  [ctx entities]
  entities)

(defn plan-delta
  "Plan datom delta with cardinality/uniqueness rules.
   Currently a pass-through shell."
  [ctx entities]
  entities)

(defn build-side-index-ops
  "Build side-index overlay deltas, allocate IDs.
   Currently a pass-through shell."
  [ctx entities]
  entities)

(defn finalize
  "Check-value-tempids, deterministic ordering, produce PreparedTx.
   Currently a pass-through shell."
  [ctx entities]
  entities)

;; ---- Prepare-time validators ----

(defn validate-prepared-datoms
  "Run logical datom validations before apply."
  [schema opts datoms]
  (doseq [^Datom datom datoms
          :when (d/datom-added datom)]
    (vld/validate-closed-schema schema opts (.-a datom) (.-v datom)))
  datoms)

(defn validate-load-datoms
  "Validate public/untrusted `load-datoms` input before apply."
  [store datoms]
  (vld/validate-datom-list datoms)
  (validate-prepared-datoms (schema store) (opts store) datoms)
  datoms)

(defn- fulltext-domains
  [attr props]
  (cond-> (vec (or (:db.fulltext/domains props) [c/default-domain]))
    (:db.fulltext/autoDomain props) (conj (u/keyword->string attr))))

(defn- vector-domains
  [attr props]
  (conj (vec (:db.vec/domains props)) (v/attr-domain attr)))

(defn- idoc-domain
  [attr props]
  (or (:db/domain props) (u/keyword->string attr)))

(defn validate-schema-side-index-domains
  "Validate side-index domain applicability for schema mutation payload."
  [new-schema available]
  (let [{:keys [fulltext vector idoc]} available]
    (doseq [[attr props] new-schema
            :let [vt (idx/value-type props)]]
      (when (:db/fulltext props)
        (doseq [domain (fulltext-domains attr props)]
          (when-not (contains? fulltext domain)
            (u/raise "Fulltext domain is not initialized for attribute " attr
                     {:error :prepare/side-index-domain
                      :kind :fulltext
                      :attribute attr
                      :domain domain}))))
      (when (identical? vt :db.type/vec)
        (doseq [domain (vector-domains attr props)]
          (when-not (contains? vector domain)
            (u/raise "Vector domain is not initialized for attribute " attr
                     {:error :prepare/side-index-domain
                      :kind :vector
                      :attribute attr
                      :domain domain}))))
      (when (identical? vt :db.type/idoc)
        (let [domain (idoc-domain attr props)]
          (when-not (contains? idoc domain)
            (u/raise "Idoc domain is not initialized for attribute " attr
                     {:error :prepare/side-index-domain
                      :kind :idoc
                      :attribute attr
                      :domain domain})))))))

(defn validate-side-index-domains
  "Validate side-index domain applicability for a prepared datom batch.
   `available` is {:fulltext #{...} :vector #{...} :idoc #{...}}."
  [schema datoms available]
  (let [{:keys [fulltext vector idoc]} available]
    (doseq [^Datom datom datoms
            :let [attr  (.-a datom)
                  props (schema attr)
                  vt    (idx/value-type props)]
            :when props]
      (when (:db/fulltext props)
        (doseq [domain (fulltext-domains attr props)]
          (when-not (contains? fulltext domain)
            (u/raise "Fulltext domain is not initialized for attribute " attr
                     {:error :prepare/side-index-domain
                      :kind :fulltext
                      :attribute attr
                      :domain domain}))))
      (when (identical? vt :db.type/vec)
        (doseq [domain (vector-domains attr props)]
          (when-not (contains? vector domain)
            (u/raise "Vector domain is not initialized for attribute " attr
                     {:error :prepare/side-index-domain
                      :kind :vector
                      :attribute attr
                      :domain domain}))))
      (when (identical? vt :db.type/idoc)
        (let [domain (idoc-domain attr props)]
          (when-not (contains? idoc domain)
            (u/raise "Idoc domain is not initialized for attribute " attr
                     {:error :prepare/side-index-domain
                      :kind :idoc
                      :attribute attr
                      :domain domain}))))))
  datoms)

(defn validate-schema-update
  "Validate schema update payload and mutation safety checks before apply."
  ([store lmdb new-schema]
   (validate-schema-update store lmdb new-schema nil))
  ([store lmdb new-schema available]
   (when new-schema
     (vld/validate-schema new-schema)
     (when available
       (validate-schema-side-index-domains new-schema available))
     (let [current-schema (schema store)]
       (doseq [[attr new-props] new-schema
               :let [old-props (current-schema attr)]
               :when old-props]
         (vld/validate-schema-mutation store lmdb attr old-props new-props))))
   new-schema))

(defn validate-option-update
  "Validate option key/value before apply."
  [k v]
  (vld/validate-option-mutation k v)
  [k v])

(defn validate-swap-attr-update
  "Validate swap-attr mutation safety checks before apply."
  ([store lmdb attr old-props new-props]
   (validate-swap-attr-update store lmdb attr old-props new-props nil))
  ([store lmdb attr old-props new-props available]
   (vld/validate-schema-mutation store lmdb attr old-props new-props)
   (when available
     (validate-schema-side-index-domains {attr new-props} available))
   new-props))

(defn validate-del-attr
  "Validate `del-attr` safety checks before apply."
  [store attr]
  (vld/validate-attr-deletable
    (populated? store :ave (d/datom c/e0 attr c/v0) (d/datom c/emax attr c/vmax)))
  attr)

(defn- validate-rename-attr*
  [s attr new-attr]
  (when-not (s attr)
    (u/raise "Cannot rename missing attribute: " attr
             {:error :schema/rename
              :attribute attr
              :target new-attr}))
  (when (and (not= attr new-attr) (s new-attr))
    (u/raise "Cannot rename to existing attribute: " new-attr
             {:error :schema/rename
              :attribute attr
              :target new-attr}))
  [attr new-attr])

(defn validate-rename-attr
  "Validate `rename-attr` safety checks before apply."
  [store attr new-attr]
  (validate-rename-attr* (schema store) attr new-attr))

(defn validate-rename-map
  "Validate ordered rename mutations against a projected schema map.
   Returns the projected schema after all renames are applied."
  [projected-schema rename-map]
  (reduce
    (fn [s [attr new-attr]]
      (validate-rename-attr* s attr new-attr)
      (if (= attr new-attr)
        s
        (let [props (s attr)]
          (-> s
              (dissoc attr)
              (assoc new-attr props)))))
    projected-schema
    rename-map))

;; ---- Top-level entry point ----

(defn- maybe-count
  [x]
  (when (counted? x) (count x)))

(defn- run-stage
  [ctx stats stage-k stage-fn entities]
  (let [start   (System/nanoTime)
        output  (stage-fn ctx entities)
        elapsed (- (System/nanoTime) start)]
    [output
     (assoc stats stage-k
            {:elapsed-ns   elapsed
             :input-count  (maybe-count entities)
             :output-count (maybe-count output)})]))

(defn prepare-tx
  "Run the full prepare pipeline. Returns a PreparedTx.
   execute-fn: (fn [entities tx-time] -> report) runs the transaction loop."
  [ctx entities tx-time execute-fn]
  (if c/*collect-prepare-stats*
    (let [[entities stats] (run-stage ctx {} :normalize normalize entities)
          [entities stats] (run-stage ctx stats :resolve-ids resolve-ids entities)
          [entities stats] (run-stage
                             ctx stats :apply-op-semantics apply-op-semantics
                             entities)
          [entities stats] (run-stage ctx stats :delta-plan plan-delta entities)
          [entities stats] (run-stage
                             ctx stats :side-index-overlay-build
                             build-side-index-ops entities)
          [entities stats] (run-stage ctx stats :finalize finalize entities)
          start            (System/nanoTime)
          report           (execute-fn entities tx-time)
          elapsed          (- (System/nanoTime) start)
          stats            (assoc stats :execute
                                  {:elapsed-ns elapsed
                                   :tx-datoms  (maybe-count (:tx-data report))
                                   :tempids    (maybe-count (:tempids report))})]
      (->PreparedTx
        (:tx-data report)
        (:db-after report)
        (:tempids report)
        (:new-attributes report)
        nil   ; tx-redundant not needed externally
        nil   ; side-index-ops (future)
        nil   ; touch-summary (future)
        stats))
    (let [entities (normalize ctx entities)
          entities (resolve-ids ctx entities)
          entities (apply-op-semantics ctx entities)
          entities (plan-delta ctx entities)
          entities (build-side-index-ops ctx entities)
          entities (finalize ctx entities)
          report   (execute-fn entities tx-time)]
      (->PreparedTx
        (:tx-data report)
        (:db-after report)
        (:tempids report)
        (:new-attributes report)
        nil
        nil
        nil
        nil))))

;; ---- Type coercion and value correction ----

(defn coerce-inst
  "Coerce a value to java.util.Date."
  [v]
  (cond
    (inst? v)    v
    (integer? v) (Date. (long v))
    :else        (u/raise "Expect java.util.Date" {:input v})))

(defn coerce-uuid
  "Coerce a value to java.util.UUID."
  [v]
  (cond
    (uuid? v)   v
    (string? v) (if-let [u (parse-uuid v)]
                  u
                  (u/raise "Unable to parse string to UUID" {:input v}))
    :else       (u/raise "Expect java.util.UUID" {:input v})))

(defn type-coercion
  "Coerce a value to the appropriate type based on value type."
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

(defn correct-datom*
  "Reconstruct datom with corrected value."
  [^Datom datom v]
  (d/datom (.-e datom) (.-a datom) v
           (d/datom-tx datom) (d/datom-added datom)))

(declare correct-value)

(defn correct-datom
  "Correct value in datom via type validation and coercion."
  [store ^Datom datom]
  (correct-datom* datom (correct-value store (.-a datom) (.-v datom))))

(defn correct-value
  "Validate type and coerce value for an attribute."
  [store a v]
  (let [props ((schema store) a)
        vt    (idx/value-type props)]
    (if (identical? vt :db.type/idoc)
      ((requiring-resolve 'datalevin.idoc/parse-value) a props (opts store) v)
      (type-coercion (vld/validate-type store a v) v))))
