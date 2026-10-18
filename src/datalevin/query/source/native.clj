(ns ^:no-doc datalevin.query.source.native
  "Pinned native views and incremental scans for mixed source queries."
  (:require [datalevin.bits :as bits]
            [datalevin.constants :as c]
            [datalevin.custom-datalog :as custom]
            [datalevin.db :as db]
            [datalevin.index :as index]
            [datalevin.interface :as i]
            [datalevin.lmdb :as lmdb]
            [datalevin.scan :as scan]
            [datalevin.storage :as storage]
            [datalevin.storage.schema :as schema]
            [datalevin.query.source :as source]
            [datalevin.query.source.inputs :as input]
            [datalevin.query.source.native-compression :as compression]
            [datalevin.query.source.native-value :as native-value]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :as value :refer [fail!]]
            [datalevin.util :as u])
  (:import [datalevin.bits Indexable]
           [datalevin.db DB]
           [datalevin.storage Store]
           [java.lang AutoCloseable Iterable]
           [java.nio ByteBuffer]
           [java.util Iterator]))

(defn validate! [database]
  (when-not (instance? DB database) (fail! :query-source/unsupported :native-view))
  (when (or (db/remote-store? (.-store ^DB database))
            (db/pending-tx-cache? database)
            (and (u/supports-virtual-threads?) (.isVirtual (Thread/currentThread)))
            (lmdb/writing? (.-lmdb ^Store (.-store ^DB database))))
    (fail! :query-source/unsupported :native-view)))

(defn with-views
  "Pin each native store on the query's platform thread for the whole invocation."
  [databases f]
  (doseq [database databases] (validate! database))
  (letfn [(pin [stores snapshots]
            (if-let [^Store store (first stores)]
              (let [kv (.-lmdb store)]
                (storage/maybe-ensure-current! store)
                (custom/with-snapshot kv
                  (let [schema (schema/load-schema kv) closed? (atom false)
                        context ^datalevin.custom_datalog.ReadContext (.get custom/read-context)
                        snapshot {:kv kv :schema schema :closed? closed?
                                  :thread (Thread/currentThread) :rtx (.-rtx context)}]
                    (try (pin (next stores) (assoc snapshots store snapshot))
                         (finally (reset! closed? true))))))
              (f snapshots)))]
    (pin (distinct (map #(.-store ^DB %) databases)) {})))

(defn- logical-scalar [t]
  (when (and (keyword? t) (not (custom/custom-type? t)))
    (let [t (if (= t :db.type/ref) :long (keyword (name t)))]
      (when (and (value/scalar-types t) (not= :data t)) t))))

(defn- attribute-type [{:keys [kv schema]} attribute]
  (let [{:db/keys [valueType tupleTypes tupleType]} (schema attribute)]
    (or (logical-scalar valueType)
        (when (or (nil? valueType) (= valueType :db.type/data)
                  (and (= valueType :db.type/tuple) (nil? tupleTypes)
                       (nil? tupleType)))
          :data)
        (when (and (= valueType :db.type/tuple) tupleType)
          (when-let [element (logical-scalar tupleType)]
            ;; Internal decoding contract; the SPI represents varying arities
            ;; as :data, without promising a fixed positional tuple descriptor.
            {:kind :native-homogeneous-tuple :element element}))
        (when (and (= valueType :db.type/tuple) (<= 2 (count tupleTypes) 255))
          (let [elements (mapv logical-scalar tupleTypes)]
            (when (every? some? elements) {:kind :tuple :elements elements})))
        (when (custom/custom-type? valueType)
          ;; Resolve metadata in the pinned view, without realizing the type's
          ;; delayed order/codec functions. Only portable Nippy payloads have a
          ;; bounded decoder; arbitrary user deserializers cannot be preflighted.
          (when (= :nippy (get-in (custom/descriptor kv valueType)
                                  [:custom/type :definition :payload]))
            {:kind :native-custom-data}))
        (fail! :query-source/unsupported :native-type))))

(defn check-view!
  "A controlled native step borrows its pinned view only on the owning thread."
  [control snapshot]
  (source/check-active! control)
  (when-not (and snapshot (:closed? snapshot) (not @(:closed? snapshot))
                 (identical? (:thread snapshot) (Thread/currentThread)))
    (fail! :query-source/invalid :native-view)))

(defn logical-type!
  "Validate a bounded native decoder without invoking arbitrary custom codecs.
  Nullable native tuple components use the broad logical :data envelope."
  [snapshot attribute]
  (let [t (attribute-type snapshot attribute)]
    (when (= :native-custom-data (:kind t))
      (fail! :query-source/unsupported :native-type))
    (if (map? t) :data t)))

(defn- matches-native-type? [type x check!]
  ;; Native heterogeneous tuples retain absent component values as nil. The
  ;; external descriptor still enforces its own (non-nullable) element types.
  (case (:kind type)
    :tuple (and (vector? x) (= (count x) (count (:elements type)))
                (every? true? (map #(or (nil? %2) (value/matches-type? %1 %2 check!))
                                  (:elements type) x)))
    :native-homogeneous-tuple
    (and (vector? x) (<= 1 (count x) 255)
         (every? #(value/matches-type? (:element type) % check!) x))
    :native-custom-data (value/matches-type? :data x check!)
    (value/matches-type? type x check!)))

(defn validate-types! [runtime snapshots entries external-types input-data]
  (doseq [{:keys [native patterns symbol]} entries :when native
          pattern patterns]
    (rt/with-context runtime {:source-symbol symbol :clause-ids [(:id pattern)]}
      (fn [{:keys [control]}]
        (source/check-active! control)
        (let [active! #(source/check-active! control)
              snapshot (get snapshots (.-store ^DB native))
              attribute (second (second (:terms pattern)))
              ;; An explicit wildcard requires only the index entry's
              ;; existence, not a supported logical value or decoder.
              t (when-not (= :ignore (first (nth (:terms pattern) 2)))
                  (attribute-type snapshot attribute))
              logical (if (#{:native-homogeneous-tuple :native-custom-data} (:kind t)) :data t)]
          (doseq [[term type logical-type]
                  (map vector (:terms pattern) [:long :keyword t] [:long :keyword logical])]
            (when (= :var (first term))
              (when-let [external (external-types (second term))]
                (when-not (= external logical-type)
                  (fail! :query-source/unsupported :types))))
            (let [check! (fn [x]
                           (when-not (matches-native-type? type x active!)
                             (fail! :query-source/unsupported :native-type)))]
              (case (first term)
                :const (check! (second term))
                :var (input/check-values! control input-data (second term) check!)
                nil))))))))

(defn- visit-value [control ^ByteBuffer buffer expanded-size attribute type payload prepare emit eid]
  ;; Reserve buffers and bounded preflight state first. Generic collections and
  ;; giant homogeneous tuples or metadata get a separate layout-derived
  ;; reservation, held through emit.
  ;; Retaining a row beyond emit requires the consumer's own reservation.
  (let [expanded-size (long expanded-size)
        attribute-size (long (value/value-charge attribute))
        ;; Both buffer sizes are at most Integer/MAX_VALUE, so this fits a long.
        token (source/reserve! control
                {:retained-bytes (+ 4096 (long (.remaining buffer))
                                   (* 8 expanded-size) attribute-size
                                   (native-value/scratch-bytes type expanded-size (boolean payload)))})]
    (try
      (let [visit (fn [buffer]
                    (let [{:keys [decode retained-bytes]} (prepare buffer)
                          decoded (when (pos? (long retained-bytes))
                                    (source/reserve! control {:retained-bytes retained-bytes}))]
                      (try
                        (let [v (decode)]
                          (source/check-active! control)
                          (emit [eid attribute v]))
                        (finally (when decoded (source/release! decoded))))))]
        (if payload
          (compression/with-buffer control (payload)
                                   #(native-value/scratch-bytes type %) visit)
          (visit buffer)))
      (finally (source/release! token)))))

(defn- visit-range!
  [control dbi rtx cur k-range k-type v-range v-type ave? visit]
  (with-open [^AutoCloseable iterator
              (.iterator ^Iterable
                (lmdb/iterate-list dbi rtx cur k-range k-type v-range v-type))]
    (loop []
      (source/check-active! control)
      (when (.hasNext ^Iterator iterator)
        (let [entry (.next ^Iterator iterator)
              eid (bits/read-buffer (if ave? (lmdb/v entry) (lmdb/k entry)) :id)
              raw (if ave? (lmdb/k entry) (lmdb/v entry))
              result (visit rtx raw eid)]
          (if (reduced? result) result (recur)))))))

(defn- value-range-type [props x]
  ;; These scalar encodings preserve an equality candidate bucket. Keep the
  ;; logical equality check: truncated strings and UTF-8 replacements can
  ;; collide, and Java key equality distinguishes signed floating-point zero.
  ;; Generic/custom payloads, decimal scales and tuples need other bounds.
  (let [t (:db/valueType props)]
    (when (and (some? x)
               (#{:db.type/boolean :db.type/long :db.type/ref :db.type/uuid
                  :db.type/instant :db.type/string :db.type/keyword :db.type/symbol
                  :db.type/float :db.type/double} t)
               (value/matches-type? (logical-scalar t) x)
               (not (and (#{:db.type/float :db.type/double} t)
                         (Double/isNaN (double x)))))
      t)))

(defn- scan-attribute!
  [control {:keys [kv schema]} attribute entity-id bound-value visit]
  (source/check-active! control)
  (when-let [aid (:db/aid (schema attribute))]
    (let [aid (int aid)
          props (schema attribute)
          value-type (value-range-type props bound-value)
          ;; Reserve before encoding both bounds, including temporary UTF-8
          ;; arrays for giant strings/keywords/symbols. No user codec runs here.
          token (when value-type
                  (source/reserve! control
                    {:retained-bytes (value/add-charge 4096
                                       (value/multiply-charge 8 (value/value-charge bound-value)))}))]
      (try
        (let [attr-range (if value-type
                           (let [^Indexable low (bits/indexable nil aid bound-value value-type c/g0)]
                             ;; Encode once; only the giant-ID suffix differs.
                             [:closed low (if (bits/giant? low)
                                            (Indexable. nil aid bound-value (.-f low) (.-b low) c/gmax)
                                            low)])
                           ;; AVG starts with an unsigned aid in both indexes;
                           ;; its successor also works at the largest legal aid.
                           [:closed-open aid (unchecked-inc-int aid)])
              range-type (if value-type :avg :int)
              ave? (and (nil? entity-id) (not (:db/noindex props)))]
          (scan/scan kv (if ave? c/ave c/eav)
            (cond
              ave? (visit-range! control dbi rtx cur attr-range range-type [:all] :id true visit)
              (some? entity-id)
              (visit-range! control dbi rtx cur [:closed entity-id entity-id] :id
                            attr-range range-type false visit)
              :else
              ;; Unindexed attributes have no AVE entries. Walk entity keys
              ;; once and seek only matching duplicates, checking cancellation
              ;; between entities even when no value matches.
              (with-open [^AutoCloseable entities
                          (.iterator ^Iterable (lmdb/iterate-key dbi rtx cur [:all] :id))]
                (scan/scan kv c/eav
                  (loop []
                    (source/check-active! control)
                    (when (.hasNext ^Iterator entities)
                      (let [eid (bits/read-buffer (lmdb/k (.next ^Iterator entities)) :id)
                            result (visit-range! control dbi rtx cur [:closed eid eid] :id
                                                 attr-range range-type false visit)]
                        (if (reduced? result) result (recur)))))
                  (throw e))))
            (throw e)))
        (finally (when token (source/release! token)))))))

(defn scan-keys!
  "Visit owned [entity attribute] pairs for an explicit value wildcard. Each
  matching index entry contributes one pair, including cardinality-many facts.
  Reserve the temporary row before scanning; never read or decode its payload.
  Use the attribute's AVE range, or an attribute-bounded EAV range when the
  entity is supplied or the attribute is unindexed."
  ([control snapshot attribute emit]
   (scan-keys! control snapshot attribute nil emit))
  ([control snapshot attribute entity-id emit]
   (let [token (source/reserve! control
                 {:retained-bytes (value/add-charge 512 (value/value-charge attribute))})]
     (try
       (scan-attribute! control snapshot attribute entity-id nil
                       (fn [_ _ eid] (emit [eid attribute])))
       (finally (source/release! token))))))

(defn raw-visitor
  "Build a synchronous bounded decoder for an existing native scan. Its emit
  callback borrows [entity attribute value] until it returns; retaining values
  requires a separate owned admission. This function opens no scan or cursor."
  [control {:keys [kv]} attribute type emit]
  (let [check! #(source/check-active! control)]
    (fn [rtx ^ByteBuffer raw eid]
      (if (= :native-custom-data (:kind type))
        (let [id (native-value/custom-reference-id raw)
              payloads (i/get-dbi kv c/custom-values false)]
          (lmdb/put-read-key payloads rtx id :id)
          (if-let [^ByteBuffer buffer (lmdb/get-kv payloads rtx)]
            (visit-value control buffer (.remaining buffer) attribute :data
                         (fn [] buffer) #(native-value/prepare-data % check!) emit eid)
            (fail! :query-source/invalid :native-value)))
        (let [giant (bits/avg->giant-id raw)]
          (if (= c/normal giant)
            (visit-value control raw (.remaining raw) attribute type
                         (when (= type :data) #(native-value/inline-data-buffer raw))
                         (if (= type :data)
                           #(native-value/prepare-data % check!)
                           #(native-value/prepare-inline % type check!)) emit eid)
            (let [giants (i/get-dbi kv c/giants false)]
              (lmdb/put-read-key giants rtx giant :id)
              (if-let [buffer (lmdb/get-kv giants rtx)]
                (visit-value control buffer (index/giant-datom-raw-size buffer) attribute type
                             #(index/giant-datom-buffer buffer)
                             #(native-value/prepare-giant % type eid attribute check!)
                             emit eid)
                (fail! :query-source/invalid :native-value)))))))))

(defn value-visitor
  "Decode values borrowed from the shared scan loop in the pinned read view."
  [control snapshot attribute]
  (check-view! control snapshot)
  (let [type (attribute-type snapshot attribute)]
    (fn [raw eid callback]
      (check-view! control snapshot)
      ;; Reuse the visitor's bounded scratch and decoding, while the consumer
      ;; owns the row before any subsequent cursor operation.
      ((raw-visitor control snapshot attribute type
                    (fn [[_ _ value]] (callback value))) (:rtx snapshot) raw eid))))

(defn scan!
  "Visit owned datoms from a pinned attribute scan without a full-result collector.
  Entity, attribute and eligible scalar value bounds precede payload decoding.
  Value bounds select candidates; the caller still checks logical equality."
  ([control snapshot attribute emit]
   (scan! control snapshot attribute nil nil emit))
  ([control snapshot attribute entity-id emit]
   (scan! control snapshot attribute entity-id nil emit))
  ([control snapshot attribute entity-id bound-value emit]
   (scan-attribute! control snapshot attribute entity-id bound-value
                    (raw-visitor control snapshot attribute (attribute-type snapshot attribute) emit))))
