(ns ^:no-doc datalevin.query.source.native
  "Pinned native views and incremental scans for mixed source queries."
  (:require [datalevin.bits :as bits]
            [datalevin.constants :as c]
            [datalevin.custom-datalog :as custom]
            [datalevin.db :as db]
            [datalevin.index :as index]
            [datalevin.interface :as i]
            [datalevin.lmdb :as lmdb]
            [datalevin.storage :as storage]
            [datalevin.storage.schema :as schema]
            [datalevin.query.source :as source]
            [datalevin.query.source.native-compression :as compression]
            [datalevin.query.source.native-value :as native-value]
            [datalevin.query.source.value :as value :refer [fail!]]
            [datalevin.util :as u])
  (:import [datalevin.db DB]
           [datalevin.storage Store]
           [java.nio ByteBuffer]))

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
    (if (map? t) :data t)))

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

