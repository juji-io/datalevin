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
  (:import [datalevin.db DB]
           [datalevin.storage Store]
           [java.lang AutoCloseable Iterable]
           [java.nio ByteBuffer]
           [java.util Iterator]))

(defn validate! [database]
  (when (or (db/remote-store? (.-store ^DB database))
            (db/pending-tx-cache? database)
            (and (u/supports-virtual-threads?) (.isVirtual (Thread/currentThread)))
            (lmdb/writing? (.-lmdb ^Store (.-store ^DB database))))
    (fail! :query-source/unsupported :native-view)))

(defn with-views
  "Pin each native store on the query's platform thread for the whole invocation."
  [databases f]
  (letfn [(pin [stores snapshots]
            (if-let [^Store store (first stores)]
              (let [kv (.-lmdb store)]
                (storage/maybe-ensure-current! store)
                (custom/with-snapshot kv
                  (let [schema (schema/load-schema kv)
                        attrs (into {} (map (fn [[a props]] [(:db/aid props) a])) schema)]
                    (pin (next stores) (assoc snapshots store {:kv kv :attrs attrs :schema schema})))))
              (f snapshots)))]
    (pin (distinct (map #(.-store ^DB %) databases)) {})))

(defn- logical-scalar [t]
  (when (and (keyword? t) (not (custom/custom-type? t)))
    (let [t (if (= t :db.type/ref) :long (keyword (name t)))]
      (when (and (value/scalar-types t) (not (#{:data :bytes} t))) t))))

(defn- attribute-type [schema attribute]
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
        (fail! :query-source/unsupported :native-type))))

(defn- matches-native-type? [type x]
  ;; Native heterogeneous tuples retain absent component values as nil. The
  ;; external descriptor still enforces its own (non-nullable) element types.
  (case (:kind type)
    :tuple (and (vector? x) (= (count x) (count (:elements type)))
                (every? true? (map #(or (nil? %2) (value/matches-type? %1 %2))
                                  (:elements type) x)))
    :native-homogeneous-tuple
    (and (vector? x) (<= 1 (count x) 255)
         (every? #(value/matches-type? (:element type) %) x))
    (value/matches-type? type x)))

(defn validate-types! [runtime snapshots entries external-types input-data]
  (doseq [{:keys [native patterns symbol]} entries :when native
          pattern patterns]
    (rt/with-context runtime {:source-symbol symbol :clause-ids [(:id pattern)]}
      (fn [{:keys [control]}]
        (let [schema (:schema (get snapshots (.-store ^DB native)))
              attribute (second (second (:terms pattern)))
              ;; An explicit wildcard requires only the index entry's
              ;; existence, not a supported logical value or decoder.
              t (when-not (= :ignore (first (nth (:terms pattern) 2)))
                  (attribute-type schema attribute))
              logical (if (= :native-homogeneous-tuple (:kind t)) :data t)]
          (doseq [[term type logical-type]
                  (map vector (:terms pattern) [:long :keyword t] [:long :keyword logical])]
            (when (= :var (first term))
              (when-let [external (external-types (second term))]
                (when-not (= external logical-type)
                  (fail! :query-source/unsupported :types))))
            (let [check! (fn [x]
                           (when-not (matches-native-type? type x)
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

(defn- scan-attribute!
  [control {:keys [kv attrs]} attribute visit]
  (scan/scan kv c/eav
    (with-open [^AutoCloseable iterator
                (.iterator ^Iterable (lmdb/iterate-list dbi rtx cur [:all] :id [:all] :avg))]
      (loop []
        (source/check-active! control)
        (when (.hasNext ^Iterator iterator)
          (let [entry (.next ^Iterator iterator)
                eid (bits/read-buffer (lmdb/k entry) :id)
                ^ByteBuffer raw (lmdb/v entry)
                result (when (= attribute (attrs (bits/avg->aid raw)))
                         (visit rtx raw eid))]
            (when-not (reduced? result)
              (recur))))))
    (throw e)))

(defn scan-keys!
  "Visit owned [entity attribute] pairs for an explicit value wildcard. Each
  matching index entry contributes one pair, including cardinality-many facts.
  Reserve the temporary row before scanning; never read or decode its payload."
  [control snapshot attribute emit]
  (let [token (source/reserve! control
                {:retained-bytes (value/add-charge 512 (value/value-charge attribute))})]
    (try
      (scan-attribute! control snapshot attribute
                      (fn [_ _ eid] (emit [eid attribute])))
      (finally (source/release! token)))))

(defn scan!
  "Visit owned datoms from one pinned EAV scan without a full-result collector."
  [control {:keys [kv schema] :as snapshot} attribute emit]
  (let [type (attribute-type schema attribute)
        check! #(source/check-active! control)]
    (scan-attribute! control snapshot attribute
      (fn [rtx ^ByteBuffer raw eid]
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
