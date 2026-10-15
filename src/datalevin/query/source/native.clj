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
            [datalevin.query.source.native-value :as native-value]
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

(defn validate-types! [snapshots entries external-types bindings]
  (doseq [{:keys [native patterns]} entries :when native
          pattern patterns
          :let [schema (:schema (get snapshots (.-store ^DB native)))
                attribute (second (second (:terms pattern)))
                t (get-in schema [attribute :db/valueType] :db.type/data)
                t (if (= t :db.type/ref) :long (keyword (name t)))]]
    (when (or (custom/custom-type? (get-in schema [attribute :db/valueType]))
              (#{:data :tuple :bytes} t))
      (fail! :query-source/unsupported :native-type))
    (doseq [[term type] (map vector (:terms pattern) [:long :keyword t])]
      (when (= :var (first term))
        (when-let [external (external-types (second term))]
          (when-not (= external type) (fail! :query-source/unsupported :types))))
      (when (or (= :const (first term))
                (and (= :var (first term)) (contains? bindings (second term))))
        (when-not (value/matches-type? type (if (= :const (first term)) (second term)
                                              (bindings (second term))))
          (fail! :query-source/unsupported :native-type))))))

(defn- scalar-type [schema attribute]
  (let [t (get-in schema [attribute :db/valueType])]
    (if (= t :db.type/ref) :long (keyword (name t)))))

(defn- inline-type [^ByteBuffer raw]
  ;; Guard the decoder as well as the declared schema: no generic/custom reader
  ;; may run under a scalar reservation, including after native schema changes.
  (case (int (.get raw 4))
    (-64 -63 -8) :long
    -15 :bigint
    -14 :bigdec
    -11 :float
    -10 :double
    -9 :instant
    -7 :uuid
    -6 :string
    -5 :keyword
    -4 :symbol
    -3 :boolean
    (fail! :query-source/unsupported :native-value)))

(defn- visit-value [control ^ByteBuffer buffer expanded-size attribute decode emit eid]
  ;; Flat scalars have no collection amplification. This covers both byte
  ;; buffers, UTF-8/UTF-16 temporaries, the scalar, and the row/binding containers.
  ;; Retaining a row beyond emit requires the consumer's own reservation.
  (let [expanded-size (long expanded-size)
        attribute-size (long (value/value-charge attribute))
        ;; Both buffer sizes are at most Integer/MAX_VALUE, so this fits a long.
        token (source/reserve! control
                {:retained-bytes (+ 4096 (long (.remaining buffer))
                                   (* 8 expanded-size) attribute-size)})]
    (try
      (let [v (decode)]
        (source/check-active! control)
        (emit [eid attribute v]))
      (finally (source/release! token)))))

(defn scan!
  "Visit owned datoms from one pinned EAV scan without a full-result collector."
  [control {:keys [kv attrs schema]} attribute emit]
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
                         (let [type (scalar-type schema attribute)
                               giant (bits/avg->giant-id raw)]
                           (if (= c/normal giant)
                             (do
                               (when-not (= type (inline-type raw))
                                 (fail! :query-source/invalid :native-value))
                               (visit-value control raw (.remaining raw) attribute
                                            #(bits/avg->inline-value raw) emit eid))
                             (let [giants (i/get-dbi kv c/giants false)]
                               (lmdb/put-read-key giants rtx giant :id)
                               (if-let [buffer (lmdb/get-kv giants rtx)]
                                 (visit-value control buffer (index/giant-datom-raw-size buffer) attribute
                                              #(native-value/giant-value
                                                 (index/giant-datom-buffer buffer) type eid attribute)
                                              emit eid)
                                 (fail! :query-source/invalid :native-value))))))]
            (when-not (reduced? result)
              (recur))))))
    (throw e)))
