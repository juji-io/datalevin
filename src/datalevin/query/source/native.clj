(ns ^:no-doc datalevin.query.source.native
  "Pinned native views and incremental scans for mixed source queries."
  (:require [datalevin.bits :as bits]
            [datalevin.constants :as c]
            [datalevin.custom-datalog :as custom]
            [datalevin.db :as db]
            [datalevin.lmdb :as lmdb]
            [datalevin.scan :as scan]
            [datalevin.storage :as storage]
            [datalevin.storage.scan :as storage-scan]
            [datalevin.storage.schema :as schema]
            [datalevin.query.source :as source]
            [datalevin.query.source.value :as value :refer [fail!]]
            [datalevin.util :as u])
  (:import [datalevin.db DB]
           [datalevin.storage Store]
           [java.lang AutoCloseable Iterable]
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

(defn scan!
  "Visit owned datoms from one pinned EAV scan without a full-result collector."
  [control {:keys [kv attrs]} attribute emit]
  (scan/scan kv c/eav
    (with-open [^AutoCloseable iterator
                (.iterator ^Iterable (lmdb/iterate-list dbi rtx cur [:all] :id [:all] :avg))]
      (loop []
        (source/check-active! control)
        (when (.hasNext ^Iterator iterator)
          (let [entry (.next ^Iterator iterator)
                eid (bits/read-buffer (lmdb/k entry) :id)
                raw (lmdb/v entry)
                result (when (= attribute (attrs (bits/avg->aid raw)))
                         ;; A giant/custom payload needs an explicit bounded
                         ;; decoder before this path can safely materialize it.
                         (when-not (= c/normal (bits/avg->giant-id raw))
                           (fail! :query-source/unsupported :native-value))
                         (let [retrieved (bits/read-buffer raw :avg)
                               datom (storage-scan/kv->datom kv attrs eid retrieved)]
                           (emit [(:e datom) (:a datom) (:v datom)])))]
            (when-not (reduced? result)
              (recur))))))
    (throw e)))
