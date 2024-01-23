(ns ^:no-doc datalevin.db
  (:refer-clojure :exclude [update])
  (:require
   [clojure.walk]
   [clojure.data]
   [clojure.set]
   [datalevin.constants :as c :refer [e0 tx0 emax txmax]]
   [datalevin.lru :as lru]
   [datalevin.datom :as d
    :refer [datom datom-added datom?]]
   [datalevin.util :as u
    :refer [case-tree raise defrecord-updatable cond+]]
   [datalevin.storage :as s]
   [datalevin.lmdb :as l]
   [datalevin.remote :as r]
   [datalevin.inline :refer [update]])
  (:import
   [datalevin.datom Datom]
   [datalevin.storage IStore Store]
   [datalevin.remote DatalogStore]
   [datalevin.lru LRU]
   [java.util SortedSet Comparator]
   [java.util.concurrent ConcurrentHashMap]
   [org.eclipse.collections.impl.set.sorted.mutable TreeSortedSet]))

;;;;;;;;;; Protocols

(defprotocol ISearch
  (-search [data pattern])
  (-count [data pattern])
  (-first [data pattern])
  (-last [data pattern]))

(defprotocol IIndexAccess
  (-populated? [db index c1 c2 c3])
  (-datoms [db index c1 c2 c3])
  (-range-datoms [db index start-datom end-datom])
  (-first-datom [db index c1 c2 c3])
  (-seek-datoms [db index c1 c2 c3])
  (-rseek-datoms [db index c1 c2 c3])
  (-index-range [db attr start end]))

(defprotocol IDB
  (-schema [db])
  (-rschema [db])
  (-attrs-by [db property])
  (-clear-tx-cache [db]))

(defprotocol ISearchable
  (-searchable? [_]))

(extend-type Object ISearchable (-searchable? [_] false))

(extend-type nil ISearchable (-searchable? [_] false))

(defprotocol IQuery
  (av->eids [db pattern pred]
    "return tuples of eids that matches pattern")
  (rev-merge-scan [db tuples veid-idx attr]
    "assume tuples already sorted by veid")
  (merge-scan [db tuples eid-idx attrs preds]
    "assume tuples already sorted by eid"))

;; ----------------------------------------------------------------------------

(declare empty-db resolve-datom validate-attr components->pattern)

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
   (refresh-cache store (s/last-modified store)))
  ([store target]
   (.put ^ConcurrentHashMap caches (s/dir store)
         (lru/lru (:cache-limit (s/opts store)) target))))

(defmacro wrap-cache
  [store pattern body]
  `(let [cache# (.get ^ConcurrentHashMap caches (s/dir ~store))]
     (if-some [cached# (get ^LRU cache# ~pattern nil)]
       cached#
       (let [res# ~body]
         (.put ^ConcurrentHashMap caches (s/dir ~store)
               (assoc cache# ~pattern res#))
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
                         ^TreeSortedSet vaet
                         pull-patterns]

  ISearchable
  (-searchable? [_] true)

  IDB
  (-schema [_] (wrap-cache store :schema (s/schema store)))
  (-rschema [_] (wrap-cache store :rschema (s/rschema store)))
  (-attrs-by [db property] ((-rschema db) property))
  (-clear-tx-cache
    [db]
    (let [clear #(.clear ^TreeSortedSet %)]
      (clear eavt)
      (clear avet)
      (clear vaet)
      db))

  ISearch
  (-search
    [db pattern]
    (let [[e a v _] pattern]
      (wrap-cache
          store
          [:search e a v]
        (case-tree
          [e a (some? v)]
          [(s/fetch store (datom e a v)) ; e a v
           (s/slice store :eav (datom e a c/v0) (datom e a c/vmax)) ; e a _
           (s/slice-filter store :eav
                           (fn [^Datom d]
                             (when ((vpred v) (.-v d)) d))
                           (datom e nil nil)
                           (datom e nil nil))  ; e _ v
           (s/slice store :eav (datom e nil nil) (datom e nil nil)) ; e _ _
           (s/slice store :ave (datom e0 a v) (datom emax a v)) ; _ a v
           (s/slice store :ave (datom e0 a nil) (datom emax a nil)) ; _ a _
           (s/slice-filter store :eav
                           (fn [^Datom d]
                             (when ((vpred v) (.-v d)) d))
                           (datom e0 nil nil)
                           (datom emax nil nil)) ; _ _ v
           (s/slice store :eav (datom e0 nil nil) (datom emax nil nil))])))) ; _ _ _

  (-first
    [db pattern]
    (let [[e a v _] pattern]
      (wrap-cache
          store
          [:first e a v]
        (case-tree
          [e a (some? v)]
          [(first (s/fetch store (datom e a v))) ; e a v
           (s/head store :eav (datom e a c/v0) (datom e a c/vmax)) ; e a _
           (s/head-filter store :eav
                          (fn [^Datom d]
                            (when ((vpred v) (.-v d)) d))
                          (datom e nil nil)
                          (datom e nil nil))  ; e _ v
           (s/head store :eav (datom e nil nil) (datom e nil nil)) ; e _ _
           (s/head store :ave (datom e0 a v) (datom emax a v)) ; _ a v
           (s/head store :ave (datom e0 a nil) (datom emax a nil)) ; _ a _
           (s/head-filter store :eav
                          (fn [^Datom d]
                            (when ((vpred v) (.-v d)) d))
                          (datom e0 nil nil)
                          (datom emax nil nil)) ; _ _ v
           (s/head store :eav (datom e0 nil nil) (datom emax nil nil))])))) ; _ _ _

  (-last
    [db pattern]
    (let [[e a v _] pattern]
      (wrap-cache
          store
          [:last e a v]
        (case-tree
          [e a (some? v)]
          [(first (s/fetch store (datom e a v))) ; e a v
           (s/tail store :eav  (datom e a c/vmax) (datom e a c/v0)) ; e a _
           (s/tail-filter store :eav
                          (fn [^Datom d]
                            (when ((vpred v) (.-v d)) d))
                          (datom e nil nil)
                          (datom e nil nil))  ; e _ v
           (s/tail store :eav (datom e nil nil) (datom e nil nil)) ; e _ _
           (s/tail store :ave (datom emax a v) (datom e0 a v)) ; _ a v
           (s/tail store :ave (datom emax a nil) (datom e0 a nil)) ; _ a _
           (s/tail-filter store :eav
                          (fn [^Datom d]
                            (when ((vpred v) (.-v d)) d))
                          (datom emax nil nil)
                          (datom e0 nil nil)) ; _ _ v
           (s/tail store :eav (datom emax nil nil) (datom e0 nil nil))]))))

  (-count
    [db pattern]
    (let [[e a v _] pattern]
      (wrap-cache
          store
          [:count e a v]
        (case-tree
          [e a (some? v)]
          [(s/size store :eav (datom e a v) (datom e a v)) ; e a v
           (s/size store :eav (datom e a c/v0) (datom e a c/vmax)) ; e a _
           (s/size-filter store :eav
                          (fn [^Datom d] ((vpred v) (.-v d)))
                          (datom e nil nil)
                          (datom e nil nil))  ; e _ v
           (s/e-size store e) ; e _ _
           (s/size store :ave (datom e0 a v) (datom emax a v)) ; _ a v
           (s/a-size store a) ; _ a _
           (s/v-size store v) ; _ _ v, for ref only
           (s/datom-count store :eav)])))) ; _ _ _

  IIndexAccess
  (-populated?
    [db index c1 c2 c3]
    (wrap-cache
        store
        [:populated? index c1 c2 c3]
      (s/populated? store index
                    (components->pattern db index c1 c2 c3 e0)
                    (components->pattern db index c1 c2 c3 emax))))

  (-datoms
    [db index c1 c2 c3]
    (wrap-cache
        store
        [:datoms index c1 c2 c3]
      (s/slice store index
               (components->pattern db index c1 c2 c3 e0)
               (components->pattern db index c1 c2 c3 emax))))

  (-range-datoms
    [db index start-datom end-datom]
    (wrap-cache
        store
        [:range-datoms index start-datom end-datom]
      (s/slice store index start-datom end-datom)))

  (-first-datom
    [db index c1 c2 c3]
    (wrap-cache
        store
        [:first-datom index c1 c2 c3]
      (s/head store index
              (components->pattern db index c1 c2 c3 e0)
              (components->pattern db index c1 c2 c3 emax))))

  (-seek-datoms
    [db index c1 c2 c3]
    (wrap-cache
        store
        [:seek index c1 c2 c3]
      (s/slice store index
               (components->pattern db index c1 c2 c3 e0)
               (datom emax nil nil))))

  (-rseek-datoms
    [db index c1 c2 c3]
    (wrap-cache
        store
        [:rseek index c1 c2 c3]
      (s/rslice store index
                (components->pattern db index c1 c2 c3 emax)
                (datom e0 nil nil))))

  (-index-range
    [db attr start end]
    (wrap-cache
        store
        [attr start end]
      (do (validate-attr attr (list '-index-range 'db attr start end))
          (s/slice store :avet (resolve-datom db nil attr start e0)
                   (resolve-datom db nil attr end emax))))))

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
          target (s/last-modified store)
          cache  (.get ^ConcurrentHashMap caches (s/dir store))]
      (when (< ^long (.-target ^LRU cache) ^long target)
        (refresh-cache store target)))
    true))

;; ----------------------------------------------------------------------------

(defn- validate-schema-key [a k v expected]
  (when-not (or (nil? v)
                (contains? expected v))
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
      (when (and comp? (not= (:db/valueType kv) :db.type/ref))
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
    (when (and (= :db.type/tuple (:db/valueType kv))
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
        (when (= :db.cardinality/many (:db/cardinality kv))
          (raise a " has :db/tupleAttrs, must be :db.cardinality/one" ex-data))

        (let [attrs (:db/tupleAttrs kv)]
          (when-not (sequential? attrs)
            (raise a " :db/tupleAttrs must be a sequential collection, got: " attrs ex-data))

          (when (empty? attrs)
            (raise a " :db/tupleAttrs can’t be empty" ex-data))

          (doseq [attr attrs
                  :let [ex-data (assoc ex-data :value attr)]]
            (when (contains? (get schema attr) :db/tupleAttrs)
              (raise a " :db/tupleAttrs can’t depend on another tuple attribute: " attr ex-data))

            (when (= :db.cardinality/many (:db/cardinality (get schema attr)))
              (raise a " :db/tupleAttrs can’t depend on :db.cardinality/many attribute: " attr ex-data))))))

    (when (contains? kv :db/tupleType)
      (let [ex-data {:error     :schema/validation
                     :attribute a
                     :key       :db/tupleType}
            attr    (:db/tupleType kv)]
        (when-not (c/datalog-value-types attr)
          (raise a " :db/tupleType must be a single value type, got: " attr ex-data))
        (when (= attr :db.type/tuple)
          (raise a " :db/tupleType cannot be :db.type/tuple" ex-data))))

    (when (contains? kv :db/tupleTypes)
      (let [ex-data {:error     :schema/validation
                     :attribute a
                     :key       :db/tupleTypes}]
        (let [attrs (:db/tupleTypes kv)]
          (when-not (and (sequential? attrs) (< 1 (count attrs))
                         (every? c/datalog-value-types attrs)
                         (not (some #(= :db.type/tuple %) attrs)))
            (raise a " :db/tupleTypes must be a sequential collection of more than one value types, got: " attrs ex-data)))))

    ))

(defn- open-store
  [dir schema opts]
  (if (r/dtlv-uri? dir)
    (r/open dir schema opts)
    (s/open dir schema opts)))

(defn new-db
  [^IStore store]
  (refresh-cache store)
  (let [db (map->DB
             {:store         store
              :max-eid       (s/init-max-eid store)
              :max-tx        (s/max-tx store)
              :eavt          (TreeSortedSet. ^Comparator d/cmp-datoms-eavt)
              :avet          (TreeSortedSet. ^Comparator d/cmp-datoms-avet)
              :vaet          (TreeSortedSet. ^Comparator d/cmp-datoms-vaet)
              :pull-patterns (lru/cache 32 :constant)})]
    (swap! dbs assoc (s/db-name store) db)
    db))

(defn ^DB empty-db
  ([] (empty-db nil nil))
  ([dir] (empty-db dir nil))
  ([dir schema] (empty-db dir schema nil))
  ([dir schema opts]
   {:pre [(or (nil? schema) (map? schema))]}
   (validate-schema schema)
   (new-db (open-store dir schema opts))))

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
   (let [store (open-store dir schema opts)]
     (s/load-datoms store datoms)
     (new-db store))))

(defn close-db [^DB db]
  (let [store ^IStore (.-store db)]
    (.remove ^ConcurrentHashMap caches (s/dir store))
    (swap! dbs dissoc (s/db-name store))
    (s/close store)
    nil))

(defn db-from-reader [{:keys [schema datoms]}]
  (init-db (map (fn [[e a v tx]] (datom e a v tx)) datoms) schema))

;; ----------------------------------------------------------------------------

(declare entid-strict entid-some ref?)

(defn- resolve-datom [db e a v default-e ]
  (when a (validate-attr a (list 'resolve-datom 'db e a v default-e)))
  (datom
    (or (entid-some db e) default-e)  ;; e
    a                                 ;; a
    (if (and (some? v) (ref? db a))   ;; v
      (entid-strict db v)
      v)))

(defn- components->pattern [db index c0 c1 c2 default-e]
  (case index
    (:eav :eavt) (resolve-datom db c0 c1 c2 default-e)
    (:ave :avet) (resolve-datom db c2 c0 c1 default-e)
    (:vae :vaet) (resolve-datom db c2 c1 c0 default-e)))

;; ----------------------------------------------------------------------------

(defn is-attr?
  ^Boolean [db attr property]
  (contains? (-attrs-by db property) attr))

(defn multival? ^Boolean [db attr] (is-attr? db attr :db.cardinality/many))

(defn ref? ^Boolean [db attr] (is-attr? db attr :db.type/ref))

(defn component? ^Boolean [db attr] (is-attr? db attr :db/isComponent))

(defn tuple-attr? ^Boolean [db attr] (is-attr? db attr :db/tupleAttrs))

(defn tuple-type? ^Boolean [db attr] (is-attr? db attr :db/tupleType))

(defn tuple-types? ^Boolean [db attr] (is-attr? db attr :db/tupleTypes))

(defn tuple-source? ^Boolean [db attr] (is-attr? db attr :db/attrTuples))

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
        (not (is-attr? db attr :db/unique))
        (raise "Lookup ref attribute should be marked as :db/unique: " eid
               {:error :lookup-ref/unique, :entity-id eid})
        (nil? value)
        nil
        :else
        (or (:e (sf (.subSet ^TreeSortedSet (:avet db)
                             (datom e0 attr value tx0)
                             (datom emax attr value txmax))))
            (:e (-first-datom db :avet attr value nil)))))

    (keyword? eid)
    (or (:e (sf (.subSet ^TreeSortedSet (:avet db)
                         (datom e0 :db/ident eid tx0)
                         (datom emax :db/ident eid txmax))))
        (:e (-first-datom db :avet :db/ident eid nil)))

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

;;;;;;;;;; Transacting

(defn validate-datom [db ^Datom datom]
  (when (and (is-attr? db (.-a datom) :db/unique) (datom-added datom))
    (when-some [found (let [a (.-a datom)
                            v (.-v datom)]
                        (or (not (.isEmpty
                                   (.subSet ^TreeSortedSet (:avet db)
                                            (d/datom e0 a v tx0)
                                            (d/datom emax a v txmax))))
                            (-populated? db :avet a v nil)))]
      (raise "Cannot add " datom " because of unique constraint: " found
             {:error     :transact/unique
              :attribute (.-a datom)
              :datom     datom})))
  db)

(defn- validate-attr [attr at]
  (when-not (or (keyword? attr) (string? attr))
    (raise "Bad entity attribute " attr " at " at ", expected keyword or string"
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

(defn- tx-id?
  ^Boolean [e]
  (or (identical? :db/current-tx e)
      (.equals ":db/current-tx" e) ;; for datascript.js interop
      (.equals "datomic.tx" e)
      (.equals "datascript.tx" e)))

(defn- tempid?
  ^Boolean [x]
  (or (and (number? x) (neg? ^long x)) (string? x)))

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
         new? (new-eid? db eid)]
     (cond-> report
       (tx-id? e)
       (update :tempids assoc e eid)

       (tempid? e)
       (update :tempids assoc e eid)

       (and (not (tempid? e)) new?)
       (update :tempids assoc eid eid)

       (and (:auto-entity-time? (s/opts (.-store ^DB db))) new?)
       (update :tx-data conj (d/datom eid :db/created-at tx-time))

       true
       (update :db-after advance-max-eid eid)))))

(defn- with-datom [db ^Datom datom]
  (let [ref? (ref? db (.-a datom))
        add  #(do (.add ^TreeSortedSet % datom) %)
        del  #(do (.remove ^TreeSortedSet % datom) %)]
    (if (datom-added datom)
      (do
        (validate-datom db datom)
        (cond-> (-> db
                    (update :eavt add)
                    (update :avet add)
                    (advance-max-eid (.-e datom)))
          ref? (update :vaet add)))
      (if (.isEmpty
            (.subSet ^TreeSortedSet (:eavt db)
                     (d/datom (.-e datom) (.-a datom) (.-v datom) tx0)
                     (d/datom (.-e datom) (.-a datom) (.-v datom) txmax)))
        db
        (cond-> (-> db
                    (update :eavt del)
                    (update :avet del))
          ref? (update :vaet del))))))

(defn- queue-tuple [queue tuple idx db e a v]
  (let [tuple-value  (or (get queue tuple)
                         (:v (sf
                               (.subSet ^TreeSortedSet (:eavt db)
                                        (d/datom e tuple nil tx0)
                                        (d/datom e tuple nil txmax))))
                         (:v (-first-datom db :eavt e tuple nil))
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

(defn reverse-ref?
  ^Boolean [attr]
  (cond
    (keyword? attr)
    (= \_ (nth (name attr) 0))

    (string? attr)
    (boolean (re-matches #"(?:([^/]+)/)?_([^/]+)" attr))

    :else
    (raise "Bad attribute type: " attr ", expected keyword or string"
           {:error :transact/syntax, :attribute attr})))

(defn reverse-ref [attr]
  (cond
    (keyword? attr)
    (if (reverse-ref? attr)
      (keyword (namespace attr) (subs (name attr) 1))
      (keyword (namespace attr) (str "_" (name attr))))

    (string? attr)
    (let [[_ ns name] (re-matches #"(?:([^/]+)/)?([^/]+)" attr)]
      (if (= \_ (nth name 0))
        (if ns (str ns "/" (subs name 1)) (subs name 1))
        (if ns (str ns "/_" name) (str "_" name))))

    :else
    (raise "Bad attribute type: " attr ", expected keyword or string"
           {:error :transact/syntax, :attribute attr})))

(defn- resolve-upserts
  "Returns [entity' upserts]. Upsert attributes that resolve to existing entities
   are removed from entity, rest are kept in entity for insertion. No validation is performed.

   upserts :: {:name  {\"Ivan\"  1}
               :email {\"ivan@\" 2}
               :alias {\"abc\"   3
                       \"def\"   4}}}"
  [db entity]
  (if-some [idents (not-empty (-attrs-by db :db.unique/identity))]
    (let [resolve (fn [a v]
                    (or (:e (sf (.subSet ^TreeSortedSet (:avet db)
                                         (d/datom e0 a v tx0)
                                         (d/datom emax a v txmax))))
                        (:e (-first-datom db :avet a v nil))))

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

            (and
              (multival? db a)
              (or
                ;; (arrays/array? v)
                (and (coll? v) (not (map? v)))))
            (let [[insert upsert] (split a v)]
              [(cond-> entity'
                 (not (empty? insert)) (assoc a insert))
               (cond-> upserts
                 (not (empty? upsert)) (assoc a upsert))])

            :else
            (if-some [e (resolve a v)]
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
    (not (or (reverse-ref? a)
             (multival? db a)))
    [vs]

    ;; not a collection at all, so definitely a single value
    (not (or ;;(arrays/array? vs)
           (and (coll? vs) (not (map? vs)))))
    [vs]

    ;; probably lookup ref
    (and (= (count vs) 2)
         (is-attr? db (first vs) :db.unique/identity))
    [vs]

    :else vs))


(defn- explode [db entity]
  (let [eid  (:db/id entity)
        ;; sort tuple attrs after non-tuple
        a+vs (apply concat
                    (reduce
                      (fn [acc [a vs]]
                        (update acc (if (tuple-attr? db a) 1 0) conj [a vs]))
                      [[] []] entity))]
    (for [[a vs] a+vs
          :when  (not= a :db/id)
          :let   [_          (validate-attr a {:db/id eid, a vs})
                  reverse?   (reverse-ref? a)
                  straight-a (if reverse? (reverse-ref a) a)
                  _          (when (and reverse? (not (ref? db straight-a)))
                               (raise "Bad attribute " a ": reverse attribute name requires {:db/valueType :db.type/ref} in schema"
                                      {:error :transact/syntax, :attribute a, :context {:db/id eid, a vs}}))]
          v      (maybe-wrap-multival db a vs)]
      (if (and (ref? db straight-a) (map? v)) ;; another entity specified as nested map
        (assoc v (reverse-ref a) eid)
        (if reverse?
          [:db/add v   straight-a eid]
          [:db/add eid straight-a v])))))

(def conjv (fnil conj []))

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
              (first (s/fetch (:store db) (datom e a v))))
          (or (sf (.subSet ^TreeSortedSet (:eavt db)
                           (datom e a nil tx0)
                           (datom e a nil txmax)))
              (s/head (:store db) :eav
                      (datom e a c/v0) (datom e a c/vmax))))]
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

(defn check-value-tempids [report]
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

(defn- retry-with-tempid [initial-report report es tempid upserted-eid]
  (if (contains? (:tempids initial-report) tempid)
    (raise "Conflicting upsert: " tempid " resolves"
           " both to " upserted-eid " and " (get-in initial-report [:tempids tempid])
           { :error :transact/upsert })
    ;; try to re-run from the beginning
    ;; but remembering that `tempid` will resolve to `upserted-eid`
    (let [tempids' (-> (:tempids report)
                       (assoc tempid upserted-eid))
          report'  (assoc initial-report :tempids tempids')]
      (local-transact-tx-data report' es))))

(def builtin-fn?
  #{:db.fn/call
    :db.fn/cas
    :db/cas
    :db/add
    :db/retract
    :db.fn/retractAttribute
    :db.fn/retractEntity
    :db/retractEntity})

;; HACK to avoid circular dependency
(def de-entity? (delay (resolve 'datalevin.entity/entity?)))
(def de-entity->txs (delay (resolve 'datalevin.entity/->txs)))

(defn- update-entity-time
  [initial-es tx-time]
  (loop [es     initial-es
         new-es (transient [])]
    (let [[entity & entities] es]
      (cond
        (empty? es)
        (persistent! new-es)

        (nil? entity)
        (recur entities new-es)

        (@de-entity? entity)
        (recur (into entities (reverse (@de-entity->txs entity))) new-es)

        (map? entity)
        (recur entities (conj! new-es (assoc entity :db/updated-at tx-time)))

        (sequential? entity)
        (let [[op e _ _] entity]
          (if (or (= op :db/retractEntity)
                  (= op :db.fn/retractEntity))
            (recur entities (conj! new-es entity))
            (recur entities (-> new-es
                                (conj! entity)
                                (conj! [:db/add e :db/updated-at tx-time])))))

        (datom? entity)
        (let [e (d/datom-e entity)]
          (recur entities (-> new-es
                              (conj! entity)
                              (conj! [:db/add e :db/updated-at tx-time]))))

        :else
        (raise "Bad entity at " entity ", expected map, vector, datom or entity"
               {:error :transact/syntax, :tx-data entity})))))

(defn flush-tuples [report]
  (let [db (:db-after report)]
    (reduce-kv
      (fn [entities eid tuples+values]
        (reduce-kv
          (fn [entities tuple value]
            (let [value   (if (every? nil? value) nil value)
                  current (or (:v (sf (.subSet ^TreeSortedSet (:eavt db)
                                               (d/datom eid tuple nil tx0)
                                               (d/datom eid tuple nil txmax))))
                              (:v (-first-datom db :eavt eid tuple nil)))]
              (cond
                (= value current) entities
                (nil? value)      (conj entities ^::internal [:db/retract eid tuple current])
                :else             (conj entities ^::internal [:db/add eid tuple value]))))
          entities
          tuples+values))
      []
      (::queued-tuples report))))

(defn- local-transact-tx-data
  ([initial-report initial-es]
   (local-transact-tx-data initial-report initial-es false))
  ([initial-report initial-es simulated?]
   (let [tx-time         (System/currentTimeMillis)
         initial-report' (-> initial-report
                             (update :db-after -clear-tx-cache))
         has-tuples?     (not (empty? (-attrs-by (:db-after initial-report)
                                                 :db.type/tuple)))
         initial-es'     (cond-> initial-es
                           (:auto-entity-time?
                            (s/opts (.-store ^DB (:db-before initial-report))))
                           (update-entity-time tx-time)
                           has-tuples?
                           (interleave (repeat ::flush-tuples)))
         rp
         (loop [report initial-report'
                es     initial-es']
           (cond+
             (empty? es)
             (-> report
                 check-value-tempids
                 (update :tempids assoc :db/current-tx (current-tx report))
                 (update :db-after update :max-tx u/long-inc))

             :let [[entity & entities] es]

             (nil? entity)
             (recur report entities)

             (= ::flush-tuples entity)
             (if (contains? report ::queued-tuples)
               (recur
                 (dissoc report ::queued-tuples)
                 (concat (flush-tuples report) entities))
               (recur report entities))

             (@de-entity? entity)
             (recur report
                    (into entities (reverse (@de-entity->txs entity))))


             :let [^DB db      (:db-after report)
                   tempids (:tempids report)]

             (map? entity)
             (let [old-eid (:db/id entity)]
               (cond+
                 ;; :db/current-tx / "datomic.tx" => tx
                 (tx-id? old-eid)
                 (let [id (current-tx report)]
                   (recur (allocate-eid tx-time report old-eid id)
                          (cons (assoc entity :db/id id) entities)))

                 ;; lookup-ref => resolved | error
                 (sequential? old-eid)
                 (let [id (entid-strict db old-eid)]
                   (recur report
                          (cons (assoc entity :db/id id) entities)))

                 ;; upserted => explode | error
                 :let [[entity' upserts] (resolve-upserts db entity)
                       upserted-eid      (validate-upserts entity' upserts)]

                 (some? upserted-eid)
                 (if (and (tempid? old-eid)
                          (contains? tempids old-eid)
                          (not= upserted-eid (get tempids old-eid)))
                   (retry-with-tempid initial-report report initial-es old-eid upserted-eid)
                   (recur (-> (allocate-eid tx-time report old-eid upserted-eid)
                              (update ::tx-redundant conjv
                                      (datom upserted-eid nil nil tx0)))
                          (concat (explode db (assoc entity' :db/id upserted-eid)) entities)))

                 ;; resolved | allocated-tempid | tempid | nil => explode
                 (or (number? old-eid)
                     (nil?    old-eid)
                     (string? old-eid))
                 (let [new-eid    (cond
                                    (nil? old-eid)    (next-eid db)
                                    (tempid? old-eid) (or (get tempids old-eid)
                                                          (next-eid db))
                                    :else             old-eid)
                       new-entity (assoc entity :db/id new-eid)]
                   (recur (allocate-eid tx-time report old-eid new-eid)
                          (concat (explode db new-entity) entities)))

                 ;; trash => error
                 :else
                 (raise "Expected number, string or lookup ref for :db/id, got " old-eid
                        { :error :entity-id/syntax, :entity entity })))

             (sequential? entity)
             (let [[op e a v] entity]
               (cond
                 (= op :db.fn/call)
                 (let [[_ f & args] entity]
                   (recur report (concat (apply f db args) entities)))

                 (and (keyword? op)
                      (not (builtin-fn? op)))
                 (if-some [ident (or (:e (sf (.subSet
                                               ^TreeSortedSet (:avet db)
                                               (d/datom e0 op nil tx0)
                                               (d/datom emax op nil txmax))))
                                     (entid db op))]
                   (let [fun  (or (:v (sf (.subSet
                                            ^TreeSortedSet (:eavt db)
                                            (d/datom ident :db/fn nil tx0)
                                            (d/datom ident :db/fn nil txmax))))
                                  (:v (-first db [ident :db/fn])))
                         args (next entity)]
                     (if (fn? fun)
                       (recur report (concat (apply fun db args) entities))
                       (raise "Entity " op " expected to have :db/fn attribute with fn? value"
                              {:error :transact/syntal, :operation :db.fn/call, :tx-data entity})))
                   (raise "Can’t find entity for transaction fn " op
                          {:error :transact/syntax, :operation :db.fn/call, :tx-data entity}))

                 (and (tempid? e) (not= op :db/add))
                 (raise "Can't use tempid in '" entity "'. Tempids are allowed in :db/add only"
                        { :error :transact/syntax, :op entity })

                 (or (= op :db.fn/cas)
                     (= op :db/cas))
                 (let [[_ e a ov nv] entity
                       e             (entid-strict db e)
                       _             (validate-attr a entity)
                       ov            (if (ref? db a) (entid-strict db ov) ov)
                       nv            (if (ref? db a) (entid-strict db nv) nv)
                       _             (validate-val nv entity)
                       datoms        (clojure.set/union
                                       (s/slice (:store db) :eav
                                                (datom e a c/v0)
                                                (datom e a c/vmax))
                                       (.subSet ^TreeSortedSet (:eavt db)
                                                (datom e a nil tx0)
                                                (datom e a nil txmax)))]
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
                 (let [upserted-eid  (when (is-attr? db a :db.unique/identity)
                                       (or (:e (sf (.subSet
                                                     ^TreeSortedSet (:avet db)
                                                     (d/datom e0 a v tx0)
                                                     (d/datom emax a v txmax))))
                                           (:e (-first-datom db :avet a v nil))))
                       allocated-eid (get tempids e)]
                   (if (and upserted-eid allocated-eid (not= upserted-eid allocated-eid))
                     (retry-with-tempid initial-report report initial-es e upserted-eid)
                     (let [eid (or upserted-eid allocated-eid (next-eid db))]
                       (recur (allocate-eid tx-time report e eid) (cons [op eid a v] entities)))))

                 (and (not (::internal (meta entity)))
                      (tuple-attr? db a))
                 ;; allow transacting in tuples if they fully match already existing values
                 (let [tuple-attrs (get-in (s/schema (.-store db))
                                           [a :db/tupleAttrs])]
                   (if (and
                         (= (count tuple-attrs) (count v))
                         (every? some? v)
                         (every?
                           (fn [[tuple-attr tuple-value]]
                             (let [db-value
                                   (or (:v (sf
                                             (.subSet
                                               ^TreeSortedSet (:eavt db)
                                               (d/datom e tuple-attr nil tx0)
                                               (d/datom e tuple-attr nil txmax))))
                                       (:v (-first-datom db :eavt e tuple-attr nil)))]
                               (= tuple-value db-value)))
                           (map vector tuple-attrs v)))
                     (recur report entities)
                     (raise "Can’t modify tuple attrs directly: " entity
                            {:error :transact/syntax, :tx-data entity})))

                 (and (not (::internal (meta entity)))
                      (or (tuple-type? db a) (tuple-types? db a))
                      (let [schema      (s/schema (.-store db))
                            tuple-types (or (get-in schema [a :db/tupleTypes])
                                            (repeat (get-in schema [a :db/tupleType])))]
                        (some #(and (identical? (first %) :db.type/ref)
                                    (tempid? (second %)))
                              (partition 2 (interleave tuple-types v)))))
                 (let [schema       (s/schema (.-store db))
                       tuple-types  (or (get-in schema [a :db/tupleTypes])
                                        (repeat (get-in schema [a :db/tupleType])))
                       [report' v'] (loop [report' report
                                           vs      (partition 2 (interleave tuple-types v))
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


                 (= op :db/add)
                 (recur (transact-add report entity) entities)

                 (and (= op :db/retract) (some? v))
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

                 (or (= op :db.fn/retractAttribute)
                     (= op :db/retract))
                 (if-some [e (entid db e)]
                   (let [_      (validate-attr a entity)
                         datoms (u/concatv
                                  (.subSet ^TreeSortedSet (:eavt db)
                                           (datom e a nil tx0)
                                           (datom e a nil txmax))
                                  (s/slice (:store db) :eav
                                           (datom e a c/v0)
                                           (datom e a c/vmax)))]
                     (recur (reduce transact-retract-datom report datoms)
                            (concat (retract-components db datoms) entities)))
                   (recur report entities))

                 (or (= op :db.fn/retractEntity)
                     (= op :db/retractEntity))
                 (if-some [e (entid db e)]
                   (let [e-datoms (u/concatv
                                    (.subSet ^TreeSortedSet (:eavt db)
                                             (datom e nil nil tx0)
                                             (datom e nil nil txmax))
                                    (s/slice (:store db) :eav
                                             (datom e nil nil)
                                             (datom e nil nil)))
                         v-datoms (u/concatv
                                    (.subSet ^TreeSortedSet (:vaet db)
                                             (datom e0 nil e tx0)
                                             (datom emax nil e txmax))
                                    (s/slice (:store db) :vae
                                             (datom e0 nil e)
                                             (datom emax nil e)))]
                     (recur (reduce transact-retract-datom report
                                    (u/concatv e-datoms v-datoms))
                            (concat (retract-components db e-datoms) entities)))
                   (recur report entities))

                 :else
                 (raise "Unknown operation at " entity ", expected :db/add, :db/retract, :db.fn/call, :db.fn/retractAttribute, :db.fn/retractEntity or an ident corresponding to an installed transaction function (e.g. {:db/ident <keyword> :db/fn <Ifn>}, usage of :db/ident requires {:db/unique :db.unique/identity} in schema)" {:error :transact/syntax, :operation op, :tx-data entity})))

             (datom? entity)
             (let [[e a v tx added] entity]
               (if added
                 (recur (transact-add report [:db/add e a v tx]) entities)
                 (recur report (cons [:db/retract e a v] entities))))

             :else
             (raise "Bad entity type at " entity ", expected map, vector, or datom"
                    {:error :transact/syntax, :tx-data entity})
             ))
         pstore (.-store ^DB (:db-after rp))]
     (when-not simulated?
       (s/load-datoms pstore (:tx-data rp))
       (refresh-cache pstore))
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

(defn transact-tx-data
  ([initial-report initial-es simulated?]
   (when-not (or (nil? initial-es)
                 (sequential? initial-es))
     (raise "Bad transaction data " initial-es ", expected sequential collection"
            {:error :transact/syntax, :tx-data initial-es}))
   (let [^DB db (:db-before initial-report)
         store  (.-store db)]
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
             (local-transact-tx-data initial-report initial-es simulated?))))
       (local-transact-tx-data initial-report initial-es simulated?)))))

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
      (l/abort-transact-kv (.-lmdb ^Store s)))))
