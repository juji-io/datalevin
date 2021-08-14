(ns ^:no-doc ^:lean-ns datalevin.db
  (:require
   [clojure.walk]
   [clojure.data]
   [clojure.set]
   [me.tonsky.persistent-sorted-set :as set]
   [datalevin.constants :as c :refer [e0 tx0 emax txmax implicit-schema]]
   [datalevin.lru :as lru]
   [datalevin.datom :as d
    :refer [datom datom-tx datom-added datom?]]
   [datalevin.util
    :refer [combine-hashes case-tree raise defrecord-updatable cond+]]
   [datalevin.storage :as s]
   [datalevin.bits :as b])
  #?(:cljs
     (:require-macros [datalevin.util
                       :refer [case-tree raise defrecord-updatable cond+]]))
  #?(:clj
     (:import [datalevin.datom Datom]
              [datalevin.storage Store]
              [datalevin.lru LRU]
              [datalevin.bits Retrieved]
              [java.util.concurrent ConcurrentHashMap])))


;;;;;;;;;; Searching

(defprotocol ISearch
  (-search [data pattern])
  (-count [data pattern])
  (-first [data pattern]))

(defprotocol IIndexAccess
  (-populated? [db index components])
  (-datoms [db index components])
  (-seek-datoms [db index components])
  (-rseek-datoms [db index components])
  (-index-range [db attr start end]))

(defprotocol IDB
  (-schema [db])
  (-attrs-by [db property]))

;; ----------------------------------------------------------------------------

(declare empty-db resolve-datom validate-attr components->pattern)
#?(:cljs (declare pr-db))

(defrecord TxReport [db-before db-after tx-data tempids tx-meta])

#?(:clj
   (defmethod print-method TxReport [^TxReport rp, ^java.io.Writer w]
     (binding [*out* w]
       (pr {:datoms-transacted (count (:tx-data rp))}))))

(defn db-transient [db]
  (-> db
      (assoc :eavt (set/sorted-set-by d/cmp-datoms-eavt))
      (assoc :avet (set/sorted-set-by d/cmp-datoms-avet))
      (assoc :veat (set/sorted-set-by d/cmp-datoms-veat))
      (update :eavt transient)
      (update :avet transient)
      (update :veat transient)))

(defn db-persistent! [db]
  (-> db
      (update :eavt persistent!)
      (update :avet persistent!)
      (update :veat persistent!)))

(defprotocol Searchable
  (-searchable? [_]))

(extend-type #?(:clj Object :cljs object)
  Searchable
  (-searchable? [_] false))

(extend-type nil
  Searchable
  (-searchable? [_] false))

(def ^:private caches (ConcurrentHashMap.))

(defmacro wrap-cache
  [store pattern body]
  `(let [cache# (.get ^ConcurrentHashMap caches ~store)]
     (if-some [cached# (get ^LRU cache# ~pattern nil)]
       cached#
       (let [res# ~body]
         (.put ^ConcurrentHashMap caches ~store (assoc cache# ~pattern res#))
         res#))))

(defrecord-updatable DB [^Store store eavt avet veat
                         max-eid max-tx schema rschema hash]

  clojure.lang.IEditableCollection
  (empty [db]         (with-meta (empty-db (s/dir store) schema) (meta db)))
  (asTransient [db] (db-transient db))

  clojure.lang.ITransientCollection
  (conj [db key] (throw (ex-info "datalevin.DB/conj! is not supported" {})))
  (persistent [db] (db-persistent! db))

  Searchable
  (-searchable? [_] true)

  IDB
  (-schema [_] (s/schema store))
  (-attrs-by [_ property] (rschema property))

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
                           (fn [^Datom d] (= v (.-v d)))
                           (datom e nil nil)
                           (datom e nil nil))  ; e _ v
           (s/slice store :eav (datom e nil nil) (datom e nil nil)) ; e _ _
           (s/slice store :ave (datom e0 a v) (datom emax a v)) ; _ a v
           (s/slice store :ave (datom e0 a nil) (datom emax a nil)) ; _ a _
           (s/slice store :vea (datom e0 nil v) (datom emax nil v)) ; _ _ v
           (s/slice store :eav (datom e0 nil nil) (datom emax nil nil))])))) ; _ _ _

  (-first
    [db pattern]
    (let [[e a v _] pattern]
      (wrap-cache
        store
        [:first e a v]
        (case-tree
          [e a (some? v)]
          [(s/fetch store (datom e a v)) ; e a v
           (s/head store :eav (datom e a c/v0) (datom e a c/vmax)) ; e a _
           (s/head-filter store :eav
                          (fn [^Datom d] (= v (.-v d)))
                          (datom e nil nil)
                          (datom e nil nil))  ; e _ v
           (s/head store :eav (datom e nil nil) (datom e nil nil)) ; e _ _
           (s/head store :ave (datom e0 a v) (datom emax a v)) ; _ a v
           (s/head store :ave (datom e0 a nil) (datom emax a nil)) ; _ a _
           (s/head store :vea (datom e0 nil v) (datom emax nil v)) ; _ _ v
           (s/head store :eav (datom e0 nil nil) (datom emax nil nil))])))) ; _ _ _

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
                          (fn [^Datom d] (= v (.-v d)))
                          (datom e nil nil)
                          (datom e nil nil))  ; e _ v
           (s/size store :eav (datom e nil nil) (datom e nil nil)) ; e _ _
           (s/size store :ave (datom e0 a v) (datom emax a v)) ; _ a v
           (s/size store :ave (datom e0 a nil) (datom emax a nil)) ; _ a _
           (s/size store :vea (datom e0 nil v) (datom emax nil v)) ; _ _ v
           (s/datom-count store :eav)])))) ; _ _ _

  IIndexAccess
  (-populated?
    [db index cs]
    (wrap-cache
      store
      [:populated? index cs]
      (s/populated? store index (components->pattern db index cs e0 tx0)
                    (components->pattern db index cs emax txmax))))

  (-datoms
    [db index cs]
    (wrap-cache
      store
      [:datoms index cs]
      (s/slice store index (components->pattern db index cs e0 tx0)
               (components->pattern db index cs emax txmax))))

  (-seek-datoms
    [db index cs]
    (wrap-cache
      store
      [:seek index cs]
      (s/slice store index (components->pattern db index cs e0 tx0)
               (datom emax nil nil txmax))))

  (-rseek-datoms
    [db index cs]
    (wrap-cache
      store
      [:rseek index cs]
      (s/rslice store index (components->pattern db index cs emax txmax)
                (datom e0 nil nil tx0))))

  (-index-range
    [db attr start end]
    (wrap-cache
      store
      [attr start end]
      (do (validate-attr attr (list '-index-range 'db attr start end))
          (s/slice store :avet (resolve-datom db nil attr start nil e0 tx0)
                   (resolve-datom db nil attr end nil emax txmax)))))

  clojure.data/EqualityPartition
  (equality-partition [x] :datalevin/db))

(defn db? [x] (-searchable? x))

;; ----------------------------------------------------------------------------

(defn attr->properties [k v]
  (case v
    :db.unique/identity  [:db/unique :db.unique/identity]
    :db.unique/value     [:db/unique :db.unique/value]
    :db.cardinality/many [:db.cardinality/many]
    :db.type/ref         [:db.type/ref]
    (when (true? v)
      (case k
        :db/isComponent [:db/isComponent]
        []))))

(defn rschema [schema]
  (reduce-kv
   (fn [m attr keys->values]
     (reduce-kv
      (fn [m key value]
        (reduce
         (fn [m prop]
           (assoc m prop (conj (get m prop #{}) attr)))
         m (attr->properties key value)))
      m keys->values))
   {} schema))

(defn- validate-schema-key [a k v expected]
  (when-not (or (nil? v)
                (contains? expected v))
    (throw (ex-info (str "Bad attribute specification for " (pr-str {a {k v}}) ", expected one of " expected)
                    {:error     :schema/validation
                     :attribute a
                     :key       k
                     :value     v}))))

(defn- validate-schema [schema]
  (doseq [[a kv] schema]
    (let [comp? (:db/isComponent kv false)]
      (validate-schema-key a :db/isComponent (:db/isComponent kv) #{true false})
      (when (and comp? (not= (:db/valueType kv) :db.type/ref))
        (throw (ex-info (str "Bad attribute specification for " a ": {:db/isComponent true} should also have {:db/valueType :db.type/ref}")
                        {:error     :schema/validation
                         :attribute a
                         :key       :db/isComponent}))))
    (validate-schema-key a :db/unique (:db/unique kv) #{:db.unique/value :db.unique/identity})
    (validate-schema-key a :db/valueType (:db/valueType kv)
                         c/datalog-value-types)
    (validate-schema-key a :db/cardinality (:db/cardinality kv) #{:db.cardinality/one :db.cardinality/many})))

(defn- open-store
  [dir schema]
  {:pre [(or (nil? schema) (map? schema))]}
  (validate-schema schema)
  (s/open dir schema))

(defn- new-db
  [store]
  (let [schema (s/schema store)]
    (.put ^ConcurrentHashMap caches store (lru/lru c/+cache-limit+))
    (map->DB
      {:store   store
       :schema  schema
       :rschema (rschema (merge implicit-schema schema))
       :eavt    (set/sorted-set-by d/cmp-datoms-eavt)
       :avet    (set/sorted-set-by d/cmp-datoms-avet)
       :veat    (set/sorted-set-by d/cmp-datoms-veat)
       :max-eid (s/init-max-eid store)
       :max-tx  tx0
       :hash    (atom 0)})))

(defn ^DB empty-db
  ([] (empty-db nil nil))
  ([dir] (empty-db dir nil))
  ([dir schema]
   {:pre [(or (nil? schema) (map? schema))]}
   (validate-schema schema)
   (new-db (open-store dir schema))))

(defn ^DB init-db
  ([datoms] (init-db datoms nil nil))
  ([datoms dir] (init-db datoms dir nil))
  ([datoms dir schema]
   (let [store (open-store dir schema)]
     (s/load-datoms store datoms)
     (new-db store))))

(defn close-db [^DB db]
  (s/close ^Store (.-store db))
  nil)

(defn db-from-reader [{:keys [schema datoms]}]
  (init-db (map (fn [[e a v tx]] (datom e a v tx)) datoms) schema))

;; ----------------------------------------------------------------------------

(declare entid-strict entid-some ref?)

(defn- resolve-datom [db e a v t default-e default-tx]
  (when a (validate-attr a (list 'resolve-datom 'db e a v t)))
  (datom
   (or (entid-some db e) default-e)  ;; e
   a                                 ;; a
   (if (and (some? v) (ref? db a))   ;; v
     (entid-strict db v)
     v)
   (or (entid-some db t) default-tx))) ;; t

(defn- components->pattern [db index [c0 c1 c2 c3] default-e default-tx]
  (case index
    :eavt (resolve-datom db c0 c1 c2 c3 default-e default-tx)
    :eav  (resolve-datom db c0 c1 c2 c3 default-e default-tx)
    :avet (resolve-datom db c2 c0 c1 c3 default-e default-tx)
    :ave  (resolve-datom db c2 c0 c1 c3 default-e default-tx)
    :veat (resolve-datom db c2 c1 c0 c3 default-e default-tx)
    :vea  (resolve-datom db c2 c1 c0 c3 default-e default-tx)))

;; ----------------------------------------------------------------------------

(defn #?@(:clj  [^Boolean is-attr?]
          :cljs [^boolean is-attr?]) [db attr property]
  (contains? (-attrs-by db property) attr))

(defn #?@(:clj  [^Boolean multival?]
          :cljs [^boolean multival?]) [db attr]
  (is-attr? db attr :db.cardinality/many))

(defn #?@(:clj  [^Boolean ref?]
          :cljs [^boolean ref?]) [db attr]
  (is-attr? db attr :db.type/ref))

(defn #?@(:clj  [^Boolean component?]
          :cljs [^boolean component?]) [db attr]
  (is-attr? db attr :db/isComponent))

(defn entid [db eid]
  (cond
    (and (integer? eid) (not (neg? (long eid))))
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
        (or (-> (set/slice (get db :avet)
                           (datom e0 attr value tx0)
                           (datom emax attr value txmax))
                first :e)
            (-> (-datoms db :avet eid) first :e))))

    #?@(:cljs [(array? eid) (recur db (array-seq eid))])

    (keyword? eid)
    (or (-> (set/slice (get db :avet)
                       (datom e0 :db/ident eid tx0)
                       (datom emax :db/ident eid txmax))
            first :e)
        (-> (-datoms db :avet [:db/ident eid]) first :e))

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
                        (or
                         (not-empty (set/slice (get db :avet)
                                               (d/datom e0 a v tx0)
                                               (d/datom emax a v txmax)))
                         (not-empty (-datoms db :avet [a v]))))]
      (raise "Cannot add " datom " because of unique constraint: " found
             {:error     :transact/unique
              :attribute (.-a datom)
              :datom     datom})))
  db)

(defn- validate-eid [eid at]
  (when-not (number? eid)
    (raise "Bad entity id " eid " at " at ", expected number"
           {:error :transact/syntax, :entity-id eid, :context at})))

(defn- validate-attr [attr at]
  (when-not (or (keyword? attr) (string? attr))
    (raise "Bad entity attribute " attr " at " at ", expected keyword or string"
           {:error :transact/syntax, :attribute attr, :context at})))

(defn- validate-val [v at]
  (when (nil? v)
    (raise "Cannot store nil as a value at " at
           {:error :transact/syntax, :value v, :context at})))

(defn- current-tx [report]
  (inc ^long (get-in report [:db-before :max-tx])))

(defn- next-eid [db]
  (inc ^long (:max-eid db)))

(defn- #?@(:clj  [^Boolean tx-id?]
           :cljs [^boolean tx-id?])
  [e]
  (or (= e :db/current-tx)
      (= e ":db/current-tx") ;; for datalevin.js interop
      (= e "datomic.tx")
      (= e "datalevin.tx")))

(defn- #?@(:clj  [^Boolean tempid?]
           :cljs [^boolean tempid?])
  [x]
  (or (and (number? x) (neg? ^long x)) (string? x)))

(defn- new-eid? [db ^long eid]
  (and (> eid ^long (:max-eid db))
       (< eid tx0))) ;; tx0 is max eid

(defn- advance-max-eid [db eid]
  (cond-> db
    (new-eid? db eid)
    (assoc :max-eid eid)))

(defn- allocate-eid
  ([report eid]
   (update report :db-after advance-max-eid eid))
  ([report e eid]
   (cond-> report
     (tx-id? e)
     (assoc-in [:tempids e] eid)
     (tempid? e)
     (assoc-in [:tempids e] eid)
     (and (not (tempid? e))
          (new-eid? (:db-after report) eid))
     (assoc-in [:tempids eid] eid)
     true
     (update :db-after advance-max-eid eid))))

;; In context of `with-datom` we can use faster comparators which
;; do not check for nil (~10-15% performance gain in `transact`)

(defn- with-datom [db ^Datom datom]
  (let [ref? (ref? db (.-a datom))]
    (if (datom-added datom)
      (do
        (validate-datom db datom)
        (cond-> db
          true (update :eavt set/conj datom d/cmp-datoms-eavt-quick)
          true (update :avet set/conj datom d/cmp-datoms-avet-quick)
          ref? (update :veat set/conj datom d/cmp-datoms-veat-quick)
          true (advance-max-eid (.-e datom))
          true (assoc :hash (atom 0))))
      (if-some [removing (first
                           (set/slice
                             (get db :eavt)
                             (d/datom (.-e datom) (.-a datom) (.-v datom) tx0)
                             (d/datom (.-e datom) (.-a datom) (.-v datom) txmax)))]
        (cond-> db
          true (update :eavt set/disj datom d/cmp-datoms-eavt-quick)
          true (update :avet set/disj datom d/cmp-datoms-avet-quick)
          ref? (update :veat set/conj datom d/cmp-datoms-veat-quick)
          true (assoc :hash (atom 0)))
        db))))

(defn- transact-report [report datom]
  (-> report
      (update :db-after with-datom datom)
      (update :tx-data conj datom)))

(defn #?@(:clj  [^Boolean reverse-ref?]
          :cljs [^boolean reverse-ref?]) [attr]
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


(defn- check-upsert-conflict [entity acc]
  (let [[e a v] acc
        _e      (:db/id entity)]
    (if (or (nil? _e)
            (tempid? _e)
            (nil? acc)
            (== ^long _e ^long e))
      acc
      (raise "Conflicting upsert: " [a v] " resolves to " e
             ", but entity already has :db/id " _e
             { :error    :transact/upsert
              :entity    entity
              :assertion acc }))))

(defn- upsert-reduce-fn [db eav a v]
  (let [e (or (:e (first (set/slice (get db :avet)
                                    (d/datom e0 a v tx0)
                                    (d/datom emax a v txmax))))
              (:e (first (-datoms db :avet [a v]))))]
    (cond
      (nil? e) ;; value not yet in db
      eav

      (nil? eav) ;; first upsert
      [e a v]

      (= (get eav 0) e) ;; second+ upsert, but does not conflict
      eav

      :else
      (let [[_e _a _v] eav]
        (raise "Conflicting upserts: " [_a _v] " resolves to " _e
               ", but " [a v] " resolves to " e
               { :error    :transact/upsert
                :assertion [e a v]
                :conflict  [_e _a _v] })))))

(defn- upsert-eid [db entity]
  (when-some [idents (not-empty (-attrs-by db :db.unique/identity))]
    (->>
     (reduce-kv
      (fn [eav a v] ;; eav = [e a v]
        (cond
          (not (contains? idents a))
          eav

          (and
           (multival? db a)
           (and (coll? v) (not (map? v))))
          (reduce #(upsert-reduce-fn db %1 a %2) eav v)

          :else
          (upsert-reduce-fn db eav a v)))
      nil
      entity)
     (check-upsert-conflict entity)
     first))) ;; getting eid from eav


;; multivals/reverse can be specified as coll or as a single value, trying to guess
(defn- maybe-wrap-multival [db a vs]
  (cond
    ;; not a multival context
    (not (or (reverse-ref? a)
             (multival? db a)))
    [vs]

    ;; not a collection at all, so definitely a single value
    (not (and (coll? vs) (not (map? vs))))
    [vs]

    ;; probably lookup ref
    (and (= (count vs) 2)
         (is-attr? db (first vs) :db.unique/identity))
    [vs]

    :else vs))


(defn- explode [db entity]
  (let [eid (:db/id entity)]
    (for [[a vs] entity
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

(defn- transact-add [report [_ e a v tx :as ent]]
  (validate-attr a ent)
  (validate-val  v ent)
  (let [tx        (or tx (current-tx report))
        db        (:db-after report)
        e         (entid-strict db e)
        v         (if (ref? db a) (entid-strict db v) v)
        new-datom (datom e a v tx)]
    (if (multival? db a)
      (if (empty? (or (set/slice (get db :eavt)
                                 (datom e a v tx0)
                                 (datom e a v txmax))
                      (-search db [e a v])))
        (transact-report report new-datom)
        report)
      (if-some [^Datom old-datom (or (first (set/slice (get db :eavt)
                                                       (datom e a nil tx0)
                                                       (datom e a nil txmax)))
                                     (first (-search db [e a])))]
        (if (= (.-v old-datom) v)
          report
          (-> report
              (transact-report (datom e a (.-v old-datom) tx false))
              (transact-report new-datom)))
        (transact-report report new-datom)))))

(defn- transact-retract-datom [report ^Datom d]
  (let [tx (current-tx report)]
    (transact-report report (datom (.-e d) (.-a d) (.-v d) tx false))))

(defn- retract-components [db datoms]
  (into #{} (comp
             (filter (fn [^Datom d] (component? db (.-a d))))
             (map (fn [^Datom d] [:db.fn/retractEntity (.-v d)]))) datoms))

(declare transact-tx-data)

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
      (transact-tx-data report' es))))

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
(def de-entity? (delay (resolve 'datalevin.impl.entity/entity?)))
(def de-entity->txs (delay (resolve 'datalevin.impl.entity/->txs)))

(defn transact-tx-data [initial-report initial-es]
  (when-not (or (nil? initial-es)
                (sequential? initial-es))
    (raise "Bad transaction data " initial-es ", expected sequential collection"
           {:error :transact/syntax, :tx-data initial-es}))
  (let [rp (loop [report (-> initial-report
                             (update :db-after transient))
                  es     initial-es]
             (let [[entity & entities] es
                   db                  (:db-after report)
                   {:keys [tempids]}   report]
               (cond
                 (empty? es)
                 (-> report
                     (assoc-in  [:tempids :db/current-tx] (current-tx report))
                     (update-in [:db-after :max-tx] inc)
                     (update :db-after persistent!))

                 (nil? entity)
                 (recur report entities)

                 (@de-entity? entity)
                 (recur report
                        (into entities (reverse (@de-entity->txs entity))))

                 (map? entity)
                 (let [old-eid (:db/id entity)]
                   (cond+
                     ;; :db/current-tx / "datomic.tx" => tx
                     (tx-id? old-eid)
                     (let [id (current-tx report)]
                       (recur (allocate-eid report old-eid id)
                              (cons (assoc entity :db/id id) entities)))

                     ;; lookup-ref => resolved | error
                     (sequential? old-eid)
                     (let [id (entid-strict db old-eid)]
                       (recur report
                              (cons (assoc entity :db/id id) entities)))

                     ;; upserted => explode | error
                     :let [upserted-eid (upsert-eid db entity)]

                     (some? upserted-eid)
                     (if (and (tempid? old-eid)
                              (contains? tempids old-eid)
                              (not= upserted-eid (get tempids old-eid)))
                       (retry-with-tempid initial-report report initial-es old-eid upserted-eid)
                       (recur (allocate-eid report old-eid upserted-eid)
                              (concat (explode db (assoc entity :db/id upserted-eid)) entities)))

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
                       (recur (allocate-eid report old-eid new-eid)
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
                     (if-some [ident (or (:e
                                          (first
                                            (set/slice
                                              (get db :avet)
                                              (d/datom e0 op nil tx0)
                                              (d/datom emax op nil txmax))))
                                         (entid db op))]
                       (let [fun  (or (-> (set/slice
                                            (get db :eavt)
                                            (d/datom ident :db/fn nil tx0)
                                            (d/datom ident :db/fn nil txmax))
                                          first :v)
                                      (-> (-search db [ident :db/fn]) first :v))
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
                                           (set/slice
                                             (get db :eavt)
                                             (datom e a nil tx0)
                                             (datom e a nil txmax))
                                           (-search db [e a]))]
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
                     (recur (allocate-eid report e (current-tx report)) (cons [op (current-tx report) a v] entities))

                     (and (ref? db a) (tx-id? v))
                     (recur (allocate-eid report v (current-tx report)) (cons [op e a (current-tx report)] entities))

                     (and (ref? db a) (tempid? v))
                     (if-some [vid (get tempids v)]
                       (recur report (cons [op e a vid] entities))
                       (recur (allocate-eid report v (next-eid db)) es))

                     (tempid? e)
                     (let [upserted-eid  (when (is-attr? db a :db.unique/identity)
                                           (or (:e
                                                (first
                                                  (set/slice
                                                    (get db :avet)
                                                    (d/datom e0 a v tx0)
                                                    (d/datom emax a v txmax))))
                                               (:e (first
                                                     (-datoms db :avet [a v])))))
                           allocated-eid (get tempids e)]
                       (if (and upserted-eid allocated-eid (not= upserted-eid allocated-eid))
                         (retry-with-tempid initial-report report initial-es e upserted-eid)
                         (let [eid (or upserted-eid allocated-eid (next-eid db))]
                           (recur (allocate-eid report e eid) (cons [op eid a v] entities)))))

                     (= op :db/add)
                     (recur (transact-add report entity) entities)

                     (and (= op :db/retract) v)
                     (if-some [e (entid db e)]
                       (let [v (if (ref? db a) (entid-strict db v) v)]
                         (validate-attr a entity)
                         (validate-val v entity)
                         (if-some [old-datom (or
                                               (first (set/slice
                                                        (get db :eavt)
                                                        (datom e a v tx0)
                                                        (datom e a v txmax)))
                                               (first (-first db [e a v])))]
                           (recur (transact-retract-datom report old-datom) entities)
                           (recur report entities)))
                       (recur report entities))

                     (or (= op :db.fn/retractAttribute)
                         (= op :db/retract))
                     (if-some [e (entid db e)]
                       (let [_      (validate-attr a entity)
                             datoms (vec
                                      (concat
                                        (set/slice (get db :eavt)
                                                   (datom e a nil tx0)
                                                   (datom e a nil txmax))
                                        (-search db [e a])))]
                         (recur (reduce transact-retract-datom report datoms)
                                (concat (retract-components db datoms) entities)))
                       (recur report entities))

                     (or (= op :db.fn/retractEntity)
                         (= op :db/retractEntity))
                     (if-some [e (entid db e)]
                       (let [e-datoms (vec
                                        (concat
                                          (set/slice (get db :eavt)
                                                     (datom e nil nil tx0)
                                                     (datom e nil nil txmax))
                                          (-search db [e])))
                             v-datoms (vec
                                        (concat
                                          (set/slice (get db :veat)
                                                     (datom e0 nil e tx0)
                                                     (datom emax nil e txmax))
                                          (-search db [nil nil e])))]
                         (recur (reduce transact-retract-datom report (concat e-datoms v-datoms))
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
                 (raise "Bad entity type at " entity ", expected map or vector"
                        {:error :transact/syntax, :tx-data entity}))))]
    (s/load-datoms (.-store ^DB (:db-after rp)) (:tx-data rp))
    (.put ^ConcurrentHashMap caches
          (.-store ^DB (:db-after rp)) (lru/lru c/+cache-limit+))
    rp))
