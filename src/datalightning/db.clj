(ns datalightning.db
  "Port datascript.db"
  (:require [datalightning.index :as idx]
            [datascript.db :as d]
            [me.tonsky.persistent-sorted-set.arrays :as arrays])
  (:import [datascript.db Datom]))

;; TODO, send upstream PR to make some of these public so I don't have to include them

(defn- equiv-db-index [x y]
  (loop [xs (seq x)
         ys (seq y)]
    (cond
      (nil? xs)                 (nil? ys)
      (= (first xs) (first ys)) (recur (next xs) (next ys))
      :else                     false)))

(defn- hash-db [^DB db]
  (let [h @(.-hash db)]
    (if (zero? h)
      (reset! (.-hash db) (d/combine-hashes (hash (.-schema db))
                                            (hash (.-eavt db))))
      h)))

(defn- diff-sorted [a b cmp]
  (loop [only-a []
         only-b []
         both   []
         a      a
         b      b]
    (cond
      (empty? a) [(not-empty only-a) (not-empty (into only-b b)) (not-empty both)]
      (empty? b) [(not-empty (into only-a a)) (not-empty only-b) (not-empty both)]
      :else
      (let [first-a (first a)
            first-b (first b)
            diff    (cmp first-a first-b)]
        (cond
          (== diff 0) (recur only-a                only-b                (conj both first-a) (next a) (next b))
          (< diff 0)  (recur (conj only-a first-a) only-b                both                (next a) b)
          (> diff 0)  (recur only-a                (conj only-b first-b) both                a        (next b)))))))

(defn- equiv-db [db other]
  (and (or (instance? datascript.db.DB other)
           (instance? datascript.db.FilteredDB other))
       (= (d/-schema db) (d/-schema other))
       (equiv-db-index (d/-datoms db :eavt []) (d/-datoms other :eavt []))))

(defn- validate-eid [eid at]
  (when-not (number? eid)
    (d/raise "Bad entity id " eid " at " at ", expected number"
           {:error :transact/syntax, :entity-id eid, :context at})))

(defn- validate-attr [attr at]
  (when-not (or (keyword? attr) (string? attr))
    (d/raise "Bad entity attribute " attr " at " at ", expected keyword or string"
           {:error :transact/syntax, :attribute attr, :context at})))

(defn- validate-val [v at]
  (when (nil? v)
    (d/raise "Cannot store nil as a value at " at
           {:error :transact/syntax, :value v, :context at})))

;; ----------------------------------------------------------------------------

(declare entid-strict entid-some ref?)

(defn- resolve-datom [db e a v t default-e default-tx]
  (when a (validate-attr a (list 'resolve-datom 'db e a v t)))
  (d/datom
   (or (entid-some db e) default-e)  ;; e
   a                                 ;; a
   (if (and (some? v) (ref? db a))   ;; v
     (entid-strict db v)
     v)
   (or (entid-some db t) default-tx))) ;; t

(defn- components->pattern [db index [c0 c1 c2 c3] default-e default-tx]
  (case index
    :eavt (resolve-datom db c0 c1 c2 c3 default-e default-tx)
    :aevt (resolve-datom db c1 c0 c2 c3 default-e default-tx)
    :avet (resolve-datom db c2 c0 c1 c3 default-e default-tx)))

;; ----------------------------------------------------------------------------

(declare empty-db)

(d/defrecord-updatable DB
  [schema
   lmdb
   eavt
   aevt
   avet
   datoms
   max-eid
   max-tx
   rschema
   hash]
  Object               (hashCode [db]      (hash-db db))
  clojure.lang.IHashEq (hasheq [db]        (hash-db db))
  clojure.lang.Seqable (seq [db]           (seq eavt))
  clojure.lang.IPersistentCollection
  (count [db]         (count eavt))
  (equiv [db other]   (equiv-db db other))
  clojure.lang.IEditableCollection
  (empty [db]         (with-meta (empty-db schema) (meta db)))
  (asTransient [db] (d/db-transient db))
  clojure.lang.ITransientCollection
  (conj [db key] (throw (ex-info "datalightning.DB/conj! is not supported" {})))
  (persistent [db] (d/db-persistent! db))

  d/IDB
  (-schema [db] (.-schema db))
  (-attrs-by [db property] ((.-rschema db) property))

  d/ISearch
  (-search [db pattern]
           (let [[e a v tx] pattern
                 eavt       (.-eavt db)
                 aevt       (.-aevt db)
                 avet       (.-avet db)]
             (d/case-tree [e a (some? v) tx]
                        [(idx/slice eavt (d/datom e a v tx) (d/datom e a v tx))                   ;; e a v tx
                         (idx/slice eavt (d/datom e a v d/tx0) (d/datom e a v d/txmax))               ;; e a v _
                         (->> (idx/slice eavt (d/datom e a nil d/tx0) (d/datom e a nil d/txmax))      ;; e a _ tx
                              (filter (fn [^Datom d] (= tx (d/datom-tx d)))))
                         (idx/slice eavt (d/datom e a nil d/tx0) (d/datom e a nil d/txmax))           ;; e a _ _
                         (->> (idx/slice eavt (d/datom e nil nil d/tx0) (d/datom e nil nil d/txmax))  ;; e _ v tx
                              (filter (fn [^Datom d] (and (= v (.-v d))
                                                         (= tx (d/datom-tx d))))))
                         (->> (idx/slice eavt (d/datom e nil nil d/tx0) (d/datom e nil nil d/txmax))  ;; e _ v _
                              (filter (fn [^Datom d] (= v (.-v d)))))
                         (->> (idx/slice eavt (d/datom e nil nil d/tx0) (d/datom e nil nil d/txmax))  ;; e _ _ tx
                              (filter (fn [^Datom d] (= tx (d/datom-tx d)))))
                         (idx/slice eavt (d/datom e nil nil d/tx0) (d/datom e nil nil d/txmax))       ;; e _ _ _
                         (if (d/indexing? db a)                                                   ;; _ a v tx
                           (->> (idx/slice avet (d/datom d/e0 a v d/tx0) (d/datom d/emax a v d/txmax))
                                (filter (fn [^Datom d] (= tx (d/datom-tx d)))))
                           (->> (idx/slice aevt (d/datom d/e0 a nil d/tx0) (d/datom d/emax a nil d/txmax))
                                (filter (fn [^Datom d] (and (= v (.-v d))
                                                           (= tx (d/datom-tx d)))))))
                         (if (d/indexing? db a)                                                   ;; _ a v _
                           (idx/slice avet (d/datom d/e0 a v d/tx0) (d/datom d/emax a v d/txmax))
                           (->> (idx/slice aevt (d/datom d/e0 a nil d/tx0) (d/datom d/emax a nil d/txmax))
                                (filter (fn [^Datom d] (= v (.-v d))))))
                         (->> (idx/slice aevt (d/datom d/e0 a nil d/tx0) (d/datom d/emax a nil d/txmax))  ;; _ a _ tx
                              (filter (fn [^Datom d] (= tx (d/datom-tx d)))))
                         (idx/slice aevt (d/datom d/e0 a nil d/tx0) (d/datom d/emax a nil d/txmax))       ;; _ a _ _
                         (filter (fn [^Datom d] (and (= v (.-v d))
                                                    (= tx (d/datom-tx d)))) eavt)                ;; _ _ v tx
                         (filter (fn [^Datom d] (= v (.-v d))) eavt)                            ;; _ _ v _
                         (filter (fn [^Datom d] (= tx (d/datom-tx d))) eavt)                      ;; _ _ _ tx
                         eavt])))                                                               ;; _ _ _ _

  d/IIndexAccess
  (-datoms [db index cs]
           (idx/slice (get db index) (components->pattern db index cs d/e0 d/tx0) (components->pattern db index cs d/emax d/txmax)))

  (-seek-datoms [db index cs]
                (idx/slice (get db index) (components->pattern db index cs d/e0 d/tx0) (d/datom d/emax nil nil d/txmax)))

  (-rseek-datoms [db index cs]
                 (idx/rslice (get db index) (components->pattern db index cs d/emax d/txmax) (d/datom d/e0 nil nil d/tx0)))

  (-index-range [db attr start end]
                (when-not (d/indexing? db attr)
                  (d/raise "Attribute " attr " should be marked as :db/index true" {}))
                (validate-attr attr (list '-index-range 'db attr start end))
                (idx/slice (.-avet db)
                           (resolve-datom db nil attr start nil d/e0 d/tx0)
                           (resolve-datom db nil attr end nil d/emax d/txmax)))

  clojure.data/EqualityPartition
  (equality-partition [x] :datascript/db)

  clojure.data/Diff
  (diff-similar [a b]
                (diff-sorted (:eavt a) (:eavt b) d/cmp-datoms-eav-quick)))

(defn- rschema [schema]
  (reduce-kv
   (fn [m attr keys->values]
     (reduce-kv
      (fn [m key value]
        (reduce
         (fn [m prop]
           (assoc m prop (conj (get m prop #{}) attr)))
         m (d/attr->properties key value)))
      m keys->values))
   {} schema))

(defn- validate-schema-key [a k v expected]
  (when-not (or (nil? v)
                (contains? expected v))
    (throw (ex-info (str "Bad attribute specification for " (pr-str {a {k v}}) ", expected one of " expected)
                    {:error :schema/validation
                     :attribute a
                     :key k
                     :value v}))))

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
    (validate-schema-key a :db/valueType (:db/valueType kv) #{:db.type/ref})
    (validate-schema-key a :db/cardinality (:db/cardinality kv) #{:db.cardinality/one :db.cardinality/many})))

(defn ^DB empty-db
  ([] (empty-db nil))
  ([schema]
   {:pre [(or (nil? schema) (map? schema))]}
   (validate-schema schema)
   (map->DB
    {:schema  schema
     :rschema (rschema (merge d/implicit-schema schema))
     :eavt    (idx/empty-index :eavt)
     :aevt    (idx/empty-index :aevt)
     :avet    (idx/empty-index :avet)
     :max-eid d/e0
     :max-tx  d/tx0
     :hash    (atom 0)})))

(defn- init-max-eid [eavt]
  (or (-> (idx/rslice eavt (d/datom (dec d/tx0) nil nil d/txmax) (d/datom d/e0 nil nil d/tx0))
        (first)
        (:e))
    d/e0))

(defn ^DB init-db
  ([datoms] (init-db datoms nil))
  ([datoms schema]
    (validate-schema schema)
    (let [rschema     (rschema (merge d/implicit-schema schema))
          indexed     (:db/index rschema)
          ;; arr         (cond-> datoms
          ;;               (not (arrays/array? datoms)) (arrays/into-array))
          ;; _           (arrays/asort arr cmp-datoms-eavt-quick)
          eavt        (idx/init-index :eavt datoms)
          ;; _           (arrays/asort arr cmp-datoms-aevt-quick)
          aevt        (idx/init-index :aevt datoms)
          ;; (set/from-sorted-array cmp-datoms-aevt arr)
          avet-datoms (filter (fn [^Datom d] (contains? indexed (.-a d))) datoms)
          ;; avet-arr    (to-array avet-datoms)
          ;; _           (arrays/asort avet-arr cmp-datoms-avet-quick)
          avet        (idx/init-index :avet datoms)
          ;; (set/from-sorted-array cmp-datoms-avet avet-arr)
          max-eid     (init-max-eid eavt)
          max-tx      (transduce (map (fn [^Datom d] (d/datom-tx d))) max d/tx0 eavt)]
      (map->DB {
        :schema  schema
        :rschema rschema
        :eavt    eavt
        :aevt    aevt
        :avet    avet
        :max-eid max-eid
        :max-tx  max-tx
        :hash    (atom 0)}))))

(defn- new-eid? [db eid]
  (and (> eid (:max-eid db))
       (< eid d/tx0))) ;; tx0 is max eid

(defn- advance-max-eid [db eid]
  (cond-> db
    (new-eid? db eid)
    (assoc :max-eid eid)))

(defn- next-eid [db]
  (inc (:max-eid db)))

(defn- ^Boolean tx-id?
  [e]
  (or (= e :db/current-tx)
      (= e ":db/current-tx") ;; for datascript.js interop
      (= e "datomic.tx")
      (= e "datascript.tx")))

(defn- ^Boolean tempid?
  [x]
  (or (and (number? x) (neg? x)) (string? x)))

(defn- allocate-eid
  ([report eid]
   (update-in report [:db-after] advance-max-eid eid))
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
     (update-in [:db-after] advance-max-eid eid))))

(defn- with-datom [db ^Datom datom]
  (d/validate-datom db datom)
  (let [indexing? (d/indexing? db (.-a datom))]
    (if (d/datom-added datom)
      (cond-> db
        true      (update-in [:eavt] idx/conj datom)
        true      (update-in [:aevt] idx/conj datom)
        indexing? (update-in [:avet] idx/conj datom)
        true      (advance-max-eid (.-e datom))
        true      (assoc :hash (atom 0)))
      (if-some [removing (first (d/-search db [(.-e datom) (.-a datom) (.-v datom)]))]
        (cond-> db
          true      (update-in [:eavt] idx/disj removing)
          true      (update-in [:aevt] idx/disj removing)
          indexing? (update-in [:avet] idx/disj removing)
          true      (assoc :hash (atom 0)))
        db))))

(defn- transact-report [report datom]
  (-> report
      (update-in [:db-after] with-datom datom)
      (update-in [:tx-data] conj datom)))

(defn- current-tx [report]
  (inc (get-in report [:db-before :max-tx])))

;; multivals/reverse can be specified as coll or as a single value, trying to guess
(defn- maybe-wrap-multival [db a vs]
  (cond
    ;; not a multival context
    (not (or (d/reverse-ref? a)
             (d/multival? db a)))
    [vs]

    ;; not a collection at all, so definitely a single value
    (not (or (arrays/array? vs)
             (and (coll? vs) (not (map? vs)))))
    [vs]

    ;; probably lookup ref
    (and (= (count vs) 2)
         (d/is-attr? db (first vs) :db.unique/identity))
    [vs]

    :else vs))

(defn- explode [db entity]
  (let [eid (:db/id entity)]
    (for [[a vs] entity
          :when  (not= a :db/id)
          :let   [_          (validate-attr a {:db/id eid, a vs})
                  reverse?   (d/reverse-ref? a)
                  straight-a (if reverse? (d/reverse-ref a) a)
                  _          (when (and reverse? (not (ref? db straight-a)))
                               (d/raise "Bad attribute " a ": reverse attribute name requires {:db/valueType :db.type/ref} in schema"
                                      {:error :transact/syntax, :attribute a, :context {:db/id eid, a vs}}))]
          v      (maybe-wrap-multival db a vs)]
      (if (and (ref? db straight-a) (map? v)) ;; another entity specified as nested map
        (assoc v (d/reverse-ref a) eid)
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
        new-datom (d/datom e a v tx)]
    (if (d/multival? db a)
      (if (empty? (d/-search db [e a v]))
        (transact-report report new-datom)
        report)
      (if-some [^datascript.db.Datom old-datom (first (d/-search db [e a]))]
        (if (= (.-v old-datom) v)
          report
          (-> report
              (transact-report (d/datom e a (.-v old-datom) tx false))
              (transact-report new-datom)))
        (transact-report report new-datom)))))

(defn- check-upsert-conflict [entity acc]
  (let [[e a v] acc
        _e (:db/id entity)]
    (if (or (nil? _e)
            (tempid? _e)
            (nil? acc)
            (== _e e))
      acc
      (d/raise "Conflicting upsert: " [a v] " resolves to " e
             ", but entity already has :db/id " _e
             { :error :transact/upsert
               :entity entity
               :assertion acc }))))

(defn- upsert-reduce-fn [db eav a v]
  (let [e (:e (first (d/-datoms db :avet [a v])))]
    (cond
      (nil? e) ;; value not yet in db
      eav

      (nil? eav) ;; first upsert
      [e a v]

      (= (get eav 0) e) ;; second+ upsert, but does not conflict
      eav

      :else
      (let [[_e _a _v] eav]
        (d/raise "Conflicting upserts: " [_a _v] " resolves to " _e
               ", but " [a v] " resolves to " e
               { :error     :transact/upsert
                 :assertion [e a v]
                 :conflict  [_e _a _v] })))))

(defn- upsert-eid [db entity]
  (when-some [idents (not-empty (d/-attrs-by db :db.unique/identity))]
    (->>
      (reduce-kv
        (fn [eav a v] ;; eav = [e a v]
          (cond
            (not (contains? idents a))
            eav

            (and
              (d/multival? db a)
              (or
                (arrays/array? v)
                (and (coll? v) (not (map? v)))))
            (reduce #(upsert-reduce-fn db %1 a %2) eav v)

            :else
            (upsert-reduce-fn db eav a v)))
        nil
        entity)
     (check-upsert-conflict entity)
     first))) ;; getting eid from eav

(defn- transact-retract-datom [report ^Datom d]
  (let [tx (current-tx report)]
    (transact-report report (d/datom (.-e d) (.-a d) (.-v d) tx false))))

(defn- retract-components [db datoms]
  (into #{} (comp
             (filter (fn [^Datom d] (d/component? db (.-a d))))
             (map (fn [^Datom d] [:db.fn/retractEntity (.-v d)]))) datoms))

(declare transact-tx-data)

(defn- retry-with-tempid [initial-report report es tempid upserted-eid]
  (if (contains? (:tempids initial-report) tempid)
    (d/raise "Conflicting upsert: " tempid " resolves"
           " both to " upserted-eid " and " (get-in initial-report [:tempids tempid])
           { :error :transact/upsert })
    ;; try to re-run from the beginning
    ;; but remembering that `tempid` will resolve to `upserted-eid`
    (let [tempids' (-> (:tempids report)
                       (assoc tempid upserted-eid))
          report'  (assoc initial-report :tempids tempids')]
      (transact-tx-data report' es))))

(defn transact-tx-data [initial-report initial-es]
  (when-not (or (nil? initial-es)
                (sequential? initial-es))
    (d/raise "Bad transaction data " initial-es ", expected sequential collection"
           {:error :transact/syntax, :tx-data initial-es}))
  (loop [report (-> initial-report
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

        (map? entity)
        (let [old-eid (:db/id entity)]
          (d/cond+
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
            (let [new-eid (cond
                            (nil? old-eid)    (next-eid db)
                            (tempid? old-eid) (or (get tempids old-eid)
                                                  (next-eid db))
                            :else             old-eid)
                  new-entity (assoc entity :db/id new-eid)]
              (recur (allocate-eid report old-eid new-eid)
                     (concat (explode db new-entity) entities)))

            ;; trash => error
            :else
            (d/raise "Expected number, string or lookup ref for :db/id, got " old-eid
              { :error :entity-id/syntax, :entity entity })))

        (sequential? entity)
        (let [[op e a v] entity]
          (cond
            (= op :db.fn/call)
            (let [[_ f & args] entity]
              (recur report (concat (apply f db args) entities)))

            (and (keyword? op)
                 (not (d/builtin-fn? op)))
            (if-some [ident (d/entid db op)]
              (let [fun  (-> (d/-search db [ident :db/fn]) first :v)
                    args (next entity)]
                (if (fn? fun)
                  (recur report (concat (apply fun db args) entities))
                  (d/raise "Entity " op " expected to have :db/fn attribute with fn? value"
                         {:error :transact/syntax, :operation :db.fn/call, :tx-data entity})))
              (d/raise "Can’t find entity for transaction fn " op
                     {:error :transact/syntax, :operation :db.fn/call, :tx-data entity}))

            (and (tempid? e) (not= op :db/add))
            (d/raise "Can't use tempid in '" entity "'. Tempids are allowed in :db/add only"
              { :error :transact/syntax, :op entity })

            (or (= op :db.fn/cas)
                (= op :db/cas))
            (let [[_ e a ov nv] entity
                  e (entid-strict db e)
                  _ (validate-attr a entity)
                  ov (if (ref? db a) (entid-strict db ov) ov)
                  nv (if (ref? db a) (entid-strict db nv) nv)
                  _ (validate-val nv entity)
                  datoms (vec (d/-search db [e a]))]
              (if (d/multival? db a)
                (if (some (fn [^datascript.db.Datom d] (= (.-v d) ov)) datoms)
                  (recur (transact-add report [:db/add e a nv]) entities)
                  (d/raise ":db.fn/cas failed on datom [" e " " a " " (map :v datoms) "], expected " ov
                         {:error :transact/cas, :old datoms, :expected ov, :new nv}))
                (let [v (:v (first datoms))]
                  (if (= v ov)
                    (recur (transact-add report [:db/add e a nv]) entities)
                    (d/raise ":db.fn/cas failed on datom [" e " " a " " v "], expected " ov
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
            (let [upserted-eid  (when (d/is-attr? db a :db.unique/identity)
                                  (:e (first (d/-datoms db :avet [a v]))))
                  allocated-eid (get tempids e)]
              (if (and upserted-eid allocated-eid (not= upserted-eid allocated-eid))
                (retry-with-tempid initial-report report initial-es e upserted-eid)
                (let [eid (or upserted-eid allocated-eid (next-eid db))]
                  (recur (allocate-eid report e eid) (cons [op eid a v] entities)))))

            (= op :db/add)
            (recur (transact-add report entity) entities)

            (and (= op :db/retract) v)
            (if-some [e (d/entid db e)]
              (let [v (if (d/ref? db a) (d/entid-strict db v) v)]
                (validate-attr a entity)
                (validate-val v entity)
                (if-some [old-datom (first (d/-search db [e a v]))]
                  (recur (transact-retract-datom report old-datom) entities)
                  (recur report entities)))
              (recur report entities))

            (or (= op :db.fn/retractAttribute)
                (= op :db/retract))
            (if-some [e (d/entid db e)]
              (let [_      (validate-attr a entity)
                    datoms (vec (d/-search db [e a]))]
                (recur (reduce transact-retract-datom report datoms)
                       (concat (retract-components db datoms) entities)))
              (recur report entities))

            (or (= op :db.fn/retractEntity)
                (= op :db/retractEntity))
            (if-some [e (d/entid db e)]
              (let [e-datoms (vec (d/-search db [e]))
                    v-datoms (vec (mapcat (fn [a] (d/-search db [nil a e])) (d/-attrs-by db :db.type/ref)))]
                (recur (reduce transact-retract-datom report (concat e-datoms v-datoms))
                       (concat (retract-components db e-datoms) entities)))
              (recur report entities))

           :else
           (d/raise "Unknown operation at " entity ", expected :db/add, :db/retract, :db.fn/call, :db.fn/retractAttribute, :db.fn/retractEntity or an ident corresponding to an installed transaction function (e.g. {:db/ident <keyword> :db/fn <Ifn>}, usage of :db/ident requires {:db/unique :db.unique/identity} in schema)" {:error :transact/syntax, :operation op, :tx-data entity})))

       (d/datom? entity)
       (let [[e a v tx added] entity]
         (if added
           (recur (transact-add report [:db/add e a v tx]) entities)
           (recur report (cons [:db/retract e a v] entities))))

       :else
       (d/raise "Bad entity type at " entity ", expected map or vector"
              {:error :transact/syntax, :tx-data entity})))))
