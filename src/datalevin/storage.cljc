(ns ^:no-doc datalevin.storage
  "Storage layer of Datalog store"
  (:require [datalevin.lmdb :as lmdb]
            [datalevin.util :as u]
            [datalevin.bits :as b]
            [datalevin.search :as s]
            [datalevin.constants :as c]
            [datalevin.datom :as d]
            [clojure.set :as set])
  (:import [java.util UUID]
           [datalevin.datom Datom]
           [datalevin.bits Retrieved]))

(if (u/graal?)
  (require 'datalevin.binding.graal)
  (require 'datalevin.binding.java))

(set! *unchecked-math* true)

(defn attr->properties
  [k v]
  (case v
    :db.unique/identity  [:db/unique :db.unique/identity]
    :db.unique/value     [:db/unique :db.unique/value]
    :db.cardinality/many [:db.cardinality/many]
    :db.type/ref         [:db.type/ref]
    (when (true? v)
      (case k
        :db/isComponent [:db/isComponent]
        []))))

(defn schema->rschema
  ([schema]
   (schema->rschema {} schema))
  ([old-rschema new-schema]
   (persistent!
     (reduce-kv
       (fn [m attr keys->values]
         (reduce-kv
           (fn [m key value]
             (reduce
               (fn [m prop]
                 (assoc! m prop (conj (get m prop #{}) attr)))
               m (attr->properties key value)))
           m keys->values))
       (transient old-rschema) new-schema))))

(defn- aids->attrs [attrs aids] (into #{} (map attrs) aids))

(defn- attrs->aids
  ([schema attrs]
   (attrs->aids #{} schema attrs))
  ([old-aids schema attrs]
   (into old-aids (map #(-> % schema :db/aid)) attrs)))

(defn- time-tx
  []
  [:put c/meta :last-modified (System/currentTimeMillis) :attr :long])

(defn- transact-schema
  [lmdb schema]
  (lmdb/transact-kv lmdb
                    (conj (for [[attr props] schema]
                            [:put c/schema attr props :attr :data])
                          (time-tx))))

(defn- load-schema
  [lmdb]
  (into {} (lmdb/get-range lmdb c/schema [:all] :attr :data)))

(defn- init-max-aid [lmdb] (lmdb/entries lmdb c/schema))

(defn- init-max-cid [lmdb] (lmdb/entries lmdb c/encla))

;; TODO schema migration
(defn- update-schema
  [lmdb old schema]
  (let [aid (volatile! (dec ^long (init-max-aid lmdb)))]
    (into {}
          (map (fn [[attr props]]
                 [attr
                  (if-let [old-props (old attr)]
                    (merge old-props props)
                    (assoc props :db/aid (vswap! aid #(inc ^long %))))]))
          schema)))

(defn- init-schema
  [lmdb schema]
  (when (empty? (load-schema lmdb))
    (transact-schema lmdb c/implicit-schema))
  (when schema
    (transact-schema lmdb (update-schema lmdb (load-schema lmdb) schema)))
  (let [now (load-schema lmdb)]
    (when-not (:db/created-at now)
      (transact-schema lmdb (update-schema lmdb now c/entity-time-schema))))
  (load-schema lmdb))

(defn- init-attrs
  [schema]
  (into {} (map (fn [[k v]] [(:db/aid v) k])) schema))

(defn- init-refs
  [schema rschema]
  (attrs->aids schema (rschema :db.type/ref)))

(defn- init-encla
  [lmdb]
  (let [res (lmdb/get-range lmdb c/encla [:all] :id :data)]
    [(into {} (map (fn [[k v]] [k (first v)])) res)
     (into {} (map (fn [[k v]] [k (peek v)])) res)]))

(defn classes->rclasses
  ([classes]
   (classes->rclasses {} classes))
  ([old-rclasses new-classes]
   (persistent!
     (reduce-kv
       (fn [m cid aids]
         (reduce
           (fn [m aid]
             (assoc! m aid (conj (m aid #{}) cid)))
           m (keys aids)))
       (transient old-rclasses) new-classes))))

(defn rentities->entities
  [rentities]
  (persistent!
    (reduce-kv
      (fn [m cid bm]
        (reduce (fn [m eid] (assoc! m eid cid)) m bm))
      (transient {}) rentities)))

(defn- init-max-gt
  [lmdb]
  (or (when-let [gt (-> (lmdb/get-first lmdb c/giants [:all-back] :id :ignore)
                        first)]
        (inc ^long gt))
      c/gt0))

#_(defn- migrate-cardinality
    [lmdb attr old new]
    (when (and (= old :db.cardinality/many) (= new :db.cardinality/one))
      ;; TODO figure out if this is consistent with data
      ;; raise exception if not
      ))

#_(defn- handle-value-type
    [lmdb attr old new]
    (when (not= old new)
      ;; TODO raise if datom already exist for this attr
      ))

#_(defn- migrate-unique
    [lmdb attr old new]
    (when (and (not old) new)
      ;; TODO figure out if the attr values are unique for each entity,
      ;; raise if not
      ;; also check if ave entries exist for this attr, create if not
      ))

#_(defn- migrate [lmdb attr old new]
    (doseq [[k v] new
            :let  [v' (old k)]]
      (case k
        :db/cardinality (migrate-cardinality lmdb attr v' v)
        :db/valueType   (handle-value-type lmdb attr v' v)
        :db/unique      (migrate-unique lmdb attr v' v)
        :pass-through)))

(defn- datom->indexable
  [schema ^Datom d high?]
  (let [e  (.-e d)
        am (if high? c/amax c/a0)
        vm (if high? c/vmax c/v0)]
    (if-let [a (.-a d)]
      (if-let [p (schema a)]
        (if-some [v (.-v d)]
          (b/indexable e (:db/aid p) v (:db/valueType p))
          (b/indexable e (:db/aid p) vm (:db/valueType p)))
        (b/indexable e c/a0 c/v0 nil))
      (if-some [v (.-v d)]
        (if (integer? v)
          (if e
            (b/indexable e am v :db.type/ref)
            (b/indexable (if high? c/emax c/e0) am v :db.type/ref))
          (u/raise "When v is known but a is unknown, v must be a :db.type/ref"
                   {:v v}))
        (b/indexable e am vm :db.type/sysMin)))))

(defn- index->dbi
  [index]
  (case index
    :eavt c/eav
    :eav  c/eav
    :avet c/ave
    :ave  c/ave))

(defn- retrieved->datom
  [lmdb attrs [^Retrieved k ^long v :as kv]]
  (when kv
    (if (= v c/normal)
      (d/datom (.-e k) (attrs (.-a k)) (.-v k))
      (lmdb/get-value lmdb c/giants v :id :datom))))

(defn- datom-pred->kv-pred
  [lmdb attrs index pred]
  (fn [kv]
    (let [^Retrieved k (b/read-buffer (lmdb/k kv) index)
          ^long v      (b/read-buffer (lmdb/v kv) :id)
          ^Datom d     (retrieved->datom lmdb attrs [k v])]
      (pred d))))

(defn- fulltext-index
  [search-engine ft-ds]
  (doseq [[op ^Datom d] ft-ds
          :let          [v (str (.-v d))]]
    (case op
      :a (s/add-doc search-engine d v)
      :d (s/remove-doc search-engine d))))

(defprotocol IStore
  (opts [this] "Return the opts map")
  (db-name [this] "Return the db-name, if it is a remote or server store")
  (dir [this] "Return the data file directory")
  (close [this] "Close storage")
  (closed? [this] "Return true if the storage is closed")
  (last-modified [this]
    "Return the unix timestamp of when the store is last modified")
  (max-gt [this])
  (advance-max-gt [this])
  (max-aid [this])
  (schema [this] "Return the schema map")
  (rschema [this] "Return the reverse schema map")
  (set-schema [this new-schema]
    "Update the schema of open storage, return updated schema")
  (attrs [this] "Return the aid -> attr map")
  (refs [this])
  (init-max-eid [this] "Initialize and return the max entity id")
  (datom-count [this index] "Return the number of datoms in the index")
  (classes [this] "Return the cid -> class map")
  (rclasses [this] "Return the aid -> classes map")
  (max-cid [this])
  (entities [this])
  (rentities [this])
  (links [this])
  (swap-attr [this attr f] [this attr f x] [this attr f x y]
    "Update an attribute, f is similar to that of swap!")
  (load-datoms [this datoms] "Load datams into storage")
  (fetch [this datom] "Return [datom] if it exists in store, otherwise '()")
  (populated? [this index low-datom high-datom]
    "Return true if there exists at least one datom in the given boundary (inclusive)")
  (size [this index low-datom high-datom]
    "Return the number of datoms within the given range (inclusive)")
  (head [this index low-datom high-datom]
    "Return the first datom within the given range (inclusive)")
  (tail [this index high-datom low-datom]
    "Return the last datom within the given range (inclusive)")
  (slice [this index low-datom high-datom]
    "Return a range of datoms within the given range (inclusive).")
  (rslice [this index high-datom low-datom]
    "Return a range of datoms in reverse within the given range (inclusive)")
  (size-filter [this index pred low-datom high-datom]
    "Return the number of datoms within the given range (inclusive) that
    return true for (pred x), where x is the datom")
  (head-filter [this index pred low-datom high-datom]
    "Return the first datom within the given range (inclusive) that
    return true for (pred x), where x is the datom")
  (tail-filter [this index pred high-datom low-datom]
    "Return the last datom within the given range (inclusive) that
    return true for (pred x), where x is the datom")
  (slice-filter [this index pred low-datom high-datom]
    "Return a range of datoms within the given range (inclusive) that
    return true for (pred x), where x is the datom")
  (rslice-filter [this index pred high-datom low-datom]
    "Return a range of datoms in reverse for the given range (inclusive)
    that return true for (pred x), where x is the datom")
  (scan-ref-v [this veid] "Return ref type datoms with given v"))

(declare transact-datoms update-encla update-links)

(deftype Store [lmdb
                opts
                search-engine
                ^:volatile-mutable attrs     ; aid -> attr
                ^:volatile-mutable refs      ; set of ref aids
                ^:volatile-mutable schema    ; attr -> props
                ^:volatile-mutable rschema   ; prop -> attrs
                ^:volatile-mutable classes   ; cid -> aids
                ^:volatile-mutable rclasses  ; aid -> cids
                ^:volatile-mutable entities  ; eid -> cid
                ^:volatile-mutable rentities ; cid -> eids bitmap
                ^:volatile-mutable links     ; vae -> link
                ^:volatile-mutable max-aid
                ^:volatile-mutable max-gt
                ^:volatile-mutable max-cid]
  IStore
  (opts [_]
    opts)

  (db-name [_]
    (:db-name opts))

  (dir [_]
    (lmdb/dir lmdb))

  (close [_]
    (lmdb/close-kv lmdb))

  (closed? [_]
    (lmdb/closed-kv? lmdb))

  (last-modified [_]
    (lmdb/get-value lmdb c/meta :last-modified :attr :long))

  (max-gt [_]
    max-gt)

  (advance-max-gt [_]
    (set! max-gt (inc ^long max-gt)))

  (max-aid [_]
    max-aid)

  (schema [_]
    schema)

  (rschema [_]
    rschema)

  (set-schema [_ new-schema]
    (set! schema (init-schema lmdb new-schema))
    (set! rschema (schema->rschema schema))
    (set! attrs (init-attrs schema))
    (set! refs (init-refs schema rschema))
    (set! max-aid (init-max-aid lmdb))
    schema)

  (attrs [_]
    attrs)

  (refs [_]
    refs)

  (init-max-eid [_]
    (or (when-let [[k v] (lmdb/get-first lmdb c/eav [:all-back] :eav :id)]
          (if (= c/overflown (.-a ^Retrieved k))
            (.-e ^Datom (lmdb/get-value lmdb c/giants v :id :datom))
            (.-e ^Retrieved k)))
        c/e0))

  (datom-count [_ index]
    (lmdb/entries lmdb (if (string? index) index (index->dbi index))))

  (max-cid [_]
    max-cid)

  (classes [_]
    classes)

  (rclasses [_]
    rclasses)

  (entities [_]
    entities)

  (rentities [_]
    rentities)

  (links [_]
    links)

  (swap-attr [this attr f]
    (swap-attr this attr f nil nil))
  (swap-attr [this attr f x]
    (swap-attr this attr f x nil))
  (swap-attr [_ attr f x y]
    (let [o (or (schema attr)
                (let [m {:db/aid max-aid}]
                  (set! max-aid (inc ^long max-aid))
                  m))
          p (cond
              (and x y) (f o x y)
              x         (f o x)
              :else     (f o))
          s {attr p}]
      ;; TODO auto schema migration
      ;; (migrate lmdb attr o p)
      (transact-schema lmdb s)
      (set! schema (assoc schema attr p))
      (set! rschema (schema->rschema rschema s))
      (let [aid (p :db/aid)]
        (set! attrs (assoc attrs aid attr))
        (when (= :db.type/ref (p :db/valueType))
          (set! refs (conj refs aid))))
      p))

  (load-datoms [this datoms]
    (try
      (locking (lmdb/write-txn lmdb)
        (let [v-classes   (volatile! classes)
              v-rclasses  (volatile! rclasses)
              v-entities  (volatile! entities)
              v-rentities (volatile! rentities)
              v-max-cid   (volatile! max-cid)
              v-links     (volatile! (transient links))
              ft-ds       (volatile! (transient []))
              del-ref-ds  (volatile! (transient #{}))]
          (lmdb/open-transact-kv lmdb)
          (-> (transact-datoms this ft-ds del-ref-ds datoms)
              (update-encla lmdb schema refs v-classes v-rclasses
                            v-entities v-rentities v-max-cid)
              (update-links lmdb @v-entities links v-links
                            (persistent! @del-ref-ds)))
          (lmdb/transact-kv lmdb [(time-tx)])
          (set! classes @v-classes)
          (set! rclasses @v-rclasses)
          (set! entities @v-entities)
          (set! rentities @v-rentities)
          (set! max-cid @v-max-cid)
          (set! links (persistent! @v-links))
          (fulltext-index search-engine (persistent! @ft-ds))))
      (catch clojure.lang.ExceptionInfo e
        (if (:resized (ex-data e))
          (load-datoms this datoms)
          (throw e)))
      (finally (lmdb/close-transact-kv lmdb))))

  (fetch [this datom]
    (mapv (partial retrieved->datom lmdb attrs)
          (when-some [kv (lmdb/get-value lmdb c/eav
                                         (datom->indexable schema datom false)
                                         :eav :id false)]
            [kv])))

  (populated? [_ index low-datom high-datom]
    (lmdb/get-first lmdb (index->dbi index)
                    [:closed
                     (datom->indexable schema low-datom false)
                     (datom->indexable schema high-datom true)]
                    index :ignore true))

  (size [_ index low-datom high-datom]
    (lmdb/range-count lmdb (index->dbi index)
                      [:closed
                       (datom->indexable schema low-datom false)
                       (datom->indexable schema high-datom true)]
                      index))

  (head [_ index low-datom high-datom]
    (retrieved->datom
      lmdb attrs (lmdb/get-first lmdb (index->dbi index)
                                 [:closed
                                  (datom->indexable schema low-datom false)
                                  (datom->indexable schema high-datom true)]
                                 index :id)))

  (tail [_ index high-datom low-datom]
    (retrieved->datom
      lmdb attrs (lmdb/get-first lmdb (index->dbi index)
                                 [:closed-back
                                  (datom->indexable schema high-datom true)
                                  (datom->indexable schema low-datom false)]
                                 index :id)))

  (slice [_ index low-datom high-datom]
    (mapv (partial retrieved->datom lmdb attrs)
          (lmdb/get-range lmdb (index->dbi index)
                          [:closed
                           (datom->indexable schema low-datom false)
                           (datom->indexable schema high-datom true)]
                          index :id)))

  (rslice [_ index high-datom low-datom]
    (mapv (partial retrieved->datom lmdb attrs)
          (lmdb/get-range lmdb (index->dbi index)
                          [:closed-back
                           (datom->indexable schema high-datom true)
                           (datom->indexable schema low-datom false)]
                          index :id)))

  (size-filter [_ index pred low-datom high-datom]
    (lmdb/range-filter-count lmdb (index->dbi index)
                             (datom-pred->kv-pred lmdb attrs index pred)
                             [:closed
                              (datom->indexable schema low-datom false)
                              (datom->indexable schema high-datom true)]
                             index))

  (head-filter [_ index pred low-datom high-datom]
    (retrieved->datom
      lmdb attrs (lmdb/get-some lmdb (index->dbi index)
                                (datom-pred->kv-pred lmdb attrs index pred)
                                [:closed
                                 (datom->indexable schema low-datom false)
                                 (datom->indexable schema high-datom true)]
                                index :id)))

  (tail-filter [_ index pred high-datom low-datom]
    (retrieved->datom
      lmdb attrs (lmdb/get-some lmdb (index->dbi index)
                                (datom-pred->kv-pred lmdb attrs index pred)
                                [:closed-back
                                 (datom->indexable schema high-datom true)
                                 (datom->indexable schema low-datom false)]
                                index :id)))

  (slice-filter [_ index pred low-datom high-datom]
    (mapv
      (partial retrieved->datom lmdb attrs)
      (lmdb/range-filter lmdb (index->dbi index)
                         (datom-pred->kv-pred lmdb attrs index pred)
                         [:closed
                          (datom->indexable schema low-datom false)
                          (datom->indexable schema high-datom true)]
                         index :id)))

  (rslice-filter
    [_ index pred high-datom low-datom]
    (mapv
      (partial retrieved->datom lmdb attrs)
      (lmdb/range-filter lmdb (index->dbi index)
                         (datom-pred->kv-pred lmdb attrs index pred)
                         [:closed
                          (datom->indexable schema high-datom true)
                          (datom->indexable schema low-datom false)]
                         index :id)))

  (scan-ref-v
    [_ in-veid]
    (when-let [vcid (entities in-veid)]
      (persistent!
        (reduce
          (fn [res [[_ aid _] [veid eeid]]]
            (if (= veid in-veid)
              (conj! res (d/datom eeid (attrs aid) veid))
              res))
          (transient #{})
          (lmdb/get-range lmdb c/links
                          [:closed [vcid c/a0 c/e0] [vcid c/amax c/emax]]
                          :int-int-int :long-long)))))
  )

(defn- insert-datom
  [^Store store ^Datom d ft-ds]
  (let [[e attr v] (d/datom-eav d)
        {:keys [db/valueType db/aid db/fulltext]}
        (or ((schema store) attr) (swap-attr store attr identity))
        i          (b/indexable e aid v valueType)]
    (when fulltext (vswap! ft-ds conj! [:a d]))
    (if (b/giant? i)
      (let [max-gt (max-gt store)]
        (advance-max-gt store)
        [[:put c/eav i max-gt :eav :id]
         [:put c/ave i max-gt :ave :id]
         [:put c/giants max-gt d :id :datom [:append]]])
      [[:put c/eav i c/normal :eav :id]
       [:put c/ave i c/normal :ave :id]])))

(defn- delete-datom
  [^Store store ^Datom d ft-ds del-ref-ds]
  (let [[e attr v] (d/datom-eav d)
        {:keys [db/valueType db/aid db/fulltext]}
        ((schema store) attr)
        i          (b/indexable e aid v valueType)
        gt         (when (b/giant? i)
                     (lmdb/get-value (.-lmdb store) c/eav i :eav :id true true))]
    (when ((refs store) aid) (vswap! del-ref-ds conj! [v aid e]))
    (when fulltext (vswap! ft-ds conj! [:d d]))
    (cond-> [[:del c/eav i :eav]
             [:del c/ave i :ave]]
      gt (conj [:del c/giants gt :id]))))

(defn- transact-datoms
  [^Store store ft-ds del-ref-ds datoms]
  (let [lmdb (.-lmdb store)]
    (persistent!
      (reduce
        (fn [eids datom]
          (lmdb/transact-kv lmdb
                            (if (d/datom-added datom)
                              (insert-datom store datom ft-ds)
                              (delete-datom store datom ft-ds del-ref-ds)))
          (conj! eids (d/datom-e datom)))
        (transient #{}) datoms))))

(defn- scan-entity
  [lmdb schema refs cur-ref-ds eid]
  (let [aid-counts (volatile! {})
        datom      (d/datom eid nil nil)]
    (lmdb/visit lmdb c/eav
                #(let [eav (lmdb/k %)
                       aid (b/read-buffer eav :eav-a)]
                   (vswap! aid-counts update aid (fnil inc 0.0))
                   (when (refs aid)
                     (vswap! cur-ref-ds conj! [(b/get-value eav 1) aid eid])))
                [:open
                 (datom->indexable schema datom false)
                 (datom->indexable schema datom true)]
                :eav-a true)
    @aid-counts))

(defn- find-classes
  [rclasses aids]
  (when (seq aids)
    (reduce (fn [cs new-cs]
              (let [cs' (set/intersection cs new-cs)]
                (if (seq cs')
                  cs'
                  (reduced nil))))
            (map rclasses aids))))

(defn- add-class
  [max-cid classes rclasses aid-counts]
  (let [cid @max-cid]
    (vswap! classes assoc cid aid-counts)
    (vswap! rclasses classes->rclasses {cid aid-counts})
    (vswap! max-cid #(inc ^long %))
    cid))

(defn- cumulative-average
  [^double average ^long n+1 ^long x]
  (+ average (/ (- x average) n+1)))

(defn- adj-class
  [classes rentities cid aid-counts]
  (let [n+1 (inc ^long (b/bitmap-size (@rentities cid)))]
    (vswap! classes update cid
            #(persistent!
               (reduce-kv
                 (fn [m aid average]
                   (assoc! m aid
                           (cumulative-average average n+1 (aid-counts aid))))
                 (transient {}) %)))
    cid))

(defn- del-entity
  [updated-cids entities rentities eid]
  (when-let [old-cid (@entities eid)]
    (vswap! rentities assoc old-cid (b/bitmap-del (@rentities old-cid) eid))
    (vswap! updated-cids conj! old-cid))
  (vswap! entities dissoc eid))

(defn- adj-entity
  [updated-cids entities rentities eid new-cid]
  (let [old-cid (@entities eid)]
    (when (not= old-cid new-cid)
      (vswap! rentities
              #(cond-> (assoc % new-cid
                              (b/bitmap-add (% new-cid (b/bitmap)) eid))
                 old-cid (assoc old-cid (b/bitmap-del (% old-cid) eid))))
      (vswap! entities assoc eid new-cid)
      (vswap! updated-cids conj! new-cid)
      (when old-cid (vswap! updated-cids conj! old-cid)))))

(defn- transact-encla
  [lmdb classes rentities cids]
  (lmdb/transact-kv
    lmdb
    (for [cid cids]
      [:put c/encla cid [(classes cid) (rentities cid)] :id :data])))

(defn- update-encla
  [eids lmdb schema refs classes rclasses entities rentities max-cid]
  (let [cur-ref-ds   (volatile! (transient #{}))
        updated-cids (volatile! (transient #{}))]
    (doseq [eid  eids
            :let [aid-counts (scan-entity lmdb schema refs cur-ref-ds eid)]]
      (if (empty? aid-counts)
        (del-entity updated-cids entities rentities eid)
        (let [my-aids (set (keys aid-counts))
              cids    (find-classes @rclasses my-aids)]
          (if-let [cid (some (fn [cid]
                               (when (= my-aids (set (keys (@classes cid))))
                                 cid))
                             cids)]
            (adj-entity updated-cids entities rentities eid
                        (adj-class classes rentities cid aid-counts))
            (adj-entity updated-cids entities rentities eid
                        (add-class max-cid classes rclasses aid-counts))))))
    (transact-encla lmdb @classes @rentities (persistent! @updated-cids))
    (persistent! @cur-ref-ds)))

(defn- update-links
  [cur-ref-ds lmdb entities old-links new-links del-ref-ds]
  (let [to-del (volatile! {})
        to-add (volatile! {})
        conj*  (fnil conj [])]
    (doseq [[v a e :as vae] cur-ref-ds
            :let            [ecid (entities e)
                             vcid (entities v)
                             new-link [vcid a ecid]]
            :when           (and vcid ecid)]
      (vswap! new-links assoc! vae new-link)
      (if-let [old-link (old-links vae)]
        (when (not= old-link new-link)
          (vswap! to-del update old-link conj* [v e])
          (vswap! to-add update new-link conj* [v e]))
        (vswap! to-add update new-link conj* [v e])))
    (doseq [[v _ e :as vae] del-ref-ds
            :when           (not (cur-ref-ds vae))]
      (vswap! new-links dissoc! vae)
      (when-let [old-link (old-links vae)]
        (vswap! to-del update old-link conj* [v e])))
    (doseq [[link lst] @to-add]
      (lmdb/put-list-items lmdb c/links link lst :int-int-int :long-long))
    (doseq [[link lst] @to-del]
      (lmdb/del-list-items lmdb c/links link lst :int-int-int :long-long))))

(defn- init-links
  [lmdb]
  (persistent!
    (reduce
      (fn [m [[_ aid _ :as link] [veid eeid]]]
        (assoc! m [veid aid eeid] link))
      (transient {})
      (lmdb/get-range lmdb c/links [:all] :int-int-int :long-long))))

(defn- transact-opts
  [lmdb opts]
  (lmdb/transact-kv lmdb
                    (conj (for [[k v] opts]
                            [:put c/opts k v :attr :data])
                          (time-tx))))

(defn- load-opts
  [lmdb]
  (into {} (lmdb/get-range lmdb c/opts [:all] :attr :data)))

(defn- open-dbis
  [lmdb]
  (lmdb/open-dbi lmdb c/meta c/+max-key-size+)
  (lmdb/open-dbi lmdb c/eav c/+max-key-size+ c/+id-bytes+)
  (lmdb/open-dbi lmdb c/ave c/+max-key-size+ c/+id-bytes+)
  (lmdb/open-dbi lmdb c/giants c/+id-bytes+)
  (lmdb/open-dbi lmdb c/encla c/+id-bytes+)
  (lmdb/open-list-dbi lmdb c/links (* 3 Integer/BYTES) (* 2 Long/BYTES))
  (lmdb/open-dbi lmdb c/schema c/+max-key-size+)
  (lmdb/open-dbi lmdb c/opts c/+max-key-size+))

(defn open
  "Open and return the storage."
  ([]
   (open nil nil))
  ([dir]
   (open dir nil))
  ([dir schema]
   (open dir schema nil))
  ([dir schema {:keys [kv-opts search-opts] :as opts}]
   (let [dir  (or dir (u/tmp-dir (str "datalevin-" (UUID/randomUUID))))
         lmdb (lmdb/open-kv dir kv-opts)]
     (open-dbis lmdb)
     (when opts (transact-opts lmdb opts))
     (let [schema              (init-schema lmdb schema)
           rschema             (schema->rschema schema)
           [classes rentities] (init-encla lmdb)]
       (->Store lmdb
                (load-opts lmdb)
                (s/new-search-engine lmdb search-opts)
                (init-attrs schema)
                (init-refs schema rschema)
                schema
                rschema
                classes
                (classes->rclasses classes)
                (rentities->entities rentities)
                rentities
                (init-links lmdb)
                (init-max-aid lmdb)
                (init-max-gt lmdb)
                (init-max-cid lmdb))))))
