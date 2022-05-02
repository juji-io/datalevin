(ns ^:no-doc datalevin.storage
  "Storage layer of Datalog store"
  (:require [datalevin.lmdb :as lmdb]
            [datalevin.util :as u]
            [datalevin.bits :as b]
            [datalevin.search :as s]
            [datalevin.constants :as c]
            [datalevin.datom :as d]
            [clojure.set :as set]
            )
  (:import [java.util UUID]
           [datalevin.datom Datom]
           [datalevin.bits Retrieved]))

(if (u/graal?)
  (require 'datalevin.binding.graal)
  (require 'datalevin.binding.java))

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

(defn- init-max-cid [lmdb] (lmdb/entries lmdb c/classes))

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

(defn- init-attrs [schema]
  (into {} (map (fn [[k v]] [(:db/aid v) k])) schema))

(defn- init-classes
  [lmdb]
  (let [res (lmdb/get-range lmdb c/classes [:all] :id :data)]
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
             (assoc! m aid (conj (get m aid #{}) cid)))
           m aids))
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

(defn- migrate-cardinality
  [lmdb attr old new]
  (when (and (= old :db.cardinality/many) (= new :db.cardinality/one))
    ;; TODO figure out if this is consistent with data
    ;; raise exception if not
    ))

(defn- handle-value-type
  [lmdb attr old new]
  (when (not= old new)
    ;; TODO raise if datom already exist for this attr
    ))

(defn- migrate-unique
  [lmdb attr old new]
  (when (and (not old) new)
    ;; TODO figure out if the attr values are unique for each entity,
    ;; raise if not
    ;; also check if ave and vea entries exist for this attr, create if not
    ))

(defn- migrate [lmdb attr old new]
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
          (b/indexable e am v :db.type/ref)
          (u/raise "When v is known but a is unknown, v must be a :db.type/ref"
                   {:v v}))
        (b/indexable e am vm :db.type/sysMin)))))

(defn- index->dbi
  [index]
  (case index
    :eavt c/eav
    :eav  c/eav
    :avet c/ave
    :ave  c/ave
    :veat c/vea
    :vea  c/vea))

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
  (init-max-eid [this] "Initialize and return the max entity id")
  (datom-count [this index] "Return the number of datoms in the index")
  (classes [this] "Return the cid -> class map")
  (set-classes [this classes])
  (rclasses [this] "Return the aid -> classes map")
  (set-rclasses [this rclasses])
  (max-cid [this])
  (set-max-cid [this max-cid])
  (entities [this])
  (set-entities [this entities])
  (rentities [this])
  (set-rentities [this rentities])
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
    that return true for (pred x), where x is the datom"))

(declare transact-datoms update-entity-classes)

(deftype Store [lmdb
                opts
                search-engine
                ^:volatile-mutable attrs     ; aid -> attr
                ^:volatile-mutable schema    ; attr -> props
                ^:volatile-mutable rschema   ; prop -> attrs
                ^:volatile-mutable classes   ; cid -> aids
                ^:volatile-mutable rclasses  ; aid -> cids
                ^:volatile-mutable entities  ; eid -> cids
                ^:volatile-mutable rentities ; cid -> eids bitmap
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
    (set! max-aid (init-max-aid lmdb))
    schema)

  (attrs [_]
    attrs)

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

  (set-max-cid [_ v]
    (set! max-cid v))

  (classes [_]
    classes)

  (set-classes [_ v]
    (set! classes v))

  (rclasses [_]
    rclasses)

  (set-rclasses [_ v]
    (set! rclasses v))

  (entities [_]
    entities)

  (set-entities [_ v]
    (set! entities v))

  (rentities [_]
    rentities)

  (set-rentities [_ v]
    (set! rentities v))

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
      (set! attrs (assoc attrs (p :db/aid) attr))
      p))

  (load-datoms [this datoms]
    (let [ft-ds (volatile! (transient []))
          eids  (volatile! (transient #{}))]
      (try
        (locking (lmdb/write-txn lmdb)
          (lmdb/open-transact-kv lmdb)
          (transact-datoms this ft-ds eids datoms)
          (lmdb/transact-kv lmdb [(time-tx)])
          (update-entity-classes this (persistent! @eids)))
        (catch clojure.lang.ExceptionInfo e
          (if (:resized (ex-data e))
            (load-datoms this datoms)
            (throw e)))
        (finally (lmdb/close-transact-kv lmdb)))
      (fulltext-index search-engine (persistent! @ft-ds))))

  (fetch [this datom]
    (mapv (partial retrieved->datom lmdb attrs)
          (when-some [kv (lmdb/get-value lmdb
                                         c/eav
                                         (datom->indexable schema datom false)
                                         :eav
                                         :id
                                         false)]
            [kv])))

  (populated? [_ index low-datom high-datom]
    (lmdb/get-first lmdb
                    (index->dbi index)
                    [:closed
                     (datom->indexable schema low-datom false)
                     (datom->indexable schema high-datom true)]
                    index
                    :ignore
                    true))

  (size [_ index low-datom high-datom]
    (lmdb/range-count lmdb
                      (index->dbi index)
                      [:closed
                       (datom->indexable schema low-datom false)
                       (datom->indexable schema high-datom true)]
                      index))

  (head [_ index low-datom high-datom]
    (retrieved->datom
      lmdb attrs (lmdb/get-first lmdb
                                 (index->dbi index)
                                 [:closed
                                  (datom->indexable schema low-datom false)
                                  (datom->indexable schema high-datom true)]
                                 index
                                 :id)))

  (tail [_ index high-datom low-datom]
    (retrieved->datom
      lmdb attrs (lmdb/get-first lmdb
                                 (index->dbi index)
                                 [:closed-back
                                  (datom->indexable schema high-datom true)
                                  (datom->indexable schema low-datom false)]
                                 index
                                 :id)))

  (slice [_ index low-datom high-datom]
    (mapv (partial retrieved->datom lmdb attrs)
          (lmdb/get-range
            lmdb
            (index->dbi index)
            [:closed
             (datom->indexable schema low-datom false)
             (datom->indexable schema high-datom true)]
            index
            :id)))

  (rslice [_ index high-datom low-datom]
    (mapv (partial retrieved->datom lmdb attrs)
          (lmdb/get-range
            lmdb
            (index->dbi index)
            [:closed-back
             (datom->indexable schema high-datom true)
             (datom->indexable schema low-datom false)]
            index
            :id)))

  (size-filter [_ index pred low-datom high-datom]
    (lmdb/range-filter-count lmdb
                             (index->dbi index)
                             (datom-pred->kv-pred lmdb attrs index pred)
                             [:closed
                              (datom->indexable schema low-datom false)
                              (datom->indexable schema high-datom true)]
                             index))

  (head-filter [_ index pred low-datom high-datom]
    (retrieved->datom
      lmdb attrs (lmdb/get-some lmdb
                                (index->dbi index)
                                (datom-pred->kv-pred lmdb attrs index pred)
                                [:closed
                                 (datom->indexable schema low-datom false)
                                 (datom->indexable schema high-datom true)]
                                index
                                :id)))

  (tail-filter [_ index pred high-datom low-datom]
    (retrieved->datom
      lmdb attrs (lmdb/get-some lmdb
                                (index->dbi index)
                                (datom-pred->kv-pred lmdb attrs index pred)
                                [:closed-back
                                 (datom->indexable schema high-datom true)
                                 (datom->indexable schema low-datom false)]
                                index
                                :id)))

  (slice-filter [_ index pred low-datom high-datom]
    (mapv
      (partial retrieved->datom lmdb attrs)
      (lmdb/range-filter
        lmdb
        (index->dbi index)
        (datom-pred->kv-pred lmdb attrs index pred)
        [:closed
         (datom->indexable schema low-datom false)
         (datom->indexable schema high-datom true)]
        index
        :id)))

  (rslice-filter
    [_ index pred high-datom low-datom]
    (mapv
      (partial retrieved->datom lmdb attrs)
      (lmdb/range-filter
        lmdb
        (index->dbi index)
        (datom-pred->kv-pred lmdb attrs index pred)
        [:closed
         (datom->indexable schema high-datom true)
         (datom->indexable schema low-datom false)]
        index
        :id))))

(defn- insert-datom
  [^Store store ^Datom d ft-ds]
  (let [attr  (.-a d)
        props (or ((schema store) attr)
                  (swap-attr store attr identity))
        ref?  (= :db.type/ref (props :db/valueType))
        i     (b/indexable (.-e d) (props :db/aid) (.-v d)
                           (props :db/valueType))]
    (when (props :db/fulltext) (vswap! ft-ds conj! [:a d]))
    (if (b/giant? i)
      (let [max-gt (max-gt store)]
        (advance-max-gt store)
        (cond-> [[:put c/eav i max-gt :eav :id]
                 [:put c/ave i max-gt :ave :id]
                 [:put c/giants max-gt d :id :datom [:append]]]
          ref? (conj [:put c/vea i max-gt :vea :id])))
      (cond-> [[:put c/eav i c/normal :eav :id]
               [:put c/ave i c/normal :ave :id]]
        ref? (conj [:put c/vea i c/normal :vea :id])))))

(defn- delete-datom
  [^Store store ^Datom d ft-ds]
  (let [props  ((schema store) (.-a d))
        ref?   (= :db.type/ref (props :db/valueType))
        i      (b/indexable (.-e d) (props :db/aid) (.-v d)
                            (props :db/valueType))
        giant? (b/giant? i)
        gt     (when giant?
                 (lmdb/get-value (.-lmdb store) c/eav i :eav :id))]
    (when (props :db/fulltext) (vswap! ft-ds conj! [:d d]))
    (cond-> [[:del c/eav i :eav]
             [:del c/ave i :ave]]
      ref? (conj [:del c/vea i :vea])
      gt   (conj [:del c/giants gt :id]))))

(defn- transact-datoms
  [^Store store ft-ds eids datoms]
  (doseq [datom datoms]
    (vswap! eids conj! (d/datom-e datom))
    (lmdb/transact-kv (.-lmdb store)
                      (if (d/datom-added datom)
                        (insert-datom store datom ft-ds)
                        (delete-datom store datom ft-ds)))))

(defn- entity-aids
  "Return the set of attribute ids of an entity"
  [^Store store eid writing?]
  (let [schema (schema store)
        datom  (d/datom eid nil nil)
        aids   (volatile! (transient #{}))
        add    #(vswap! aids conj! (b/read-buffer (lmdb/k %) :eav-a))]
    (lmdb/visit (.-lmdb store) c/eav add
                [:open
                 (datom->indexable schema datom false)
                 (datom->indexable schema datom true)]
                :eav-a writing?)
    (persistent! @aids)))

(defn aids->attrs
  [store aids]
  (let [attrs (attrs store)]
    (set (map attrs aids))))

(defn attrs->aids
  [store attrs]
  (let [schema (schema store)]
    (set (map #(some-> % schema :db/aid) attrs))))

(defn entity-attrs
  "Return the set of attributes of an entity"
  [^Store store eid]
  (aids->attrs store (entity-aids store eid false)))

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
  [max-cid classes rclasses aids]
  (let [cid @max-cid]
    (vswap! classes assoc cid aids)
    (vswap! rclasses classes->rclasses {cid aids})
    (vswap! max-cid #(inc ^long %))
    cid))

(defn- add-entity
  [entities rentities eid cid]
  (let [old-cid (@entities eid)
        old-bm  (@rentities old-cid)]
    (vswap! rentities
            #(cond-> %
               true   (assoc cid (b/bitmap-add (@rentities cid (b/bitmap)) eid))
               old-bm (assoc old-cid (b/bitmap-del old-bm eid))))
    (vswap! entities assoc eid cid)
    old-cid))

(defn- del-entity
  [entities rentities eid]
  (let [old-cid (@entities eid)
        old-bm  (@rentities old-cid)]
    (when old-bm
      (vswap! rentities assoc old-cid (b/bitmap-del old-bm eid)))
    (vswap! entities dissoc eid)
    old-cid))

(defn- transact-entity-classes
  [lmdb classes rentities cids]
  (lmdb/transact-kv lmdb
                    (map (fn [cid]
                           (let [aids (classes cid)
                                 bm   (rentities cid)]
                             (when (and aids bm)
                               [:put c/classes cid [aids bm] :id :data])))
                         cids)))

(defn- update-entity-classes
  [^Store store eids]
  (let [updated-cids (volatile! (transient #{}))
        classes      (volatile! (classes store))
        rclasses     (volatile! (rclasses store))
        entities     (volatile! (entities store))
        rentities    (volatile! (rentities store))
        max-cid      (volatile! (max-cid store))
        adjust
        (fn [eid new-cid]
          (vswap! updated-cids conj! new-cid)
          (when-let [old-cid (add-entity entities rentities eid new-cid)]
            (vswap! updated-cids conj! old-cid)))]
    (doseq [eid eids]
      (let [my-aids (entity-aids store eid true)]
        (if (empty? my-aids)
          (vswap! updated-cids conj! (del-entity entities rentities eid))
          (let [cids (find-classes @rclasses my-aids)]
            (if (empty? cids)
              (adjust eid (add-class max-cid classes rclasses my-aids))
              (if-let [cid (some (fn [cid]
                                   (when (= my-aids (@classes cid)) cid))
                                 cids)]
                (adjust eid cid)
                (adjust eid (add-class max-cid classes rclasses my-aids))))))))
    (transact-entity-classes (.-lmdb store) @classes @rentities
                             (persistent! @updated-cids))
    (set-classes store @classes)
    (set-rclasses store @rclasses)
    (set-entities store @entities)
    (set-rentities store @rentities)
    (set-max-cid store @max-cid)))

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
  (lmdb/open-dbi lmdb c/eav c/+max-key-size+ c/+id-bytes+)
  (lmdb/open-dbi lmdb c/ave c/+max-key-size+ c/+id-bytes+)
  (lmdb/open-dbi lmdb c/vea c/+max-key-size+ c/+id-bytes+)
  (lmdb/open-dbi lmdb c/giants c/+id-bytes+)
  (lmdb/open-dbi lmdb c/schema c/+max-key-size+)
  (lmdb/open-dbi lmdb c/classes c/+id-bytes+)
  (lmdb/open-dbi lmdb c/meta c/+max-key-size+)
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
           [classes rentities] (init-classes lmdb)]
       (->Store lmdb
                (load-opts lmdb)
                (s/new-search-engine lmdb search-opts)
                (init-attrs schema)
                schema
                (schema->rschema schema)
                classes
                (classes->rclasses classes)
                (rentities->entities rentities)
                rentities
                (init-max-aid lmdb)
                (init-max-gt lmdb)
                (init-max-cid lmdb))))))
