(ns datalevin.storage
  "storage layer of datalevin"
  (:require [datalevin.lmdb :as lmdb]
            [datalevin.util :as u]
            [datalevin.bits :as b]
            [datalevin.constants :as c]
            [datalevin.datom :as d]
            [taoensso.nippy :as nippy]
            [taoensso.timbre :as log])
  (:import [datalevin.lmdb LMDB]
           [datalevin.datom Datom]
           [datalevin.bits Indexable Retrieved]
           [org.lmdbjava PutFlags]))

(defn- transact-schema
  [lmdb schema]
  (lmdb/transact lmdb (for [[attr props] schema]
                        [:put c/schema attr props :attr :data]))
  schema)

(defn- init-schema
  [lmdb]
  (or (when-let [coll (seq (lmdb/get-range lmdb c/schema [:all] :attr :data))]
        (into {} coll))
      (transact-schema lmdb c/implicit-schema)))

(defn- init-max-aid
  [lmdb]
  (lmdb/entries lmdb c/schema))

(defn- init-max-gt
  [lmdb]
  (or (when-let [gt (-> (lmdb/get-first lmdb c/giants [:all-back] :long :ignore)
                        first)]
        (inc ^long gt))
      c/gt0))

(defn- migrate-cardinality
  [lmdb attr old new]
  (when (and (= old :db.cardinality/many) (= new :db.cardinality/one))
    ;; TODO figure out if this is consistent with data
    ;; raise exception if not
    ))

(defn- migrate-index
  [lmdb attr old new]
  (cond
    (and (not old) (true? new))
    ;; TODO create ave and vae entries if datoms for this attr exists
    (and (true? old) (false? new))
    ;; TODO remove ave and vae entries for this attr
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
    ;; also check if ave and vae entries exist for this attr, create if not
    ))

(defn- migrate [lmdb attr old new]
  (doseq [[k v] new
          :let  [v' (old k)]]
    (case k
      :db/cardinality (migrate-cardinality lmdb attr v' v)
      :db/index       (migrate-index lmdb attr v' v)
      :db/valueType   (handle-value-type lmdb attr v' v)
      :db/unique      (migrate-unique lmdb attr v' v)
      :pass-through)))

(defn- low-datom->indexable
  [schema ^Datom d]
  (let [e (.-e d)]
    (if-let [a (.-a d)]
      (if-let [p (schema a)]
        (if-let [v (.-v d)]
          (b/indexable e (:db/aid p) v (:db/valueType p))
          (b/indexable e (:db/aid p) c/v0 (:db/valueType p)))
        (u/raise "Cannot slice with unknown attribute " a {}))
      (if-let [v (.-v d)]
        (b/indexable e c/a0 v nil)
        (b/indexable e c/a0 c/v0 :db.type/sysMin)))))

(defn- high-datom->indexable
  [schema ^Datom d]
  (let [e (.-e d)]
    (if-let [a (.-a d)]
      (if-let [p (schema a)]
        (if-let [v (.-v d)]
          (b/indexable e (:db/aid p) v (:db/valueType p))
          (b/indexable e (:db/aid p) c/vmax (:db/valueType p)))
        (u/raise "Cannot slice with unknown attribute " a {}))
      (if-let [v (.-v d)]
        (b/indexable e c/amax v nil)
        (b/indexable e c/amax c/vmax :db.type/sysMax)))))

(defn- index->dbi
  [index]
  (case index
    :eavt c/eav
    :eav  c/eav
    :aevt c/aev
    :aev  c/aev
    :avet c/ave
    :ave  c/ave
    :vaet c/vae
    :vae  c/vae))

(defn- retrieved->datom
  [lmdb attrs [^Retrieved k ^long v]]
  (if (= v c/normal)
    (d/datom (.-e k) (attrs (.-a k)) (.-v k))
    (lmdb/get-value lmdb c/giants v :long)))

(defn- datom-pred->kv-pred
  [lmdb attrs index pred]
  (fn [kv]
    (let [^Retrieved k (b/read-buffer (key kv) index)
          ^long v      (b/read-buffer (val kv) :long)
          ^Datom d     (retrieved->datom lmdb attrs [k v])]
      (pred d))))

(defprotocol IStore
  (close [this] "Close storage")
  (max-gt [this])
  (max-aid [this])
  (schema [this] "Return the schema map")
  (attrs [this] "Return the aid -> attr map")
  (init-max-eid [this] "Initialize and return the max entity id")
  (datom-count [this index] "Return the number of datoms in the index")
  (swap-attr [this attr f] [this attr f x] [this attr f x y]
    "Update an attribute, f is similar to that of swap!")
  (insert [this datom] "Insert an datom")
  (delete [this datom] "Delete an datom")
  (slice [this index low-datom high-datom]
    "Return a range of datoms within the given boundary (inclusive).")
  (rslice [this index high-datom low-datom]
    "Return a range of datoms in reverse within the given boundary (inclusive)")
  (slice-filter [this index pred low-datom high-datom]
    "Return a range of datoms within the given boundary (inclusive) that
    return true for (pred x), where x is the datom")
  (rslice-filter [this index pred high-datom low-datom]
    "Return a range of datoms in reverse for the given boundary (inclusive)
    that return true for (pred x), where x is the datom")
  (-seq [this index])
  (-rseq [this index])
  (-count [this index]))

(deftype Store [^LMDB lmdb
                ^:volatile-mutable schema
                ^:volatile-mutable attrs
                ^:volatile-mutable ^long max-aid
                ^:volatile-mutable ^long max-gt]
  IStore
  (close [_]
    (lmdb/close lmdb))
  (max-gt [_]
    max-gt)
  (max-aid [_]
    max-aid)
  (schema [_]
    schema)
  (attrs [_]
    attrs)
  (init-max-eid [_]
    (or (when-let [[r _] (lmdb/get-first lmdb c/eav [:all-back] :eav :ignore)]
          (.-e ^Retrieved r))
        c/e0))
  (swap-attr [this attr f]
    (swap-attr this attr f nil nil))
  (swap-attr [this attr f x]
    (swap-attr this attr f x nil))
  (swap-attr [_ attr f x y]
    (let [o (or (schema attr)
                (let [m {:db/aid max-aid}]
                  (set! max-aid (inc max-aid))
                  m))
          p (cond
              (and x y) (f o x y)
              x         (f o x)
              :else     (f o))]
      (migrate lmdb attr o p)
      (transact-schema lmdb {attr p})
      (set! schema (assoc schema attr p))
      (set! attrs (assoc attrs (:db/aid p) attr))
      p))
  (datom-count [_ index]
    (lmdb/entries lmdb index))
  (insert [this datom]
    (let [attr      (.-a ^Datom datom)
          props     (or (schema attr)
                        (swap-attr this attr identity))
          indexing? (or (:db/index props)
                        (:db/unique props)
                        (= :db.type/ref (:db/valueType props)))
          i         (b/indexable (.-e ^Datom datom)
                                 (:db/aid props)
                                 (.-v ^Datom datom)
                                 (:db/valueType props))]
      (if (b/giant? i)
        (locking max-gt
          (lmdb/transact
           lmdb
           (cond-> [[:put c/eav i max-gt :eav :long]
                    [:put c/aev i max-gt :aev :long]
                    [:put c/giants max-gt datom :long :datom
                     [PutFlags/MDB_APPEND]]]
             indexing? (concat
                        [[:put c/ave i max-gt :ave :long]
                         [:put c/vae i max-gt :vae :long]])))
          (set! max-gt (inc max-gt)))
        (lmdb/transact
         lmdb
         (cond-> [[:put c/eav i c/normal :eav :long]
                  [:put c/aev i c/normal :aev :long]]
           indexing? (concat
                      [[:put c/ave i c/normal :ave :long]
                       [:put c/vae i c/normal :vae :long]]))))))
  (delete [_ datom]
    (let [props (schema (.-a ^Datom datom))
          i     (b/indexable (.-e ^Datom datom)
                             (:db/aid props)
                             (.-v ^Datom datom)
                             (:db/valueType props))]
      (lmdb/transact
       lmdb
       (cond-> [[:del c/eav i :eav]
                [:del c/aev i :aev]
                [:del c/ave i :ave]
                [:del c/vae i :vae]]
         (b/giant? i)
         (conj [:del c/giants (lmdb/get-value lmdb c/eav i :eav :long)
                :long])))))
  (slice [_ index low-datom high-datom]
    (map
     (partial retrieved->datom lmdb attrs)
     (lmdb/get-range
      lmdb
      (index->dbi index)
      [:closed
       (low-datom->indexable schema low-datom)
       (high-datom->indexable schema high-datom)]
      index
      :long)))
  (rslice [_ index high-datom low-datom]
    (map
     (partial retrieved->datom lmdb attrs)
     (lmdb/get-range
      lmdb
      (index->dbi index)
      [:closed-back
       (high-datom->indexable schema high-datom)
       (low-datom->indexable schema low-datom)]
      index
      :long)))
  (slice-filter [_ index pred low-datom high-datom]
    (map
     (partial retrieved->datom lmdb attrs)
     (lmdb/range-filter
      lmdb
      (index->dbi index)
      (datom-pred->kv-pred lmdb attrs index pred)
      [:closed
       (low-datom->indexable schema low-datom)
       (high-datom->indexable schema high-datom)]
      index
      :long)))
  (rslice-filter [_ index pred high-datom low-datom]
    (map
     (partial retrieved->datom lmdb attrs)
     (lmdb/range-filter
      lmdb
      (index->dbi index)
      (datom-pred->kv-pred lmdb attrs index pred)
      [:closed
       (high-datom->indexable schema high-datom)
       (low-datom->indexable schema low-datom)]
      index
      :long)))
  (-seq [_ index]
    (map
     (partial retrieved->datom lmdb attrs)
     (lmdb/get-range lmdb (index->dbi index) [:all] index :long)))
  (-rseq [_ index]
    (map
     (partial retrieved->datom lmdb attrs)
     (lmdb/get-range lmdb (index->dbi index) [:all-back] index :long)))
  (-count [_ index]
    (lmdb/entries lmdb (index->dbi index))))

(defn- init-attrs [schema]
  (into {} (map (fn [[k v]] [(:db/aid v) k])) schema))

(defn open
  "Open and return the storage."
  [dir]
  (let [lmdb (lmdb/open-lmdb dir)]
    (lmdb/open-dbi lmdb c/eav c/+max-key-size+ Long/BYTES)
    (lmdb/open-dbi lmdb c/aev c/+max-key-size+ Long/BYTES)
    (lmdb/open-dbi lmdb c/ave c/+max-key-size+ Long/BYTES)
    (lmdb/open-dbi lmdb c/vae c/+max-key-size+ Long/BYTES)
    (lmdb/open-dbi lmdb c/giants Long/BYTES)
    (lmdb/open-dbi lmdb c/schema c/+max-key-size+)
    (let [schema (init-schema lmdb)]
      (->Store lmdb
               schema
               (init-attrs schema)
               (init-max-aid lmdb)
               (init-max-gt lmdb)))))
