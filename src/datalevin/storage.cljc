(ns ^:no-doc datalevin.storage
  "storage layer of Datalevin"
  (:require [datalevin.lmdb :as lmdb]
            [datalevin.util :as u]
            [datalevin.bits :as b]
            [datalevin.binding.graal]
            [datalevin.binding.java]
            [datalevin.constants :as c]
            [datalevin.datom :as d])
  (:import [java.util UUID]
           [datalevin.datom Datom]
           [datalevin.bits Retrieved]))

(defn- transact-schema
  [lmdb schema]
  (lmdb/transact-kv lmdb (for [[attr props] schema]
                           [:put c/schema attr props :attr :data])))

(defn- load-schema
  [lmdb]
  (into {} (lmdb/get-range lmdb c/schema [:all] :attr :data)))

(defn- init-max-aid
  [lmdb]
  (lmdb/entries lmdb c/schema))

;; TODO schema migration
(defn- update-schema
  [lmdb old schema]
  (let [^long init-aid (init-max-aid lmdb)
        i              (atom 0)]
    (into {}
          (map (fn [[attr props]]
                 (if-let [old-props (old attr)]
                   [attr (merge old-props props)]
                   (let [res [attr (assoc props :db/aid (+ init-aid ^long @i))]]
                     (swap! i inc)
                     res))))
          schema)))

(defn- init-schema
  [lmdb schema]
  (when (empty? (load-schema lmdb))
    (transact-schema lmdb c/implicit-schema))
  (when schema
    (let [now (load-schema lmdb)]
      (transact-schema lmdb (update-schema lmdb now schema))))
  (load-schema lmdb))

(defn- init-attrs [schema]
  (into {} (map (fn [[k v]] [(:db/aid v) k])) schema))

(defn- init-max-gt
  [lmdb]
  (or (when-let [gt (-> (lmdb/get-first lmdb c/giants [:all-back] :id :ignore)
                        first)]
        (inc ^long gt))
      c/gt0))

(defn- init-max-cls
  [lmdb]
  (or (when-let [cls (-> (lmdb/get-first lmdb c/classes [:all-back] :int :ignore)
                         first)]
        (inc ^int cls))
      0))

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

(defn- low-datom->indexable
  [schema ^Datom d]
  (let [e (.-e d)]
    (if-let [a (.-a d)]
      (if-let [p (schema a)]
        (if-some [v (.-v d)]
          (b/indexable e (:db/aid p) v (:db/valueType p))
          (b/indexable e (:db/aid p) c/v0 (:db/valueType p)))
        (b/indexable e c/a0 c/v0 nil))
      (if-some [v (.-v d)]
        (if (integer? v)
          (b/indexable e c/a0 v :db.type/ref)
          (u/raise "When v is known but a is unknown, v must be a :db.type/ref"
                   {:v v}))
        (b/indexable e c/a0 c/v0 :db.type/sysMin)))))

(defn- high-datom->indexable
  [schema ^Datom d]
  (let [e (.-e d)]
    (if-let [a (.-a d)]
      (if-let [p (schema a)]
        (if-some [v (.-v d)]
          (b/indexable e (:db/aid p) v (:db/valueType p))
          (b/indexable e (:db/aid p) c/vmax (:db/valueType p)))
        ;; same as low-datom-indexable to get [] fast
        (b/indexable e c/a0 c/v0 nil))
      (if-some [v (.-v d)]
        (if (integer? v)
          (b/indexable e c/amax v :db.type/ref)
          (u/raise "When v is known but a is unknown, v must be a :db.type/ref"
                   {:v v}))
        (b/indexable e c/amax c/vmax :db.type/sysMax)))))

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

(defprotocol IStore
  (max-gt [this])
  (advance-max-gt [this])
  (max-cls [this])
  (advance-max-cls [this])
  (max-aid [this])
  (schema [this] "Return the schema map")
  (set-schema [this new-schema]
    "Update the schema of open storage, return updated schema")
  (attrs [this] "Return the aid -> attr map")
  (init-max-eid [this] "Initialize and return the max entity id")
  (swap-attr [this attr f] [this attr f x] [this attr f x y]
    "Update an attribute, f is similar to that of swap!"))

(deftype Store [lmdb
                ^:volatile-mutable schema    ; attr -> props
                ^:volatile-mutable attrs     ; aid -> attr
                ^:volatile-mutable max-aid
                ^:volatile-mutable max-gt
                ^:volatile-mutable max-cls]
  IStore
  (max-gt [_]
    max-gt)

  (advance-max-gt [_]
    (set! max-gt (inc ^long max-gt)))

  (max-cls [_]
    max-cls)

  (advance-max-cls [_]
    (set! max-cls (inc ^int max-cls)))

  (max-aid [_]
    max-aid)

  (schema [_]
    schema)

  (set-schema [_ new-schema]
    (set! schema (init-schema lmdb new-schema))
    (set! attrs (init-attrs schema))
    schema)

  (attrs [_]
    attrs)

  (init-max-eid [_]
    (or (when-let [[k v] (lmdb/get-first lmdb c/eav [:all-back] :eav :id)]
          (if (= c/overflown (.-a ^Retrieved k))
            (.-e ^Datom (lmdb/get-value lmdb c/giants v :id :datom))
            (.-e ^Retrieved k)))
        c/e0))

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
              :else     (f o))]
      (migrate lmdb attr o p)
      ;; TODO remove this tx, return tx-data instead
      (transact-schema lmdb {attr p})
      (set! schema (assoc schema attr p))
      (set! attrs (assoc attrs (:db/aid p) attr))
      p)))

(defn dir
  "Return the data file directory"
  [^Store store]
  (lmdb/dir (.-lmdb store)))

(defn close
  "Close storage"
  [^Store store]
  (lmdb/close-kv (.-lmdb store)))

(defn closed?
  "Return true if the storage is closed"
  [^Store store]
  (lmdb/closed-kv? (.-lmdb store)))

(defn datom-count
  "Return the number of datoms in the store"
  [^Store store index]
  (lmdb/entries (.-lmdb store)
                (if (string? index) index (index->dbi index))))

(defn- insert-datom
  [^Store store ^Datom d]
  (let [props  ((schema store) (.-a d))
        ref?   (= :db.type/ref (:db/valueType props))
        i      (b/indexable (.-e d) (:db/aid props) (.-v d)
                            (:db/valueType props))
        max-gt (max-gt store)]
    (if (b/giant? i)
      [(cond-> [[:put c/eav i max-gt :eav :id]
                [:put c/ave i max-gt :ave :id]
                [:put c/giants max-gt d :id :datom true]]
         ref? (conj [:put c/vea i max-gt :vea :id]))
       true]
      [(cond-> [[:put c/eav i c/normal :eav :id]
                [:put c/ave i c/normal :ave :id]]
         ref? (conj [:put c/vea i c/normal :vea :id]))
       false])))

(defn- delete-datom
  [^Store store ^Datom d]
  (let [props  ((schema store) (.-a d))
        ref?   (= :db.type/ref (:db/valueType props))
        i      (b/indexable (.-e d)
                            (:db/aid props)
                            (.-v d)
                            (:db/valueType props))
        giant? (b/giant? i)]
    (cond-> [[:del c/eav i :eav]
             [:del c/ave i :ave]]
      ref?   (conj [:del c/vea i :vea])
      giant? (conj [:del c/giants
                    (lmdb/get-value (.-lmdb store) c/eav i :eav :id)
                    :id]))))

(defn- handle-datom
  [store holder datom]
  (if (d/datom-added datom)
    (let [[data giant?] (insert-datom store datom)]
      (when giant? (advance-max-gt store))
      (reduce conj! holder data))
    (reduce conj! holder (delete-datom store datom))))

(defn entity-attrs
  "Return the set of attributes of an entity"
  [^Store store eid]
  (let [lmdb   (.-lmdb store)
        schema (schema store)
        attrs  (attrs store)
        datom  (d/datom eid nil nil)]
    (into #{}
          (comp (map first)
             (map attrs))
          (lmdb/get-range lmdb
                          c/eav
                          [:closed
                           (low-datom->indexable schema datom)
                           (high-datom->indexable schema datom)]
                          :eav-a
                          :ignore))))

(defn- entity-meta-data
  [store cur-eid add-attrs del-attrs]
  (let [old-attrs (entity-attrs store cur-eid)]))

(defn- transact-meta-data
  [store batch]
  (loop [cur-eid nil add-attrs #{} del-attrs #{} remain batch]
    (if-let [^Datom datom (first remain)]
      (let [eid  (.-e datom)
            attr (.-a datom)
            add? (d/datom-added datom)
            add  #(if add? (conj add-attrs attr) add-attrs)
            del  #(if-not add? (conj del-attrs attr) del-attrs)
            rr   (rest remain)]
        (or ((schema store) attr) (swap-attr store attr identity))
        (if (= cur-eid eid)
          (recur cur-eid (add) (del) rr)
          (if cur-eid
            (do (entity-meta-data store cur-eid add-attrs del-attrs)
                (recur eid #{} #{} rr))
            (recur eid (add) (del) rr))))
      (entity-meta-data store cur-eid add-attrs del-attrs))))

(defn- load-batch
  [^Store store batch]
  (let [batch (sort-by d/datom-e batch)]
    (transact-meta-data store batch)
    (lmdb/transact-kv (.-lmdb store)
                      (persistent!
                        (reduce (partial handle-datom store)
                                (transient [])
                                batch)))))

(defn load-datoms
  "Load datams into storage"
  [store datoms]
  (locking store
    (doseq [batch (partition c/+tx-datom-batch-size+
                             c/+tx-datom-batch-size+
                             nil
                             datoms)]
      (load-batch store batch))))

(defn fetch
  "Return [datom] if it exists in store, otherwise '()"
  [^Store store datom]
  (let [lmdb   (.-lmdb store)
        schema (schema store)
        attrs  (attrs store)]
    (mapv (partial retrieved->datom lmdb attrs)
          (when-some [kv (lmdb/get-value lmdb
                                         c/eav
                                         (low-datom->indexable schema datom)
                                         :eav
                                         :id
                                         false)]
            [kv]))))

(defn populated?
  "Return true if there exists at least one datom in the given boundary
  (inclusive)"
  [^Store store index low-datom high-datom]
  (let [lmdb   (.-lmdb store)
        schema (schema store)]
    (lmdb/get-first lmdb
                    (index->dbi index)
                    [:closed
                     (low-datom->indexable schema low-datom)
                     (high-datom->indexable schema high-datom)]
                    index
                    :ignore
                    true)))

(defn size
  "Return the number of datoms within the given range (inclusive)"
  [^Store store index low-datom high-datom]
  (let [lmdb   (.-lmdb store)
        schema (schema store)]
    (lmdb/range-count lmdb
                      (index->dbi index)
                      [:closed
                       (low-datom->indexable schema low-datom)
                       (high-datom->indexable schema high-datom)]
                      index)))

(defn head
  "Return the first datom within the given range (inclusive)"
  [^Store store index low-datom high-datom]
  (let [lmdb   (.-lmdb store)
        schema (schema store)
        attrs  (attrs store)]
    (retrieved->datom
      lmdb attrs (lmdb/get-first lmdb
                                 (index->dbi index)
                                 [:closed
                                  (low-datom->indexable schema low-datom)
                                  (high-datom->indexable schema high-datom)]
                                 index
                                 :id))))

(defn slice
  "Return a range of datoms within the given range (inclusive)."
  [^Store store index low-datom high-datom]
  (let [lmdb   (.-lmdb store)
        schema (schema store)
        attrs  (attrs store)]
    (mapv (partial retrieved->datom lmdb attrs)
          (lmdb/get-range
            lmdb
            (index->dbi index)
            [:closed
             (low-datom->indexable schema low-datom)
             (high-datom->indexable schema high-datom)]
            index
            :id))))

(defn rslice
  "Return a range of datoms in reverse within the given range (inclusive)"
  [^Store store index high-datom low-datom]
  (let [lmdb   (.-lmdb store)
        schema (schema store)
        attrs  (attrs store)]
    (mapv (partial retrieved->datom lmdb attrs)
          (lmdb/get-range
            lmdb
            (index->dbi index)
            [:closed-back
             (high-datom->indexable schema high-datom)
             (low-datom->indexable schema low-datom)]
            index
            :id))))

(defn size-filter
  "Return the number of datoms within the given range (inclusive) that
    return true for (pred x), where x is the datom"
  [^Store store index pred low-datom high-datom]
  (let [lmdb   (.-lmdb store)
        schema (schema store)
        attrs  (attrs store)]
    (lmdb/range-filter-count lmdb
                             (index->dbi index)
                             (datom-pred->kv-pred lmdb attrs index pred)
                             [:closed
                              (low-datom->indexable schema low-datom)
                              (high-datom->indexable schema high-datom)]
                             index)))

(defn head-filter
  "Return the first datom within the given range (inclusive) that
    return true for (pred x), where x is the datom"
  [^Store store index pred low-datom high-datom]
  (let [lmdb   (.-lmdb store)
        schema (schema store)
        attrs  (attrs store)]
    (retrieved->datom
      lmdb attrs (lmdb/get-some lmdb
                                (index->dbi index)
                                (datom-pred->kv-pred lmdb attrs index pred)
                                [:closed
                                 (low-datom->indexable schema low-datom)
                                 (high-datom->indexable schema high-datom)]
                                index
                                :id))))

(defn slice-filter
  "Return a range of datoms within the given range (inclusive) that
    return true for (pred x), where x is the datom"
  [^Store store index pred low-datom high-datom]
  (let [lmdb   (.-lmdb store)
        schema (schema store)
        attrs  (attrs store)]
    (mapv
      (partial retrieved->datom lmdb attrs)
      (lmdb/range-filter
        lmdb
        (index->dbi index)
        (datom-pred->kv-pred lmdb attrs index pred)
        [:closed
         (low-datom->indexable schema low-datom)
         (high-datom->indexable schema high-datom)]
        index
        :id))))

(defn rslice-filter
  "Return a range of datoms in reverse for the given range (inclusive)
    that return true for (pred x), where x is the datom"
  [^Store store index pred high-datom low-datom]
  (let [lmdb   (.-lmdb store)
        schema (schema store)
        attrs  (attrs store)]
    (mapv
      (partial retrieved->datom lmdb attrs)
      (lmdb/range-filter
        lmdb
        (index->dbi index)
        (datom-pred->kv-pred lmdb attrs index pred)
        [:closed
         (high-datom->indexable schema high-datom)
         (low-datom->indexable schema low-datom)]
        index
        :id))))

(defn open
  "Open and return the storage."
  ([]
   (open nil nil))
  ([dir]
   (open dir nil))
  ([dir schema]
   (let [dir  (or dir (u/tmp-dir (str "datalevin-" (UUID/randomUUID))))
         lmdb (lmdb/open-kv dir)]
     (lmdb/open-dbi lmdb c/eav c/+max-key-size+ c/+id-bytes+)
     (lmdb/open-dbi lmdb c/ave c/+max-key-size+ c/+id-bytes+)
     (lmdb/open-dbi lmdb c/vea c/+max-key-size+ c/+id-bytes+)
     (lmdb/open-dbi lmdb c/giants c/+id-bytes+)
     (lmdb/open-dbi lmdb c/schema c/+max-key-size+)
     (lmdb/open-dbi lmdb c/classes c/+short-id-bytes+)
     (lmdb/open-dbi lmdb c/links (* c/+short-id-bytes+ 2))
     (let [schema' (init-schema lmdb schema)]
       (->Store lmdb
                schema'
                (init-attrs schema')
                (init-max-aid lmdb)
                (init-max-gt lmdb)
                (init-max-cls lmdb))))))
