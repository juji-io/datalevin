(ns ^:no-doc datalevin.storage
  "storage layer of Datalevin"
  (:require [clojure.set :as set]
            [datalevin.lmdb :as lmdb]
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

(defn- load-classes
  [lmdb]
  (into {} (lmdb/get-range lmdb c/classes [:all] :bitmap :data)))

(defn- transact-classes
  [lmdb classes]
  (lmdb/transact-kv lmdb (for [[bm props] classes]
                           [:put c/classes bm props :bitmap :data])))

(defn- init-attrs [schema]
  (into {} (map (fn [[k v]] [(:db/aid v) k])) schema))

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

(defprotocol IStore
  (max-gt [this])
  (advance-max-gt [this])
  (max-aid [this])
  (init-max-eid [this] "Initialize and return the max entity id")
  (classes [this] "Return the classes map")
  (set-classes [this new-classes]
    "Update the classes, return updated classes")
  (schema [this] "Return the schema map")
  (set-schema [this new-schema]
    "Update the schema of open storage, return updated schema")
  (attrs [this] "Return the aid -> attr map")
  (swap-attr [this attr f] [this attr f x] [this attr f x y]
    "Update an attribute, f is similar to that of swap!"))

(deftype Store [lmdb
                ^:volatile-mutable schema    ; attr -> props
                ^:volatile-mutable classes   ; aids-bitmap -> class-props
                ^:volatile-mutable attrs     ; aid -> attr
                ^:volatile-mutable max-aid
                ^:volatile-mutable max-gt]
  IStore
  (max-gt [_]
    max-gt)

  (advance-max-gt [_]
    (set! max-gt (inc ^long max-gt)))

  (max-aid [_]
    max-aid)

  (schema [_]
    schema)

  (set-schema [_ new-schema]
    (set! schema (init-schema lmdb new-schema))
    (set! attrs (init-attrs schema))
    schema)

  (classes [_]
    classes)

  (set-classes [_ new-classes]
    (transact-classes lmdb new-classes)
    (set! classes (merge classes new-classes))
    classes)

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
        i      (b/indexable (.-e d) (:db/aid props) (.-v d)
                            (:db/valueType props))
        giant? (b/giant? i)
        gt     (when giant?
                 (lmdb/get-value (.-lmdb store) c/eav i :eav :id))]
    (cond-> [[:del c/eav i :eav]
             [:del c/ave i :ave]]
      ref? (conj [:del c/vea i :vea])
      gt   (conj [:del c/giants gt :id]))))

(defn- handle-datom
  [store holder datom]
  (if (d/datom-added datom)
    (let [[data giant?] (insert-datom store datom)]
      (when giant? (advance-max-gt store))
      (reduce conj! holder data))
    (reduce conj! holder (delete-datom store datom))))

(defn- entity-aids
  "Return the set of attribute ids of an entity"
  [^Store store eid]
  (let [lmdb   (.-lmdb store)
        schema (schema store)
        datom  (d/datom eid nil nil)]
    (into #{}
          (map first)
          (lmdb/get-range lmdb
                          c/eav
                          [:closed
                           (datom->indexable schema datom false)
                           (datom->indexable schema datom true)]
                          :eav-a
                          :ignore))))

(defn entity-attrs
  "Return the set of attributes of an entity"
  [^Store store eid]
  (set (map (attrs store) (entity-aids store eid))))

(defn- del-attr
  [old-attrs del-attrs schema]
  (reduce (fn [s a]
            (if (= (:db/cardinality (schema a)) :db.cardinality/many)
              s
              (set/difference s #{a})))
          old-attrs
          del-attrs))

(defn- attr->aid
  [schema attr]
  (some-> attr schema :db/aid))

(defn- attrs->aids
  [schema attrs]
  (->> attrs
       (map (partial attr->aid schema))
       sort
       b/bitmap))

(defn- entity-class
  [new-cls ^Store store cur-eid add-attrs del-attrs]
  (let [schema    (schema store)
        old-attrs (entity-attrs store cur-eid)
        new-attrs (cond-> old-attrs
                    (seq del-attrs) (del-attr del-attrs schema)
                    (seq add-attrs) (set/union add-attrs))]
    (if (= new-attrs old-attrs)
      new-cls
      (let [old-aids  (attrs->aids schema old-attrs)
            new-aids  (attrs->aids schema new-attrs)
            classes   (classes store)
            old-props (get classes old-aids)
            new-props (get classes new-aids)]
        (cond-> new-cls
          ;; old-props (assoc! old-aids (update old-props :eids
          ;;                                    #(b/bitmap-del % cur-eid)))
          true      (assoc! new-aids (update new-props :eids
                                             (fnil #(b/bitmap-add % cur-eid)
                                                   (b/bitmap)))))))))

(defn- collect-classes
  [^Store store batch]
  (persistent!
    (loop [new-cls   (transient {})
           cur-eid   nil
           add-attrs #{}
           del-attrs #{}
           remain    batch]
      (if-let [^Datom datom (first remain)]
        (let [eid  (.-e datom)
              attr (.-a datom)
              add? (d/datom-added datom)
              add  #(if add? (conj % attr) %)
              del  #(if-not add? (conj % attr) %)
              rr   (rest remain)]
          (or ((schema store) attr) (swap-attr store attr identity))
          (if (= cur-eid eid)
            (recur new-cls cur-eid (add add-attrs) (del del-attrs) rr)
            (if cur-eid
              (recur (entity-class new-cls store cur-eid add-attrs del-attrs)
                     eid (add #{}) (del #{}) rr)
              (recur new-cls eid (add add-attrs) (del del-attrs) rr))))
        (entity-class new-cls store cur-eid add-attrs del-attrs)))))

(defn- transact-datoms
  [^Store store batch]
  (lmdb/transact-kv (.-lmdb store)
                    (persistent!
                      (reduce (partial handle-datom store) (transient []) batch))))

(defn- load-batch
  [^Store store batch]
  (let [batch (sort-by d/datom-e batch)]
    (transact-classes (.-lmdb store) (collect-classes store batch))
    (transact-datoms store batch)))

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
                                         (datom->indexable schema datom false)
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
                     (datom->indexable schema low-datom false)
                     (datom->indexable schema high-datom true)]
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
                       (datom->indexable schema low-datom false)
                       (datom->indexable schema high-datom true)]
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
                                  (datom->indexable schema low-datom false)
                                  (datom->indexable schema high-datom true)]
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
             (datom->indexable schema low-datom false)
             (datom->indexable schema high-datom true)]
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
             (datom->indexable schema high-datom true)
             (datom->indexable schema low-datom false)]
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
                              (datom->indexable schema low-datom false)
                              (datom->indexable schema high-datom true)]
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
                                 (datom->indexable schema low-datom false)
                                 (datom->indexable schema high-datom true)]
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
         (datom->indexable schema low-datom false)
         (datom->indexable schema high-datom true)]
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
         (datom->indexable schema high-datom true)
         (datom->indexable schema low-datom false)]
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
     (lmdb/open-dbi lmdb c/classes c/+max-key-size+)
     (let [schema' (init-schema lmdb schema)]
       (->Store lmdb
                schema'
                (load-classes lmdb)
                (init-attrs schema')
                (init-max-aid lmdb)
                (init-max-gt lmdb))))))
