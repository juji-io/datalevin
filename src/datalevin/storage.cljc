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

(defn schema->rschema [schema]
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

(defn- transact-schema
  [lmdb schema]
  (lmdb/transact-kv lmdb (conj (for [[attr props] schema]
                                 [:put c/schema attr props :attr :data])
                               [:put c/meta :last-modified
                                (System/currentTimeMillis) :attr :long])))

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
  (classes [this] "Return the classes map")
  (set-classes [this new-classes]
    "Update the classes, return updated classes")
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

(declare load-batch)

(deftype Store [db-name
                lmdb
                search-engine
                ^:volatile-mutable schema
                ^:volatile-mutable rschema
                ^:volatile-mutable attrs    ; aid -> attr
                ^:volatile-mutable max-aid
                ^:volatile-mutable max-gt]
  IStore

  (db-name [_] db-name)

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
      (set! rschema (schema->rschema schema))
      (set! attrs (assoc attrs (:db/aid p) attr))
      p))

  (datom-count [_ index]
    (lmdb/entries lmdb (if (string? index) index (index->dbi index))))

  (load-datoms [this datoms]
    (locking this
      (let [ft-ds (volatile! [])] ; fulltext datoms
        (doseq [batch (partition c/+tx-datom-batch-size+
                                 c/+tx-datom-batch-size+
                                 nil
                                 datoms)]
          (load-batch this ft-ds batch))
        (doseq [[op ^Datom d] @ft-ds
                :let          [v (str (.-v d))]]
          (case op
            :a (s/add-doc search-engine d v)
            :d (s/remove-doc search-engine d))))))

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
  (let [attr   (.-a d)
        props  (or ((schema store) attr)
                   (swap-attr store attr identity))
        ref?   (= :db.type/ref (:db/valueType props))
        i      (b/indexable (.-e d) (:db/aid props) (.-v d)
                            (:db/valueType props))
        max-gt (max-gt store)]
    (when (:db/fulltext props) (vswap! ft-ds conj [:a d]))
    (if (b/giant? i)
      [(cond-> [[:put c/eav i max-gt :eav :id]
                [:put c/ave i max-gt :ave :id]
                [:put c/giants max-gt d :id :datom [:append]]]
         ref? (conj [:put c/vea i max-gt :vea :id]))
       true]
      [(cond-> [[:put c/eav i c/normal :eav :id]
                [:put c/ave i c/normal :ave :id]]
         ref? (conj [:put c/vea i c/normal :vea :id]))
       false])))

(defn- delete-datom
  [^Store store ^Datom d ft-ds]
  (let [props  ((schema store) (.-a d))
        ref?   (= :db.type/ref (:db/valueType props))
        i      (b/indexable (.-e d) (:db/aid props) (.-v d)
                            (:db/valueType props))
        giant? (b/giant? i)
        gt     (when giant?
                 (lmdb/get-value (.-lmdb store) c/eav i :eav :id))]
    (when (:db/fulltext props) (vswap! ft-ds conj [:d d]))
    (cond-> [[:del c/eav i :eav]
             [:del c/ave i :ave]]
      ref? (conj [:del c/vea i :vea])
      gt   (conj [:del c/giants gt :id]))))

(defn- handle-datom
  [store ft-ds holder datom]
  (if (d/datom-added datom)
    (let [[data giant?] (insert-datom store datom ft-ds)]
      (when giant? (advance-max-gt store))
      (reduce conj! holder data))
    (reduce conj! holder (delete-datom store datom ft-ds))))

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
          true (assoc! new-aids (update new-props :eids
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
  [^Store store ft-ds batch]
  (lmdb/transact-kv
    (.-lmdb store)
    (conj (persistent!
            (reduce (partial handle-datom store ft-ds) (transient []) batch))
          [:put c/meta :last-modified
           (System/currentTimeMillis) :attr :long])))

(defn- load-batch
  [^Store store ft-ds batch]
  (let [batch (sort-by d/datom-e batch)]
    ;; (transact-classes (.-lmdb store) (collect-classes store batch))
    (transact-datoms store ft-ds batch)))

(defn open
  "Open and return the storage."
  ([]
   (open nil nil))
  ([dir]
   (open dir nil))
  ([dir schema]
   (open dir schema nil))
  ([dir schema db-name]
   (let [dir  (or dir (u/tmp-dir (str "datalevin-" (UUID/randomUUID))))
         lmdb (lmdb/open-kv dir)]
     (lmdb/open-dbi lmdb c/eav c/+max-key-size+ c/+id-bytes+)
     (lmdb/open-dbi lmdb c/ave c/+max-key-size+ c/+id-bytes+)
     (lmdb/open-dbi lmdb c/vea c/+max-key-size+ c/+id-bytes+)
     (lmdb/open-dbi lmdb c/giants c/+id-bytes+)
     (lmdb/open-dbi lmdb c/schema c/+max-key-size+)
     (lmdb/open-dbi lmdb c/meta c/+max-key-size+)
     (let [schema (init-schema lmdb schema)]
       (->Store db-name
                lmdb
                (s/new-search-engine lmdb)
                schema
                (schema->rschema schema)
                (init-attrs schema)
                (init-max-aid lmdb)
                (init-max-gt lmdb))))))
