(ns ^:no-doc datalevin.storage
  "Storage layer of Datalog store"
  (:require [datalevin.lmdb :as lmdb]
            [datalevin.util :as u]
            [datalevin.bits :as b]
            [datalevin.search :as s]
            [datalevin.constants :as c]
            [datalevin.datom :as d]
            [clojure.set :as set]
            [taoensso.timbre :as log]
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

(defn- transact-schema
  [lmdb schema]
  (lmdb/transact-kv
    lmdb
    (conj (for [[attr props] schema]
            [:put c/schema attr props :attr :data])
          [:put c/meta :last-modified (System/currentTimeMillis) :attr :long])))

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
  (into {} (lmdb/get-range lmdb c/classes [:all] :id :data)))



(defn- classes->rclasses
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

(defn- init-entities
  [lmdb]
  (into {} (lmdb/get-range lmdb c/entities [:all] :id :id)))

(defn- entities->rentities
  [entities]
  (reduce-kv
    (fn [m eid cid]
      (assoc m cid (b/bitmap-add (get m cid (b/bitmap)) eid)))
    {} entities))

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
  (rclasses [this] "Return the aid -> classes map")
  (add-classes [this new-classes] "Add new classes, return updated classes")
  (max-cid [this])
  (advance-max-cid [this])
  (entities [this])
  (rentities [this])
  (update-entities [this new-entities del-entities])
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

  (classes [_]
    classes)

  (rclasses [_]
    rclasses)

  (add-classes [_ new-classes]
    (set! classes (merge classes new-classes))
    classes)

  (max-cid [_]
    max-cid)

  (advance-max-cid [_]
    (set! max-cid (inc ^long max-cid)))

  (entities [_]
    entities)

  (rentities [_]
    rentities)

  (update-entities [_ new-entities del-entities])

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
      ;; TODO remove this tx, return tx-data instead
      (transact-schema lmdb s)
      (set! schema (assoc schema attr p))
      (set! rschema (schema->rschema rschema s))
      (set! attrs (assoc attrs (p :db/aid) attr))
      p))

  (load-datoms [this datoms]
    (locking this
      (let [;; fulltext datoms, [:a d] or [:d d]
            ft-ds (volatile! (transient []))
            ;; touched entity ids
            eids  (volatile! (transient #{}))]
        (doseq [batch (partition-all c/+tx-datom-batch-size+ datoms)
                #_    (partition c/+tx-datom-batch-size+
                                 c/+tx-datom-batch-size+
                                 nil
                                 datoms)]
          (transact-datoms this ft-ds eids batch))
        ;; (update-entity-classes this (persistent @eids))
        (doseq [[op ^Datom d] (persistent! @ft-ds)
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
  (let [attr  (.-a d)
        props (or ((schema store) attr)
                  (swap-attr store attr identity))
        ref?  (= :db.type/ref (:db/valueType props))
        i     (b/indexable (.-e d) (:db/aid props) (.-v d)
                           (:db/valueType props))]
    (when (:db/fulltext props) (vswap! ft-ds conj! [:a d]))
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
        ref?   (= :db.type/ref (:db/valueType props))
        i      (b/indexable (.-e d) (:db/aid props) (.-v d)
                            (:db/valueType props))
        giant? (b/giant? i)
        gt     (when giant?
                 (lmdb/get-value (.-lmdb store) c/eav i :eav :id))]
    (when (:db/fulltext props) (vswap! ft-ds conj! [:d d]))
    (cond-> [[:del c/eav i :eav]
             [:del c/ave i :ave]]
      ref? (conj [:del c/vea i :vea])
      gt   (conj [:del c/giants gt :id]))))

(defn- handle-datom
  [store ft-ds eids holder datom]
  (vswap! eids conj! (d/datom-e datom))
  (if (d/datom-added datom)
    (reduce conj! holder (insert-datom store datom ft-ds))
    (reduce conj! holder (delete-datom store datom ft-ds))))

(defn- transact-datoms
  [^Store store ft-ds eids batch]
  (lmdb/transact-kv
    (.-lmdb store)
    (persistent!
      (conj!
        (reduce (partial handle-datom store ft-ds eids) (transient []) batch)
        [:put c/meta :last-modified (System/currentTimeMillis) :attr :long]))))

(defn- entity-aids
  "Return the set of attribute ids of an entity"
  [^Store store eid]
  (let [schema (schema store)
        datom  (d/datom eid nil nil)
        aids   (volatile! (transient #{}))
        add    #(vswap! aids conj! (b/read-buffer (lmdb/k %) :eav-a))]
    (lmdb/visit (.-lmdb store) c/eav add
                [:closed
                 (datom->indexable schema datom false)
                 (datom->indexable schema datom true)]
                :eav-a)
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
  (aids->attrs store (entity-aids store eid)))

(defn find-classes
  [^Store store aids]
  (when (seq aids)
    (let [rclasses (rclasses store)]
      (reduce (fn [cs new-cs]
                (let [cs' (set/intersection cs new-cs)]
                  (if (seq cs')
                    cs'
                    (reduced nil))))
              (map rclasses aids)))))

(defn- collect-updates
  [store new-classes new-entities del-entities eid]
  (let [my-aids (entity-aids store eid)]
    (if (empty? my-aids)
      (vswap! del-entities conj! eid)                   ; non-existent eid
      (let [num-cids (count (find-classes store my-aids))
            new-cid  (some (fn [[cid aids]]
                             (when (= aids my-aids) cid))
                           @new-classes)]
        (cond
          (and (zero? num-cids) (nil? new-cid))         ; unseen class
          (let [cid (max-cid store)]
            (vswap! new-classes assoc! cid my-aids)
            (advance-max-cid store)
            (vswap! new-entities assoc! eid cid))
          )))))

(defn- transact-entity-classes
  [lmdb new-classes new-entities del-entities]
  (lmdb/transact-kv lmdb (for [[cid aids] new-classes]
                           [:put c/classes cid aids :id :data]))
  (lmdb/transact-kv lmdb (for [[eid cid] new-entities]
                           [:put c/entities eid cid :id :id]))
  (lmdb/transact-kv lmdb (for [[cid props] new-classes]
                           [:put c/classes cid props :id :data])))

(defn- update-entity-classes
  [^Store store eids]
  (let [new-classes  (volatile! (transient {}))
        new-entities (volatile! (transient {}))
        del-entities (volatile! (transient #{}))]
    (doseq [eid eids]
      (collect-updates store new-classes new-entities del-entities eid))
    (transact-entity-classes store
                             (persistent! @new-classes)
                             (persistent! @new-entities)
                             (persistent! @del-entities))))

;; (defn- del-attr
;;   [old-attrs del-attrs schema]
;;   (reduce (fn [s a]
;;             (if (= (:db/cardinality (schema a)) :db.cardinality/many)
;;               s
;;               (set/difference s #{a})))
;;           old-attrs
;;           del-attrs))

;; (defn- attr->aid
;;   [schema attr]
;;   (some-> attr schema :db/aid))

;; (defn- attrs->aids
;;   [schema attrs]
;;   (into #{} (map (partial attr->aid schema) attrs)))


#_(defn- entity-class
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
            old-props (assoc! old-aids (update old-props :eids
                                               #(b/bitmap-del % cur-eid)))
            true      (assoc! new-aids (update new-props :eids
                                               (fnil #(b/bitmap-add % cur-eid)
                                                     (b/bitmap)))))))))

#_(defn- collect-classes
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

(defn- transact-opts
  [lmdb opts]
  (lmdb/transact-kv
    lmdb
    (conj (for [[k v] opts]
            [:put c/opts k v :attr :data])
          [:put c/meta :last-modified (System/currentTimeMillis) :attr :long])))

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
  (lmdb/open-dbi lmdb c/entities c/+id-bytes+ c/+id-bytes+)
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
  ([dir schema opts]
   (let [dir  (or dir (u/tmp-dir (str "datalevin-" (UUID/randomUUID))))
         lmdb (lmdb/open-kv dir)]
     (open-dbis lmdb)
     (when opts (transact-opts lmdb opts))
     (let [schema   (init-schema lmdb schema)
           classes  (init-classes lmdb)
           entities (init-entities lmdb)]
       (->Store lmdb
                (load-opts lmdb)
                (s/new-search-engine lmdb (:search-engine opts))
                (init-attrs schema)
                schema
                (schema->rschema schema)
                classes
                (classes->rclasses classes)
                entities
                (entities->rentities entities)
                (init-max-aid lmdb)
                (init-max-gt lmdb)
                (init-max-cid lmdb))))))
