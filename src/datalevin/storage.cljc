(ns ^:no-doc datalevin.storage
  "Storage layer of Datalog store"
  (:require [datalevin.lmdb :as lmdb]
            [datalevin.util :as u]
            [datalevin.relation :as r]
            [datalevin.bits :as b]
            [datalevin.search :as s]
            [datalevin.constants :as c]
            [datalevin.datom :as d]
            ;; [taoensso.timbre :as log]
            [clojure.set :as set]
            [datalevin.lmdb :as l])
  (:import [java.util UUID ArrayList]
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

(defn- attr->aid [schema attr] (-> attr schema :db/aid))

(defn- attrs->aids
  ([schema attrs]
   (attrs->aids #{} schema attrs))
  ([old-aids schema attrs]
   (into old-aids (map (partial attr->aid schema)) attrs)))

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

(defn- init-max-cid [lmdb] (lmdb/entries lmdb c/encla))

(defn- init-max-aid
  [schema]
  (inc ^long (apply max (map :db/aid (vals schema)))))

;; TODO schema migration
(defn- update-schema
  [old schema]
  (let [aid (volatile! (dec ^long (init-max-aid old)))]
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
    (transact-schema lmdb (update-schema (load-schema lmdb) schema)))
  (let [now (load-schema lmdb)]
    (when-not (:db/created-at now)
      (transact-schema lmdb (update-schema now c/entity-time-schema))))
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

(defn- find-classes
  [rclasses aids]
  (when (seq aids)
    (reduce (fn [cs new-cs]
              (let [cs' (set/intersection cs new-cs)]
                (if (seq cs')
                  cs'
                  (reduced nil))))
            (map rclasses aids))))

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
  [schema max-gt ^Datom d high?]
  (let [e  (.-e d)
        am (if high? c/amax c/a0)
        vm (if high? c/vmax c/v0)]
    (if-let [a (.-a d)]
      (if-let [p (schema a)]
        (if-some [v (.-v d)]
          (b/indexable e (:db/aid p) v (:db/valueType p) max-gt)
          (b/indexable e (:db/aid p) vm (:db/valueType p) max-gt))
        (b/indexable e c/a0 c/v0 nil max-gt))
      (if-some [v (.-v d)]
        (if (integer? v)
          (if e
            (b/indexable e am v :db.type/ref max-gt)
            (b/indexable (if high? c/emax c/e0) am v :db.type/ref max-gt))
          (u/raise "When v is known but a is unknown, v must be a :db.type/ref"
                   {:v v}))
        (b/indexable e am vm :db.type/sysMin max-gt)))))

(defn- index->dbi
  [index]
  (case index
    :eavt c/eav
    :eav  c/eav
    :avet c/ave
    :ave  c/ave))

(defn- index->vtype
  [index]
  (case index
    :eavt :avg
    :eav  :avg
    :avet :veg
    :ave  :veg))

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
      :a (s/add-doc search-engine d v true)
      :d (s/remove-doc search-engine d true))))

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
  (rlinks [this])
  (swap-attr [this attr f] [this attr f x] [this attr f x y]
    "Update an attribute, f is similar to that of swap!")
  (del-attr [this attr]
    "Delete an attribute, throw if there is still datom related to it")
  (load-datoms [this datoms] "Load datams into storage")
  (populated? [this index low-datom high-datom]
    "Return true if there exists at least one datom in the given boundary
     (inclusive)")
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
  (scan-ref-v [this veid] "Return ref type datoms with given v")
  (pivot-scan [this esym sym->attr] [this esym sym->attr pred]
    "Return a relation of those entities belonging to the enclas defined by the
     vals of sym->attr, with pred applied if given")
  (cardinality [this attrs] [this attrs pred]
    "Return the estimated sum of cardinality of the enclas defined by the attrs,
     with pred applied through sampling if given.")
  (bounded-pivot-scan
    [this esym sym->attr bindings] [this esym sym->attr bindings pred]
    "Return a relation of those entities belonging to the enclas defined by the
     vals of sym->attr, with some values bounded, pred applied if given")
  (bounded-cardinality [this attrs bindings] [this attrs bindings pred]
    "Return the sum of cardinality of the enclas defined by the attrs,
     where some values are bounded.")
  (index-join [this esym sym->attr rel] [this esym esym->attr rel pred]
    "Return a relation that is the join of the enclas defined by the
     attrs and the given relation.")
  (index-join-cardinality [this attrs rel] [this attrs rel pred]
    "Return the cardinality of the joins between the enclas defined by the
     attrs and the given relation."))

(declare transact-datoms update-encla update-links pivot-scan*
         cardinality*)

(deftype Store [lmdb
                opts
                search-engine
                ^:volatile-mutable attrs     ; aid -> attr
                ^:volatile-mutable refs      ; set of ref aids
                ^:volatile-mutable schema    ; attr -> props
                ^:volatile-mutable rschema   ; prop -> attrs
                ^:volatile-mutable classes   ; cid -> aid -> cardinality
                ^:volatile-mutable rclasses  ; aid -> cids
                ^:volatile-mutable entities  ; eid -> cid
                ^:volatile-mutable rentities ; cid -> eids bitmap
                ^:volatile-mutable links     ; link -> cardinality
                ^:volatile-mutable rlinks    ; vae -> link
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
    (set! max-aid (init-max-aid schema))
    schema)

  (attrs [_]
    attrs)

  (refs [_]
    refs)

  (init-max-eid [_]
    (or (when-let [[e _] (lmdb/get-first lmdb c/eav [:all-back] :id :avg)]
          e)
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

  (rlinks [_]
    rlinks)

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

  (del-attr [this attr]
    (if (populated? this :ave (d/datom c/e0 attr c/v0)
                    (d/datom c/emax attr c/vmax))
      (u/raise "Cannot delete attribute with datoms" {})
      (let [aid (:db/aid (schema attr))]
        (lmdb/transact-kv lmdb [[:del c/schema attr :attr]])
        (set! schema (dissoc schema attr))
        (set! rschema (schema->rschema schema))
        (set! attrs (dissoc attrs aid))
        attrs)))

  (load-datoms [this datoms]
    (locking (lmdb/write-txn lmdb)
      (try
        (let [v-classes   (volatile! classes)
              v-rclasses  (volatile! rclasses)
              v-entities  (volatile! entities)
              v-rentities (volatile! rentities)
              v-max-cid   (volatile! max-cid)
              v-links     (volatile! links)
              v-rlinks    (volatile! (transient rlinks))
              ft-ds       (volatile! (transient []))
              del-ref-ds  (volatile! (transient #{}))]
          (lmdb/open-transact-kv lmdb)
          (-> (transact-datoms this ft-ds del-ref-ds datoms)
              (update-encla lmdb schema max-gt refs v-classes v-rclasses
                            v-entities v-rentities v-max-cid)
              (update-links lmdb @v-entities v-links rlinks v-rlinks
                            (persistent! @del-ref-ds)))
          (lmdb/transact-kv lmdb [(time-tx)])
          (fulltext-index search-engine (persistent! @ft-ds))
          (set! classes @v-classes)
          (set! rclasses @v-rclasses)
          (set! entities @v-entities)
          (set! rentities @v-rentities)
          (set! max-cid @v-max-cid)
          (set! links @v-links)
          (set! rlinks (persistent! @v-rlinks)))
        (catch clojure.lang.ExceptionInfo e
          (if (:resized (ex-data e))
            (load-datoms this datoms)
            (throw e)))
        (finally (lmdb/close-transact-kv lmdb)))))

  (populated? [_ index low-datom high-datom]
    (lmdb/get-first lmdb (index->dbi index)
                    [:closed
                     (datom->indexable schema c/g0 low-datom false)
                     (datom->indexable schema c/gmax high-datom true)]
                    index :ignore true))

  (size [_ index low-datom high-datom]
    (lmdb/range-count lmdb (index->dbi index)
                      [:closed
                       (datom->indexable schema c/g0 low-datom false)
                       (datom->indexable schema c/gmax high-datom true)]
                      index))

  (head [_ index low-datom high-datom]
    (retrieved->datom
      lmdb attrs (lmdb/get-first
                   lmdb (index->dbi index)
                   [:closed
                    (datom->indexable schema c/g0 low-datom false)
                    (datom->indexable schema c/gmax high-datom true)]
                   index :id)))

  (tail [_ index high-datom low-datom]
    (retrieved->datom
      lmdb attrs (lmdb/get-first lmdb (index->dbi index)
                                 [:closed-back
                                  (datom->indexable schema c/g0 high-datom true)
                                  (datom->indexable schema c/gmax low-datom false)]
                                 index :id)))

  (slice [_ index low-datom high-datom]
    (mapv (partial retrieved->datom lmdb attrs)
          (lmdb/get-range lmdb (index->dbi index)
                          [:closed
                           (datom->indexable schema c/g0 low-datom false)
                           (datom->indexable schema c/gmax high-datom true)]
                          index :id)))

  (rslice [_ index high-datom low-datom]
    (mapv (partial retrieved->datom lmdb attrs)
          (lmdb/get-range lmdb (index->dbi index)
                          [:closed-back
                           (datom->indexable schema c/g0 high-datom true)
                           (datom->indexable schema c/gmax low-datom false)]
                          index :id)))

  (size-filter [_ index pred low-datom high-datom]
    (lmdb/range-filter-count lmdb (index->dbi index)
                             (datom-pred->kv-pred lmdb attrs index pred)
                             [:closed
                              (datom->indexable schema c/g0 low-datom false)
                              (datom->indexable schema c/gmax high-datom true)]
                             index))

  (head-filter [_ index pred low-datom high-datom]
    (retrieved->datom
      lmdb attrs (lmdb/get-some lmdb (index->dbi index)
                                (datom-pred->kv-pred lmdb attrs index pred)
                                [:closed
                                 (datom->indexable schema c/g0 low-datom false)
                                 (datom->indexable schema c/gmax high-datom true)]
                                index :id)))

  (tail-filter [_ index pred high-datom low-datom]
    (retrieved->datom
      lmdb attrs (lmdb/get-some lmdb (index->dbi index)
                                (datom-pred->kv-pred lmdb attrs index pred)
                                [:closed-back
                                 (datom->indexable schema c/g0 high-datom true)
                                 (datom->indexable schema c/gmax low-datom false)]
                                index :id)))

  (slice-filter [_ index pred low-datom high-datom]
    (mapv
      (partial retrieved->datom lmdb attrs)
      (lmdb/range-filter lmdb (index->dbi index)
                         (datom-pred->kv-pred lmdb attrs index pred)
                         [:closed
                          (datom->indexable schema c/g0 low-datom false)
                          (datom->indexable schema c/gmax high-datom true)]
                         index :id)))

  (rslice-filter
    [_ index pred high-datom low-datom]
    (mapv
      (partial retrieved->datom lmdb attrs)
      (lmdb/range-filter lmdb (index->dbi index)
                         (datom-pred->kv-pred lmdb attrs index pred)
                         [:closed
                          (datom->indexable schema c/g0 high-datom true)
                          (datom->indexable schema c/gmax low-datom false)]
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

  (pivot-scan
    [this esym sym->attr]
    (.pivot-scan this esym sym->attr nil))
  (pivot-scan
    [this esym sym->attr pred]
    (let [pairs (sort-by peek (map (fn [[sym attr]]
                                     [sym (attr->aid schema attr)])
                                   sym->attr))
          n     (count pairs)
          aids  (mapv peek pairs)
          al    (attrs (first aids))
          ah    (attrs (peek aids))]
      (when-let [cids (find-classes rclasses (set aids))]
        (r/->Relation
          (zipmap (conj (map first pairs) esym) (range))
          (reduce
            (fn [tuples cid]
              (reduce
                (fn [tuples eid]
                  (pivot-scan* this tuples pred eid aids n al ah))
                tuples (rentities cid)))
            [] cids)))))

  (cardinality
    [this attrs]
    (.cardinality this attrs nil))
  (cardinality
    [this attrs pred]
    (let [aids   (map (partial attr->aid schema) attrs)
          a-freq (frequencies aids)]
      (if-let [cs (find-classes rclasses aids)]
        (transduce (map (partial cardinality* this a-freq pred)) + cs)
        0.0))))

(defn- pred-factor
  [store pred bm]
  1.0)

(def *sampling?* true)

(defn- cardinality*
  [store a-freq pred cid]
  (let [factors ((classes store) cid)
        scores  (map (fn [[aid freq]] (* freq (factors aid))) a-freq)
        bm      ((rentities store) cid)
        base    (apply * (b/bitmap-size bm) scores)]
    (if pred
      (* base (if *sampling?*
                (pred-factor store pred bm)
                c/+magic-factor+))
      base)))

(defn- attr-values->tuples
  "tuples are basically cartesian product of attribute values"
  [attr-values eid ^long n]
  (let [orig-values (vec attr-values)]
    (letfn [(fill [vs]
              (let [^"[Ljava.lang.Object;" tuple (make-array Object (inc n))]
                (aset tuple 0 eid)
                (dotimes [i n] (aset tuple (inc i) (first (vs i))))
                tuple))
            (step [values]
              (let [increment
                    (fn [vs]
                      (loop [i (dec (count vs)) vs vs]
                        (when-not (= i -1)
                          (if-let [rst (next (vs i))]
                            (assoc vs i rst)
                            (recur (dec i) (assoc vs i (orig-values i)))))))]
                (when values
                  (cons (fill values) (step (increment values))))))]
      (when (every? seq attr-values) (step orig-values)))))

(defn- convert-values
  [lmdb ^ArrayList values pred n]
  (dotimes [i n]
    (let [^ArrayList vs (.get values i)]
      (dotimes [j (.size vs)]
        (let [v (.get vs j)]
          (if (d/datom? v)
            (.set vs j (.-v ^Datom v))
            (let [d (lmdb/get-value lmdb c/giants v :id :datom)]
              (if (or (not pred) (pred d))
                (.set vs j (.-v ^Datom d))
                (.set vs j nil))))))
      (.set values i (remove nil? vs)))))

(defn- pivot-scan*
  [^Store store tuples pred eid aids n al ah]
  (let [lmdb    (.-lmdb store)
        schema  (schema store)
        max-gt  (max-gt store)
        attrs   (attrs store)
        values  (ArrayList.)
        i       (volatile! 0)
        add     (fn [aid kv eav]
                  (let [vb  (lmdb/v kv)
                        v   (b/read-buffer vb :id)
                        d   (if (= v c/normal)
                              (d/datom eid (attrs aid) (b/get-value eav 1))
                              v)
                        dt? (d/datom? d)]
                    (when (or (not pred) (not dt?) (pred d))
                      (loop [j @i]
                        (when (and (< j n) (= aid (nth aids j)))
                          (.add ^ArrayList (.get values j) d)
                          (recur (inc j)))))))
        advance (fn [aid kv eav]
                  (loop [j @i]
                    (when (< j n)
                      (let [cur-aid (nth aids j)]
                        (if (= aid cur-aid)
                          (do (vreset! i j) (add aid kv eav))
                          (when (> aid cur-aid) (recur (inc j))))))))
        collect (fn [kv]
                  (let [eav     (lmdb/k kv)
                        aid     (b/read-buffer eav :eav-a)
                        cur-aid (nth aids @i)]
                    (cond
                      (= aid cur-aid) (add aid kv eav)
                      (> aid cur-aid) (advance aid kv eav)
                      :else           :skip)))]
    (dotimes [_ n] (.add values (ArrayList.)))
    (lmdb/visit lmdb c/eav collect
                [:open
                 (datom->indexable schema max-gt (d/datom eid al nil) false)
                 (datom->indexable schema max-gt (d/datom eid ah nil) true)] :eav-a)
    (convert-values lmdb values pred n)
    (into tuples (attr-values->tuples values eid n))))

(defn- insert-datom
  [^Store store ^Datom d ft-ds]
  (let [[e attr v] (d/datom-eav d)
        {:keys [db/valueType db/aid db/fulltext]}
        (or ((schema store) attr) (swap-attr store attr identity))
        max-gt     (max-gt store)
        i          (b/indexable e aid v valueType max-gt)]
    (or (not (:validate-data? (.-opts store)))
        (b/valid-data? v valueType)
        (u/raise "Invalid data, expecting " valueType {:input v}))
    (when fulltext (vswap! ft-ds conj! [:a d]))
    (if (b/giant? i)
      (do (advance-max-gt store)
          [[:put c/eav e i :id :avg]
           [:put c/ave aid i :short-id :veg]
           [:put c/giants max-gt d :id :datom [:append]]])
      [[:put c/eav e i :id :avg]
       [:put c/ave aid i :short-id :veg]])))

(defn- delete-datom
  [^Store store ^Datom d ft-ds del-ref-ds]
  (let [[e attr v] (d/datom-eav d)
        {:keys [db/valueType db/aid db/fulltext]}
        ((schema store) attr)
        i          (b/indexable e aid v valueType (max-gt store))
        gt         (when (b/giant? i)
                     )]
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
  [lmdb schema max-gt refs cur-ref-ds eid]
  (let [aid-counts (volatile! {})
        datom      (d/datom eid nil nil)]
    (lmdb/visit lmdb c/eav
                #(let [eav (lmdb/k %)
                       aid (b/read-buffer eav :eav-a)]
                   (vswap! aid-counts update aid (fnil inc 0.0))
                   (when (refs aid)
                     (vswap! cur-ref-ds conj! [(b/get-value eav 1) aid eid])))
                [:open
                 (datom->indexable schema max-gt datom false)
                 (datom->indexable schema max-gt datom true)]
                :eav-a true)
    @aid-counts))

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
  [eids lmdb schema max-gt refs classes rclasses entities rentities max-cid]
  (let [cur-ref-ds   (volatile! (transient #{}))
        updated-cids (volatile! (transient #{}))]
    (doseq [eid  eids
            :let [aid-counts (scan-entity lmdb max-gt schema refs cur-ref-ds eid)]]
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
  [cur-ref-ds lmdb entities links old-rlinks new-rlinks del-ref-ds]
  (let [to-del (volatile! {})
        to-add (volatile! {})
        conj*  (fnil conj [])]
    (doseq [[v a e :as vae] cur-ref-ds
            :let            [ecid (entities e)
                             vcid (entities v)
                             new-link [vcid a ecid]]
            :when           (and vcid ecid)]
      (vswap! new-rlinks assoc! vae new-link)
      (if-let [old-link (old-rlinks vae)]
        (when (not= old-link new-link)
          (vswap! to-del update old-link conj* [v e])
          (vswap! to-add update new-link conj* [v e]))
        (vswap! to-add update new-link conj* [v e])))
    (doseq [[v _ e :as vae] del-ref-ds
            :when           (not (cur-ref-ds vae))]
      (vswap! new-rlinks dissoc! vae)
      (when-let [old-link (old-rlinks vae)]
        (vswap! to-del update old-link conj* [v e])))
    (doseq [[link lst] @to-add]
      (vswap! links update link (fnil #(+ ^long % (count lst)) 0))
      (lmdb/put-list-items lmdb c/links link lst :int-int-int :long-long))
    (doseq [[link lst] @to-del]
      (let [r (- ^long (@links link) (count lst))]
        (if (< 0 r)
          (vswap! links assoc link r)
          (vswap! links dissoc link)))
      (lmdb/del-list-items lmdb c/links link lst :int-int-int :long-long))))

(defn- init-links
  [lmdb]
  (let [links  (volatile! {})
        rlinks (persistent!
                 (reduce
                   (fn [m [[_ aid _ :as link] [veid eeid]]]
                     (vswap! links update link (fnil inc 0))
                     (assoc! m [veid aid eeid] link))
                   (transient {})
                   (lmdb/get-range lmdb c/links [:all]
                                   :int-int-int :long-long)))]
    [@links rlinks]))

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
  (lmdb/open-list-dbi lmdb c/eav {:key-size c/+id-bytes+
                                  :val-size c/+max-key-size+})
  (lmdb/open-list-dbi lmdb c/ave {:key-size c/+short-id-bytes+
                                  :val-size c/+max-key-size+})
  (lmdb/open-list-dbi lmdb c/links {:key-size (* 3 c/+short-id-bytes+)
                                    :val-size (* 2 c/+id-bytes+)})
  (lmdb/open-dbi lmdb c/giants {:key-size c/+id-bytes+})
  (lmdb/open-dbi lmdb c/encla {:key-size c/+id-bytes+})
  (lmdb/open-dbi lmdb c/schema {:key-size c/+max-key-size+})
  (lmdb/open-dbi lmdb c/meta {:key-size c/+max-key-size+})
  (lmdb/open-dbi lmdb c/opts {:key-size c/+max-key-size+}))

(defn open
  "Open and return the storage."
  ([]
   (open nil nil))
  ([dir]
   (open dir nil))
  ([dir schema]
   (open dir schema nil))
  ([dir schema {:keys [kv-opts search-opts validate-data? auto-entity-time?]
                :or   {validate-data?    false
                       auto-entity-time? false}
                :as   opts}]
   (let [dir  (or dir (u/tmp-dir (str "datalevin-" (UUID/randomUUID))))
         lmdb (lmdb/open-kv dir kv-opts)]
     (open-dbis lmdb)
     (transact-opts lmdb opts)
     (let [schema (init-schema lmdb schema)
           rschema             (schema->rschema schema)
           [classes rentities] (init-encla lmdb)
           [links rlinks]      (init-links lmdb)]
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
                links
                rlinks
                (init-max-aid schema)
                (init-max-gt lmdb)
                (init-max-cid lmdb))))))
