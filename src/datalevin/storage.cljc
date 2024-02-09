(ns ^:no-doc datalevin.storage
  "Storage layer of Datalog store"
  (:require [datalevin.lmdb :as lmdb :refer [IWriting]]
            [datalevin.util :as u]
            [datalevin.bits :as b]
            [datalevin.search :as s]
            [datalevin.constants :as c]
            [datalevin.datom :as d]
            [clojure.string :as str])
  (:import [java.util UUID]
           [datalevin.datom Datom]
           [datalevin.bits Retrieved]))

(if (u/graal?)
  (require 'datalevin.binding.graal)
  (require 'datalevin.binding.java))

(defn- attr->properties [k v]
  (case v
    :db.unique/identity  [:db/unique :db.unique/identity]
    :db.unique/value     [:db/unique :db.unique/value]
    :db.cardinality/many [:db.cardinality/many]
    (cond
      (and (= :db/valueType k) (= :db.type/ref v)) [:db.type/ref]
      (and (= :db/isComponent k) (true? v))        [:db/isComponent]
      (= :db/tupleAttrs k)                         [:db.type/tuple
                                                    :db/tupleAttrs]
      (= :db/tupleType k)                          [:db.type/tuple
                                                    :db/tupleType]
      (= :db/tupleTypes k)                         [:db.type/tuple
                                                    :db/tupleTypes]
      :else                                        [])))

(def conjs (fnil conj #{}))

(defn attr-tuples
  "e.g. :reg/semester => #{:reg/semester+course+student ...}"
  [schema rschema]
  (reduce
    (fn [m tuple-attr] ;; e.g. :reg/semester+course+student
      (u/reduce-indexed
        (fn [m src-attr idx] ;; e.g. :reg/semester
          (update m src-attr assoc tuple-attr idx))
        m
        (-> schema tuple-attr :db/tupleAttrs)))
    {}
    (:db/tupleAttrs rschema)))

(defn schema->rschema
  ":db/unique           => #{attr ...}
   :db.unique/identity  => #{attr ...}
   :db.unique/value     => #{attr ...}
   :db.cardinality/many => #{attr ...}
   :db.type/ref         => #{attr ...}
   :db/isComponent      => #{attr ...}
   :db.type/tuple       => #{attr ...}
   :db/tupleAttr        => #{attr ...}
   :db/tupleType        => #{attr ...}
   :db/tupleTypes       => #{attr ...}
   :db/attrTuples       => {attr => {tuple-attr => idx}}"
  [schema]
  (let [rschema (reduce-kv
                  (fn [rschema attr attr-schema]
                    (reduce-kv
                      (fn [rschema key value]
                        (reduce
                          (fn [rschema prop]
                            (update rschema prop conjs attr))
                          rschema (attr->properties key value)))
                      rschema attr-schema))
                  {} schema)]
    (assoc rschema :db/attrTuples (attr-tuples schema rschema))))

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
  [schema]
  (inc ^long (apply max (map :db/aid (vals schema)))))

(defn- update-schema
  [old schema]
  (let [^long init-aid (init-max-aid old)
        i              (volatile! 0)]
    (into {}
          (map (fn [[attr props]]
                 (if-let [old-props (old attr)]
                   [attr (assoc props :db/aid (old-props :db/aid))]
                   (let [res [attr (assoc props :db/aid (+ init-aid ^long @i))]]
                     (vswap! i u/long-inc)
                     res))))
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

(defn- init-attrs [schema]
  (into {} (map (fn [[k v]] [(:db/aid v) k])) schema))

(defn- init-max-gt
  [lmdb]
  (or (when-let [gt (-> (lmdb/get-first lmdb c/giants [:all-back] :id :ignore)
                        first)]
        (inc ^long gt))
      c/gt0))

(defn- init-max-tx
  [lmdb]
  (or (lmdb/get-value lmdb c/meta :max-tx :attr :long)
      c/tx0))

(defn- value-type
  [props]
  (if-let [vt (:db/valueType props)]
    (if (= vt :db.type/tuple)
      (if-let [tts (:db/tupleTypes props)]
        tts
        (if-let [tt (:db/tupleType props)] [tt] :data))
      vt)
    :data))

(defn- low-datom->indexable
  [schema ^Datom d]
  (let [e (.-e d)]
    (if-let [a (.-a d)]
      (if-let [p (schema a)]
        (if-some [v (.-v d)]
          (b/indexable e (:db/aid p) v (value-type p))
          (b/indexable e (:db/aid p) c/v0 (value-type p)))
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
          (b/indexable e (:db/aid p) v (value-type p))
          (b/indexable e (:db/aid p) c/vmax (value-type p)))
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

(defn gt->datom
  [lmdb gt]
  (lmdb/get-value lmdb c/giants gt :id :datom))

;; (declare attrs)

(defprotocol IStore
  (opts [this] "Return the opts map")
  (assoc-opt [this k v] "Set an option")
  (db-name [this] "Return the db-name")
  (dir [this] "Return the data file directory")
  (close [this] "Close storage")
  (closed? [this] "Return true if the storage is closed")
  (last-modified [this]
    "Return the unix timestamp of when the store is last modified")
  (max-gt [this])
  (advance-max-gt [this])
  (max-tx [this])
  (advance-max-tx [this])
  (max-aid [this])
  (schema [this] "Return the schema map")
  (rschema [this] "Return the reverse schema map")
  (set-schema [this new-schema]
    "Update the schema of open storage, return updated schema")
  (attrs [this] "Return the aid -> attr map")
  (init-max-eid [this] "Initialize and return the max entity id")
  (datom-count [this index] "Return the number of datoms in the index")
  (swap-attr [this attr f] [this attr f x] [this attr f x y]
    "Update the properties of an attribute, f is similar to that of swap!")
  (del-attr [this attr]
    "Delete an attribute, throw if there is still datom related to it")
  (rename-attr [this attr new-attr] "Rename an attribute")
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
  )

(defn e-aid-v->datom
  [store e-aid-v]
  (d/datom (nth e-aid-v 0) ((attrs store) (nth e-aid-v 1)) (peek e-aid-v)))

(defn- retrieved->datom
  [lmdb attrs [^Retrieved k ^long v :as kv]]
  (when kv
    (if (= v c/normal)
      (d/datom (.-e k) (attrs (.-a k)) (.-v k))
      (gt->datom lmdb v))))

(defn- datom-pred->kv-pred
  [lmdb attrs index pred]
  (fn [kv]
    (let [^Retrieved k (b/read-buffer (lmdb/k kv) index)
          ^long v      (b/read-buffer (lmdb/v kv) :id)
          ^Datom d     (retrieved->datom lmdb attrs [k v])]
      (pred d))))

(declare insert-data delete-data fulltext-index check transact-opts)

(deftype Store [lmdb
                search-engines
                ^:volatile-mutable opts
                ^:volatile-mutable schema
                ^:volatile-mutable rschema
                ^:volatile-mutable attrs    ; aid -> attr
                ^:volatile-mutable max-aid
                ^:volatile-mutable max-gt
                ^:volatile-mutable max-tx
                write-txn]

  IWriting

  (write-txn [_] write-txn)

  IStore

  (opts [_] opts)

  (assoc-opt [_ k v]
    (let [new-opts (assoc opts k v)]
      (set! opts new-opts)
      (transact-opts lmdb new-opts)))

  (db-name [_] (:db-name opts))

  (dir [_] (lmdb/dir lmdb))

  (close [_] (lmdb/close-kv lmdb))

  (closed? [_] (lmdb/closed-kv? lmdb))

  (last-modified [_] (lmdb/get-value lmdb c/meta :last-modified :attr :long))

  (max-gt [_] max-gt)

  (advance-max-gt [_] (set! max-gt (inc ^long max-gt)))

  (max-tx [_] max-tx)

  (advance-max-tx [_] (set! max-tx (inc ^long max-tx)))

  (max-aid [_] max-aid)

  (schema [_] schema)

  (rschema [_] rschema)

  (set-schema [this new-schema]
    (doseq [[attr new] new-schema
            :let       [old (schema attr)]
            :when      old]
      (check this attr old new))
    (set! schema (init-schema lmdb new-schema))
    (set! rschema (schema->rschema schema))
    (set! attrs (init-attrs schema))
    (set! max-aid (init-max-aid schema))
    schema)

  (attrs [_] attrs)

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
  (swap-attr [this attr f x y]
    (let [o (or (schema attr)
                (let [m {:db/aid max-aid}]
                  (set! max-aid (inc ^long max-aid))
                  m))
          p (cond
              (and x y) (f o x y)
              x         (f o x)
              :else     (f o))]
      (check this attr o p)
      (transact-schema lmdb {attr p})
      (set! schema (assoc schema attr p))
      (set! rschema (schema->rschema schema))
      (set! attrs (assoc attrs (:db/aid p) attr))
      p))

  (del-attr [this attr]
    (if (populated? this :ave (d/datom c/e0 attr c/v0) (d/datom c/emax attr c/vmax))
      (u/raise "Cannot delete attribute with datoms" {})
      (let [aid (:db/aid (schema attr))]
        (lmdb/transact-kv lmdb [[:del c/schema attr :attr]
                                [:put c/meta :last-modified
                                 (System/currentTimeMillis) :attr :long]])
        (set! schema (dissoc schema attr))
        (set! rschema (schema->rschema schema))
        (set! attrs (dissoc attrs aid))
        attrs)))

  (rename-attr [_ attr new-attr]
    (let [props (schema attr)]
      (lmdb/transact-kv lmdb [[:del c/schema attr :attr]
                              [:put c/schema new-attr props :attr]
                              [:put c/meta :last-modified
                               (System/currentTimeMillis) :attr :long]])
      (set! schema (-> schema (dissoc attr) (assoc new-attr props)))
      (set! rschema (schema->rschema schema))
      (set! attrs (assoc attrs (:db/aid props) new-attr))
      attrs))

  (datom-count [_ index]
    (lmdb/entries lmdb (if (string? index) index (index->dbi index))))

  (load-datoms [this datoms]
    (locking (lmdb/write-txn lmdb)
      ;; fulltext [:a d [e aid v]], [:d d [e aid v]],
      ;; [:g d [gt v]] or [:r d gt]
      (let [ft-ds  (transient [])
            ;; needed because a giant may be deleted in the same tx
            giants (volatile! {})
            add-fn (fn [holder datom]
                     (if (d/datom-added datom)
                       (reduce conj! holder
                               (insert-data this datom ft-ds giants))
                       (reduce conj! holder
                               (delete-data this datom ft-ds giants))))]
        (lmdb/transact-kv
          lmdb (persistent! (reduce add-fn (transient []) datoms)))
        (fulltext-index search-engines ft-ds)
        (lmdb/transact-kv
          lmdb [[:put c/meta :max-tx (advance-max-tx this) :attr :long]
                [:put c/meta :last-modified (System/currentTimeMillis)
                 :attr :long]]))))

  (fetch [_ datom]
    (mapv (partial retrieved->datom lmdb attrs)
          (when-some [kv (lmdb/get-value lmdb
                                         c/eav
                                         (low-datom->indexable schema datom)
                                         :eav
                                         :id
                                         false)]
            [kv])))

  (populated? [_ index low-datom high-datom]
    (lmdb/get-first lmdb
                    (index->dbi index)
                    [:closed
                     (low-datom->indexable schema low-datom)
                     (high-datom->indexable schema high-datom)]
                    index
                    :ignore
                    true))

  (size [_ index low-datom high-datom]
    (lmdb/range-count lmdb
                      (index->dbi index)
                      [:closed
                       (low-datom->indexable schema low-datom)
                       (high-datom->indexable schema high-datom)]
                      index))

  (head [_ index low-datom high-datom]
    (retrieved->datom
      lmdb attrs (lmdb/get-first lmdb
                                 (index->dbi index)
                                 [:closed
                                  (low-datom->indexable schema low-datom)
                                  (high-datom->indexable schema high-datom)]
                                 index
                                 :id)))

  (tail [_ index high-datom low-datom]
    (retrieved->datom
      lmdb attrs (lmdb/get-first lmdb
                                 (index->dbi index)
                                 [:closed-back
                                  (high-datom->indexable schema high-datom)
                                  (low-datom->indexable schema low-datom)]
                                 index
                                 :id)))

  (slice [_ index low-datom high-datom]
    (mapv (partial retrieved->datom lmdb attrs)
          (lmdb/get-range
            lmdb
            (index->dbi index)
            [:closed
             (low-datom->indexable schema low-datom)
             (high-datom->indexable schema high-datom)]
            index
            :id)))

  (rslice [_ index high-datom low-datom]
    (mapv (partial retrieved->datom lmdb attrs)
          (lmdb/get-range
            lmdb
            (index->dbi index)
            [:closed-back
             (high-datom->indexable schema high-datom)
             (low-datom->indexable schema low-datom)]
            index
            :id)))

  (size-filter [_ index pred low-datom high-datom]
    (lmdb/range-filter-count lmdb
                             (index->dbi index)
                             (datom-pred->kv-pred lmdb attrs index pred)
                             [:closed
                              (low-datom->indexable schema low-datom)
                              (high-datom->indexable schema high-datom)]
                             index))

  (head-filter [_ index pred low-datom high-datom]
    (retrieved->datom
      lmdb attrs (lmdb/get-some lmdb
                                (index->dbi index)
                                (datom-pred->kv-pred lmdb attrs index pred)
                                [:closed
                                 (low-datom->indexable schema low-datom)
                                 (high-datom->indexable schema high-datom)]
                                index
                                :id)))

  (tail-filter [_ index pred high-datom low-datom]
    (retrieved->datom
      lmdb attrs (lmdb/get-some lmdb
                                (index->dbi index)
                                (datom-pred->kv-pred lmdb attrs index pred)
                                [:closed-back
                                 (high-datom->indexable schema high-datom)
                                 (low-datom->indexable schema low-datom)]
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
         (low-datom->indexable schema low-datom)
         (high-datom->indexable schema high-datom)]
        index
        :id)))

  (rslice-filter [_ index pred high-datom low-datom]
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

(defn fulltext-index
  [search-engines ft-ds]
  (doseq [res  (persistent! ft-ds)
          :let [op (peek res)
                d (nth op 1)]]
    (doseq [domain (nth res 0)
            :let   [engine (search-engines domain)]]
      (case (nth op 0)
        :a (s/add-doc engine d (peek d) false)
        :d (s/remove-doc engine d)
        :g (s/add-doc engine [:g (nth d 0)] (peek d) false)
        :r (s/remove-doc engine [:g d])))))

(defn- check-cardinality
  [^Store store attr old new]
  (when (and (= old :db.cardinality/many) (= new :db.cardinality/one))
    (let [low-datom  (d/datom c/e0 attr c/v0)
          high-datom (d/datom c/emax attr c/vmax)]
      (when (populated? store :ave low-datom high-datom)
        (u/raise "Cardinality change is not allowed when data exist"
                 {:attribute attr})))))

(defn- check-value-type
  [^Store store attr old new]
  (when (not= old new)
    (let [low-datom  (d/datom c/e0 attr c/v0)
          high-datom (d/datom c/emax attr c/vmax)]
      (when (populated? store :ave low-datom high-datom)
        (u/raise "Value type change is not allowed when data exist"
                 {:attribute attr})))))

(defn- violate-unique?
  [^Store store low-datom high-datom]
  (let [prev-v   (volatile! nil)
        violate? (volatile! false)
        schema   (schema store)
        visitor  (fn [kv]
                   (let [^Retrieved ave (b/read-buffer (lmdb/k kv) :ave)
                         v              (.-v ave)]
                     (if (= @prev-v v)
                       (do (vreset! violate? true)
                           :datalevin/terminate-visit)
                       (vreset! prev-v v))))]
    (lmdb/visit (.-lmdb store) c/ave visitor
                [:open
                 (low-datom->indexable schema low-datom)
                 (high-datom->indexable schema high-datom)]
                :ave)
    @violate?))

(defn- check-unique
  [store attr old new]
  (when (and (not old) new)
    (let [low-datom  (d/datom c/e0 attr c/v0)
          high-datom (d/datom c/emax attr c/vmax)]
      (when (populated? store :ave low-datom high-datom)
        (when (violate-unique? store low-datom high-datom)
          (u/raise "Attribute uniqueness change is inconsistent with data"
                   {:attribute attr}))))))

(defn- check [store attr old new]
  (doseq [[k v] new
          :let  [v' (old k)]]
    (case k
      :db/cardinality (check-cardinality store attr v' v)
      :db/valueType   (check-value-type store attr v' v)
      :db/unique      (check-unique store attr v' v)
      :pass-through)))

(defn- collect-fulltext
  [ft-ds attr props v op]
  (when-not (str/blank? v)
    (conj! ft-ds [(cond-> (or (props :db.fulltext/domains) [c/default-domain])
                    (props :db.fulltext/autoDomain)
                    (conj (u/keyword->string attr)))
                  op])))

(defn- insert-data
  [^Store store ^Datom d ft-ds giants]
  (let [attr  (.-a d)
        props (or ((schema store) attr)
                  (swap-attr store attr identity))
        vt    (value-type props)
        ref?  (= :db.type/ref vt)
        e     (.-e d)
        v     (.-v d)
        aid   (:db/aid props)
        _     (or (not (:validate-data? (opts store)))
                (b/valid-data? v vt)
                (u/raise "Invalid data, expecting " vt {:input v}))
        i     (b/indexable e aid v vt)
        ft?   (:db/fulltext props)]
    (if (b/giant? i)
      (let [max-gt (max-gt store)]
        (advance-max-gt store)
        (vswap! giants assoc [e aid v] max-gt)
        (when ft?
          (let [v (str v)]
            (collect-fulltext ft-ds attr props v [:g [max-gt v]])))
        (cond-> [[:put c/eav i max-gt :eav :id]
                 [:put c/ave i max-gt :ave :id]
                 [:put c/giants max-gt d :id :datom [:append]]]
          ref? (conj [:put c/vea i max-gt :vea :id])))
      (do (when ft?
            (let [v (str v)]
              (collect-fulltext ft-ds attr props v [:a [e aid v]])))
          (cond-> [[:put c/eav i c/normal :eav :id]
                   [:put c/ave i c/normal :ave :id]]
            ref? (conj [:put c/vea i c/normal :vea :id]))))))

(defn- delete-data
  [^Store store ^Datom d ft-ds giants]
  (let [attr  (.-a d)
        props ((schema store) attr)
        vt    (value-type props)
        ref?  (= :db.type/ref vt)
        e     (.-e d)
        aid   (:db/aid props)
        v     (.-v d)
        i     (b/indexable e aid v vt)
        gt    (when (b/giant? i)
                (or (@giants [e aid v])
                    (lmdb/get-value (.-lmdb store) c/eav i :eav :id)))]
    (when (props :db/fulltext)
      (let [v (str v)]
        (collect-fulltext ft-ds attr props v (if gt [:r gt] [:d [e aid v]]))))
    (cond-> [[:del c/eav i :eav]
             [:del c/ave i :ave]]
      ref? (conj [:del c/vea i :vea])
      gt   (conj [:del c/giants gt :id]))))

(defn- transact-opts
  [lmdb opts]
  (lmdb/transact-kv lmdb
                    (conj (for [[k v] opts]
                            [:put c/opts k v :attr :data])
                          [:put c/meta :last-modified
                           (System/currentTimeMillis) :attr :long])))

(defn- load-opts
  [lmdb]
  (into {} (lmdb/get-range lmdb c/opts [:all] :attr :data)))

(defn- open-dbis
  [lmdb]
  (lmdb/open-dbi lmdb c/eav {:key-size c/+max-key-size+ :val-size c/+id-bytes+})
  (lmdb/open-dbi lmdb c/ave {:key-size c/+max-key-size+ :val-size c/+id-bytes+})
  (lmdb/open-dbi lmdb c/vea {:key-size c/+max-key-size+ :val-size c/+id-bytes+})
  (lmdb/open-dbi lmdb c/giants {:key-size c/+id-bytes+})
  (lmdb/open-dbi lmdb c/schema {:key-size c/+max-key-size+})
  (lmdb/open-dbi lmdb c/meta {:key-size c/+max-key-size+})
  (lmdb/open-dbi lmdb c/opts {:key-size c/+max-key-size+}))

(defn- default-domain
  [dms search-opts search-domains]
  (let [new-opts (assoc (or (get search-domains c/default-domain)
                            search-opts
                            {})
                        :domain c/default-domain)]
    (assoc dms c/default-domain
           (if-let [opts (dms c/default-domain)]
             (merge opts new-opts)
             new-opts))))

(defn- listed-domains
  [dms domains search-domains]
  (reduce (fn [m domain]
            (let [new-opts (assoc (get search-domains domain {})
                                  :domain domain)]
              (assoc m domain
                     (if-let [opts (m domain)]
                       (merge opts new-opts)
                       new-opts))))
          dms
          domains))

(defn- init-domains
  [search-domains0 schema search-opts search-domains]
  (reduce-kv
    (fn [dms attr
        {:keys [db/fulltext db.fulltext/domains db.fulltext/autoDomain]}]
      (if fulltext
        (cond-> (if (seq domains)
                  (listed-domains dms domains search-domains)
                  (default-domain dms search-opts search-domains))
          autoDomain (#(let [domain (u/keyword->string attr)]
                         (assoc
                           % domain
                           (let [new-opts (assoc (get search-domains domain {})
                                                 :domain domain)]
                             (if-let [opts (% domain)]
                               (merge opts new-opts)
                               new-opts))))))
        dms))
    (or search-domains0 {})
    schema))

(defn- init-engines
  [lmdb domains]
  (reduce-kv
    (fn [m domain opts]
      (assoc m domain (s/new-search-engine lmdb opts)))
    {}
    domains))

(defn open
  "Open and return the storage."
  ([]
   (open nil nil))
  ([dir]
   (open dir nil))
  ([dir schema]
   (open dir schema nil))
  ([dir schema {:keys [kv-opts search-opts search-domains validate-data?
                       auto-entity-time? db-name cache-limit]
                :or   {validate-data?    false
                       auto-entity-time? false
                       db-name           (str (UUID/randomUUID))
                       cache-limit       100}
                :as   opts}]
   (let [dir  (or dir (u/tmp-dir (str "datalevin-" (UUID/randomUUID))))
         lmdb (lmdb/open-kv dir kv-opts)]
     (open-dbis lmdb)
     (let [schema  (init-schema lmdb schema)
           opts0   (load-opts lmdb)
           domains (init-domains (:search-domains opts0)
                                 schema search-opts search-domains)]
       (transact-opts lmdb (merge opts0
                                  opts
                                  {:validate-data?    validate-data?
                                   :auto-entity-time? auto-entity-time?
                                   :db-name           db-name
                                   :cache-limit       cache-limit
                                   :search-domains    domains}))
       (->Store lmdb
                (init-engines lmdb domains)
                (load-opts lmdb)
                schema
                (schema->rschema schema)
                (init-attrs schema)
                (init-max-aid schema)
                (init-max-gt lmdb)
                (init-max-tx lmdb)
                (volatile! :storage-mutex))))))

(defn- transfer-engines
  [engines lmdb]
  (zipmap (keys engines) (map #(s/transfer % lmdb) (vals engines))))

(defn transfer
  "transfer state of an existing store to a new store that has a different
  LMDB instance"
  [^Store old lmdb]
  (->Store lmdb
           (transfer-engines (.-search-engines old) lmdb)
           (opts old)
           (schema old)
           (rschema old)
           (attrs old)
           (max-aid old)
           (max-gt old)
           (max-tx old)
           (.-write-txn old)))
