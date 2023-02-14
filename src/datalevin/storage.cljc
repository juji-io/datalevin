(ns ^:no-doc datalevin.storage
  "Storage layer of Datalog store"
  (:require
   [datalevin.lmdb :as lmdb :refer [IWriting]]
   [datalevin.util :as u]
   [datalevin.bits :as b]
   [datalevin.search :as s]
   [datalevin.constants :as c]
   [datalevin.datom :as d]
   [clojure.string :as str])
  (:import
   [java.util UUID Map$Entry]
   [org.eclipse.collections.impl.map.mutable UnifiedMap]
   [org.eclipse.collections.impl.list.mutable FastList]
   [datalevin.datom Datom]
   [datalevin.bits Retrieved Indexable]))

(if (u/graal?)
  (require 'datalevin.binding.graal)
  (require 'datalevin.binding.java))

(defn- attr->properties [k v]
  (case v
    :db.unique/identity  [:db/unique :db.unique/identity]
    :db.unique/value     [:db/unique :db.unique/value]
    :db.cardinality/many [:db.cardinality/many]
    :db.type/ref         [:db.type/ref]
    (cond
      (and (= :db/isComponent k) (true? v)) [:db/isComponent]
      (= :db/tupleAttrs k)                  [:db.type/tuple]
      :else                                 [])))

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
    (:db.type/tuple rschema)))

(def conjs (fnil conj #{}))

(defn schema->rschema
  ":db/unique           => #{attr ...}
   :db.unique/identity  => #{attr ...}
   :db.unique/value     => #{attr ...}
   :db.cardinality/many => #{attr ...}
   :db.type/ref         => #{attr ...}
   :db/isComponent      => #{attr ...}
   :db.type/tuple       => #{attr ...}
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
      c/g0))

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

(defn- datom->indexable
  [schema max-gt ^Datom d high?]
  (let [e  (.-e d)
        vm (if high? c/vmax c/v0)]
    (if-let [a (.-a d)]
      (if-let [p (schema a)]
        (if-some [v (.-v d)]
          (b/indexable e (:db/aid p) v (value-type p) max-gt)
          (b/indexable e (:db/aid p) vm (value-type p) max-gt))
        (b/indexable e c/a0 c/v0 nil max-gt))
      (let [am (if high? c/amax c/a0)]
        (if-some [v (.-v d)]
          (if (integer? v)
            (if e
              (b/indexable e am v :db.type/ref max-gt)
              (b/indexable (if high? c/emax c/e0) am v :db.type/ref max-gt))
            (u/raise
              "When v is known but a is unknown, v must be a :db.type/ref"
              {:v v}))
          (b/indexable e am vm :db.type/sysMin max-gt))))))

(defn- index->dbi
  [index]
  (case index
    (:eav :eavt) c/eav
    (:ave :avet) c/ave
    (:vea :veat) c/vea))

(defn- index->vtype
  [index]
  (case index
    (:eav :eavt) :avg
    (:ave :avet) :veg
    (:vea :veat) :eag))

(defn- index->ktype
  [index]
  (case index
    (:eav :eavt) :id
    (:ave :avet) :int
    (:vea :veat) :id))

(defn- index->k
  [index schema ^Datom datom high?]
  (case index
    (:eav :eavt) (or (.-e datom) (if high? c/emax c/e0))
    (:ave :avet) (or (:db/aid (schema (.-a datom)))
                     (if high? c/amax c/a0))
    (:vea :veat) (or (.-v datom) (if high? c/vmax c/v0))))

(defn gt->datom
  [lmdb gt]
  (lmdb/get-value lmdb c/giants gt :id :datom))

(declare attrs)

(defn e-aid-v->datom
  [store e-aid-v]
  (d/datom (nth e-aid-v 0) ((attrs store) (nth e-aid-v 1)) (peek e-aid-v)))

(defn- retrieved->datom
  [lmdb attrs [k ^Retrieved r :as kv]]
  (when kv
    (let [g (.-g r)]
      (if (= g c/normal)
        (let [e (.-e r)
              a (.-a r)
              v (.-v r)]
          (cond
            (nil? e) (d/datom k (attrs a) v)
            (nil? a) (d/datom e (attrs (int k)) v)
            (nil? v) (d/datom e (attrs a) k)))
        (gt->datom lmdb g)))))

(defn- datom-pred->kv-pred
  [lmdb attrs index pred]
  (fn [kv]
    (let [k            (b/read-buffer (lmdb/k kv) (index->ktype index))
          ^Retrieved v (b/read-buffer (lmdb/v kv) (index->vtype index))
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

(declare insert-datom delete-datom transact-list transact-giants
         fulltext-index check)

(deftype Store [lmdb
                search-engine
                opts
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
    (let [e    (volatile! c/e0)
          read (fn [kv]
                 (when-let [res (b/read-buffer (lmdb/k kv) :id)]
                   (vreset! e res)
                   :datalevin/terminate-visit))]
      (lmdb/visit lmdb c/eav read [:all-back])
      @e))

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
    (if (populated? this :ave
                    (d/datom c/e0 attr c/v0) (d/datom c/emax attr c/vmax))
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
      (let [;; fulltext [:a [e aid v]], [:d [e aid v]], [:g [gt v]] or [:r gt]
            ft-ds  (FastList.)
            txs    (FastList.)
            giants (UnifiedMap.)]
        (doseq [datom datoms]
          (if (d/datom-added datom)
            (insert-datom this datom txs ft-ds giants)
            (delete-datom this datom txs ft-ds giants)))
        (lmdb/transact-kv
          lmdb (doto txs
                 (.add [:put c/meta :max-tx (advance-max-tx this)
                        :attr :long])
                 (.add [:put c/meta :last-modified (System/currentTimeMillis)
                        :attr :long])))
        (fulltext-index search-engine ft-ds))))

  (fetch [_ datom]
    (mapv (partial retrieved->datom lmdb attrs)
          (let [lk (index->k :eav schema datom false)
                hk (index->k :eav schema datom true)
                lv (datom->indexable schema c/g0 datom false)
                hv (datom->indexable schema c/gmax datom true)]
            (lmdb/list-range lmdb (index->dbi :eav)
                             [:closed lk hk] :id
                             [:closed lv hv] :avg))))

  (populated? [_ index low-datom high-datom]
    (let [lk (index->k index schema low-datom false)
          hk (index->k index schema high-datom true)
          lv (datom->indexable schema c/g0 low-datom false)
          hv (datom->indexable schema c/gmax high-datom true)]
      (lmdb/list-range-first
        lmdb (index->dbi index)
        [:closed lk hk] (index->ktype index)
        [:closed lv hv] (index->vtype index))))

  (size [_ index low-datom high-datom]
    (lmdb/list-range-count
      lmdb (index->dbi index)
      [:closed (index->k index schema low-datom false)
       (index->k index schema high-datom true)] (index->ktype index)
      [:closed
       (datom->indexable schema c/g0 low-datom false)
       (datom->indexable schema c/gmax high-datom true)] (index->vtype index)))

  (head [this index low-datom high-datom]
    (retrieved->datom lmdb attrs
                      (.populated? this index low-datom high-datom)))

  (tail [_ index high-datom low-datom]
    (retrieved->datom
      lmdb attrs
      (lmdb/list-range-first
        lmdb (index->dbi index)
        [:closed-back (index->k index schema high-datom true)
         (index->k index schema low-datom false)] (index->ktype index)
        [:closed-back
         (datom->indexable schema c/gmax high-datom true)
         (datom->indexable schema c/g0 low-datom false)]
        (index->vtype index))))

  (slice [_ index low-datom high-datom]
    (mapv (partial retrieved->datom lmdb attrs)
          (let [lk (index->k index schema low-datom false)
                hk (index->k index schema high-datom true)
                lv (datom->indexable schema c/g0 low-datom false)
                hv (datom->indexable schema c/gmax high-datom true)]
            (lmdb/list-range
              lmdb (index->dbi index)
              [:closed lk hk] (index->ktype index)
              [:closed lv hv] (index->vtype index)))))

  (rslice [_ index high-datom low-datom]
    (mapv (partial retrieved->datom lmdb attrs)
          (lmdb/list-range
            lmdb (index->dbi index)
            [:closed-back (index->k index schema high-datom true)
             (index->k index schema low-datom false)] (index->ktype index)
            [:closed-back
             (datom->indexable schema c/gmax high-datom true)
             (datom->indexable schema c/g0 low-datom false)]
            (index->vtype index))))

  (size-filter [_ index pred low-datom high-datom]
    (lmdb/list-range-filter-count
      lmdb (index->dbi index)
      (datom-pred->kv-pred lmdb attrs index pred)
      [:closed (index->k index schema low-datom false)
       (index->k index schema high-datom true)] (index->ktype index)
      [:closed (datom->indexable schema c/g0 low-datom false)
       (datom->indexable schema c/gmax high-datom true)]
      (index->vtype index)))

  (head-filter [_ index pred low-datom high-datom]
    (retrieved->datom
      lmdb attrs
      (lmdb/list-range-some
        lmdb (index->dbi index)
        (datom-pred->kv-pred lmdb attrs index pred)
        [:closed (index->k index schema low-datom false)
         (index->k index schema high-datom true)] (index->ktype index)
        [:closed
         (datom->indexable schema c/g0 low-datom false)
         (datom->indexable schema c/gmax high-datom true)]
        (index->vtype index))))

  (tail-filter [_ index pred high-datom low-datom]
    (retrieved->datom
      lmdb attrs
      (lmdb/list-range-some
        lmdb (index->dbi index)
        (datom-pred->kv-pred lmdb attrs index pred)
        [:closed-back (index->k index schema high-datom true)
         (index->k index schema low-datom false)] (index->ktype index)
        [:closed-back
         (datom->indexable schema c/gmax high-datom true)
         (datom->indexable schema c/g0 low-datom false)]
        (index->vtype index))))

  (slice-filter [_ index pred low-datom high-datom]
    (mapv (partial retrieved->datom lmdb attrs)
          (lmdb/list-range-filter
            lmdb (index->dbi index)
            (datom-pred->kv-pred lmdb attrs index pred)
            [:closed (index->k index schema low-datom false)
             (index->k index schema high-datom true)] (index->ktype index)
            [:closed
             (datom->indexable schema c/g0 low-datom false)
             (datom->indexable schema c/gmax high-datom true)]
            (index->vtype index))))

  (rslice-filter [_ index pred high-datom low-datom]
    (mapv (partial retrieved->datom lmdb attrs)
          (lmdb/list-range-filter
            lmdb (index->dbi index)
            (datom-pred->kv-pred lmdb attrs index pred)
            [:closed-back (index->k index schema high-datom true)
             (index->k index schema low-datom false)] (index->ktype index)
            [:closed-back
             (datom->indexable schema c/gmax high-datom true)
             (datom->indexable schema c/g0 low-datom false)]
            (index->vtype index))))
  )

(defn fulltext-index
  [search-engine ft-ds]
  (doseq [res  ft-ds
          :let [d (nth res 1)]]
    (case (nth res 0)
      :a (s/add-doc search-engine d (peek d) false)
      :d (s/remove-doc search-engine d)
      :g (s/add-doc search-engine [:g (nth d 0)] (peek d) false)
      :r (s/remove-doc search-engine [:g d]))))

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
    (when ((schema store) attr)
      (let [low-datom  (d/datom c/e0 attr c/v0)
            high-datom (d/datom c/emax attr c/vmax)]
        (when (populated? store :ave low-datom high-datom)
          (u/raise "Value type change is not allowed when data exist"
                   {:attribute attr}))))))

(defn- violate-unique?
  [^Store store low-datom high-datom]
  (let [prev-v   (volatile! nil)
        violate? (volatile! false)
        schema   (schema store)
        visitor  (fn [kv]
                   (let [^Retrieved veg (b/read-buffer (lmdb/v kv) :veg)
                         v              (.-v veg)]
                     (if (= @prev-v v)
                       (do (vreset! violate? true)
                           :datalevin/terminate-visit)
                       (vreset! prev-v v))))]
    (lmdb/visit-list-range
      (.-lmdb store) c/ave visitor
      [:closed (index->k :ave schema low-datom false)
       (index->k :ave schema high-datom true)] (index->ktype :ave)
      [:closed (datom->indexable schema c/g0 low-datom false)
       (datom->indexable schema c/gmax high-datom true)]
      (index->vtype :ave))
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

(defn- insert-datom
  [^Store store ^Datom d ^FastList txs ^FastList ft-ds ^UnifiedMap giants]
  (let [attr   (.-a d)
        props  (or ((schema store) attr)
                   (swap-attr store attr identity))
        vt     (value-type props)
        ref?   (= :db.type/ref vt)
        e      (.-e d)
        v      (.-v d)
        aid    (:db/aid props)
        max-gt (max-gt store)
        i      (b/indexable e aid v vt max-gt)
        ft?    (:db/fulltext props)
        giant? (b/giant? i)]
    (or (not (:validate-data? (.-opts store)))
        (b/valid-data? v vt)
        (u/raise "Invalid data, expecting " vt {:input v}))
    (.add txs [:put c/eav e i :id :avg])
    (.add txs [:put c/ave aid i :int :veg])
    (when ref? (.add txs [:put c/vea v i :id :eag]))
    (when giant?
      (advance-max-gt store)
      (let [gd [e attr v]]
        (.put giants gd max-gt)
        (.add txs
              [:put c/giants max-gt (apply d/datom gd) :id :data [:append]])))
    (when ft?
      (let [v (str v)]
        (when-not (str/blank? v)
          (.add ft-ds (if giant? [:g [max-gt v]] [:a [e aid v]])))))))

(defn- delete-datom
  [^Store store ^Datom d ^FastList txs ^FastList ft-ds ^UnifiedMap giants]
  (let [attr         (.-a d)
        props        ((schema store) attr)
        vt           (value-type props)
        ref?         (= :db.type/ref vt)
        e            (.-e d)
        aid          (:db/aid props)
        v            (.-v d)
        ^Indexable i (b/indexable e aid v vt c/g0)
        d-eav        [e attr v]
        gt-this-tx   (.get giants d-eav)
        gt           (when (b/giant? i)
                       (or gt-this-tx
                           (let [[_ ^Retrieved r]
                                 (nth
                                   (lmdb/list-range
                                     (.-lmdb store) c/eav [:closed e e] :id
                                     [:closed i
                                      (Indexable. e aid v (.-f i) (.-b i)
                                                  c/gmax)] :avg)
                                   0)]
                             (.-g r))))]
    (when (:db/fulltext props)
      (let [v (str v)]
        (when-not (str/blank? v)
          (.add ft-ds (if gt [:r gt] [:d [e aid v]])))))
    (let [ii (Indexable. e aid v (.-f i) (.-b i) (or gt c/normal))]
      (.add txs [:del-list c/eav e [ii] :id :avg])
      (.add txs [:del-list c/ave aid [ii] :int :veg])
      (when ref? (.add txs [:del-list c/vea v [ii] :id :eag])))
    (when gt
      (when gt-this-tx (.remove giants d-eav))
      (.add txs [:del c/giants gt :id]))))

(defn- transact-opts
  [lmdb opts]
  (lmdb/transact-kv lmdb (conj (for [[k v] opts]
                                 [:put c/opts k v :attr :data])
                               [:put c/meta :last-modified
                                (System/currentTimeMillis) :attr :long])))

(defn- load-opts
  [lmdb]
  (into {} (lmdb/get-range lmdb c/opts [:all] :attr :data)))

(defn- open-dbis
  [lmdb]
  (lmdb/open-list-dbi
    lmdb c/eav {:key-size c/+id-bytes+ :val-size c/+max-key-size+})
  (lmdb/open-list-dbi
    lmdb c/ave {:key-size c/+short-id-bytes+ :val-size c/+max-key-size+})
  (lmdb/open-list-dbi
    lmdb c/vea {:key-size c/+id-bytes+ :val-size 20})
  (lmdb/open-dbi lmdb c/giants {:key-size c/+id-bytes+})
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
     (let [schema (init-schema lmdb schema)]
       (->Store lmdb
                (s/new-search-engine lmdb (assoc search-opts
                                                 :index-position? false))
                (load-opts lmdb)
                schema
                (schema->rschema schema)
                (init-attrs schema)
                (init-max-aid schema)
                (init-max-gt lmdb)
                (init-max-tx lmdb)
                (volatile! :storage-mutex))))))

(defn transfer
  "transfer state of an existing store to a new store that has a different
  LMDB instance"
  [^Store old lmdb]
  (->Store lmdb
           (s/transfer (.-search-engine old) lmdb)
           (.-opts old)
           (schema old)
           (rschema old)
           (attrs old)
           (max-aid old)
           (max-gt old)
           (max-tx old)
           (.-write-txn old)))
