(ns ^:no-doc datalevin.storage
  "Storage layer of Datalog store"
  (:require
   [datalevin.lmdb :as lmdb :refer [IWriting]]
   [datalevin.util :as u]
   [datalevin.relation :as r]
   [datalevin.bits :as b]
   [datalevin.search :as s]
   [datalevin.constants :as c]
   [datalevin.datom :as d]
   [clojure.string :as str])
  (:import
   [java.util UUID List]
   [java.nio ByteBuffer]
   [org.eclipse.collections.impl.list.mutable FastList]
   [org.eclipse.collections.impl.map.mutable UnifiedMap]
   [org.eclipse.collections.impl.map.mutable.primitive LongObjectHashMap]
   [org.eclipse.collections.impl.set.mutable.primitive LongHashSet]
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

(def conjs (fnil conj #{}))

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
  [schema ^Datom d high?]
  (let [e  (.-e d)
        vm (if high? c/vmax c/v0)

        gm (if high? c/gmax c/g0)]
    (if-let [a (.-a d)]
      (if-let [p (schema a)]
        (if-some [v (.-v d)]
          (b/indexable e (:db/aid p) v (value-type p) gm)
          (b/indexable e (:db/aid p) vm (value-type p) gm))
        (b/indexable e c/a0 c/v0 nil gm))
      (let [am (if high? c/amax c/a0)]
        (if-some [v (.-v d)]
          (if (integer? v)
            (if e
              (b/indexable e am v :db.type/ref gm)
              (b/indexable (if high? c/emax c/e0) am v :db.type/ref gm))
            (u/raise
              "When v is known but a is unknown, v must be a :db.type/ref"
              {:v v}))
          (b/indexable e am vm :db.type/sysMin gm))))))

(defn- index->dbi
  [index]
  (case index
    (:eav :eavt) c/eav
    (:ave :avet) c/ave
    (:vae :vaet) c/vae))

(defn- index->vtype
  [index]
  (case index
    (:eav :eavt) :avg
    (:ave :avet) :veg
    (:vae :vaet) :ae))

(defn- index->ktype
  [index]
  (case index
    (:eav :eavt) :id
    (:ave :avet) :int
    (:vae :vaet) :id))

(defn- index->k
  [index schema ^Datom datom high?]
  (case index
    (:eav :eavt) (or (.-e datom) (if high? c/emax c/e0))
    (:ave :avet) (or (:db/aid (schema (.-a datom)))
                     (if high? c/amax c/a0))
    (:vae :vaet) (or (.-v datom) (if high? c/vmax c/v0))))

(defn gt->datom [lmdb gt] (lmdb/get-value lmdb c/giants gt :id :datom))

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
  (size [this index low-datom high-datom] [this index low-datom high-datom cap]
    "Return the number of datoms within the given range (inclusive)")
  (e-size [this e]
    "Return the numbers of datoms with the given e value")
  (a-size [this a]
    "Return the numbers of datoms with the given a value")
  (v-size [this v]
    "Return the numbers of ref datoms with the given v value")
  (head [this index low-datom high-datom]
    "Return the first datom within the given range (inclusive)")
  (tail [this index high-datom low-datom]
    "Return the last datom within the given range (inclusive)")
  (slice [this index low-datom high-datom]
    "Return a range of datoms within the given range (inclusive).")
  (rslice [this index high-datom low-datom]
    "Return a range of datoms in reverse within the given range (inclusive)")
  (size-filter [this index pred low-datom high-datom] [this index pred low-datom high-datom cap]
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
  (ave-tuples
    [this attr val-range]
    [this attr val-range vpred]
    [this attr val-range vpred get-v?]
    "Return tuples of e or e and v using :ave index")
  (eav-scan-v
    [this tuples eid-idx attrs vpreds]
    [this tuples eid-idx attrs vpreds skip-attrs]
    "Scan values and merge into tuples using :eav index., assuming attrs are
    already sorted by aid")
  (vae-scan-e [this tuples veid-idx attr]
    "Return merged tuples with eid as the last column using :vae index")
  (ave-scan-e [this tuples v-idx attr]
    "Return merged tuples with eid as the last column using :ave index"))

(defn e-aid-v->datom
  [store e-aid-v]
  (d/datom (nth e-aid-v 0) ((attrs store) (nth e-aid-v 1)) (peek e-aid-v)))

(defn- retrieved->datom
  [lmdb attrs [k ^Retrieved r :as kv]]
  (when kv
    (let [g (.-g r)]
      (if (or (nil? g) (= g c/normal))
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

(defn- retrieved->v
  [lmdb ^Retrieved r]
  (let [g (.-g r)]
    (if (= g c/normal)
      (.-v r)
      (d/datom-v (gt->datom lmdb g)))))

#_(defn- vpred->kv-pred
    [lmdb index pred]
    (fn [kv]
      (let [^Retrieved r (b/read-buffer (lmdb/v kv) (index->vtype index))]
        (pred (retrieved->v lmdb r)))))

(declare insert-datom delete-datom transact-list transact-giants
         fulltext-index check transact-opts)

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
      (let [;; fulltext [:a d [e aid v]], [:d d [e aid v]],
            ;; [:g d [gt v]] or [:r d gt]
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
        (fulltext-index search-engines ft-ds))))

  (fetch [_ datom]
    (mapv (partial retrieved->datom lmdb attrs)
          (let [lk (index->k :eav schema datom false)
                hk (index->k :eav schema datom true)
                lv (datom->indexable schema datom false)
                hv (datom->indexable schema datom true)]
            (lmdb/list-range lmdb (index->dbi :eav)
                             [:closed lk hk] :id
                             [:closed lv hv] :avg))))

  (populated? [_ index low-datom high-datom]
    (let [lk (index->k index schema low-datom false)
          hk (index->k index schema high-datom true)
          lv (datom->indexable schema low-datom false)
          hv (datom->indexable schema high-datom true)]
      (lmdb/list-range-first
        lmdb (index->dbi index)
        [:closed lk hk] (index->ktype index)
        [:closed lv hv] (index->vtype index))))

  (size [store index low-datom high-datom]
    (.size store index low-datom high-datom nil))
  (size [_ index low-datom high-datom cap]
    (lmdb/list-range-count
      lmdb (index->dbi index)
      [:closed (index->k index schema low-datom false)
       (index->k index schema high-datom true)] (index->ktype index)
      [:closed
       (datom->indexable schema low-datom false)
       (datom->indexable schema high-datom true)] (index->vtype index)
      cap))

  (e-size [_ e]
    (lmdb/list-count lmdb (index->dbi :eav) e (index->ktype :eav)))

  (a-size [_ a]
    (lmdb/list-count lmdb (index->dbi :ave)
                     (:db/aid (schema a)) (index->ktype :ave)))

  (v-size [_ v]
    (lmdb/list-count lmdb (index->dbi :vae) v (index->ktype :vae)))

  (head [this index low-datom high-datom]
    (retrieved->datom lmdb attrs
                      (populated? this index low-datom high-datom)))

  (tail [_ index high-datom low-datom]
    (retrieved->datom
      lmdb attrs
      (lmdb/list-range-first
        lmdb (index->dbi index)
        [:closed-back (index->k index schema high-datom true)
         (index->k index schema low-datom false)] (index->ktype index)
        [:closed-back
         (datom->indexable schema high-datom true)
         (datom->indexable schema low-datom false)]
        (index->vtype index))))

  (slice [_ index low-datom high-datom]
    (mapv (partial retrieved->datom lmdb attrs)
          (let [lk (index->k index schema low-datom false)
                hk (index->k index schema high-datom true)
                lv (datom->indexable schema low-datom false)
                hv (datom->indexable schema high-datom true)]
            (lmdb/list-range
              lmdb (index->dbi index)
              [:closed lk hk] (index->ktype index)
              [:closed lv hv] (index->vtype index)))))

  (rslice [_ index high-datom low-datom]
    (mapv (partial retrieved->datom lmdb attrs)
          (lmdb/list-range
            lmdb (index->dbi index)
            [:closed-back (index->k index schema high-datom true)
             (index->k index schema low-datom false)]
            (index->ktype index)
            [:closed-back
             (datom->indexable schema high-datom true)
             (datom->indexable schema low-datom false)]
            (index->vtype index))))

  (size-filter [store index pred low-datom high-datom]
    (.size-filter store index pred low-datom high-datom nil))
  (size-filter [_ index pred low-datom high-datom cap]
    (lmdb/list-range-filter-count
      lmdb (index->dbi index)
      (datom-pred->kv-pred lmdb attrs index pred)
      [:closed (index->k index schema low-datom false)
       (index->k index schema high-datom true)]
      (index->ktype index)
      [:closed (datom->indexable schema low-datom false)
       (datom->indexable schema high-datom true)]
      (index->vtype index)
      true cap))

  (head-filter [_ index pred low-datom high-datom]
    (lmdb/list-range-some
      lmdb (index->dbi index)
      (datom-pred->kv-pred lmdb attrs index pred)
      [:closed (index->k index schema low-datom false)
       (index->k index schema high-datom true)]
      (index->ktype index)
      [:closed
       (datom->indexable schema low-datom false)
       (datom->indexable schema high-datom true)]
      (index->vtype index)))

  (tail-filter [_ index pred high-datom low-datom]
    (lmdb/list-range-some
      lmdb (index->dbi index)
      (datom-pred->kv-pred lmdb attrs index pred)
      [:closed-back (index->k index schema high-datom true)
       (index->k index schema low-datom false)]
      (index->ktype index)
      [:closed-back
       (datom->indexable schema high-datom true)
       (datom->indexable schema low-datom false)]
      (index->vtype index)))

  (slice-filter [_ index pred low-datom high-datom]
    (lmdb/list-range-keep
      lmdb (index->dbi index)
      (datom-pred->kv-pred lmdb attrs index pred)
      [:closed (index->k index schema low-datom false)
       (index->k index schema high-datom true)]
      (index->ktype index)
      [:closed
       (datom->indexable schema low-datom false)
       (datom->indexable schema high-datom true)]
      (index->vtype index)))

  (rslice-filter [_ index pred high-datom low-datom]
    (lmdb/list-range-keep
      lmdb (index->dbi index)
      (datom-pred->kv-pred lmdb attrs index pred)
      [:closed-back (index->k index schema high-datom true)
       (index->k index schema low-datom false)]
      (index->ktype index)
      [:closed-back
       (datom->indexable schema high-datom true)
       (datom->indexable schema low-datom false)]
      (index->vtype index)))

  (ave-tuples [store attr val-range]
    (.ave-tuples store attr val-range nil false))
  (ave-tuples [store attr val-range vpred]
    (.ave-tuples store attr val-range vpred false))
  (ave-tuples [_ attr val-range vpred get-v?]
    (when-let [props (schema attr)]
      (let [vt   (value-type props)
            aid  (props :db/aid)
            seen (LongHashSet.)
            res  ^FastList (FastList.)
            operator
            (fn [iterable]
              (let [iter (lmdb/val-iterator iterable)]
                (loop [next? (lmdb/seek-key iter aid :int)]
                  (when next?
                    (let [veg (lmdb/next-val iter)]
                      (if (or vpred get-v?)
                        (let [r (b/read-buffer veg :veg)
                              v (retrieved->v lmdb r)
                              e (.-e ^Retrieved r)]
                          (if get-v?
                            (if vpred
                              (when (vpred v)
                                (.add seen e)
                                (.add res (object-array [e v])))
                              (do (.add seen e)
                                  (.add res (object-array [e v]))))
                            (when-not (.contains seen e)
                              (when (vpred v)
                                (.add seen e)
                                (.add res (object-array [e]))))))
                        (let [e (b/veg->e veg)]
                          (when-not (.contains seen e)
                            (.add seen e)
                            (.add res (object-array [e]))))))
                    (recur (lmdb/has-next-val iter))))))]
        (lmdb/operate-list-val-range
          lmdb c/ave operator
          (let [[op lv hv] val-range]
            (case op
              :all          val-range
              :at-least     [op (b/indexable c/e0 aid lv vt c/g0)]
              :at-most      [op (b/indexable c/emax aid lv vt c/gmax)]
              :closed       [op (b/indexable c/e0 aid lv vt c/g0)
                             (b/indexable c/emax aid hv vt c/gmax)]
              :closed-open  [op (b/indexable c/e0 aid lv vt c/g0)
                             (b/indexable c/e0 aid hv vt c/g0)]
              :greater-than [op (b/indexable c/emax aid lv vt c/gmax)]
              :less-than    [op (b/indexable c/e0 aid lv vt c/g0)]
              :open         [op (b/indexable c/emax aid lv vt c/gmax)
                             (b/indexable c/e0 aid hv vt c/g0)]
              :open-closed  [op (b/indexable c/emax aid lv vt c/gmax)
                             (b/indexable c/emax aid hv vt c/gmax)]))
          :veg)
        res)))

  (eav-scan-v [store tuples eid-idx as vpreds]
    (.eav-scan-v store tuples eid-idx as vpreds []))
  (eav-scan-v
    [_ tuples eid-idx as vpreds skip-as]
    (let [attr->aid #(-> % schema :db/aid)
          aids      (mapv attr->aid as)]
      (when (and (seq tuples) (seq aids) (not-any? nil? aids))
        (let [skip-aids (set (map attr->aid skip-as))
              na        (count aids)
              aid->pred (zipmap aids vpreds)
              many      (set (filter #(= (-> % attrs schema :db/cardinality)
                                         :db.cardinality/many) aids))
              aids      (int-array aids)
              res       ^FastList (FastList.)
              operator
              (fn [iterable]
                (let [iter (lmdb/val-iterator iterable)
                      seen (LongObjectHashMap.)]
                  (dotimes [i (.size ^List tuples)]
                    (let [tuple ^objects (.get ^List tuples i)
                          te    ^long (aget tuple eid-idx)]
                      (if-let [ts (.get seen te)]
                        (.addAll res ts)
                        (if (seq many)
                          (loop [next? (lmdb/seek-key iter te :id)
                                 ai    0
                                 dups  nil
                                 vs    (transient [])]
                            (if next?
                              (let [vb ^ByteBuffer (lmdb/next-val iter)
                                    a  (b/read-buffer (.rewind vb) :int)]
                                (if (= a (aget aids ai))
                                  (if (skip-aids a)
                                    (if (many a)
                                      (recur (lmdb/has-next-val iter) ai true vs)
                                      (recur (lmdb/has-next-val iter)
                                             (u/long-inc ai) nil vs))
                                    (let [v    (retrieved->v lmdb (b/avg->r vb))
                                          pred (aid->pred a)
                                          go?  (or (nil? pred) (pred v))]
                                      (if (many a)
                                        (recur (lmdb/has-next-val iter)
                                               ai
                                               (when go?
                                                 ((fnil u/list-add (FastList.))
                                                  dups v))
                                               vs)
                                        (when go?
                                          (recur (lmdb/has-next-val iter)
                                                 (u/long-inc ai)
                                                 nil
                                                 (conj! vs v))))))
                                  (if dups
                                    (recur true (u/long-inc ai) nil
                                           (if (true? dups) vs (conj! vs dups)))
                                    (recur (lmdb/has-next-val iter) ai nil vs))))
                              (let [ai (if dups (u/long-inc ai) ai)
                                    vs (if (and dups (not (true? dups)))
                                         (conj! vs dups) vs)]
                                (when (= ai na)
                                  (let [new (r/prod-tuples
                                              (r/single-tuples tuple)
                                              (r/many-tuples (persistent! vs)))]
                                    (.put seen te new)
                                    (.addAll res new))))))
                          (loop [next? (lmdb/seek-key iter te :id)
                                 ai    0
                                 vs    (transient [])]
                            (if next?
                              (let [vb (lmdb/next-val iter)
                                    a  (b/read-buffer vb :int)]
                                (if (= a (aget aids ai))
                                  (if (skip-aids a)
                                    (recur (lmdb/has-next-val iter)
                                           (u/long-inc ai)
                                           vs)
                                    (let [v    (retrieved->v lmdb (b/avg->r vb))
                                          pred (aid->pred a)]
                                      (when (or (nil? pred) (pred v))
                                        (recur (lmdb/has-next-val iter)
                                               (u/long-inc ai)
                                               (conj! vs v)))))
                                  (recur (lmdb/has-next-val iter) ai vs)))
                              (when (= ai na)
                                (let [values (persistent! vs)]
                                  (when (seq values)
                                    (let [new (r/join-tuples
                                                tuple (object-array values))]
                                      (.put seen te [new])
                                      (.add res new)))))))))))))]
          (lmdb/operate-list-val-range
            lmdb c/eav operator
            [:closed
             (b/indexable nil (aget aids 0) c/v0 nil c/g0)
             (b/indexable nil (aget aids (dec (alength aids))) c/vmax nil c/gmax)]
            :avg)
          res))))

  (vae-scan-e [_ tuples veid-idx attr]
    (when (and (seq tuples) attr)
      (when-let [props (schema attr)]
        (assert (= :db.type/ref (props :db/valueType))
                (str attr " is not a :db.type/ref"))
        (let [aid (props :db/aid)
              res ^FastList (FastList.)
              operator
              (fn [iterable]
                (let [iter (lmdb/val-iterator iterable)]
                  (dotimes [i (.size ^List tuples)]
                    (let [tuple ^objects (.get ^List tuples i)
                          tv    ^long (aget tuple veid-idx)]
                      (loop [next? (lmdb/seek-key iter tv :id)
                             es    (transient [])]
                        (if next?
                          (let [vb ^ByteBuffer (lmdb/next-val iter)
                                e  (b/read-buffer (.position vb 4) :id)]
                            (recur (lmdb/has-next-val iter)
                                   (conj! es e)))
                          (.addAll res (r/prod-tuples
                                         (r/single-tuples tuple)
                                         (r/vertical-tuples
                                           (persistent! es))))))))))]
          (lmdb/operate-list-val-range
            lmdb c/vae operator
            [:closed
             (b/indexable c/e0 aid nil :db.type/ref c/g0)
             (b/indexable c/emax aid nil :db.type/ref c/gmax)] :ae)
          res))))

  (ave-scan-e [_ tuples v-idx attr]
    (when (and (seq tuples) attr)
      (when-let [props (schema attr)]
        (let [vt  (value-type props)
              aid (props :db/aid)
              res ^FastList (FastList.)
              scan-e
              (fn [tuple v]
                (let [operator
                      (fn [iterable]
                        (let [iter (lmdb/val-iterator iterable)]
                          (loop [next? (lmdb/seek-key iter aid :int)
                                 es    (transient [])]
                            (if next?
                              (let [e (b/veg->e (lmdb/next-val iter))]
                                (recur (lmdb/has-next-val iter)
                                       (conj! es e)))
                              (.addAll res (r/prod-tuples
                                             (r/single-tuples tuple)
                                             (r/vertical-tuples
                                               (persistent! es))))))))]
                  (lmdb/operate-list-val-range
                    lmdb c/ave operator
                    [:closed
                     (b/indexable c/e0 aid v vt c/g0)
                     (b/indexable c/emax aid v vt c/gmax)] :veg)))]
          (dotimes [i (.size ^List tuples)]
            (let [tuple ^objects (.get ^List tuples i)
                  tv    ^long (aget tuple v-idx)]
              (scan-e tuple tv)))
          res)))))

(defn fulltext-index
  [search-engines ft-ds]
  (doseq [res  ft-ds
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
      [:closed (datom->indexable schema low-datom false)
       (datom->indexable schema high-datom true)]
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

(defn- collect-fulltext
  [^FastList ft-ds attr props v op]
  (when-not (str/blank? v)
    (.add ft-ds
          [(cond-> (or (props :db.fulltext/domains) [c/default-domain])
             (props :db.fulltext/autoDomain)
             (conj (u/keyword->string attr)))
           op])))

(defn- insert-datom
  [^Store store ^Datom d ^FastList txs ^FastList ft-ds ^UnifiedMap giants]
  (let [attr   (.-a d)
        props  (or ((schema store) attr)
                   (swap-attr store attr identity))
        vt     (value-type props)
        e      (.-e d)
        v      (.-v d)
        aid    (props :db/aid)
        max-gt (max-gt store)
        _      (or (not (:validate-data? (opts store)))
                   (b/valid-data? v vt)
                   (u/raise "Invalid data, expecting" vt {:input v}))
        i      (b/indexable e aid v vt max-gt)
        giant? (b/giant? i)]
    (.add txs [:put c/eav e i :id :avg])
    (.add txs [:put c/ave aid i :int :veg])
    (when (= :db.type/ref vt) (.add txs [:put c/vae v i :id :ae]))
    (when giant?
      (advance-max-gt store)
      (let [gd [e attr v]]
        (.put giants gd max-gt)
        (.add txs
              [:put c/giants max-gt (apply d/datom gd) :id :data [:append]])))
    (when (props :db/fulltext)
      (let [v (str v)]
        (collect-fulltext ft-ds attr props v
                          (if giant? [:g [max-gt v]] [:a [e aid v]]))))))

(defn- delete-datom
  [^Store store ^Datom d ^FastList txs ^FastList ft-ds ^UnifiedMap giants]
  (let [attr         (.-a d)
        props        ((schema store) attr)
        vt           (value-type props)
        e            (.-e d)
        aid          (props :db/aid)
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
    (when (props :db/fulltext)
      (let [v (str v)]
        (collect-fulltext ft-ds attr props v
                          (if gt [:r gt] [:d [e aid v]]))))
    (let [ii (Indexable. e aid v (.-f i) (.-b i) (or gt c/normal))]
      (.add txs [:del-list c/eav e [ii] :id :avg])
      (.add txs [:del-list c/ave aid [ii] :int :veg])
      (when (= :db.type/ref vt) (.add txs [:del-list c/vae v [ii] :id :ae])))
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
    lmdb c/vae {:key-size c/+id-bytes+ :val-size 20})
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
                       cache-limit       32}
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
