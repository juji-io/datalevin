(ns datalevin.storage
  "storage layer of datalevin"
  (:require [datalevin.lmdb :as lmdb]
            [datalevin.util :as util]
            [datalevin.bits :as b]
            [datalevin.constants :as c]
            [datalevin.datom :as d]
            [taoensso.nippy :as nippy]
            [taoensso.timbre :as log])
  (:import [datalevin.lmdb LMDB]
           [datalevin.datom Datom]
           [datalevin.bits Indexable Retrieved]
           [org.lmdbjava PutFlags CursorIterable$KeyVal]))

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

(defprotocol IStore
  (close [this] "Close storage")
  (max-gt [this])
  (max-aid [this])
  (schema [this] "Return the schema map")
  (init-max-eid [this] "Initialize and return the max entity id")
  (datom-count [this index] "Return the number of datoms in the index")
  (swap-attr [this attr f] [this attr f x] [this attr f x y]
    "Update an attribute, f is similar to that of swap!")
  (insert [this datom] "Insert an datom")
  (delete [this datom] "Delete an datom")
  (slice [this index start-datom end-datom]
    "Return a range of datoms for the given index with the given boundary
    (inclusive). When one boundary is nil, that side is unbounded. Both
    nil means all.")
  (rslice [this index start-datom end-datom]
    "Return a range of datoms in reverse for the given index"))

(deftype Store [^LMDB lmdb
                ^:volatile-mutable schema
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
  (slice [_ index start-datom end-datom]
    (cond
      (and start-datom end-datom)
      ()))
  (rslice [_ index start-datom end-datom]
    ))

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
    (->Store lmdb (init-schema lmdb) (init-max-aid lmdb) (init-max-gt lmdb))))
