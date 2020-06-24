(ns datalevin.storage
  "storage layer of datalevin"
  (:require [datalevin.lmdb :as lmdb]
            [datalevin.util :as util]
            [datalevin.bits :as b]
            [datalevin.constants :as c]
            [datalevin.datom :as d]
            [taoensso.nippy :as nippy])
  (:import [datalevin.lmdb LMDB]
           [datalevin.bits Indexable]
           [org.lmdbjava PutFlags CursorIterable$KeyVal]))

(defprotocol IStore
  (close [this] "Close storage")
  (init-max-tx [this] "Initialize and return the max transaction id")
  (init-max-eid [this] "Initialize and return the max entity id")
  (init-schema [this] "Initialize and return the schema")
  (update-schema [this attr props] "Update the schema")
  (insert [this datom indexing?] "Insert an datom")
  (delete [this datom] "Delete an datom")
  (slice [this index start-datom end-datom]
    "Return a range of datoms for the given index")
  (rslice [this index start-datom end-datom]
    "Return a range of datoms in reverse for the given index"))

(deftype Store [^LMDB lmdb ^:volatile-mutable schema ^:volatile-mutable max-dt]
  IStore
  (close [_]
    (lmdb/close lmdb))
  (init-max-tx [_]
    )
  (init-max-eid [this]
    )
  (init-schema [this]
    )
  (update-schema [this attr props]
    )
  (insert [_ datom indexing?]
    (locking max-dt
      (let [s (schema (.-a datom))
            i (b/indexable (.-e datom)
                           (:db/aid s)
                           (.-v datom)
                           (d/datom-tx datom)
                           (:db/valueType s))]
        (lmdb/transact lmdb
                       (cond-> [[:put c/eavt i max-dt :eavt :long]
                                [:put c/aevt i max-dt :aevt :long]
                                [:put c/datoms max-dt datom :long :datom
                                 [PutFlags/MDB_APPEND]]]
                         indexing? (concat
                                    [[:put c/avet i max-dt :avet :long]
                                     [:put c/vaet i max-dt :vaet :long]])))
        (set! max-dt (inc max-dt)))))
  (delete [_ datom]
    )
  (slice [_ index start-datom end-datom]
    )
  (rslice [_ index start-datom end-datom]
    ))

(defn- init-max-dt
  [lmdb]
  (or (first (lmdb/get-first lmdb c/datoms [:all-back] :long :ignore))
      0))

(defn open
  "Open and return the storage."
  [dir]
  (let [lmdb (lmdb/open-lmdb dir)]
    (lmdb/open-dbi lmdb c/eavt c/+max-key-size+ Long/BYTES)
    (lmdb/open-dbi lmdb c/aevt c/+max-key-size+ Long/BYTES)
    (lmdb/open-dbi lmdb c/avet c/+max-key-size+ Long/BYTES)
    (lmdb/open-dbi lmdb c/vaet c/+max-key-size+ Long/BYTES)
    (lmdb/open-dbi lmdb c/datoms Long/BYTES)
    (lmdb/open-dbi lmdb c/schema c/+max-key-size+)
    (->Store lmdb (init-max-dt lmdb))))
