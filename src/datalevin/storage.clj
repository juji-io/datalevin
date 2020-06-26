(ns datalevin.storage
  "storage layer of datalevin"
  (:require [datalevin.lmdb :as lmdb]
            [datalevin.util :as util]
            [datalevin.bits :as b]
            [datalevin.constants :as c]
            [datalevin.datom :as d]
            [taoensso.nippy :as nippy])
  (:import [datalevin.lmdb LMDB]
           [datalevin.datom Datom]
           [datalevin.bits Indexable]
           [org.lmdbjava PutFlags CursorIterable$KeyVal]))

(defprotocol IStore
  (close [this] "Close storage")
  (init-max-eid [this] "Initialize and return the max entity id")
  (init-schema [this] "Initialize and return the schema")
  (update-schema [this attr props] "Update the schema")
  (insert [this datom indexing?] "Insert an datom")
  (delete [this datom] "Delete an datom")
  (slice [this index start-datom end-datom]
    "Return a range of datoms for the given index")
  (rslice [this index start-datom end-datom]
    "Return a range of datoms in reverse for the given index"))

(deftype Store [^LMDB lmdb
                ^:volatile-mutable schema
                ^:volatile-mutable ^long max-gt]
  IStore
  (close [_]
    (lmdb/close lmdb))
  (init-max-eid [this]
    )
  (init-schema [this]
    )
  (update-schema [this attr props]
    )
  (insert [_ datom indexing?]
    (locking max-gt
      (let [s (schema (.-a ^Datom datom))
            i (b/indexable (.-e ^Datom datom)
                           (:db/aid s)
                           (.-v ^Datom datom)
                           (:db/valueType s))]
        (lmdb/transact lmdb
                       (cond-> [[:put c/eav i max-gt :eav :long]
                                [:put c/aev i max-gt :aev :long]
                                [:put c/giants max-gt datom :long :datom
                                 [PutFlags/MDB_APPEND]]]
                         indexing? (concat
                                    [[:put c/ave i max-gt :ave :long]
                                     [:put c/vae i max-gt :vae :long]])))
        (set! max-gt (inc max-gt)))))
  (delete [_ datom]
    )
  (slice [_ index start-datom end-datom]
    )
  (rslice [_ index start-datom end-datom]
    ))

(defn- init-max-gt
  [lmdb]
  (or (first (lmdb/get-first lmdb c/giants [:all-back] :long :ignore))
      c/gt0))

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
    (->Store lmdb (init-max-gt lmdb))))
