(ns datalevin.storage
  "storage layer of datalevin"
  (:require [datalevin.lmdb :as lmdb]
            [datalevin.util :as util]
            [datalevin.constants :as c]
            [datalevin.datom :as d]
            [taoensso.nippy :as nippy])
  (:import [datalevin.lmdb LMDB]
           [org.lmdbjava PutFlags]))

(defprotocol IStore
  (close [this] "Close storage")
  (init-max-tx [this] "Initialize and return the max transaction id")
  (init-max-eid [this] "Initialize and return the max entity id")
  (insert [this datom] "Insert an datom")
  (delete [this datom] "Delete an datom")
  (slice [this index start-datom end-datom]
    "Return a range of datoms for the given index")
  (rslice [this index start-datom end-datom]
    "Return a range of datoms in reverse for the given index"))

(deftype Store [^LMDB lmdb ^:volatile-mutable max-dt]
  IStore
  (close [_]
    (lmdb/close lmdb))
  (init-max-tx [_]
    )
  (init-max-eid [this]
    )
  (insert [_ [e a v t]]
    ;; datoms dbi should put with PutFlags/MDB_APPEND
    (let []
      (lmdb/transact lmdb
                    [[:put c/eavt ]]))
    )
  (delete [_ datom])
  (slice [_ index start-datom end-datom])
  (rslice [_ index start-datom end-datom]))

(defn- init-max-dt
  [lmdb]
  )

(defn open
  "Open and return the storage."
  [dir]
  (let [lmdb (lmdb/open-lmdb dir)]
    (lmdb/open-dbi lmdb c/eavt c/+max-key-size+ Long/BYTES)
    (lmdb/open-dbi lmdb c/aevt c/+max-key-size+ Long/BYTES)
    (lmdb/open-dbi lmdb c/avet c/+max-key-size+ Long/BYTES)
    (lmdb/open-dbi lmdb c/vaet c/+max-key-size+ Long/BYTES)
    (lmdb/open-dbi lmdb c/datoms Long/BYTES)
    (lmdb/open-dbi lmdb c/config c/+config-key-size+)
    (->Store lmdb (init-max-dt lmdb))))
