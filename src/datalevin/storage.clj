(ns datalevin.storage
  "storage layer of datalevin"
  (:require [datalevin.lmdb :as lmdb]
            [datalevin.util :as util])
  (:import [datalevin.lmdb LMDB]
           [org.lmdbjava DbiFlags KeyRange]))

;; dbi-names
(def ^:const eavt "eavt")
(def ^:const aevt "aevt")
(def ^:const avet "avet")
(def ^:const datoms "datoms")
(def ^:const schemas "schemas")
(def ^:const counts "counts")  ;;

(def ^:const +internal-key-size+ 64)
(def ^:const +schema-val-size+ 4064)

(defn init-system
  "create db and necessary dbis; max-datom-size is in bytes"
  [dir max-datom-size]
  (let [lmdb (lmdb/open-lmdb dir)]
    (lmdb/open-dbi
     lmdb eavt lmdb/+max-key-size+ Long/BYTES lmdb/default-dbi-flags)
    (lmdb/open-dbi
     lmdb aevt lmdb/+max-key-size+ Long/BYTES lmdb/default-dbi-flags)
    (lmdb/open-dbi
     lmdb avet lmdb/+max-key-size+ Long/BYTES lmdb/default-dbi-flags)
    (lmdb/open-dbi lmdb datoms Long/BYTES max-datom-size
                   (conj lmdb/default-dbi-flags DbiFlags/MDB_INTEGERKEY))
    (lmdb/open-dbi lmdb schemas +internal-key-size+ )
    lmdb))

(defprotocol IIndex
  (slice [this start-datom end-datom] "Return a range of index")
  (rslice [this start-datom end-datom] "Return a range of index in reverse"))

(deftype EAVT [^LMDB lmdb]
  IIndex
  )

(deftype AEVT [^LMDB lmdb]
  IIndex
  (empty-index [_]
    (lmdb/open-dbi lmdb
                   aevt
                   lmdb/+max-key-size+
                   Long/BYTES
                   lmdb/default-dbi-flags)))
