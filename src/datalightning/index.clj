(ns datalightning.index
  "Implement indices on LMDB"
  (:require [datalightning.lmdb :as lmdb]
            [datalightning.util :as util])
  (:import [datalightning.lmdb LMDB]
           [org.lmdbjava DbiFlags KeyRange]))

;; dbi-names
(def ^:const eavt "eavt")
(def ^:const aevt "aevt")
(def ^:const avet "avet")
(def ^:const datoms "datoms")
(def ^:const config "config")

(defn init-system
  "create db and necessary dbis; max-val is the maximal datom size in bytes"
  [dir max-val]
  (let [lmdb (lmdb/open-lmdb dir)]
    (lmdb/open-dbi
     lmdb eavt lmdb/+max-key-size+ Long/BYTES lmdb/default-dbi-flags)
    (lmdb/open-dbi
     lmdb aevt lmdb/+max-key-size+ Long/BYTES lmdb/default-dbi-flags)
    (lmdb/open-dbi
     lmdb avet lmdb/+max-key-size+ Long/BYTES lmdb/default-dbi-flags)
    (lmdb/open-dbi lmdb datoms Long/BYTES max-val
                   (conj lmdb/default-dbi-flags DbiFlags/MDB_INTEGERKEY))
    (lmdb/open-dbi lmdb config )
    lmdb))


(defprotocol IIndex
  (add [this datom])
  (rmv [this datom])
  (slice [this start-datom end-datom])
  (rslice [this start-datom end-datom]))

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
