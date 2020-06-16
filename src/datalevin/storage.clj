(ns datalevin.storage
  "storage layer of datalevin"
  (:require [datalevin.lmdb :as lmdb]
            [datalevin.util :as util]
            [taoensso.nippy :as nippy])
  (:import [datalevin.lmdb LMDB]
           [org.lmdbjava DbiFlags KeyRange]))

;; dbi-names
(def ^:const eavt "eavt")
(def ^:const aevt "aevt")
(def ^:const avet "avet")
(def ^:const vaet "vaet") ;; for datoms with ref attr
(def ^:const datoms "datoms")
(def ^:const config "config")

(def ^:const +config-key-size+ 1)
(def ^:const +config-val-size+ 4095)

;; config keys
(def max-eid (byte 0x01))
(def max-tx (byte 0x02))
(def schema (byte 0x03))
(def rschema (byte 0x04))

(def separator (byte 0x7f))

(defn init
  "create db and necessary dbis; max-datom-size is in bytes"
  [dir max-datom-size]
  (let [lmdb (lmdb/open-lmdb dir)]
    (lmdb/open-dbi
     lmdb eavt lmdb/+max-key-size+ Long/BYTES lmdb/default-dbi-flags)
    (lmdb/open-dbi
     lmdb aevt lmdb/+max-key-size+ Long/BYTES lmdb/default-dbi-flags)
    (lmdb/open-dbi
     lmdb avet lmdb/+max-key-size+ Long/BYTES lmdb/default-dbi-flags)
    (lmdb/open-dbi
     lmdb datoms Long/BYTES max-datom-size lmdb/default-dbi-flags)
    (lmdb/open-dbi
     lmdb config +config-key-size+ +config-val-size+ lmdb/default-dbi-flags)
    lmdb))

(defprotocol IStore
  (insert [this datom])
  (slice [this start-datom end-datom] "Return a range of datoms")
  (rslice [this start-datom end-datom] "Return a range of datoms in reverse"))

(vec (nippy/fast-freeze :a/a))
;; => [106 3 97 47 97]
(vec (nippy/fast-freeze :aa/a))
;; => [106 4 97 97 47 97]
(vec (nippy/fast-freeze :a/b))
;; => [106 3 97 47 98]
(vec (nippy/fast-freeze :b/a))
;; => [106 3 98 47 97]
