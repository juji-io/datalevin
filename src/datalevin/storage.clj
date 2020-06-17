(ns datalevin.storage
  "storage layer of datalevin"
  (:require [datalevin.lmdb :as lmdb]
            [datalevin.util :as util]
            [datalevin.datom :as datom]
            [taoensso.nippy :as nippy])
  (:import [datalevin.lmdb LMDB]
           [org.lmdbjava DbiFlags KeyRange]))

;; dbi-names
(def ^:const eavt "eavt")
(def ^:const aevt "aevt")
(def ^:const avet "avet")
(def ^:const vaet "vaet") ; for datoms with ref attr
(def ^:const datoms "datoms")
(def ^:const config "config")

(def ^:const +config-key-size+ 1)
(def ^:const +config-val-size+ 102400) ; 100 kilobytes

;; config keys
(def max-eid (byte 0x01))
(def max-tx (byte 0x02))
(def schema (byte 0x03))
(def rschema (byte 0x04))

(def separator (byte 0x7f))

(defprotocol IStore
  (close [this] "Close storage")
  (insert [this datom] "Insert an datom")
  (delete [this datom] "Delete an datom")
  (slice [this index start-datom end-datom]
    "Return a range of datoms for the given index")
  (rslice [this index start-datom end-datom]
    "Return a range of datoms in reverse for the given index"))

(deftype Store [^LMDB lmdb ^long max-datom-size]
  IStore
  (close [_]
    (lmdb/close lmdb))
  (insert [_ datom])
  (delete [_ datom])
  (slice [_ index start-datom end-datom])
  (rslice [_ index start-datom end-datom]))

(defn open
  "Open and return the storage. max-datom-size is in bytes, default is 16kb"
  ([dir]
   (open dir lmdb/+default-val-size+))
  ([dir max-datom-size]
   (let [lmdb (lmdb/open-lmdb dir)]
     (lmdb/open-dbi
      lmdb eavt lmdb/+max-key-size+ Long/BYTES lmdb/default-dbi-flags)
     (lmdb/open-dbi
      lmdb aevt lmdb/+max-key-size+ Long/BYTES lmdb/default-dbi-flags)
     (lmdb/open-dbi
      lmdb avet lmdb/+max-key-size+ Long/BYTES lmdb/default-dbi-flags)
     (lmdb/open-dbi
      lmdb vaet lmdb/+max-key-size+ Long/BYTES lmdb/default-dbi-flags)
     (lmdb/open-dbi
      lmdb datoms Long/BYTES max-datom-size lmdb/default-dbi-flags)
     (lmdb/open-dbi
      lmdb config +config-key-size+ +config-val-size+ lmdb/default-dbi-flags)
     (->Store lmdb max-datom-size))))
