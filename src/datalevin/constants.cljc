(ns datalevin.constants)

;; datom

(def ^:const e0    0)
(def ^:const tx0   0x2000000000000000)
(def ^:const emax  0x7FFFFFFFFFFFFFFF)
(def ^:const txmax 0x7FFFFFFFFFFFFFFF)
(def ^:const implicit-schema {:db/ident {:db/unique :db.unique/identity}})

(def ^:const +max-attr-size+ 400)
(def ^:const +idx-attr+prefix-size+ 493) ; 511 - e - t - 2s

;; lmdb

(def ^:const +max-dbs+ 128)
(def ^:const +max-readers+ 126)
(def ^:const +init-db-size+ 100) ; in megabytes
(def ^:const +max-key-size+ 511) ; in bytes
(def ^:const +default-val-size+ 16384) ; 16 kilobytes

;; storage

(def ^:const buffer-overflow "BufferOverflow:")

(def ^:const +config-key-size+ 1)

(def ^:const separator (byte 0x7F))

;; dbi-names
(def ^:const eavt "eavt")
(def ^:const aevt "aevt")
(def ^:const avet "avet")
(def ^:const vaet "vaet")
(def ^:const datoms "datoms")
(def ^:const config "config")

;; config keys
(def ^:const schema (byte 0x01))
(def ^:const rschema (byte 0x02))
