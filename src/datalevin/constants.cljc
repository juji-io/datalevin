(ns datalevin.constants)

;; datom

(def ^:const e0    0)
(def ^:const tx0   0x2000000000000000)
(def ^:const emax  0x7FFFFFFFFFFFFFFF)
(def ^:const txmax 0x7FFFFFFFFFFFFFFF)
(def ^:const implicit-schema {:db/ident {:db/unique :db.unique/identity}})

;; lmdb

(def ^:const +max-dbs+ 128)
(def ^:const +max-readers+ 126)
(def ^:const +init-db-size+ 10) ; in megabytes
(def ^:const +max-key-size+ 511) ; in bytes
(def ^:const +default-val-size+ 16384) ; 16 kilobytes

(def ^:const +val-prefix-size+ 490) ; 511 - eid - aid - tx - s
(def ^:const +val-bytes-wo-hdr+ 489) ; sans 1 byte header
(def ^:const +val-bytes-trunc+ 485) ; sans 4 byte hash

;; index storage

(def ^:const buffer-overflow "BufferOverflow:")

;; value headers, use forbidden bytes of utf-8
(def ^:const type-long-neg (byte 0xC0))
(def ^:const type-long-pos (byte 0xC1))
(def ^:const type-double-neg (byte 0xF5))
(def ^:const type-double-pos (byte 0xF6))
(def ^:const type-instant (byte 0xF7))
(def ^:const type-ref (byte 0xF8))
(def ^:const type-uuid (byte 0xF9))
(def ^:const type-string (byte 0xFA))
(def ^:const type-keyword (byte 0xFB))
(def ^:const type-symbol (byte 0xFC))
(def ^:const type-boolean (byte 0xFD))
(def ^:const type-bytes (byte 0xFE))
;;(def ^:const type-TBD (byte 0xFF))

(def ^:const false-value (byte 0x01))
(def ^:const true-value (byte 0x02))

(def ^:const separator (byte 0x00))

;; dbi-names
(def ^:const eavt "eavt")
(def ^:const aevt "aevt")
(def ^:const avet "avet")
(def ^:const vaet "vaet")
(def ^:const datoms "datoms")
(def ^:const schema "schema")
