(ns ^:no-doc datalevin.constants
  (:import [java.util UUID Arrays]))

;; datom

(def ^:const e0    0)
(def ^:const tx0   0x20000000)
(def ^:const emax  0x7FFFFFFF)
(def ^:const txmax 0x7FFFFFFF)
(def ^:const implicit-schema
  {:db/ident {:db/unique :db.unique/identity :db/aid 0}})

(def ^:const v0    :db.value/sysMin)
(def ^:const vmax  :db.value/sysMax)
(def ^:const a0    0)
(def ^:const amax  0x7FFFFFFF)

;; lmdb

(def ^:const +max-dbs+          128)
(def ^:const +max-readers+      126)
(def ^:const +use-readers+      24)    ; leave the rest to others
(def ^:const +init-db-size+     100)   ; in megabytes
(def ^:const +default-val-size+ 16384) ; in bytes
(def ^:const +max-key-size+     511)   ; in bytes

;; index storage

(def ^:const +val-prefix-size+  498)  ; - eid - aid - s
(def ^:const +val-bytes-wo-hdr+ 496)  ; - hdr - 1 byte TBD
(def ^:const +val-bytes-trunc+  491)  ; - hsh - tr

;; value headers
(def ^:const type-long-neg (unchecked-byte 0xC0))
(def ^:const type-long-pos (unchecked-byte 0xC1))
(def ^:const type-float    (unchecked-byte 0xF5))
(def ^:const type-double   (unchecked-byte 0xF6))
(def ^:const type-instant  (unchecked-byte 0xF7))
(def ^:const type-ref      (unchecked-byte 0xF8))
(def ^:const type-uuid     (unchecked-byte 0xF9))
(def ^:const type-string   (unchecked-byte 0xFA))
(def ^:const type-keyword  (unchecked-byte 0xFB))
(def ^:const type-symbol   (unchecked-byte 0xFC))
(def ^:const type-boolean  (unchecked-byte 0xFD))
(def ^:const type-bytes    (unchecked-byte 0xFE))

(def ^:const false-value   (unchecked-byte 0x01))
(def ^:const true-value    (unchecked-byte 0x02))

(def ^:const separator     (unchecked-byte 0x00))
(def ^:const truncator     (unchecked-byte 0xFF))
(def ^:const slash         (unchecked-byte 0x2F))

(def separator-ba (byte-array [(unchecked-byte 0x00)]))

(def max-uuid (UUID. -1 -1))
(def min-uuid (UUID. 0 0))

(def max-bytes (let [ba (byte-array +val-bytes-wo-hdr+)]
                 (Arrays/fill ba (unchecked-byte 0xFF))
                 ba))
(def min-bytes (byte-array 0))

(def ^:const normal 0)
(def ^:const gt0 1)

;; dbi-names
(def ^:const eav "eav")
(def ^:const aev "aev")
(def ^:const ave "ave")
(def ^:const vae "vae")
(def ^:const giants "giants")
(def ^:const schema "schema")

(def ^:const buffer-overflow "BufferOverflow:")
