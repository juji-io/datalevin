(ns datalevin.constants)

;; TODO remove all traces of tx
;; datom

(def ^:const e0    0)
(def ^:const tx0   0x2000000000000000)
(def ^:const emax  0x7FFFFFFFFFFFFFFF)
(def ^:const txmax 0x7FFFFFFFFFFFFFFF)
(def ^:const implicit-schema
  {:db/ident {:db/unique :db.unique/identity :db/aid 0}})

;; lmdb

(def ^:const +max-dbs+          128)
(def ^:const +max-readers+      126)
(def ^:const +init-db-size+     10)    ; in megabytes
(def ^:const +default-val-size+ 16384) ; in bytes
(def ^:const +max-key-size+     511)   ; in bytes

;; index storage

(def ^:const +val-prefix-size+  498)  ; - eid - aid - s
(def ^:const +val-bytes-wo-hdr+ 497)  ; - hdr
(def ^:const +val-bytes-trunc+  492)  ; - hsh - tr

;; value headers
(def ^:const type-long-neg (byte 0xC0))
(def ^:const type-long-pos (byte 0xC1))
(def ^:const type-float    (byte 0xF5))
(def ^:const type-double   (byte 0xF6))
(def ^:const type-instant  (byte 0xF7))
(def ^:const type-ref      (byte 0xF8))
(def ^:const type-uuid     (byte 0xF9))
(def ^:const type-string   (byte 0xFA))
(def ^:const type-keyword  (byte 0xFB))
(def ^:const type-symbol   (byte 0xFC))
(def ^:const type-boolean  (byte 0xFD))
(def ^:const type-bytes    (byte 0xFE))
;;(def ^:const type-TBD (byte 0xFF))

(def ^:const false-value   (byte 0x01))
(def ^:const true-value    (byte 0x02))

(def ^:const separator     (byte 0x00))
(def ^:const truncator     (byte 0xFF))
(def ^:const slash         (byte 0x2F))

(def separator-ba (byte-array [(byte 0x00)]))

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
