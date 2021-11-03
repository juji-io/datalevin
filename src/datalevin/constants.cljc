(ns ^:no-doc datalevin.constants
  (:refer-clojure :exclude [meta])
  (:import [java.util UUID Arrays]))

;;---------------------------------------------
;; system constants, fixed

;; datom

(def ^:const e0    0)
(def ^:const tx0   0x20000000)
(def ^:const emax  0x7FFFFFFF)
(def ^:const txmax 0x7FFFFFFF)
(def ^:const implicit-schema
  {:db/ident {:db/unique    :db.unique/identity
              :db/valueType :db.type/keyword
              :db/aid       0}})

(def ^:const v0    :db.value/sysMin)
(def ^:const vmax  :db.value/sysMax)
(def ^:const a0    0)
(def ^:const amax  0x7FFFFFFF)

;; lmdb

(def default-env-flags [:nordahead :mapasync :writemap])

(def default-dbi-flags [:create])

(def read-dbi-flags [])

(def default-put-flags [])

(def ^:const +max-key-size+     511)   ; in bytes

;; index storage

(def ^:const +val-prefix-size+  498)  ; - eid - aid - s
(def ^:const +val-bytes-wo-hdr+ 496)  ; - hdr - 1 byte TBD
(def ^:const +val-bytes-trunc+  491)  ; - hsh - tr

(def ^:const +id-bytes+ Long/BYTES)

(def ^:const lmdb-data-types #{:data :string :int :long :id :float :double
                               :byte :bytes :keyword :boolean :instant :uuid
                               :datom :attr :eav :ave :vea})

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

(def ^:const overflown :overflown-key)
(def ^:const normal 0)
(def ^:const gt0 1)

(def ^:const index-types #{:eavt :eav :ave :avet :vea :veat})


;; dbi-names
(def ^:const eav "datalevin/eav")
(def ^:const ave "datalevin/ave")
(def ^:const vea "datalevin/vea")
(def ^:const giants "datalevin/giants")
(def ^:const schema "datalevin/schema")
(def ^:const meta "datalevin/meta")
(def ^:const terms "datalevin/terms")         ; term-id -> term
(def ^:const docs "datalevin/docs")           ; doc-ref -> doc-id,norm
(def ^:const term-freq "datalevin/term-freq") ; term-id,doc-id -> freq
(def ^:const term-docs "datalevin/term-docs") ; term-id -> doc-id (list)
(def ^:const positions "datalevin/positions") ; doc-id,term-id -> position,offset (list)

(def ^:const datalog-value-types #{:db.type/keyword :db.type/symbol
                                   :db.type/string :db.type/boolean
                                   :db.type/long :db.type/double
                                   :db.type/float :db.type/ref
                                   :db.type/instant :db.type/uuid
                                   :db.type/bytes})

;; search engine

(def ^:const +max-term-length+ 128) ; we ignore exceedingly long strings

;; server / client

(def ^:const default-port (int 8898))

(def ^:const
  system-dir "system")

(def ^:const default-username "datalevin")
(def ^:const default-password "datalevin")

(def ^:const db-store-datalog "datalog")
(def ^:const db-store-kv "kv")

(def ^:const dl-type :datalog)
(def ^:const kv-type :key-value)

(def ^:const message-header-size 5) ; bytes, 1 type + 4 length

(def ^:const message-format-transit (unchecked-byte 0x01))
(def ^:const message-format-nippy (unchecked-byte 0x02))


;;-------------------------------------------------------------

;; user configurable TODO: make it so

;; general

(def +buffer-grow-factor+ 10)

;; lmdb

(def +max-dbs+          128)
(def +max-readers+      126)
(def +use-readers+      32)    ; leave the rest to others
(def +init-db-size+     100)   ; in megabytes
(def +default-val-size+ 16384) ; in bytes

;; storage

(def +tx-datom-batch-size+ 100000)

;; query

(def +cache-limit+ 1000)  ; per Datalog db

;; client/server

(def +default-buffer-size+ 65536) ; in bytes

(def +wire-datom-batch-size+ 1000)

(def connection-pool-size 5)
(def connection-timeout 30000) ; in milliseconds

;;search engine

(def en-stop-words (hash-set "a", "an", "and", "are", "as", "at", "be",
                             "but", "by", "for", "if", "in", "into", "is",
                             "it", "no", "not", "of", "on", "or", "such",
                             "that", "the",  "their", "then", "there", "these",
                             "they", "this", "to", "was", "will", "with"))

(def en-punctuations (hash-set \: \/ \. \; \, \! \= \? \" \' \( \) \[ \] \{ \}
                               \| \< \> \& \@ \# \^ \* \\ \~ \`))

(def dict-max-edit-distance 2)

(def dict-prefix-length 10)
