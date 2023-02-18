(ns ^:no-doc datalevin.constants
  (:refer-clojure :exclude [meta])
  (:require
   [datalevin.util :as u])
  (:import
   [java.util UUID Arrays HashSet]
   [java.math BigInteger BigDecimal]))

;;---------------------------------------------
;; system constants, fixed

;; datom

(def ^:const e0    0)
(def ^:const emax  0x7FFFFFFFFFFFFFFF)
(def ^:const tx0   1)
(def ^:const txmax 0x7FFFFFFFFFFFFFFF)
(def ^:const v0    :db.value/sysMin)
(def ^:const vmax  :db.value/sysMax)
(def ^:const g0    1)
(def ^:const gmax  0x7FFFFFFFFFFFFFFF)
(def ^:const a0    0)
(def ^:const amax  0x7FFFFFFF)

;; schema

(def ^:const implicit-schema
  {:db/ident      {:db/unique    :db.unique/identity
                   :db/valueType :db.type/keyword
                   :db/aid       0}
   :db/created-at {:db/valueType   :db.type/long
                   :db/cardinality :db.cardinality/one
                   :db/aid         1}
   :db/updated-at {:db/valueType   :db.type/long
                   :db/cardinality :db.cardinality/one
                   :db/aid         2}})

(def ^:const entity-time-schema
  {:db/created-at {:db/valueType   :db.type/long
                   :db/cardinality :db.cardinality/one}
   :db/updated-at {:db/valueType   :db.type/long
                   :db/cardinality :db.cardinality/one}})

;; lmdb

(def default-env-flags [:nordahead :mapasync :writemap :notls])

(def default-dbi-flags [:create])

(def read-dbi-flags [])

(def default-put-flags [])

(def ^:const +max-key-size+     511)   ; in bytes

;; tmp lmdb

(def ^:const +default-spill-threshold+ 80)   ; percentage of Xmx
(def ^:const +default-spill-root+ (u/tmp-dir))

(def ^:const tmp-dbi "t")


;; index storage

(def ^:const +val-bytes-wo-hdr+ 493)  ; - hdr - eid - s - gid
(def ^:const +val-bytes-trunc+  492)  ; - tr

(def ^:const +id-bytes+ Long/BYTES)
(def ^:const +short-id-bytes+ Integer/BYTES)

;; value headers
(def ^:const type-long-neg   (unchecked-byte 0xC0))
(def ^:const type-long-pos   (unchecked-byte 0xC1))
(def ^:const type-bigint     (unchecked-byte 0xF1))
(def ^:const type-bigdec     (unchecked-byte 0xF2))
(def ^:const type-homo-tuple (unchecked-byte 0xF3))
(def ^:const type-hete-tuple (unchecked-byte 0xF4))
(def ^:const type-float      (unchecked-byte 0xF5))
(def ^:const type-double     (unchecked-byte 0xF6))
(def ^:const type-instant    (unchecked-byte 0xF7))
(def ^:const type-ref        (unchecked-byte 0xF8))
(def ^:const type-uuid       (unchecked-byte 0xF9))
(def ^:const type-string     (unchecked-byte 0xFA))
(def ^:const type-keyword    (unchecked-byte 0xFB))
(def ^:const type-symbol     (unchecked-byte 0xFC))
(def ^:const type-boolean    (unchecked-byte 0xFD))
(def ^:const type-bytes      (unchecked-byte 0xFE))

(def ^:const false-value     (unchecked-byte 0x01))
(def ^:const true-value      (unchecked-byte 0x02))

(def ^:const separator       (unchecked-byte 0x00))
(def ^:const truncator       (unchecked-byte 0xFF))
(def ^:const slash           (unchecked-byte 0x2F))

(def separator-ba (byte-array [(unchecked-byte 0x00)]))

(def max-uuid (UUID. -1 -1))
(def min-uuid (UUID. 0 0))

(def max-bytes (let [ba (byte-array +val-bytes-wo-hdr+)]
                 (Arrays/fill ba (unchecked-byte 0xFF))
                 ba))
(def min-bytes (byte-array 0))

(defn- max-bigint-bs
  ^bytes []
  (let [^bytes bs (byte-array 127)]
    (aset bs 0 (unchecked-byte 0x7f))
    (dotimes [i 126] (aset bs (inc i) (unchecked-byte 0xff)))
    bs))

(def max-bigint (BigInteger. (max-bigint-bs)))

(defn- min-bigint-bs
  ^bytes []
  (let [bs (byte-array 127)]
    (aset bs 0 (unchecked-byte 0x80))
    (dotimes [i 126] (aset bs (inc i) (unchecked-byte 0x00)))
    bs))

(def min-bigint (BigInteger. (min-bigint-bs)))

(def max-bigdec (BigDecimal. ^BigInteger max-bigint Integer/MIN_VALUE))

(def min-bigdec (BigDecimal. ^BigInteger min-bigint Integer/MIN_VALUE))

(def ^:const normal 0)  ; non-giant datom

;; dbi-names

;; kv
(def ^:const kv-meta "datalevin/kv-meta")

;; dl
(def ^:const eav "datalevin/eav")
(def ^:const ave "datalevin/ave")
(def ^:const vea "datalevin/vea")
(def ^:const giants "datalevin/giants")
(def ^:const schema "datalevin/schema")
(def ^:const meta "datalevin/meta")
(def ^:const opts "datalevin/opts")

;; search
(def ^:const terms "terms")
(def ^:const docs "docs")
(def ^:const positions "positions")

(def ^:const datalog-value-types
  #{:db.type/keyword :db.type/symbol :db.type/string :db.type/boolean
    :db.type/long :db.type/double :db.type/float :db.type/ref
    :db.type/bigint :db.type/bigdec :db.type/instant :db.type/uuid
    :db.type/bytes :db.type/tuple})

(def ^:const kv-value-types
  #{:keyword :symbol :string :boolean :long :double :float :instant :uuid
    :bytes :bigint :bigdec :data})

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

;; dynamic


;;-------------------------------------------------------------

;; user configurable TODO: make it so


;; general

(def +buffer-grow-factor+ 10)

;; serialization

(def ^{:dynamic true
       :doc     "set of additional serializable classes, e.g.
                  `#{\"my.package.*\"}`"}
  *data-serializable-classes* #{})

;; lmdb

(def +max-dbs+          128)
(def +max-readers+      126)
(def +use-readers+      32)    ; leave the rest to others
(def +init-db-size+     100)   ; in megabytes
(def +default-val-size+ 16384) ; in bytes

;; query

(def +cache-limit+ 100)  ; per Datalog db

;; client/server

(def +default-buffer-size+ 65536) ; in bytes

(def +wire-datom-batch-size+ 1000)

(def connection-pool-size 3)
(def connection-timeout 60000) ; in milliseconds

;;search engine

(def en-stop-words-set
  (let [s (HashSet.)]
    (doseq [w ["a",    "an",   "and",   "are",  "as",    "at",   "be",
               "but",  "by",   "for",   "if",   "in",    "into", "is",
               "it",   "no",   "not",   "of",   "on",    "or",   "such",
               "that", "the",  "their", "then", "there", "these",
               "they", "this", "to",    "was",  "will",  "with"]]
      (.add s w))
    s))

(defn en-stop-words?
  "return true if the given word is an English stop words"
  [w]
  (.contains ^HashSet en-stop-words-set w))

(def en-punctuations-set
  (let [s (HashSet.)]
    (doseq [c [\: \/ \. \; \, \! \= \? \" \' \( \) \[ \] \{ \}
               \| \< \> \& \@ \# \^ \* \\ \~ \`]]
      (.add s c))
    s))

(defn en-punctuations? [c] (.contains ^HashSet en-punctuations-set c))
