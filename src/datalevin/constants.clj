(ns datalevin.constants
  "System vars. Some can be dynamically rebound to change system behavior."
  (:refer-clojure :exclude [meta])
  (:require
   [datalevin.util :as u])
  (:import
   [java.io File]
   [java.util UUID Arrays HashSet]
   [java.math BigInteger BigDecimal]))

;;---------------------------------------------
;; system constants, fixed
;;---------------------------------------------

;; datom

(def ^:no-doc ^:const e0    0)
(def ^:no-doc ^:const emax  0x7FFFFFFFFFFFFFFF)
(def ^:no-doc ^:const tx0   1)
(def ^:no-doc ^:const txmax 0x7FFFFFFFFFFFFFFF)
(def ^:no-doc ^:const g0    1)
(def ^:no-doc ^:const gmax  0x7FFFFFFFFFFFFFFF)
(def ^:no-doc ^:const a0    0)
(def ^:no-doc ^:const amax  0x7FFFFFFF)
(def ^:no-doc v0    :db.value/sysMin)
(def ^:no-doc vmax  :db.value/sysMax)

;; schema

(def ^:no-doc implicit-schema
  {:db/ident      {:db/unique    :db.unique/identity
                   :db/valueType :db.type/keyword
                   :db/aid       0}
   :db/created-at {:db/valueType   :db.type/long
                   :db/cardinality :db.cardinality/one
                   :db/aid         1}
   :db/updated-at {:db/valueType   :db.type/long
                   :db/cardinality :db.cardinality/one
                   :db/aid         2}})

(def ^:no-doc entity-time-schema
  {:db/created-at {:db/valueType   :db.type/long
                   :db/cardinality :db.cardinality/one}
   :db/updated-at {:db/valueType   :db.type/long
                   :db/cardinality :db.cardinality/one}})

;; lmdb

(def default-env-flags
  "Default LMDB env flag is `#{:nordahead :notls :writemap :nometasync}`. See
  http://www.lmdb.tech/doc/group__mdb__env.html for full list of flags.

  Passed as `:flags` option value to `open-kv` function."
  #{:nordahead :notls :writemap :nometasync})

(def default-dbi-flags
  "Default LMDB dbi flag is `#{:create}`. See http://www.lmdb.tech/doc/group__mdb__dbi__open.html for full list of flags"
  #{:create})

(def default-put-flags
  "Default LMDB put flag is `#{}`. See http://www.lmdb.tech/doc/group__mdb__put.html for full list of flags"
  #{})

(def ^:const +max-key-size+
  "Maximum LMDB key size is 511 bytes"
  511)

(def ^:no-doc ^:const +buffer-grow-factor+ 10)

;; # of times db can be auto enlarged in a tx
(def ^:no-doc ^:const +in-tx-overflow-times+ 5)

;; tmp lmdb

(def ^:const default-spill-threshold
  "Default percentage of heap memory (Xmx) is 95, over which spill-to-disk
  will be triggered"
  95)

(def default-spill-root
  "Default root directory of spilled files is platform dependent. the same as
  the value of Java property `java.io.tmpdir`"
  (u/tmp-dir))

(def ^:const tmp-dbi
  "Default dbi name of the spilled db is `t`"
  "t")

;; index storage

(def ^:no-doc ^:const +val-bytes-wo-hdr+ 497)  ; - hdr - s - g - a
(def ^:no-doc ^:const +val-bytes-trunc+  496)  ; - tr

(def ^:no-doc ^:const +id-bytes+ Long/BYTES)
(def ^:no-doc ^:const +short-id-bytes+ Integer/BYTES)

;; value headers
(def ^:no-doc ^:const type-long-neg   (unchecked-byte 0xC0))
(def ^:no-doc ^:const type-long-pos   (unchecked-byte 0xC1))
(def ^:no-doc ^:const type-bigint     (unchecked-byte 0xF1))
(def ^:no-doc ^:const type-bigdec     (unchecked-byte 0xF2))
(def ^:no-doc ^:const type-homo-tuple (unchecked-byte 0xF3))
(def ^:no-doc ^:const type-hete-tuple (unchecked-byte 0xF4))
(def ^:no-doc ^:const type-float      (unchecked-byte 0xF5))
(def ^:no-doc ^:const type-double     (unchecked-byte 0xF6))
(def ^:no-doc ^:const type-instant    (unchecked-byte 0xF7))
(def ^:no-doc ^:const type-ref        (unchecked-byte 0xF8))
(def ^:no-doc ^:const type-uuid       (unchecked-byte 0xF9))
(def ^:no-doc ^:const type-string     (unchecked-byte 0xFA))
(def ^:no-doc ^:const type-keyword    (unchecked-byte 0xFB))
(def ^:no-doc ^:const type-symbol     (unchecked-byte 0xFC))
(def ^:no-doc ^:const type-boolean    (unchecked-byte 0xFD))
(def ^:no-doc ^:const type-bytes      (unchecked-byte 0xFE))

(def ^:no-doc ^:const false-value     (unchecked-byte 0x01))
(def ^:no-doc ^:const true-value      (unchecked-byte 0x02))

(def ^:no-doc ^:const separator       (unchecked-byte 0x00))
(def ^:no-doc ^:const truncator       (unchecked-byte 0xFF))
(def ^:no-doc ^:const slash           (unchecked-byte 0x2F))

(def ^:no-doc separator-ba (byte-array [(unchecked-byte 0x00)]))

(def ^:no-doc max-uuid (UUID. -1 -1))
(def ^:no-doc min-uuid (UUID. 0 0))

(def ^:no-doc max-bytes (let [ba (byte-array +val-bytes-wo-hdr+)]
                          (Arrays/fill ba (unchecked-byte 0xFF))
                          ba))
(def ^:no-doc min-bytes (byte-array [0x00]))

(def ^:const +tuple-max+
  "Maximum length of a tuple is 255 bytes"
  255)

(def ^:no-doc tuple-max-bytes (let [ba (byte-array +tuple-max+)]
                                (Arrays/fill ba (unchecked-byte 0xFF))
                                ba))

(defn- max-bigint-bs
  ^bytes []
  (let [^bytes bs (byte-array 127)]
    (aset bs 0 (unchecked-byte 0x7f))
    (dotimes [i 126] (aset bs (inc i) (unchecked-byte 0xff)))
    bs))

(def max-bigint
  "Maximum big integer is `2^1015-1`"
  (BigInteger. (max-bigint-bs)))

(defn- min-bigint-bs
  ^bytes []
  (let [bs (byte-array 127)]
    (aset bs 0 (unchecked-byte 0x80))
    (dotimes [i 126] (aset bs (inc i) (unchecked-byte 0x00)))
    bs))

(def min-bigint
  "Minimum big integer is `-2^1015`"
  (BigInteger. (min-bigint-bs)))

(def max-bigdec
  "Maximum big decimal has value of `(2^1015-1) x 10^2147483648`"
  (BigDecimal. ^BigInteger max-bigint Integer/MIN_VALUE))

(def min-bigdec
  "Minimum big decimal has value of `-2^1015 x 10^2147483648`"
  (BigDecimal. ^BigInteger min-bigint Integer/MIN_VALUE))

(def ^:no-doc ^:const normal 0)  ; non-giant datom

;; dbi-names

;; kv
(def ^:const kv-info
  "dbi name for kv store system information is `datalevin/kv-info`"
  "datalevin/kv-info")

;; dl
(def ^:const eav
  "dbi name for Datalog EAV index is `datalevin/eav`"
  "datalevin/eav")
(def ^:const ave
  "dbi name for Datalog AVE index is `datalevin/ave`"
  "datalevin/ave")
(def ^:const vae
  "dbi name for Datalog VAE index is `datalevin/vae`"
  "datalevin/vae")
(def ^:const giants
  "dbi name for Datalog large datoms is `datalevin/giants`"
  "datalevin/giants")
(def ^:const schema
  "dbi name for Datalog schema is `datalevin/schema`"
  "datalevin/schema")
(def ^:const meta
  "dbi name for Datalog runtime meta information is `datalevin/meta`"
  "datalevin/meta")
(def ^:const opts
  "dbi name for Datalog options is `datalevin/opts`"
  "datalevin/opts")

;; compression

(def ^:no-doc ^:const +key-compress-num-symbols+ 65536)

(def ^:no-doc ^:const +value-compress-threshold+ 36)

;; search

(def ^:const default-domain "datalevin")

(def default-display
  "default `search` function `:display` option value is `:refs`"
  :refs)

(def ^:const default-top
  "default `search` function `:top` option value is `10`"
  10)

(def ^:const default-proximity-expansion
  "default `search` function `:proximity-expansion` option value is `2`"
  2)

(def ^:const default-proximity-max-dist
  "default `search` function `:proximity-max-dist` option value is `45`"
  45)

(def default-doc-filter (constantly true))

(def ^:const terms
  "dbi name suffix for search engine terms index is `terms`"
  "terms")

(def ^:const docs
  "dbi name suffix for search engine documents index is `docs`"
  "docs")

(def ^:const positions
  "dbi name suffix for search engine positions index is `positions`"
  "positions")

(def ^:const rawtext
  "dbi name suffix for search engine raw text is `rawtext`"
  "rawtext")

(def ^:const +max-term-length+
  "The full text search engine ignores exceedingly long strings. The maximal
  allowed term length is 128 characters"
  128)

;; data types

(def ^:no-doc datalog-value-types
  #{:db.type/keyword :db.type/symbol :db.type/string :db.type/boolean
    :db.type/long :db.type/double :db.type/float :db.type/ref
    :db.type/bigint :db.type/bigdec :db.type/instant :db.type/uuid
    :db.type/bytes :db.type/tuple})

(def ^:no-doc kv-value-types
  #{:keyword :symbol :string :boolean :long :double :float :instant :uuid
    :bytes :bigint :bigdec :data})

;; server / client

(def ^:no-doc ^:const +buffer-size+ 65536)

(def ^:no-doc ^:const +wire-datom-batch-size+ 1000)

(def ^:const default-port
  "The server default port number is 8898"
  (int 8898))

(def ^:const default-idle-timeout
  "The server session default idle timeout is 172800000ms (48 hours)"
  172800000)

(def ^:const default-connection-pool-size
  "The default client connection pool size is 3"
  3)

(def ^:const default-connection-timeout
  "The default connection timeout is 60000ms (1 minute)"
  60000)

(def ^:no-doc ^:const system-dir "system")

(def ^:const default-username
  "The server default username is `datalevin`"
  "datalevin")

(def ^:const default-password
  "The server default password is `datalevin`"
  "datalevin")

(def ^:no-doc ^:const db-store-datalog "datalog")
(def ^:no-doc ^:const db-store-kv "kv")

(def ^:no-doc dl-type :datalog)
(def ^:no-doc kv-type :key-value)

(def ^:no-doc ^:const message-header-size 5) ; bytes, 1 type + 4 length

(def ^:no-doc ^:const message-format-transit (unchecked-byte 0x01))
(def ^:no-doc ^:const message-format-nippy (unchecked-byte 0x02))

;;-------------------------------------------------------------
;; user configurable
;;-------------------------------------------------------------

;; serialization

(def ^{:dynamic true
       :doc     "set of additional serializable classes, e.g.
                  `#{\"my.package.*\"}`"}
  *data-serializable-classes* #{})

;; lmdb

(def ^{:dynamic true
       :doc     "Maximum number of sub-databases allowed in a db file"}
  *max-dbs* 128)

(def ^{:dynamic true
       :doc     "Maximum number of readers allowed for a db file"}
  *max-readers* 126)

(def ^{:dynamic true
       :doc     "Initial db file size is 1000 MiB, automatically grown"}
  *init-db-size* 1000)

(def ^{:dynamic true
       :doc     "Initial maximal value size is 16384 bytes, automatically grown"}
  *init-val-size* 16384)

(defn ^:no-doc pick-mapsize
  "pick a map size from the growing factor schedule that is larger than or
  equal to the current size"
  [dir]
  (let [^File file (u/file (str dir u/+separator+ "data.mdb"))
        cur-size   (.length file)]
    (some #(when (<= cur-size (* ^long % 1024 1024)) %)
          (iterate #(* ^long +buffer-grow-factor+ ^long %)
                   *init-db-size*))))

(def ^{:dynamic true
       :doc     "The number of samples considered when build the key compression dictionary is 65536"}
  *compress-sample-size* 65536)

;; datalog db

(def ^{:dynamic true
       :doc     "batch size (# of datoms) when filling DB"}
  *fill-db-batch-size* 1048576)

;; datalog query engine

(def ^{:dynamic true
       :doc     "Cost associated with running a predicate during scan"}
  magic-cost-pred 2.0)

(def ^{:dynamic true
       :doc     "Cost associated with adding attributes when estimating size"}
  magic-cost-attr 1.3)

(def ^{:dynamic true
       :doc     "Cost associated with running a filter during scan"}
  magic-cost-fidx 0.9)

(def ^{:dynamic true
       :doc     "Cost associated with hash join"}
  magic-cost-hash 2.5)

(def ^{:dynamic true
       :doc     "Size below which the initial plan will execute during planning,
above which, the same number of items will be sampled instead"}
  init-exec-size-threshold 1024)

(def ^{:dynamic true
       :doc     "The number of plans considered for initial full planning, above which greedy search is performed"}
  plan-space-reduction-threshold 100000)

;; search engine

(def ^{:dynamic true
       :doc     "The set of English stop words, a Java HashSet, contains
the same words as that of Lucene. Used in English analyzer."}
  *en-stop-words-set*
  (let [s (HashSet.)]
    (doseq [w ["a",    "an",   "and",   "are",  "as",    "at",   "be",
               "but",  "by",   "for",   "if",   "in",    "into", "is",
               "it",   "no",   "not",   "of",   "on",    "or",   "such",
               "that", "the",  "their", "then", "there", "these",
               "they", "this", "to",    "was",  "will",  "with"]]
      (.add s w))
    s))

(def ^{:dynamic true
       :doc     "The set of English punctuation characters, a Java HashSet.
Used in English analyzer."}
  *en-punctuations-set*
  (let [s (HashSet.)]
    (doseq [c [\: \/ \. \; \, \! \= \? \" \' \( \) \[ \] \{ \}
               \| \< \> \& \@ \# \^ \* \\ \~ \`]]
      (.add s c))
    s))

(def ^{:dynamic true
       :doc     "batch size when using search index writer and `:index-position?` is `false`"}
  *index-writer-batch-size* 500)

(def ^{:dynamic true
       :doc     "batch size  when using search index writer and `:index-position?` is `true`"}
  *index-writer-batch-size-pos* 200000)
