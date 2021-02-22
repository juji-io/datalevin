(ns datalevin.lmdb "API for Key Value Store"
    (:require [datalevin.util :as u]))

(defprotocol ^:no-doc IBuffer
  (put-key [this data k-type] "put data in key buffer")
  (put-val [this data v-type] "put data in val buffer"))

(defprotocol ^:no-doc IRange
  (range-info [this range-type k1 k2]
    "return necessary range information for iterators")
  (put-start-key [this data k-type] "put data in start-key buffer.")
  (put-stop-key [this data k-type] "put data in stop-key buffer."))

(defprotocol ^:no-doc IRtx
  (close-rtx [this] "close the read-only transaction")
  (reset [this] "reset transaction so it can be reused upon renew")
  (renew [this] "renew and return previously reset transaction for reuse"))

(defprotocol ^:no-doc IRtxPool
  (close-pool [this] "Close all read-only transactions in the pool")
  (new-rtx [this] "Create a new read-only transaction")
  (get-rtx [this] "Obtain a ready-to-use read-only transaction"))

(defprotocol ^:no-doc IDB
  (dbi-name [this] "Return string name of the dbi")
  (put [this txn] [this txn append?]
    "Put kv pair given in `put-key` and `put-val` of dbi")
  (del [this txn] "Delete the key given in `put-key` of dbi")
  (get-kv [this rtx] "Get value of the key given in `put-key` of rtx")
  (iterate-kv [this rtx range-info] "Return an Iterable given the range"))

(defprotocol ^:no-doc IKV
  (k [this] "key of a key value pair")
  (v [this] "value of a key value pair"))

(defprotocol ILMDB
  (close-kv [db] "Close this LMDB env")
  (closed? [db] "Return true if this LMDB env is closed")
  (dir [db] "Return the directory path of LMDB env")
  (open-dbi
    [db dbi-name]
    [db dbi-name key-size]
    [db dbi-name key-size val-size]
    [db dbi-name key-size val-size flags]
    "Open a named DBI (i.e. sub-db) in the LMDB env")
  (clear-dbi [db dbi-name]
    "Clear data in the DBI (i.e sub-db), but leave it open")
  (drop-dbi [db dbi-name]
    "Clear data in the DBI (i.e. sub-db), then delete it")
  (get-dbi [db dbi-name]
    "Lookup open DBI (i.e. sub-db) by name, throw if it's not open")
  (stat
    [db]
    [db dbi-name]
    "Return the statitics of the unnamed top level database or a named DBI
     (i.e. sub-database) as a map:
     * `:psize` is the size of database page
     * `:depth` is the depth of the B-tree
     * `:branch-pages` is the number of internal pages
     * `:leaf-pages` is the number of leaf pages
     * `:overflow-pages` is the number of overflow-pages
     * `:entries` is the number of data entries")
  (entries [db dbi-name]
    "Get the number of data entries in a DBI (i.e. sub-db)")
  (get-txn [db])
  (transact-kv [db txs]
    "Update DB, insert or delete key value pairs.

     `txs` is a seq of `[op dbi-name k v k-type v-type append?]`
     when `op` is `:put`, for insertion of a key value pair `k` and `v`;
     or `[op dbi-name k k-type]` when `op` is `:del`, for deletion of key `k`;

     `dbi-name` is the name of the DBI (i.e sub-db) to be transacted, a string.

     `k-type`, `v-type` and `append?` are optional.

    `k-type` indicates the data type of `k`, and `v-type` indicates the data type
    of `v`. The allowed data types are described in [[datalevin.bits/put-buffer]]

    Set `append?` to true when the data is sorted to gain better write performance.

    Example:

            (transact-kv
                      lmdb
                      [ [:put \"a\" 1 2]
                        [:put \"a\" 'a 1]
                        [:put \"a\" 5 {}]
                        [:put \"a\" :annunaki/enki true :attr :data]
                        [:put \"a\" :datalevin [\"hello\" \"world\"]]
                        [:put \"a\" 42 (d/datom 1 :a/b {:id 4}) :long :datom]
                        [:put \"a\" (byte 0x01) #{1 2} :byte :data]
                        [:put \"a\" (byte-array [0x41 0x42]) :bk :bytes :data]
                        [:put \"a\" [-1 -235254457N] 5]
                        [:put \"a\" :a 4]
                        [:put \"a\" :bv (byte-array [0x41 0x42 0x43]) :data :bytes]
                        [:put \"a\" :long 1 :data :long]
                        [:put \"a\" 2 3 :long :long]
                        [:del \"a\" 1]
                        [:del \"a\" :non-exist] ])")
  (get-value
    [db dbi-name k]
    [db dbi-name k k-type]
    [db dbi-name k k-type v-type]
    [db dbi-name k k-type v-type ignore-key?]
    "Get kv pair of the specified key `k`.

    `k-type` and `v-type` are data types of `k` and `v`, respectively.
     The allowed data types are described in [[datalevin.bits/read-buffer]].

     If `ignore-key?` is true (default `true`), only return the value,
     otherwise return `[k v]`, where `v` is the value

     Examples:

              (get-value lmdb \"a\" 1)
              ;;==> 2

              ;; specify data types
              (get-value lmdb \"a\" :annunaki/enki :attr :data)
              ;;==> true

              ;; return key value pair
              (get-value lmdb \"a\" 1 :data :data false)
              ;;==> [1 2]

              ;; key doesn't exist
              (get-value lmdb \"a\" 2)
              ;;==> nil ")
  (get-first
    [db dbi-name k-range]
    [db dbi-name k-range k-type]
    [db dbi-name k-range k-type v-type]
    [db dbi-name k-range k-type v-type ignore-key?]
    "Return the first kv pair in the specified key range;

     `k-range` is a vector `[range-type k1 k2]`, `range-type` can be one of
     `:all`, `:at-least`, `:at-most`, `:closed`, `:closed-open`, `:greater-than`,
     `:less-than`, `:open`, `:open-closed`, plus backward variants that put a
     `-back` suffix to each of the above, e.g. `:all-back`;

    `k-type` and `v-type` are data types of `k` and `v`, respectively.
     The allowed data types are described in [[datalevin.bits/read-buffer]].

     Only the value will be returned if `ignore-key?` is `true`;
     If value is to be ignored, put `:ignore` as `v-type`

     If both key and value are ignored, return true if found an entry, otherwise
     return nil.

     Examples:


              (get-first lmdb \"c\" [:all] :long :long)
              ;;==> [0 1]

              ;; ignore value
              (get-first lmdb \"c\" [:all-back] :long :ignore)
              ;;==> [999 nil]

              ;; ignore key
              (get-first lmdb \"a\" [:greater-than 9] :long :data true)
              ;;==> {:some :data}

              ;; ignore both, this is like testing if the range is empty
              (get-first lmdb \"a\" [:greater-than 5] :long :ignore true)
              ;;==> true")
  (get-range
    [db dbi-name k-range]
    [db dbi-name k-range k-type]
    [db dbi-name k-range k-type v-type]
    [db dbi-name k-range k-type v-type ignore-key?]
    "Return a seq of kv pairs in the specified key range;

     `k-range` is a vector `[range-type k1 k2]`, `range-type` can be one of
     `:all`, `:at-least`, `:at-most`, `:closed`, `:closed-open`, `:greater-than`,
     `:less-than`, `:open`, `:open-closed`, plus backward variants that put a
     `-back` suffix to each of the above, e.g. `:all-back`;

    `k-type` and `v-type` are data types of `k` and `v`, respectively.
     The allowed data types are described in [[datalevin.bits/read-buffer]].

     Only the value will be returned if `ignore-key?` is `true`;
     If value is to be ignored, put `:ignore` as `v-type`

     Examples:


              (get-range lmdb \"c\" [:at-least 9] :long :long)
              ;;==> [[10 11] [11 15] [13 14]]

              ;; ignore value
              (get-range lmdb \"c\" [:all-back] :long :ignore)
              ;;==> [[999 nil] [998 nil]]

              ;; ignore keys, only return values
              (get-range lmdb \"a\" [:closed 9 11] :long :long true)
              ;;==> [10 11 12]

              ;; out of range
              (get-range lmdb \"c\" [:greater-than 1500] :long :ignore)
              ;;==> [] ")
  (range-count
    [db dbi-name k-range]
    [db dbi-name k-range k-type]
    "Return the number of kv pairs in the specified key range, does not process
     the kv pairs.

     `k-range` is a vector `[range-type k1 k2]`, `range-type` can be one of
     `:all`, `:at-least`, `:at-most`, `:closed`, `:closed-open`, `:greater-than`,
     `:less-than`, `:open`, `:open-closed`, plus backward variants that put a
     `-back` suffix to each of the above, e.g. `:all-back`;

    `k-type` and `v-type` are data types of `k` and `v`, respectively.
     The allowed data types are described in [[datalevin.bits/read-buffer]].

     Examples:


              (range-count lmdb \"c\" [:at-least 9] :long)
              ;;==> 10 ")
  (get-some
    [db dbi-name pred k-range]
    [db dbi-name pred k-range k-type]
    [db dbi-name pred k-range k-type v-type]
    [db dbi-name pred k-range k-type v-type ignore-key?]
    "Return the first kv pair that has logical true value of `(pred x)`,
     where `pred` is a function, `x` is an `IKV` fetched from the store,
     with both key and value fields being a `ByteBuffer`.

     `pred` can use [[datalevin.bits/read-buffer]] to read the content.

     `k-range` is a vector `[range-type k1 k2]`, `range-type` can be one of
     `:all`, `:at-least`, `:at-most`, `:closed`, `:closed-open`, `:greater-than`,
     `:less-than`, `:open`, `:open-closed`, plus backward variants that put a
     `-back` suffix to each of the above, e.g. `:all-back`;

    `k-type` and `v-type` are data types of `k` and `v`, respectively.
     The allowed data types are described in [[datalevin.bits/read-buffer]].

     Only the value will be returned if `ignore-key?` is `true`;
     If value is to be ignored, put `:ignore` as `v-type`

     Examples:

              (require ' [datalevin.bits :as b])

              (def pred (fn [kv]
                         (let [^long k (b/read-buffer (key kv) :long)]
                          (> k 15)))

              (get-some lmdb \"c\" pred [:less-than 20] :long :long)
              ;;==> [16 2]

              ;; ignore key
              (get-some lmdb \"c\" pred [:greater-than 9] :long :data true)
              ;;==> 16 ")
  (range-filter
    [db dbi-name pred k-range]
    [db dbi-name pred k-range k-type]
    [db dbi-name pred k-range k-type v-type]
    [db dbi-name pred k-range k-type v-type ignore-key?]
    "Return a seq of kv pair in the specified key range, for only those
     return true value for `(pred x)`, where `pred` is a function, and `x`
     is an `IKV`, with both key and value fields being a `ByteBuffer`.

     `pred` can use [[datalevin.bits/read-buffer]] to read the buffer content.

     `k-range` is a vector `[range-type k1 k2]`, `range-type` can be one of
     `:all`, `:at-least`, `:at-most`, `:closed`, `:closed-open`, `:greater-than`,
     `:less-than`, `:open`, `:open-closed`, plus backward variants that put a
     `-back` suffix to each of the above, e.g. `:all-back`;

    `k-type` and `v-type` are data types of `k` and `v`, respectively.
     The allowed data types are described in [[datalevin.bits/read-buffer]].

     Only the value will be returned if `ignore-key?` is `true`;
     If value is to be ignored, put `:ignore` as `v-type`

     Examples:

              (require ' [datalevin.bits :as b])

              (def pred (fn [kv]
                         (let [^long k (b/read-buffer (key kv) :long)]
                          (> k 15)))

              (range-filter lmdb \"a\" pred [:less-than 20] :long :long)
              ;;==> [[16 2] [17 3]]

              ;; ignore key
              (range-filter lmdb \"a\" pred [:greater-than 9] :long :data true)
              ;;==> [16 17] ")
  (range-filter-count
    [db dbi-name pred k-range]
    [db dbi-name pred k-range k-type]
    "Return the number of kv pairs in the specified key range, for only those
     return true value for `(pred x)`, where `pred` is a function, and `x`
     is an `IMapEntry`, with both key and value fields being a `ByteBuffer`.
     Does not process the kv pairs.

     `pred` can use [[datalevin.bits/read-buffer]] to read the buffer content.

    `k-type` indicates data type of `k` and the allowed data types are described
    in [[datalevin.bits/read-buffer]].

     `k-range` is a vector `[range-type k1 k2]`, `range-type` can be one of
     `:all`, `:at-least`, `:at-most`, `:closed`, `:closed-open`, `:greater-than`,
     `:less-than`, `:open`, `:open-closed`, plus backward variants that put a
     `-back` suffix to each of the above, e.g. `:all-back`;

     Examples:


              (require ' [datalevin.bits :as b])

              (def pred (fn [kv]
                         (let [^long k (b/read-buffer (key kv) :long)]
                          (> k 15)))

              (range-filter-count lmdb \"a\" pred [:less-than 20] :long)
              ;;==> 3"))

(defmulti open-kv
  "Open an LMDB database, return the connection.

  `dir` is a string directory path in which the data are to be stored;

  Will detect the platform this code is running in, and dispatch accordingly.

  Please note:

  > LMDB uses POSIX locks on files, and these locks have issues if one process
  > opens a file multiple times. Because of this, do not mdb_env_open() a file
  > multiple times from a single process. Instead, share the LMDB environment
  > that has opened the file across all threads. Otherwise, if a single process
  > opens the same environment multiple times, closing it once will remove all
  > the locks held on it, and the other instances will be vulnerable to
  > corruption from other processes.'

  Therefore, a LMDB connection should be managed as a stateful resource.
  Multiple connections to the same DB in the same process are not recommended.
  The recommendation is to use a mutable state management library, for
  example, in Clojure, use [component](https://github.com/stuartsierra/component),
  [mount](https://github.com/tolitius/mount),
  [integrant](https://github.com/weavejester/integrant), or something similar
  to hold on to and manage the connection. "
  {:arglists '([dir])}
  (fn [_] (if (u/graal?) :graal :java)))
