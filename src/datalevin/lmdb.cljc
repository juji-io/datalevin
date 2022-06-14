(ns datalevin.lmdb
  "API for LMDB Key Value Store"
  (:require [datalevin.util :as u]))

(defprotocol IBuffer
  (put-key [this data k-type] "put data in key buffer")
  (put-val [this data v-type] "put data in val buffer"))

(defprotocol IRange
  (range-info [this range-type k1 k2]
    "return necessary range information for iterators")
  (put-start-key [this data k-type] "put data in start-key buffer.")
  (put-stop-key [this data k-type] "put data in stop-key buffer."))

(defprotocol IRtx
  (read-only? [this])
  (get-txn [this] "access the transaction")
  (close-rtx [this] "close the read-only transaction")
  (reset [this] "reset transaction so it can be reused upon renew")
  (renew [this] "renew and return previously reset transaction for reuse"))

(defprotocol IDB
  (dbi [this] "Return the underlying dbi")
  (dbi-name [this] "Return string name of the dbi")
  (put [this txn] [this txn append?]
    "Put kv pair given in `put-key` and `put-val` of dbi")
  (del [this txn] [this txn all?]
    "Delete the key given in `put-key` of dbi")
  (get-kv [this rtx] "Get value of the key given in `put-key` of rtx")
  (iterate-kv [this rtx range-info] "Return an Iterable given the range")
  (get-cursor [this txn] "Get a reusable read-only cursor")
  (close-cursor [this cur] "Close cursor")
  (return-cursor [this cur] "Return a read-only cursor after use"))

(defprotocol IKV
  (k [this] "Key of a key value pair")
  (v [this] "Value of a key value pair"))

(defprotocol IList
  (put-list-items [db list-name k vs k-type v-type]
    "put an list by key")
  (del-list-items
    [db list-name k k-type]
    [db list-name k vs k-type v-type]
    "delete an list or its items by key")
  (get-list
    [db list-name k k-type v-type]
    [db list-name k k-type v-type writing?]
    "get a list by key")
  (visit-list
    [db list-name visitor k k-type]
    [db list-name visitor k k-type writing?]
    "visit a list, presumably for side effects")
  (list-count
    [db list-name k k-type]
    [db list-name k k-type writing?]
    "get the number of items in the list")
  (in-list?
    [db list-name k v k-type v-type]
    [db list-name k v k-type v-type writing?]
    "return true if an item is in the list")
  (list-range-filter
    [db list-name pred k k-type v-range v-type]
    [db list-name pred k k-type v-range v-type writing?]
    "Return a seq of values in the specified value range of the key")
  (list-range-filter-count
    [db list-name pred k k-type v-range v-type]
    [db list-name pred k k-type v-range v-type writing?]
    "Return the count of values in the specified value range of the key")
  (list-some
    [db list-name pred k k-type v-range v-type]
    [db list-name pred k k-type v-range v-type writing?]
    "Return the first value in the specified value range of the key"))

(defprotocol ILMDB
  (close-kv [db] "Close this LMDB env")
  (closed-kv? [db] "Return true if this LMDB env is closed")
  (dir [db] "Return the directory path of LMDB env")
  (open-dbi
    [db dbi-name]
    [db dbi-name opts]
    "Open a named DBI (i.e. sub-db) in the LMDB env")

  (open-list-dbi
    [db list-name]
    [db list-name opts]
    "Open a named list, a special dbi, that permits a list of
     values for the same key, with some corresponding special functions")

  (clear-dbi [db dbi-name]
    "Clear data in the DBI (i.e sub-db), but leave it open")
  (drop-dbi [db dbi-name]
    "Clear data in the DBI (i.e. sub-db), then delete it")
  (get-dbi [db dbi-name] [db dbi-name create?])
  (list-dbis [db] "List the names of the sub-databases")
  (copy
    [db dest]
    [db dest compact?]
    "Copy the database to a destination directory path, optionally compact
     while copying, default not compact. ")
  (stat
    [db]
    [db dbi-name]
    "Return the statitics of the unnamed top level database or a named DBI
     (i.e. sub-database) as a map")
  (entries [db dbi-name]
    "Get the number of data entries in a DBI (i.e. sub-db)")
  (get-rtx [db] "get a read-only rtx")
  (return-rtx [db rtx] "return the read-only rtx back to pool")
  (open-transact-kv [db]
    "open a read/write rtx, set write-txn")
  (close-transact-kv [db] "close and commit the write-txn")
  (write-txn [db] "return the write-txn")
  (transact-kv [db txs]
    "Update DB, insert or delete key value pairs.")
  (get-value
    [db dbi-name k]
    [db dbi-name k k-type]
    [db dbi-name k k-type v-type]
    [db dbi-name k k-type v-type ignore-key?]
    [db dbi-name k k-type v-type ignore-key? writing?]
    "Get kv pair of the specified key `k`. ")
  (get-first
    [db dbi-name k-range]
    [db dbi-name k-range k-type]
    [db dbi-name k-range k-type v-type]
    [db dbi-name k-range k-type v-type ignore-key?]
    [db dbi-name k-range k-type v-type ignore-key? writing?]
    "Return the first kv pair in the specified key range;")
  (get-range
    [db dbi-name k-range]
    [db dbi-name k-range k-type]
    [db dbi-name k-range k-type v-type]
    [db dbi-name k-range k-type v-type ignore-key?]
    [db dbi-name k-range k-type v-type ignore-key? writing?]
    "Return a seq of kv pairs in the specified key range;")
  (range-count
    [db dbi-name k-range]
    [db dbi-name k-range k-type]
    [db dbi-name k-range k-type writing?]
    "Return the number of kv pairs in the specified key range, does not process
     the kv pairs.")
  (get-some
    [db dbi-name pred k-range]
    [db dbi-name pred k-range k-type]
    [db dbi-name pred k-range k-type v-type]
    [db dbi-name pred k-range k-type v-type ignore-key?]
    [db dbi-name pred k-range k-type v-type ignore-key? writing?]
    "Return the first kv pair that has logical true value of `(pred kv)`")
  (range-filter
    [db dbi-name pred k-range]
    [db dbi-name pred k-range k-type]
    [db dbi-name pred k-range k-type v-type]
    [db dbi-name pred k-range k-type v-type ignore-key?]
    [db dbi-name pred k-range k-type v-type ignore-key? writing?]
    "Return a seq of kv pair in the specified key range, for only those
     return true value for `(pred kv)`.")
  (range-filter-count
    [db dbi-name pred k-range]
    [db dbi-name pred k-range k-type]
    [db dbi-name pred k-range k-type writing?]
    "Return the number of kv pairs in the specified key range, for only those
     return true value for `(pred kv)`")
  (visit
    [db dbi-name visitor k-range]
    [db dbi-name visitor k-range k-type]
    [db dbi-name visitor k-range k-type writing?]
    "Call `visitor` function on each kv pairs in the specified key range, presumably
     for side effects. Return nil."))

(deftype Range [f?  ; forward?
                s?  ; has start?
                is? ; inclusive at start?
                e?  ; has end?
                ie? ; inclusive at end?
                s   ; start value
                e   ; end value
                ])

(defn- pick-binding [] (if (u/graal?) :graal :java))

(defmulti open-kv
  (constantly (pick-binding)))
