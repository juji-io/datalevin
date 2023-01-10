(ns ^:no-doc datalevin.lmdb
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
  (read-only? [this] "is this a read only transaction")
  (get-txn [this] "access the transaction object")
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
  (return-cursor [this cur] "Return a read-only cursor after use"))

(defprotocol IKV
  (k [this] "Key of a key value pair")
  (v [this] "Value of a key value pair"))

(defprotocol IInvertedList
  (put-list-items [db list-name k vs k-type v-type]
    "put an inverted list by key")
  (del-list-items
    [db list-name k k-type]
    [db list-name k vs k-type v-type]
    "delete an inverted list by key")
  (get-list [db list-name k k-type v-type] "get a list by key")
  (visit-list [db list-name visitor k k-type]
    "visit a list, presumably for side effects")
  (list-count [db list-name k k-type]
    "get the number of items in the inverted list")
  (filter-list [db list-name k pred k-type v-type] "predicate filtered items of a list")
  (filter-list-count [db list-name k pred k-type]
    "get the count of predicate filtered items of a list")
  (in-list? [db list-name k v k-type v-type]
    "return true if an item is in an inverted list"))

(defprotocol ILMDB
  (close-kv [db] "Close this LMDB env")
  (closed-kv? [db] "Return true if this LMDB env is closed")
  (dir [db] "Return the directory path of LMDB env")
  (opts [db] "Rturn the option map")
  (open-dbi
    [db dbi-name]
    [db dbi-name opts]
    "Open a named DBI (i.e. sub-db) in the LMDB env")

  (open-inverted-list
    [db list-name]
    [db list-name opts]
    "Open a named inverted list, a special dbi, that permits a list of
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
  (get-rtx [db])
  (return-rtx [db rtx])
  (open-transact-kv [db] "open an explicit read/write rtx, return writing db")
  (close-transact-kv [db] "close and commit the read/write rtx")
  (abort-transact-kv [db] "abort the explicit read/write rtx")
  (transact-kv [db txs]
    "Update DB, insert or delete key value pairs.")
  (get-value
    [db dbi-name k]
    [db dbi-name k k-type]
    [db dbi-name k k-type v-type]
    [db dbi-name k k-type v-type ignore-key?]
    "Get kv pair of the specified key `k`. ")
  (get-first
    [db dbi-name k-range]
    [db dbi-name k-range k-type]
    [db dbi-name k-range k-type v-type]
    [db dbi-name k-range k-type v-type ignore-key?]
    "Return the first kv pair in the specified key range;")
  (range-seq
    [db dbi-name k-range]
    [db dbi-name k-range k-type]
    [db dbi-name k-range k-type v-type]
    [db dbi-name k-range k-type v-type ignore-key?]
    [db dbi-name k-range k-type v-type ignore-key? opts]
    "Return a lazy seq of kv pairs in the specified key range;")
  (get-range
    [db dbi-name k-range]
    [db dbi-name k-range k-type]
    [db dbi-name k-range k-type v-type]
    [db dbi-name k-range k-type v-type ignore-key?]
    "Return a eager seq of kv pairs in the specified key range;")
  (range-count
    [db dbi-name k-range]
    [db dbi-name k-range k-type]
    "Return the number of kv pairs in the specified key range, does not process
     the kv pairs.")
  (get-some
    [db dbi-name pred k-range]
    [db dbi-name pred k-range k-type]
    [db dbi-name pred k-range k-type v-type]
    [db dbi-name pred k-range k-type v-type ignore-key?]
    "Return the first kv pair that has logical true value of `(pred kv)`")
  (range-filter
    [db dbi-name pred k-range]
    [db dbi-name pred k-range k-type]
    [db dbi-name pred k-range k-type v-type]
    [db dbi-name pred k-range k-type v-type ignore-key?]
    "Return a seq of kv pair in the specified key range, for only those
     return true value for `(pred kv)`.")
  (range-filter-count
    [db dbi-name pred k-range]
    [db dbi-name pred k-range k-type]
    "Return the number of kv pairs in the specified key range, for only those
     return true value for `(pred kv)`")
  (visit
    [db dbi-name visitor k-range]
    [db dbi-name visitor k-range k-type]
    "Call `visitor` function on each kv pairs in the specified key range, presumably
     for side effects. Return nil."))

(defprotocol IWriting
  "Used to mark the db so that it should use the write-txn"
  (writing? [db] "return true if this db should use write-txn")
  (write-txn [db]
    "return deref'able object that is the write-txn or a mutex for locking")
  (mark-write [db] "return a new db what uses write-txn"))

(defn- pick-binding [] (if (u/graal?) :graal :java))

(defmulti open-kv
  (constantly (pick-binding)))
