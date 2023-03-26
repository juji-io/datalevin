(ns ^:no-doc datalevin.lmdb
  "API for LMDB Key Value Store"
  (:require [datalevin.util :as u]))

(defprotocol IBuffer
  (put-key [this data k-type] "put data in key buffer")
  (put-val [this data v-type] "put data in val buffer"))

(defprotocol IRange
  (range-info [this range-type k1 k2 kt]
    "return key range information for kv iterators")
  (list-range-info [this k-range-type k1 k2 kt v-range-type b1 b2 vt]
    "return key value range information for list iterators"))

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
  (get-kv [this rtx]
    "Get value of the key given in `put-key` of rtx, return a byte buffer")
  (iterate-kv [this rtx cur k-range k-type v-type]
    "Return an Iterable of key-values, given the key range")
  (iterate-key [this rtx cur k-range k-type]
    "Return an Iterable of keys only, given the key range")
  (iterate-list [this rtx cur k-range k-type v-range v-type]
    "Return an Iterable of key-values given key range and value range,
     applicable only to list dbi")
  (get-cursor [this rtx] "Get a reusable read-only cursor")
  (close-cursor [this cur] "Close cursor")
  (return-cursor [this cur] "Return a read-only cursor after use"))

(defprotocol IKV
  (k [this] "Key of a key value pair")
  (v [this] "Value of a key value pair"))

(defprotocol IList
  (put-list-items [db list-name k vs k-type v-type]
    "put some list items by a key")
  (del-list-items
    [db list-name k k-type]
    [db list-name k vs k-type v-type]
    "delete a list or some items of a list by the key")
  (get-list [db list-name k k-type v-type] "get a list by key")
  (visit-list [db list-name visitor k k-type]
    "visit a list, presumably for side effects")
  (list-count [db list-name k k-type]
    "return the number of items in the list of a key")
  (in-list? [db list-name k v k-type v-type]
    "return true if an item is in the value list of the key")
  (list-range
    [db list-name k-range k-type v-range v-type]
    "Return a seq of key-values in the specified value range of the
     specified key range")
  (list-range-first
    [db list-name k-range k-type v-range v-type]
    "Return the first key-value pair in the specified value range of the
     specified key range")
  (list-range-count
    [db list-name k-range k-type v-range v-type]
    "Return the number of key-values in the specified value range of the
     specified key range")
  (list-range-filter
    [db list-name pred k-range k-type v-range v-type]
    "Return a seq of key-values in the specified value range of the
     specified key range, filtered by pred")
  (list-range-some
    [db list-name pred k-range k-type v-range v-type]
    "Return the first kv pair that has logical true value of `(pred kv)`in
     the specified value range of the specified key range")
  (list-range-filter-count
    [db list-name pred k-range k-type v-range v-type]
    "Return the count of key-values in the specified value range of the
     specified key range")
  (visit-list-range
    [db list-name visitor k-range k-type v-range v-type]
    "visit a list range, presumably for side effects"))

(defprotocol ILMDB
  (close-kv [db] "Close this LMDB env")
  (closed-kv? [db] "Return true if this LMDB env is closed")
  (dir [db] "Return the directory path of LMDB env")
  (opts [db] "Rturn the option map")
  (open-dbi
    [db dbi-name]
    [db dbi-name opts]
    "Open a named DBI (i.e. sub-db) in the LMDB env")

  (open-list-dbi
    [db list-name]
    [db list-name opts]
    "Open a named inverted list, a special dbi, that permits a list of
     values for the same key, with some corresponding special functions")

  (clear-dbi [db dbi-name]
    "Clear data in the DBI (i.e sub-db), but leave it open")
  (drop-dbi [db dbi-name]
    "Clear data in the DBI (i.e. sub-db), then delete it")
  (get-dbi [db dbi-name])
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
  (transact-kv
    [db txs]
    [db dbi-name txs]
    [db dbi-name txs k-type]
    [db dbi-name txs k-type v-type]
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
    "Return an eager seq of kv pairs in the specified key range;")
  (key-range
    [db dbi-name k-range]
    [db dbi-name k-range k-type]
    "Return an eager seq of keys in the specified key range, does not read
values;")
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

(defmacro with-transaction-kv
  [[db orig-db] & body]
  `(locking (write-txn ~orig-db)
     (let [writing# (writing? ~orig-db)]
       (try
         (let [~db (if writing# ~orig-db (open-transact-kv ~orig-db))]
           (try
             ~@body
             (catch Exception ~'e
               (if (and (:resized (ex-data ~'e)) (not writing#))
                 (do ~@body)
                 (throw ~'e)))))
         (finally
           (when-not writing# (close-transact-kv ~orig-db)))))))

(defn range-table
  "Produce the following context values for iterator control logic:
    * forward?
    * include-start?
    * include-stop?
    * start-buffer
    * stop-buffer"
  [range-type k1 k2 b1 b2]
  (let [chk1 #(if k1
                %1
                (u/raise "Missing start/end key for range type " %2 {}))
        chk2 #(if (and k1 k2)
                %1
                (u/raise "Missing start/end key for range type " %2 {}))]
    (case range-type
      :all               [true false false nil nil]
      :all-back          [false false false nil nil]
      :at-least          (chk1 [true true false b1 nil] :at-least)
      :at-most-back      (chk1 [false true false b1 nil] :at-most-back)
      :at-most           (chk1 [true false true nil b1] :at-most)
      :at-least-back     (chk1 [false false true nil b1] :at-least-back)
      :closed            (chk2 [true true true b1 b2] :closed)
      :closed-back       (chk2 [false true true b1 b2] :closed-back)
      :closed-open       (chk2 [true true false b1 b2] :closed-open)
      :closed-open-back  (chk2 [false true false b1 b2] :closed-open-back)
      :greater-than      (chk1 [true false false b1 nil] :greater-than)
      :less-than-back    (chk1 [false false false b1 nil] :less-than-back)
      :less-than         (chk1 [true false false nil b1] :less-than)
      :greater-than-back (chk1 [false false false nil b1] :greater-than-back)
      :open              (chk2 [true false false b1 b2] :open)
      :open-back         (chk2 [false false false b1 b2] :open-back)
      :open-closed       (chk2 [true false true b1 b2] :open-closed)
      :open-closed-back  (chk2 [false false true b1 b2] :open-closed-back)
      (u/raise "Unknown range type" range-type {}))))
