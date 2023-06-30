(ns ^:no-doc datalevin.lmdb
  "API for LMDB Key Value Store"
  (:require
   [clojure.edn :as edn]
   [clojure.pprint :as p]
   [clojure.java.io :as io]
   [taoensso.nippy :as nippy]
   [datalevin.bits :as b]
   [datalevin.util :as u]
   [datalevin.constants :as c])
  (:import
   [java.lang AutoCloseable]
   [java.lang RuntimeException]
   [java.io BufferedReader BufferedWriter PushbackReader FileOutputStream
    FileInputStream DataOutputStream DataInputStream OutputStreamWriter
    InputStreamReader IOException]
   [java.nio.channels FileChannel FileLock OverlappingFileLockException]
   [java.nio.file StandardOpenOption]))

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
  (close-cursor [this cur] "Close cursor")
  (return-cursor [this cur] "Return a read-only cursor after use"))

(defprotocol IKV
  (k [this] "Key of a key value pair")
  (v [this] "Value of a key value pair"))

(defprotocol IList
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
  (check-ready [db] "check if db is ready to be operated on, block if not")
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

(defmulti open-kv (constantly (pick-binding)))

(defn dump-dbis-list
  ([lmdb]
   (p/pprint (set (list-dbis lmdb))))
  ([lmdb data-output]
   (if data-output
     (nippy/freeze-to-out!
       data-output
       (set (list-dbis lmdb)))
     (dump-dbis-list lmdb))))

(defn- nippy-dbi [lmdb dbi]
  [{:dbi dbi :entries (entries lmdb dbi)}
   (for [[k v] (get-range lmdb dbi [:all] :raw :raw)]
     [(b/encode-base64 k) (b/encode-base64 v)])])

(defn dump-dbi
  ([lmdb dbi]
   (p/pprint {:dbi dbi :entries (entries lmdb dbi)})
   (doseq [[k v] (get-range lmdb dbi [:all] :raw :raw)]
     (p/pprint [(b/encode-base64 k) (b/encode-base64 v)])))
  ([lmdb dbi data-output]
   (if data-output
     (nippy/freeze-to-out! data-output (nippy-dbi lmdb dbi))
     (dump-dbi lmdb dbi))))

(defn dump-all
  ([lmdb]
   (doseq [dbi (set (list-dbis lmdb))] (dump-dbi lmdb dbi)))
  ([lmdb data-output]
   (if data-output
     (nippy/freeze-to-out!
       data-output
       (for [dbi (set (list-dbis lmdb))] (nippy-dbi lmdb dbi)))
     (dump-all lmdb))))

(defn- load-kv [dbi [k v]]
  [:put dbi (b/decode-base64 k) (b/decode-base64 v) :raw :raw])

(defn load-dbi
  ([lmdb dbi in nippy?]
   (if nippy?
     (let [[_ kvs] (first (nippy/thaw-from-in! in))]
       (open-dbi lmdb dbi)
       (transact-kv lmdb (map (partial load-kv dbi) kvs)))
     (load-dbi lmdb dbi in)))
  ([lmdb dbi in]
   (try
     (with-open [^PushbackReader r in]
       (let [read-form         #(edn/read {:eof ::EOF} r)
             {:keys [entries]} (read-form)]
         (open-dbi lmdb dbi)
         (transact-kv lmdb (->> (repeatedly read-form)
                                (take-while #(not= ::EOF %))
                                (take entries)
                                (map (partial load-kv dbi))))))
     (catch IOException e
       (u/raise "IO error while loading raw data: " (ex-message e) {}))
     (catch RuntimeException e
       (u/raise "Parse error while loading raw data: " (ex-message e) {}))
     (catch Exception e
       (u/raise "Error loading raw data: " (ex-message e) {})))))

(defn load-all
  ([lmdb in nippy?]
   (if nippy?
     (doseq [[{:keys [dbi]} kvs] (nippy/thaw-from-in! in)]
       (open-dbi lmdb dbi)
       (transact-kv lmdb (map (partial load-kv dbi) kvs)))
     (load-all lmdb in)))
  ([lmdb in]
   (try
     (with-open [^PushbackReader r in]
       (let [read-form #(edn/read {:eof ::EOF} r)
             load-dbi  (fn [[ms vs]]
                         (doseq [{:keys [dbi]} (butlast ms)]
                           (open-dbi lmdb dbi))
                         (let [{:keys [dbi entries]} (last ms)]
                           (open-dbi lmdb dbi)
                           (->> vs
                                (take entries)
                                (map (partial load-kv dbi)))))]
         (transact-kv lmdb (->> (repeatedly read-form)
                                (take-while #(not= ::EOF %))
                                (partition-by map?)
                                (partition 2 2 nil)
                                (mapcat load-dbi)))))
     (catch IOException e
       (u/raise "IO error while loading raw data: " (ex-message e) {}))
     (catch RuntimeException e
       (u/raise "Parse error while loading raw data: " (ex-message e) {}))
     (catch Exception e
       (u/raise "Error loading raw data: " (ex-message e) {})))))

(defn clear [lmdb]
  (doseq [dbi (set (list-dbis lmdb)) ] (clear-dbi lmdb dbi)))

(defn dump
  [db ^String dumpfile]
  (let [d (DataOutputStream. (FileOutputStream. dumpfile))]
    (dump-all db d)
    (.flush d)
    (.close d)))

(defn- load
  [db ^String dumpfile]
  (let [f  (FileInputStream. dumpfile)
        in (DataInputStream. f)]
    (load-all db in true)
    (.close f)))

(defprotocol IAdmin
  "Some administrative functions"
  (re-index [db opts] [db schema opts] "dump and reload the data"))

(defn re-index*
  [db opts]
  (try
    (let [d        (dir db)
          dumpfile (str d u/+separator+ "kv-dump")]
      (dump db dumpfile)
      (clear db)
      (close-kv db)
      (let [db (open-kv d opts)]
        (load db dumpfile)
        db))
    (catch Exception e
      (u/raise "Unable to re-index" e {:dir (dir db)}))))

(defn resized? [e] (:resized (ex-data e)))

(defmacro with-transaction-kv
  "Evaluate body within the context of a single new read/write transaction,
  ensuring atomicity of key-value operations.

  `db` is a new identifier of the kv database with a new read/write transaction attached, and `orig-db` is the original kv database.

  `body` should refer to `db`.

  Example:

          (with-transaction-kv [kv lmdb]
            (let [^long now (get-value kv \"a\" :counter)]
              (transact-kv kv [[:put \"a\" :counter (inc now)]])
              (get-value kv \"a\" :counter)))"
  [[db orig-db] & body]
  `(locking (write-txn ~orig-db)
     (let [writing#   (writing? ~orig-db)
           condition# (fn [~'e] (and (resized? ~'e) (not writing#)))]
       (u/repeat-try-catch
         ~c/+in-tx-overflow-times+
         condition#
         (try
           (let [~db (if writing# ~orig-db (open-transact-kv ~orig-db))]
             (u/repeat-try-catch
               ~c/+in-tx-overflow-times+
               condition#
               ~@body))
           (finally (when-not writing# (close-transact-kv ~orig-db))))))))
