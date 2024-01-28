(ns ^:no-doc datalevin.lmdb
  "API for LMDB Key Value Store"
  (:refer-clojure :exclude [load])
  (:require
   [clojure.edn :as edn]
   [clojure.pprint :as p]
   [taoensso.nippy :as nippy]
   [datalevin.bits :as b]
   [datalevin.util :as u]
   [datalevin.compress :as cp]
   [datalevin.buffer :as bf]
   [datalevin.constants :as c])
  (:import
   [datalevin.utl BitOps]
   [java.nio ByteBuffer]
   [java.io PushbackReader FileOutputStream FileInputStream DataOutputStream
    DataInputStream IOException]
   [java.lang RuntimeException]))

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
    "Return an Iterable based on key range only")
  (iterate-list [this rtx cur k-range k-type v-range v-type]
    "Return an Iterable of key-values given key range and value range,
     applicable only to list dbi")
  (iterate-list-val [this rtx cur v-range v-type]
    "Return a IListRandKeyValIterable given the value range,
     which allows randomly seek key and iterate its values forwardly,
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
  (visit-list
    [db list-name visitor k k-type]
    [db list-name visitor k k-type v-type]
    [db list-name visitor k k-type v-type raw-pred?]
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
    [db list-name pred k-range k-type v-range v-type raw-pred?]
    "Return a seq of key-values in the specified value range of the
     specified key range, filtered by pred call")
  (list-range-keep
    [db list-name pred k-range k-type v-range v-type]
    [db list-name pred k-range k-type v-range v-type raw-pred?]
    "Return the non-nil results of pred calls in
     the specified value range of the specified key range")
  (list-range-some
    [db list-name pred k-range k-type v-range v-type]
    [db list-name pred k-range k-type v-range v-type raw-pred?]
    "Return the first logical true result of pred calls in
     the specified value range of the specified key range")
  (list-range-filter-count
    [db list-name pred k-range k-type v-range v-type]
    [db list-name pred k-range k-type v-range v-type raw-pred?]
    "Return the count of key-values in the specified value range of the
     specified key range for those pred call is true")
  (visit-list-range
    [db list-name visitor k-range k-type v-range v-type]
    [db list-name visitor k-range k-type v-range v-type raw-pred?]
    "visit a list range, presumably for side effects of vistor call")
  (operate-list-val-range
    [db list-name operator v-range v-type]
    "Take an operator function that operates a ListRandKeyValIterable"))

(defprotocol IListRandKeyValIterable
  (val-iterator [this]
    "Return an IListRandKeyValIterator that can seek random key and iterate
     its values forwardly"))

(defprotocol IListRandKeyValIterator
  (seek-key [this k-value k-type])
  (has-next-val [this])
  (next-val [this]))

(defprotocol ILMDB
  (check-ready [db] "check if db is ready to be operated on")
  (close-kv [db] "Close this LMDB env")
  (closed-kv? [db] "Return true if this LMDB env is closed")
  (dir [db] "Return the directory path of LMDB env")
  (opts [db] "Return the option map of LMDB env")
  (dbi-opts [db dbi-name] "Return option map of a given DBI")
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
    [db dbi-name pred k-range k-type v-type ignore-key? raw-pred?]
    "Return the first kv pair that has logical true value of pred call")
  (range-filter
    [db dbi-name pred k-range]
    [db dbi-name pred k-range k-type]
    [db dbi-name pred k-range k-type v-type]
    [db dbi-name pred k-range k-type v-type ignore-key?]
    [db dbi-name pred k-range k-type v-type ignore-key? raw-pred?]
    "Return a seq of kv pair in the specified key range, for only those
     return true value for pred call.")
  (range-keep
    [db dbi-name pred k-range]
    [db dbi-name pred k-range k-type]
    [db dbi-name pred k-range k-type v-type]
    [db dbi-name pred k-range k-type v-type raw-pred?]
    "Return a seq of non-nil results of pred calls in the specified key
     range.")
  (range-some
    [db dbi-name pred k-range]
    [db dbi-name pred k-range k-type]
    [db dbi-name pred k-range k-type v-type]
    [db dbi-name pred k-range k-type v-type raw-pred?]
    "Return the first logical true result of pred calls in the specified key
     range.")
  (range-filter-count
    [db dbi-name pred k-range]
    [db dbi-name pred k-range k-type]
    [db dbi-name pred k-range k-type v-type]
    [db dbi-name pred k-range k-type v-type raw-pred?]
    "Return the number of kv pairs in the specified key range, for only those
     return true value for pred call")
  (visit
    [db dbi-name visitor k-range]
    [db dbi-name visitor k-range k-type]
    [db dbi-name visitor k-range k-type v-type]
    [db dbi-name visitor k-range k-type v-type raw-pred?]
    "Call `visitor` function on each k, v pairs in the specified key range,
     presumably for side effects of visitor call. Return nil."))

(defprotocol IWriting
  "Used to mark the db so that it should use the write-txn"
  (writing? [db] "return true if this db should use write-txn")
  (write-txn [db]
    "return deref'able object that is the write-txn or a mutex for locking")
  (mark-write [db] "return a new db what uses write-txn"))

(defn- pick-binding [] (if (u/graal?) :graal :java))

(defmulti open-kv (constantly (pick-binding)))

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

(defn dump-dbis-list
  ([lmdb]
   (p/pprint (set (list-dbis lmdb))))
  ([lmdb data-output]
   (if data-output
     (nippy/freeze-to-out!
       data-output
       (set (list-dbis lmdb)))
     (dump-dbis-list lmdb))))

(defn list-dbi? [db dbi-name] (:dupsort? (dbi-opts db dbi-name)))

(defn sample-key-freqs
  "return a long array of frequencies of 2 bytes symbols if there are enough
  keys, otherwise return nil"
  (^longs [db dbi-name]
   (sample-key-freqs db dbi-name c/*compress-sample-size*))
  (^longs [db dbi-name size]
   (sample-key-freqs db dbi-name size nil))
  (^longs [db dbi-name ^long size compressor]
   (let [list?   (list-dbi? db dbi-name)
         ^long n (if list?
                   (list-range-count db dbi-name [:all] :raw [:all] :raw)
                   (range-count db dbi-name [:all]))]
     (when-let [ia (u/reservoir-sampling n size)]
       (let [b (when compressor (bf/get-direct-buffer c/+max-key-size+))
             f (cp/init-key-freqs)
             i (volatile! 0)
             j (volatile! 0)
             v (fn [kv]
                 (if (= @j size)
                   :datalevin/terminate-visit
                   (do
                     (when (= @i (aget ^longs ia @j))
                       (vswap! j u/long-inc)
                       (let [bf             (if list? (v kv) (k kv))
                             ^ByteBuffer bf (if b
                                              (do
                                                (.clear ^ByteBuffer b)
                                                (cp/bf-uncompress
                                                  compressor bf b)
                                                (.flip b)
                                                b)
                                              bf)
                             total          (.remaining bf)
                             t-1            (dec total)]
                         (loop [i 0]
                           (when (<= i total)
                             (let [idx (if (= i t-1)
                                         (-> (.get bf)
                                             (BitOps/intAnd 0x000000FF)
                                             (bit-shift-left 8))
                                         (BitOps/intAnd (.getShort bf)
                                                        0x0000FFFF))
                                   cur (aget f idx)]
                               (aset f idx (inc cur))
                               (recur (+ i 2)))))))
                     (vswap! i u/long-inc))))]
         (if list?
           (visit-list-range db dbi-name v [:all] :raw [:all] :raw)
           (visit db dbi-name v [:all]))
         f)))))

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
   (dump-dbi lmdb c/kv-info)
   (doseq [dbi (set (list-dbis lmdb))] (dump-dbi lmdb dbi)))
  ([lmdb data-output]
   (if data-output
     (nippy/freeze-to-out!
       data-output
       (conj (for [dbi (set (list-dbis lmdb))] (nippy-dbi lmdb dbi))
             (nippy-dbi lmdb c/kv-info)))
     (dump-all lmdb))))

(defn- load-kv [dbi [k v]]
  [:put dbi (b/decode-base64 k) (b/decode-base64 v) :raw :raw])

(defn load-dbi
  ([lmdb dbi in nippy?]
   (if nippy?
     (let [[_ kvs] (nippy/thaw-from-in! in)]
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

#_(defmacro with-read-transaction-kv
    "Evaluate body within the context of a single read-only transaction,
  ensuring consistent view of key-value store.

  `db` is a new identifier of the kv database with a single read-only
  transaction attached, and `orig-db` is the original kv database.

  `body` should refer to `db`. "
    [[db orig-db] & body]
    `(let []))
