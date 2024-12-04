(ns ^:no-doc datalevin.lmdb
  "API for LMDB Key Value Store"
  (:refer-clojure :exclude [load sync])
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
   [clojure.lang IPersistentVector]
   [datalevin.utl BitOps]
   [java.nio ByteBuffer]
   [java.io Writer PushbackReader FileOutputStream FileInputStream DataOutputStream
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
  (iterate-list-val-full [this rtx cur]
    "Return a IListRandKeyValIterable,
     which allows randomly seek key and iterate all its values forwardly,
     applicable only to list dbi")
  (get-cursor [this rtx] "Get a reusable read-only cursor")
  (cursor-count [this cur] "get number of list items under the cursor")
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
  (list-range-first-n
    [db list-name n k-range k-type v-range v-type]
    "Return the first n key-value pairs in the specified value range of the
     specified key range")
  (list-range-count
    [db list-name k-range k-type v-range v-type]
    [db list-name k-range k-type v-range v-type cap]
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
    [db list-name pred k-range k-type v-range v-type raw-pred? cap]
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
     its values forwardly. Use with `with-open`."))

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
    "Update DB, insert or delete key value pairs. 2-arity variation's txs can be a seq of KVTxData")
  (sync [db] "force synchronous flush to disk")
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
  (get-first-n
    [db dbi-name n k-range]
    [db dbi-name n k-range k-type]
    [db dbi-name n k-range k-type v-type]
    [db dbi-name n k-range k-type v-type ignore-key?]
    "Return the first n kv pairs in the specified key range;")
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
  (visit-key-range
    [db dbi-name visitor k-range]
    [db dbi-name visitor k-range k-type]
    [db dbi-name visitor k-range k-type raw-pred?]
    "Visit keys in the specified key range for side effects. Return nil.")
  (key-range-count
    [db dbi-name k-range]
    [db dbi-name k-range k-type]
    [db dbi-name k-range k-type cap]
    "Return the number of keys in the specified key range, does not read
values;")
  (key-range-list-count
    [db dbi-name k-range k-type]
    [db dbi-name k-range k-type cap]
    "Return the total number of list items in the specified key range, does not read values;")
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

(defn- pick-binding [] :cpp
  #_(if (u/graal?) :graal :java))

(defmulti open-kv (constantly (pick-binding)))

(deftype RangeContext [^boolean forward?
                       ^boolean include-start?
                       ^boolean include-stop?
                       start-bf
                       stop-bf])

(defn range-table
  "Provide context for range iterators"
  [range-type b1 b2]
  (case range-type
    :all               (RangeContext. true false false nil nil)
    :all-back          (RangeContext. false false false nil nil)
    :at-least          (RangeContext. true true false b1 nil)
    :at-most-back      (RangeContext. false true false b1 nil)
    :at-most           (RangeContext. true false true nil b1)
    :at-least-back     (RangeContext. false false true nil b1)
    :closed            (RangeContext. true true true b1 b2)
    :closed-back       (RangeContext. false true true b1 b2)
    :closed-open       (RangeContext. true true false b1 b2)
    :closed-open-back  (RangeContext. false true false b1 b2)
    :greater-than      (RangeContext. true false false b1 nil)
    :less-than-back    (RangeContext. false false false b1 nil)
    :less-than         (RangeContext. true false false nil b1)
    :greater-than-back (RangeContext. false false false nil b1)
    :open              (RangeContext. true false false b1 b2)
    :open-back         (RangeContext. false false false b1 b2)
    :open-closed       (RangeContext. true false true b1 b2)
    :open-closed-back  (RangeContext. false false true b1 b2)
    (u/raise "Unknown range type" range-type {})))

(defprotocol IKVTxable (kv-txable? [_]))

(extend-type Object IKVTxable (kv-txable? [_] false))
(extend-type nil IKVTxable (kv-txable? [_] false))

(deftype KVTxData [op
                   ^String dbi-name
                   k
                   v
                   kt
                   vt
                   flags]
  IKVTxable
  (kv-txable? [_] true))

(defmethod print-method KVTxData
  [^KVTxData d, ^Writer w]
  (.write w (pr-str [(.-op d) (.-dbi-name d) (.-k d) (.-v d) (.-kt d)
                     (.-vt d)])))

(defn kv-tx
  ([op dbi k]
   (KVTxData. op dbi k nil nil nil nil))
  ([op dbi k v]
   (if (= op :del)
     (KVTxData. op dbi k nil v nil nil)
     (KVTxData. op dbi k v nil nil nil)))
  ([op dbi k v kt]
   (KVTxData. op dbi k v kt nil nil))
  ([op dbi k v kt vt]
   (KVTxData. op dbi k v kt vt nil))
  ([op dbi k v kt vt f]
   (KVTxData. op dbi k v kt vt f)))

(defn ->kv-tx-data
  ([x]
   (if (kv-txable? x)
     x
     (if (vector? x)
       (let [tx  ^IPersistentVector x
             cnt (.length tx)
             op  (.nth tx 0)]
         (KVTxData. op
                    (.nth tx 1)
                    (.nth tx 2)
                    (when-not (= :del op) (.nth tx 3))
                    (if (= :del op)
                      (when (< 3 cnt) (.nth tx 3))
                      (when (< 4 cnt) (.nth tx 4)))
                    (when (< 5 cnt) (.nth tx 5))
                    (when (< 6 cnt) (.nth tx 6))))
       (u/raise "Invalid KV transaction data " x {}))))
  ([x kt vt]
   (if (vector? x)
     (let [tx  ^IPersistentVector x
           cnt (.length tx)]
       (KVTxData. (.nth tx 0)
                  nil
                  (.nth tx 1)
                  (when (< 2 cnt) (.nth tx 2))
                  kt
                  vt
                  (when (< 3 cnt) (.nth tx 3))))
     (u/raise "Invalid KV transaction data " x {}))))

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
  (kv-tx :put dbi (b/decode-base64 k) (b/decode-base64 v) :raw :raw))

(defn load-dbi
  ([lmdb dbi in nippy?]
   (if nippy?
     (let [[_ kvs] (nippy/thaw-from-in! in)]
       (open-dbi lmdb dbi)
       (transact-kv lmdb (map #(load-kv dbi %) kvs)))
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
                                (map #(load-kv dbi %))))))
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
       (transact-kv lmdb (map #(load-kv dbi %) kvs)))
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
                                (map #(load-kv dbi %)))))]
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
      (let [db (open-kv d (update opts :flags conj :nosync))]
        (load db dumpfile)
        (close-kv db))
      (open-kv d opts))
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
