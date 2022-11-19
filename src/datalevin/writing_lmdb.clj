(ns datalevin.writing-lmdb
  "Special LMDB instance that should be using the write-txn"
  (:require [datalevin.lmdb :as l
             :refer [ILMDB IWritingLMDB]]
            [datalevin.scan :as scan]))

(deftype WritingLMDB [lmdb]
  IWritingLMDB
  (writing? [_] true)

  ILMDB
  (close-kv [_] (l/close-kv lmdb))

  (closed-kv? [_] (l/closed-kv? lmdb))

  (dir [_] (l/dir lmdb))

  (open-dbi [db dbi-name]
    (.open-dbi db dbi-name nil))
  (open-dbi [_ dbi-name opts]
    (l/open-dbi lmdb dbi-name opts))

  (get-dbi [_ dbi-name] (l/get-dbi lmdb dbi-name))
  (get-dbi [_ dbi-name create?] (l/get-dbi lmdb dbi-name create?))

  (clear-dbi [_ dbi-name] (l/clear-dbi lmdb dbi-name))

  (drop-dbi [_ dbi-name] (l/drop-dbi lmdb dbi-name))

  (list-dbis [_] (l/list-dbis lmdb))

  (copy [db dest] (.copy db dest false))
  (copy [_ dest compact?] (l/copy lmdb dest compact?))

  (get-rtx [_] (l/get-rtx lmdb))

  (return-rtx [_ rtx] (l/return-rtx lmdb rtx))

  (stat [db] (.stat db nil))
  (stat [_ dbi-name] (l/stat lmdb dbi-name))

  (entries [_ dbi-name] (l/entries lmdb dbi-name))

  (write-txn [_] (l/write-txn lmdb))

  (transact-kv [_ txs] (l/transact-kv lmdb txs))

  (close-transact-kv [_] (l/close-transact-kv lmdb))

  (get-value [db dbi-name k]
    (.get-value db dbi-name k :data :data true))
  (get-value [db dbi-name k k-type]
    (.get-value db dbi-name k k-type :data true))
  (get-value [db dbi-name k k-type v-type]
    (.get-value db dbi-name k k-type v-type true))
  (get-value [db dbi-name k k-type v-type ignore-key?]
    (scan/get-value db dbi-name k k-type v-type ignore-key?))

  (get-first [db dbi-name k-range]
    (.get-first db dbi-name k-range :data :data false))
  (get-first [db dbi-name k-range k-type]
    (.get-first db dbi-name k-range k-type :data false))
  (get-first [db dbi-name k-range k-type v-type]
    (.get-first db dbi-name k-range k-type v-type false))
  (get-first [db dbi-name k-range k-type v-type ignore-key?]
    (scan/get-first db dbi-name k-range k-type v-type ignore-key?))

  (get-range [db dbi-name k-range]
    (.get-range db dbi-name k-range :data :data false))
  (get-range [db dbi-name k-range k-type]
    (.get-range db dbi-name k-range k-type :data false))
  (get-range [db dbi-name k-range k-type v-type]
    (.get-range db dbi-name k-range k-type v-type false))
  (get-range [db dbi-name k-range k-type v-type ignore-key?]
    (scan/get-range db dbi-name k-range k-type v-type ignore-key?))

  (range-count [db dbi-name k-range]
    (.range-count db dbi-name k-range :data))
  (range-count [db dbi-name k-range k-type]
    (scan/range-count db dbi-name k-range k-type))

  (get-some [db dbi-name pred k-range]
    (.get-some db dbi-name pred k-range :data :data false))
  (get-some [db dbi-name pred k-range k-type]
    (.get-some db dbi-name pred k-range k-type :data false))
  (get-some [db dbi-name pred k-range k-type v-type]
    (.get-some db dbi-name pred k-range k-type v-type false))
  (get-some [db dbi-name pred k-range k-type v-type ignore-key?]
    (scan/get-some db dbi-name pred k-range k-type v-type ignore-key?))

  (range-filter [db dbi-name pred k-range]
    (.range-filter db dbi-name pred k-range :data :data false))
  (range-filter [db dbi-name pred k-range k-type]
    (.range-filter db dbi-name pred k-range k-type :data false))
  (range-filter [db dbi-name pred k-range k-type v-type]
    (.range-filter db dbi-name pred k-range k-type v-type false))
  (range-filter [db dbi-name pred k-range k-type v-type ignore-key?]
    (scan/range-filter db dbi-name pred k-range k-type v-type ignore-key?))

  (range-filter-count [db dbi-name pred k-range]
    (.range-filter-count db dbi-name pred k-range :data))
  (range-filter-count [db dbi-name pred k-range k-type]
    (scan/range-filter-count db dbi-name pred k-range k-type))

  (visit [db dbi-name visitor k-range]
    (.visit db dbi-name visitor k-range :data))
  (visit [db dbi-name visitor k-range k-type]
    (scan/visit db dbi-name visitor k-range k-type)))
