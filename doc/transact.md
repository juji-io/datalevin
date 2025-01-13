# Transactions in Datalevin

Datalevin relies on the transaction mechanism of the underlying key-value store,
LMDB (Lightening Memory-Mapped Database), to achieve ACID.

In LMDB, read and write are independent and do not block each other. Read
requires a read transaction. Write requires a read-write transaction. These are
normally two different transactions.

Writes are serialized, only one thread can write at a time. When multiple
threads write concurrently to the same key, whoever writes later wins
eventually, because writes are serialized. The first write succeeds, but the
value will then be overwritten by the second write.

Reads can be concurrent. Basically each reader thread reads its own view of the
data created at the moment the read transaction starts. When read is concurrent
with write, the newly written values are invisible to the reader, because read
transaction sees a view of the database that is consistent and up to the time
when the read transaction starts, which is before the write transaction commits.

LMDB suggests that:

> Avoid long-lived transactions. Read transactions prevent reuse of pages freed
> by newer write transactions, thus the database can grow quickly. Write
> transactions prevent other write transactions, since writes are serialized.

## Batching of Transaction Data

By default, each write transaction flushes to disk when it commits, which is an
expensive operation even with today's SSD disks.

Batching transacation data together reduces the number of such expensive commit
calls, which enhances write throughput. It is therefore recommended to
batch transaction data in user code if possible.

## Env Flags

Datalevin adopts the same default write condition as that of LMDB. That is, the
write transactions by default are guranteed to be durable, i.e. no risk of data
loss or DB corruption in case of system crash. As mentioned above, this fully
safe write condition has some cost since syncing to disk is expensive.

Datalevin supports several faster, albeit less safe write conditions by passing in
some Env flags when openning the DB. The follwing table lists these flags and
their implications.

| Flags | Meaning | Implication |
| ----- | ----- | ----- |
| :writemap | Writeable memory map | Buggy native code may accidentally overwrite memory to corrupt DB; File system failure may crash process; On OS don't support sparse files, e.g. OSX, underlaying storage will be immediately fully preallocated to specificed map size. |
| :mapasync | use asynchronous msync when :writemap is used | DB may be corrupted when system crashes |
| :nometasync | don't fsync metapage when commit | The committed data in the last transaction may be lost when system crashes |
| :nosync | don't fsync when commit | DB may be corrupted when system crashes |

These flags normally improve write speed signficantly. User can then manually
call `sync` function to force flusing to disk at appropriate time. Combining
these techniques appropriately may achieve desirable write speed and safety for
some use cases.

Here are some examples of passing the env flags:

```Clojure
(require '[datalevin.core :as d])
(require '[datalevin.constant :as c])

;; Pass :nosync to my-kvdb KV store
(d/open-kv "/tmp/my-kvdb" {:flags (conj c/defaultl-env-flags :nosync)))})

;; Set :temp? true for a KV store automaticaly adds :nosync, this DB will also
;; be deleted on JVM exit.
(d/open-kv "/tmp/tmp-kvdb" {:temp? true))})

;; Pass :nometasync to testdb Datalog store
(d/get-conn "/tmp/testdb" {} {:kv-opts {:flags (conj c/default-env-flags :nometasync)}})

```

## Explicit Synchronous Transaction

To obtain features such as compare-and-swap semantics, that is, a group of reads
and writes are treated as a single atomic action, Datalevin exposes explicit
synchronous transaction.

For key-value API, `with-transaction-kv` macro is used for explicit transaction.
`with-transaction` macro is used for Datalog API. Basically, all the code in the
body of the macros run inside a single read/write transaction with a single
thread. These work the same in all modes of Datalevin: embedded, client/server,
or babashka pod. For usage examples, see tests in `datalevin.withtxn-test` or
`datalevin.remote-withtxnkv-test`.

Rollback from within the transaction can be done with `abort-transact-kv` and
`abort-transact`.

Datalog functions such as `transact!` use `with-transaction` internally.

## Asynchronous Transaction

Datalevin provides `transact-kv-async` function for asynchronous transaction in
the KV store. This function returns a future that is realized when data is
committed.

An auto-batching mechanism is designed to automatically batch
transaction data when this function is called. The batching is adaptive to write
load. The higher the load, the bigger the batch size. So write throughput is
higher than `transact-kv`.

Similarly, for Datalog store, `transact-async` can be used for auto-batching.
`transact` function also use asynchronous transaction, but it blocks until the
future is realized.

One can call a sequence of `transact-async`, followed by a `transact` to achive
good batching effect and determinstic commit at the same time.

## Transaction Functions in Datalog Store

In addition to `with-transaction`, transaction functions can be used in Datalog
store for atomic actions. Two types of transaction functions can be used.

`:db/fn` allows stored transaction functions. Such functions need to be defined
using `inter-fn` or `definterfn`.  This is necessary in order to support
de-serialization of functions without calling Clojure `eval`. This requirement
is needed to accommodate GraalVM native image, because `eval `cannot be used in
native image, which has a closed world assumption.  This way of defining a
function is also necessary when a function needs to be passed over the wire to
server or babashka. The source code of the function will be interpreted by
[sci](https://github.com/babashka/sci) instead, so there's currently some
limitations, e.g. except for built-in ones, normal Clojure vars are not
accessible. We will address these limitations in the future.

`:db.fn/call` is another way to call a transaction function, which does not
store the function in the database, so this is usable in embedded mode, where that function is available in user code to call and that function can be a regular Clojure function.

For usage examples, see tests in `datalevin.test.transact`.

## Bulk Load Data into Datalog Store

### By Transaction

The most straightforward method of transacting data at a time using `transact!`
works quite well for many cases. To have higher throughput, use
`transact-async`.

Because Datalevin supports only a single write thread at a time, parallel
transactions actually slow writes down significantly due to the thread switching
overhead.

However, transacting Datalog data involves a great number of data transformation
and integrity checks, hence it can be slow. When initializing a DB with data, it
may not be necessary to pay the price of this overhead.

### By `init-db` and `fill-db`

If it is possible, a much faster way of bulk loading data into an empty DB is to
directly load a collection of prepared datoms using `init-db` function. However,
it is the caller's responsibility to ensure these datoms are correct because data
integrity checks and temporary entity ID resolution are not performed.

Similarly, `fill-db` can be used to bulk load additional collections of prepared
datoms into a DB that is not empty. The same caution on datoms preparation need
to apply.

## Transactable Entities in Datalog store

In other Datalog DBs (Datomic®, DataScript, and Datahike) `d/entity` returns a type
that errors on associative updates. This makes sense because Entity represents
a snapshot state of a DB Entity and `d/transact` demarcates transactions.
However, this API leads to a cumbersome developer experience, especially
for the removal of fields where vectors of `[:db/retract <eid> <attr> <optional eid>]`
must be used in transactions because `nil` values are not allowed.

Datalevin ships with a special Entity type that allows for associative updates
while remaining immutable until expanded during transaction time (`d/transact`).
This type works the same in either local or remote mode.

Below are some examples. Look for the `:<STAGED>` keyword in the printed entities

```clojure
(require '[datalevin.core :as d])

(def db
  (-> (d/empty-db nil {:user/handle  #:db{:valueType :db.type/string
                                          :unique    :db.unique/identity}
                       :user/friends #:db{:valueType   :db.type/ref
                                          :cardinality :db.cardinality/many}})
      (d/db-with [{:user/handle  "ava"
                   :user/friends [{:user/handle "fred"}
                                  {:user/handle "jane"}]}])))

(def ava (d/entity db [:user/handle "ava"]))

(d/touch ava)
; => {:user/handle ava, :user/friends #{#:db{:id 3} #:db{:id 2}}, :db/id 1}
(assoc ava :user/age 42)
; => {:user/handle  ava
;     :user/friends #{#:db{:id 3} #:db{:id 2}},
;     :db/id        1,
;     :<STAGED>     #:user{:age [{:op :assoc} 42]}} <-- staged transaction!

(d/touch (d/entity db [:user/handle "ava"]))
; => {:user/handle ava, :user/friends #{#:db{:id 3} #:db{:id 2}}, :db/id 1}
; immutable! – db entity remains unchanged

(def db2 (d/db-with db [(assoc ava :user/age 42)]))

(def ava-with-age (d/entity db [:user/handle "ava"]))

(d/touch ava-with-age)
;=> {:user/handle "ava",
;    :user/friends #{#:db{:id 3} #:db{:id 2}},
;    :user/age 42, <-- age was transacted!
;    :db/id 1}

(def db3
  (d/db-with db2 [(-> ava-with-age
                      (update :user/age inc)
                      (d/add :user/friends {:user/handle "eve"}))]))

;; eve exists
(d/touch (d/entity db3 [:user/handle "eve"]))
;; => {:user/handle "eve", :db/id 4}

; eve is a friend of ada
(d/touch (d/entity db3 [:user/handle "ava"]))
;=> {:user/handle "ava",
;    :user/friends #{#:db{:id 4} <-- that's eve!
;                    #:db{:id 3}
;                    #:db{:id 2}},
;    :user/age 43,
;    :db/id 1}

; Oh no! That was a short-lived friendship.
; eve and ava got into an argument 😔

(def db4
  (d/db-with
    db3
    [(d/retract (d/entity db3 [:user/handle "ava"]) :user/friends [{:db/id 4}])]))

(d/touch (d/entity db4 [:user/handle "ava"]))
;=> {:user/handle "ava",
;    :user/friends #{#:db{:id 3} #:db{:id 2}}, ; <-- eve is not a friend anymore
;    :user/age 43,
;    :db/id 1}
```

For more examples have a look at the [tests](https://github.com/juji-io/datalevin/blob/master/test/datalevin/test/entity.clj#L45-L116).

This Entity API is new and can be improved. For example, it does not currently
resolve lookup refs like `[:user/handle "eve"]`. If you'd like to help, feel
free to reach out to @den1k.
