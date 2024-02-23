<p align="center"><img src="logo.png" alt="datalevin logo" height="140"></img></p>
<h1 align="center">Datalevin</h1>
<p align="center"> 🧘 Simple, fast and versatile Datalog database for everyone 💽 </p>
<p align="center">
<a href="https://cljdoc.org/d/datalevin/datalevin"><img src="https://cljdoc.org/badge/datalevin/datalevin" alt="datalevin on cljdoc"></img></a>
<a href="https://clojars.org/datalevin"><img src="https://img.shields.io/clojars/v/datalevin.svg?color=success" alt="datalevin on clojars"></img></a>
<a href="https://github.com/juji-io/datalevin/blob/master/doc/install.md#babashka-pod"><img src="https://raw.githubusercontent.com/babashka/babashka/master/logo/badge.svg" alt="bb compatible"></img></a>
</p>
<p align="center">
<a href="https://github.com/juji-io/datalevin/actions"><img
src="https://github.com/juji-io/datalevin/actions/workflows/release.binaries.yml/badge.svg?branch=0.8.27"
alt="datalevin linux/macos amd64 build status"></img></a>
<a href="https://ci.appveyor.com/project/huahaiy/datalevin"><img src="https://ci.appveyor.com/api/projects/status/github/juji-io/datalevin?svg=true" alt="datalevin windows build status"></img></a>
<a href="https://cirrus-ci.com/github/juji-io/datalevin"><img src="https://api.cirrus-ci.com/github/juji-io/datalevin.svg"
alt="datalevin apple silicon build status"></img></a>
</p>

## :hear_no_evil: What and why

> I love Datalog, why hasn't everyone used this already?

Datalevin is a simple durable [Datalog](https://en.wikipedia.org/wiki/Datalog)
database. Here's what a Datalog query looks like:

```Clojure
(d/q '[:find  ?name ?total
       :in    $ ?year
       :where [?sales :sales/year ?year]
              [?sales :sales/total ?total]
              [?sales :sales/customer ?customer]
              [?customer :customers/name ?name]]
      (d/db conn) 2023)
```

The rationale is to have a simple, fast and open source Datalog query engine
running on durable storage.  It is our observation that many developers prefer
the flavor of Datalog popularized by [Datomic®](https://www.datomic.com) over
any flavor of SQL, once they get to use it. Perhaps it is because Datalog is
more declarative and composable than SQL, e.g. the automatic implicit joins seem
to be its killer feature.

The feature set of Datomic®  may be an overkill for some use cases. One thing
that may confuse casual users is its [temporal
features](https://docs.datomic.com/cloud/whatis/data-model.html#time-model). To
keep things simple and familiar, Datalevin does not store transaction ids along
with the datoms, and behaves the same way as most other databases: when data are
deleted, they are gone.

Datalevin started out as a port of
[Datascript](https://github.com/tonsky/datascript) in-memory Datalog database to
[Lightning Memory-Mapped Database
(LMDB)](https://en.wikipedia.org/wiki/Lightning_Memory-Mapped_Database). It
retains the library property of Datascript, and it is meant to be embedded in
applications to manage state. Because data is persistent on disk in Datalevin,
application state can survive application restarts, and data size can be larger
than memory.

Datalevin relies on the robust ACID transactional database features of LMDB.
Designed for concurrent read intensive workloads, LMDB is used in many projects,
e.g.
[Cloudflare](https://blog.cloudflare.com/introducing-quicksilver-configuration-distribution-at-internet-scale/)
global configuration distribution. LMDB also [performs
well](http://www.lmdb.tech/bench/ondisk/) in writing large values (> 2KB).
Therefore, it is fine to store documents in Datalevin.

Datalevin uses a covering index and has no write-ahead log, so once the data are
written, they are indexed. There are no separate processes or threads for
indexing, compaction or doing any database maintenance work that compete with
your applications for resources. Since Datalog is simply a more ergonomic query
language than SQL, Datalevin can serve the role of an easier-to-use and
more lightweight relational database (RDBMS), e.g. where SQLite or Firebird is
called for.

Independent from Datalog, Datalevin can be used as a fast key-value store for
[EDN](https://en.wikipedia.org/wiki/Extensible_Data_Notation) data, with support
for range queries, predicate filtering and more. The native EDN data capability
of Datalevin should be beneficial for Clojure programs. One can use this feature
in situations where something like Redis is called for, for instance.

Datalevin can also run in an event-driven networked
[client/server](https://github.com/juji-io/datalevin/blob/master/doc/server.md)
mode (default port is 8898). The mode change is transparent. In the local mode,
a data directory path, e.g. `/data/mydb`, is needed for database location,
whereas a URI, e.g. `dtlv://myname:secret@myhost.in.cloud/mydb` is used in the
client/server mode. The same set of core functions work in both modes. In
addition, full-fledged role-based access control (RBAC) is provided on the
server.

In addition, Datalevin has a [built-in full-text search
engine](https://github.com/juji-io/datalevin/blob/master/doc/search.md) that has
[competitive](https://github.com/juji-io/datalevin/tree/master/search-bench)
search performance. It integrates nicely with other database features, and works in
all modes of Datalevin operation: embedded library, client/server, native
command line, and as a [Babashka](https://github.com/babashka/babashka) Pod.

Our goal is to simplify data storage and access by supporting diverse use cases
and paradigms, because maximal flexibility is the core strength of a Datalog
store. Datalevin may not be the fastest or the most scalable solution for one
particular use case, but it would surely support the most number of them in a
coherent and elegant manner.

Using one data store for different use cases simplifies and reduces the cost of
software development, deployment and maintenance. Therefore, we plan to
implement necessary extensions to make Datalevin also a vector databse, a production rule engine,
a graph database, and a document database, since the storage and index structure
of Datalevin is already compatible with all of them.

Presentation:

* [2020 London Clojurians Meetup](https://youtu.be/-5SrIUK6k5g)

## :truck: [Installation](doc/install.md)

As a Clojure library, Datalevin is simple to add as a dependency to your Clojure
project. There are also several other options. Please see details in [Installation Documentation](doc/install.md)

## :tada: Usage

Datalevin is aimed to be a versatile database.

### Use as a Datalog store

In addition to [our API doc](https://cljdoc.org/d/datalevin/datalevin),
since Datalevin has almost the same Datalog API as Datascript, which in turn has
almost the same API as Datomic®, please consult the abundant tutorials, guides
and learning sites available online to learn about the usage of Datomic® flavor
of Datalog.

Here is a simple code example using Datalevin:

```clojure
(require '[datalevin.core :as d])

;; Define an optional schema.
;; Note that pre-defined schema is optional, as Datalevin does schema-on-write.
;; However, attributes requiring special handling need to be defined in schema,
;; e.g. many cardinality, uniqueness constraint, reference type, and so on.
(def schema {:aka  {:db/cardinality :db.cardinality/many}
             ;; :db/valueType is optional, if unspecified, the attribute will be
             ;; treated as EDN blobs, and may not be optimal for range queries
             :name {:db/valueType :db.type/string
                    :db/unique    :db.unique/identity}})

;; Create DB on disk and connect to it, assume write permission to create given dir
(def conn (d/get-conn "/tmp/datalevin/mydb" schema))
;; or if you have a Datalevin server running on myhost with default port 8898
;; (def conn (d/get-conn "dtlv://myname:mypasswd@myhost/mydb" schema))

;; Transact some data
;; Notice that :nation is not defined in schema, so it will be treated as an EDN blob
(d/transact! conn
             [{:name "Frege", :db/id -1, :nation "France", :aka ["foo" "fred"]}
              {:name "Peirce", :db/id -2, :nation "france"}
              {:name "De Morgan", :db/id -3, :nation "English"}])

;; Query the data
(d/q '[:find ?nation
       :in $ ?alias
       :where
       [?e :aka ?alias]
       [?e :nation ?nation]]
     (d/db conn)
     "fred")
;; => #{["France"]}

;; Retract the name attribute of an entity
(d/transact! conn [[:db/retract 1 :name "Frege"]])

;; Pull the entity, now the name is gone
(d/q '[:find (pull ?e [*])
       :in $ ?alias
       :where
       [?e :aka ?alias]]
     (d/db conn)
     "fred")
;; => ([{:db/id 1, :aka ["foo" "fred"], :nation "France"}])

;; Close DB connection
(d/close conn)
```

### Use as a key-value store

Datalevin packages the underlying LMDB database as a convenient key-value store
for EDN data.

```clojure
(require '[datalevin.core :as d])
(import '[java.util Date])

;; Open a key value DB on disk and get the DB handle
(def db (d/open-kv "/tmp/datalevin/mykvdb"))
;; or if you have a Datalevin server running on myhost with default port 8898
;; (def db (d/open-kv "dtlv://myname:mypasswd@myhost/mykvdb" schema))

;; Define some table (called "dbi", or sub-databases in LMDB) names
(def misc-table "misc-test-table")
(def date-table "date-test-table")

;; Open the tables
(d/open-dbi db misc-table)
(d/open-dbi db date-table)

;; Transact some data, a transaction can put data into multiple tables
;; Optionally, data type can be specified to help with range query
(d/transact-kv
  db
  [[:put misc-table :datalevin "Hello, world!"]
   [:put misc-table 42 {:saying "So Long, and thanks for all the fish"
                       :source "The Hitchhiker's Guide to the Galaxy"}]
   [:put date-table #inst "1991-12-25" "USSR broke apart" :instant]
   [:put date-table #inst "1989-11-09" "The fall of the Berlin Wall" :instant]])

;; Get the value with the key
(d/get-value db misc-table :datalevin)
;; => "Hello, world!"
(d/get-value db misc-table 42)
;; => {:saying "So Long, and thanks for all the fish",
;;     :source "The Hitchhiker's Guide to the Galaxy"}


;; Range query, from unix epoch time to now
(d/get-range db date-table [:closed (Date. 0) (Date.)] :instant)
;; => [[#inst "1989-11-09T00:00:00.000-00:00" "The fall of the Berlin Wall"]
;;     [#inst "1991-12-25T00:00:00.000-00:00" "USSR broke apart"]]

;; This returns a PersistentVector - e.g. reads all data in JVM memory
(d/get-range db misc-table [:all])
;; => [[42 {:saying "So Long, and thanks for all the fish",
;;          :source "The Hitchhiker's Guide to the Galaxy"}]
;;     [:datalevin "Hello, world!"]]

;; This allows you to iterate over all DB keys inside a transaction.
;; You can perform writes inside the transaction.
;; kv is of of type https://www.javadoc.io/doc/org.lmdbjava/lmdbjava/latest/org/lmdbjava/CursorIterable.KeyVal.html
;; Avoid long-lived transactions. Read transactions prevent reuse of pages freed by newer write transactions, thus the database can grow quickly.
;; Write transactions prevent other write transactions, since writes are serialized.
;; LMDB advice: http://www.lmdb.tech/doc/index.html
;; Conclusion: It's ok to have long transactions if using a single thread.
(d/visit db misc-table
            (fn [kv]
               (let [k (d/read-buffer (d/k kv) :data)]
                  (when (= k 42)
                    (d/transact-kv db [[:put misc-table 42 "Don't panic"]]))))
              [:all])

(d/get-range db misc-table [:all])
;; => [[42 "Don't panic"] [:datalevin "Hello, world!"]]

;; Delete some data
(d/transact-kv db [[:del misc-table 42]])

;; Now it's gone
(d/get-value db misc-table 42)
;; => nil

;; Close key value db
(d/close-kv db)
```
### Entities with staged transactions (Datalog store)

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
  (-> (d/empty-db nil {:user/handle  #:db {:valueType :db.type/string
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

For more examples have a look at the [tests](https://github.com/juji-io/datalevin/blob/master/test/datalevin/test/entity.cljc#L42-L109).

This Entity API is new and can be improved. For example, it does not currently resolve
lookup refs like `[:user/handle "eve"]`. If you'd like to help, feel free to reach out to
@den1k.

## :green_book: Documentation

Please refer to the [API
documentation](https://cljdoc.org/d/datalevin/datalevin) for more details.
You may also consult online materials for Datascript or Datomic®, as the Datalog API is
similar.

## :birthday: Upgrade

Please read
[this](https://github.com/juji-io/datalevin/blob/master/doc/upgrade.md) for
information regarding upgrading your existing database from older versions.

## :earth_americas: Roadmap

These are the tentative goals that we try to reach as soon as we can. We may
adjust the priorities based on feedback.

* 0.4.0 ~~Native image and native command line tool.~~ [Done 2021/02/27]
* 0.5.0 ~~Native networked server mode with role based access control.~~ [Done 2021/09/06]
* 0.6.0 ~~As a search engine: full-text search across database.~~ [Done 2022/03/10]
* 0.7.0 ~~Explicit transactions, lazy results loading, and results spill to disk when memory is low.~~ [Done 2022/12/15]
* 0.8.0 ~~Long ids; composite tuples; enhanced search engine ingestion speed.~~ [Done 2023/01/19]
* 0.9.0 New Datalog query engine with improved performance.
* 0.10.0 Option to store data in compressed form.
* 0.11.0 As a vector DB: dense numeric vector indexing.
* 1.0.0 Transaction log storage and access API.
* 1.1.0 Read-only replicas for server.
* 1.2.0 Fully automatic schema migration.
* 2.0.0 As a production rule engine with truth maintenance.
* 3.0.0 As a document store: automatic indexing and incremental updates.
* 4.0.0 As a graph database: implementing [loom](https://github.com/aysylu/loom) graph protocols.
* 5.0.0 Distributed mode.

## :rocket: Status

Both Datascript and LMDB are mature and stable libraries. Building on top of
them, Datalevin is extensively tested with property-based testing. It is also used
in production at [Juji](https://juji.io).

Running the [benchmark suite adopted from Datascript](https://github.com/juji-io/datalevin/tree/master/bench), which write 100K random datoms in several conditions, and run several queries on them, on a Ubuntu Linux server with an Intel i7 3.6GHz CPU and a 1TB SSD drive, here is how it looks.

<p align="center">
<img src="bench/datalevin-bench-query-04-27-2023.png" alt="query benchmark" height="300"></img>
<img src="bench/datalevin-bench-write-04-27-2023.png" alt="write benchmark" height="300"></img>
</p>

In this benchmark, Datomic is running in in-memory mode, as it requires another
database for persistence. Also, the `init` write condition, i.e. bulk loading
prepared datoms, is not available.

In all benchmarked queries, Datalevin is faster than Datascript and Datomic.
Considering that we are comparing a disk store with memory stores, this result
may be counter-intuitive. One possible reason is that Datalevin caches more
aggressively, whereas Datascript chose not to do so (e.g. see [this
issue](https://github.com/tonsky/datascript/issues/6)). In addition, we
implemented more query optimizations.

Writes are slower, as expected, as Datalevin is writing to disk while others are
in memory. The bulk loading speed is good, writing 100K datoms to disk in less
than 0.2 seconds; the same data can also be transacted with all the integrity
checks as a whole or five datoms at a time in less than 1.5 seconds. Transacting
one datom at a time, it takes longer time. Therefore, it is preferable to have
batch transactions.

In short, Datalevin is quite capable for small or medium projects right now.

## :floppy_disk: Differences from Datascript

Datascript is developed by [Nikita Prokopov](https://tonsky.me/) that "is built
totally from scratch and is not related by any means to" Datomic®. Although
a port, Datalevin differs from Datascript in more significant ways
than just the difference in data durability and running mode:

* As mentioned, Datalevin is not an immutable database, and there is no
  "database as a value" feature.  Since history is not kept, transaction ids are
  not stored.

* Datoms in a transaction are committed together as a batch, rather than being
  saved by `with-datom` one at a time.

* ACID transaction and rollback are supported.

* Lazy results set and spill to disk are supported.

* Entity and transaction integer ids are 64 bits long, instead of 32 bits.

* Respects `:db/valueType`. Currently, most [Datomic® value
  types](https://docs.datomic.com/on-prem/schema.html#value-types) are
  supported, except uri. Values of the attributes that
  are not defined in the schema or have unspecified types are treated as
  [EDN](https://en.wikipedia.org/wiki/Extensible_Data_Notation) blobs, and are
  de/serialized with [nippy](https://github.com/ptaoussanis/nippy).

* In addition to composite tuples, Datalevin also supports heterogeneous and
  homogeneous tuples.

* Has a value leading index (VEA) for datoms with `:db.type/ref` type attribute;
  The attribute and value leading index (AVE) is enabled for all datoms, so
  there is no need to specify `:db/index`, similar to Datomic® Cloud. Does not
  have AEV index, in order to save storage and improve write speed.

* Transaction functions should be defined with `inter-fn`, for
  function serialization requires special care in order to support GraalVM.

* Attributes are stored in indices as integer ids, thus attributes in index
  access are returned in attribute creation order, not in lexicographic order
  (i.e. do not expect `:b` to come after `:a`). This is the same as Datomic®.

* Has no features that are applicable only for in-memory DBs, such as DB as an
  immutable data structure, DB pretty print, etc.

## :baby: Limitations

* Attribute names have a length limitation: an attribute name cannot be more
  than 511 bytes long, due to LMDB key size limit.

* Because keys are compared bitwise, for range queries to work as expected on an
  attribute, its `:db/valueType` should be specified.

* Floating point `NaN` cannot be stored.

* Big integers do not go beyond the range of `[-2^1015, 2^1015-1]`, the
  unscaled value of big decimal has the same limit.

* The maximum individual value size is 2GB. Limited by the maximum size of
  off-heap byte buffer that can be allocated in JVM.

* The total data size of a Datalevin database has the same limit as LMDB's, e.g.
  128TB on a modern 64-bit machine that implements 48-bit address spaces.

* Currently supports Clojure on JVM 8 or the above, but adding support for other
  Clojure-hosting runtime is possible, since bindings for LMDB
  exist in almost all major languages and available on most platforms.

## :shopping: Alternatives

If you are interested in using the dialect of Datalog pioneered by Datomic®, here are your current options:

* If you need time travel and features backed by the authors of Clojure, you should use [Datomic®](https://www.datomic.com).

* If you need an in-memory store that has almost the same API as Datomic®, [Datascript](https://github.com/tonsky/datascript) is for you.

* If you need a graph database, you may try [Asami](https://github.com/threatgrid/asami).

* If you need features such as bi-temporal graph queries, you may try [XTDB](https://github.com/xtdb/xtdb).

* If you need a durable store with some storage choices, you may try [Datahike](https://github.com/replikativ/datahike).

* There was also [Eva](https://github.com/Workiva/eva/), a distributed store, but it is no longer in active development.

* If you need a simple, fast and versatile durable store with a battle tested backend, give [Datalevin](https://github.com/juji-io/datalevin) a try.

## :arrows_clockwise: Contact

We appreciate and welcome your contribution or suggestion. Please feel free to
file issues or pull requests.

If commercial support is needed for Datalevin, talk to us.

You can talk to us in the `#datalevin` channel on [Clojurians Slack](http://clojurians.net/).

## License

Copyright © 2020-2024 [Juji, Inc.](https://juji.io).

Licensed under Eclipse Public License (see [LICENSE](LICENSE)).
