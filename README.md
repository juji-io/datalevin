<p align="center"><img src="logo.png" alt="datalevin logo"
height="140"></img></p>
<h1 align="center">Datalevin</h1>
<p align="center"> 🧘 Simple, fast and versatile Datalog database for everyone
💽 </p>
<p align="center">
<a href="https://cljdoc.org/d/datalevin/datalevin"><img
src="https://cljdoc.org/badge/8datalevin/datalevin" alt="datalevin on
cljdoc"></img></a>
<a href="https://clojars.org/datalevin"><img
src="https://img.shields.io/clojars/v/datalevin.svg?color=success"
alt="datalevin on clojars"></img></a>
<a
href="https://github.com/juji-io/datalevin/blob/master/doc/install.md#babashka-pod"><img
src="https://raw.githubusercontent.com/babashka/babashka/master/logo/badge.svg"
alt="bb compatible"></img></a>
</p>
<p align="center">
<a href="https://github.com/juji-io/datalevin/actions"><img
src="https://github.com/juji-io/datalevin/actions/workflows/release.binaries.yml/badge.svg"
alt="datalevin linux/macos amd64 build status"></img></a>
<a href="https://ci.appveyor.com/project/huahaiy/datalevin"><img
src="https://ci.appveyor.com/api/projects/status/github/juji-io/datalevin?svg=true"
alt="datalevin windows build status"></img></a>
<a href="https://cirrus-ci.com/github/juji-io/datalevin"><img
src="https://api.cirrus-ci.com/github/juji-io/datalevin.svg" alt="datalevin
apple silicon build status"></img></a>
</p>


> I love Datalog, why hasn't everyone used this already?

Datalevin is a simple durable [Datalog](https://en.wikipedia.org/wiki/Datalog)
database. Here's what a Datalog query looks like in Datalevin:

```Clojure
(d/q '[:find  ?name ?total
       :in    $ ?year
       :where [?sales :sales/year ?year]
              [?sales :sales/total ?total]
              [?sales :sales/customer ?customer]
              [?customer :customers/name ?name]]
      (d/db conn) 2024)
```

## :question: Why

The rationale is to have a simple, fast and open source Datalog query engine
running on durable storage.

It is our observation that many developers prefer
the flavor of Datalog popularized by [Datomic®](https://www.datomic.com) over
any flavor of SQL, once they get to use it. Perhaps it is because Datalog is
more declarative and composable than SQL, e.g. the automatic implicit joins seem
to be its killer feature. In addition, the recursive rules feature of Datalog
makes it suitable for graph processing and deductive reasoning.

The feature set of Datomic® may not be a good fit for some use cases. One thing
that may [confuse some
users](https://vvvvalvalval.github.io/posts/2017-07-08-Datomic-this-is-not-the-history-youre-looking-for.html)
is its [temporal
features](https://docs.datomic.com/cloud/whatis/data-model.html#time-model). To
keep things simple and familiar, Datalevin behaves the same way as most other
databases: when data are deleted, they are gone. Datalevin also follows the
widely accepted principles of ACID, instead of introducing [unusual
semantics](https://jepsen.io/analyses/datomic-pro-1.0.7075).

Datalevin has a [novel cost-based query optimizer](doc/query.md), with a query
performance [competitive](benchmarks/JOB-bench) with SQL RDBMS such as
PostgreSQL.

Datalevin can be used as a library, embedded in applications to manage state, e.g.
used like SQLite; or it can run in a networked
[client/server](https://github.com/juji-io/datalevin/blob/master/doc/server.md)
mode (default port is 8898) with full-fledged role-based access control (RBAC)
on the server, e.g. used like PostgreSQL.

Datalevin relies on the robust ACID transactional database features of LMDB.
Designed for concurrent read intensive workloads, LMDB also [performs
well](http://www.lmdb.tech/bench/ondisk/) in writing large values (> 2KB).
Therefore, it is fine to store documents in Datalevin.

Datalevin can be used as a fast key-value store for
[EDN](https://en.wikipedia.org/wiki/Extensible_Data_Notation) data. The native
EDN data capability of Datalevin should be beneficial for Clojure programs.

Moreover, Datalevin has a [built-in full-text search
engine](https://github.com/juji-io/datalevin/blob/master/doc/search.md) that has
[competitive](https://github.com/juji-io/datalevin/tree/master/benchmarks/search-bench)
search performance.

Additional materials in chronological order:

* [Competing for the JOB with a Triplestore](https://yyhh.org/blog/2024/09/competing-for-the-job-with-a-triplestore/)
* [If I had to Pick One: Datalevin](https://vimsical.notion.site/If-I-Had-To-Pick-One-Datalevin-be5c4b62cda342278a10a5e5cdc2206d)
* [T-Wand: Beat Lucene in Less Than 600 Lines of Code](https://yyhh.org/blog/2021/11/t-wand-beat-lucene-in-less-than-600-lines-of-code/)
* [2020 London Clojurians Meetup](https://youtu.be/-5SrIUK6k5g)

## :truck: [Installation](doc/install.md)

As a Clojure library, Datalevin is simple to add as a dependency to your Clojure
project. There are also several other options. Please see details in
[Installation Documentation](doc/install.md)

## :birthday: Upgrade

Please read
[Upgrade
Documentation](https://github.com/juji-io/datalevin/blob/master/doc/upgrade.md)
for information regarding upgrading your existing Datalevin database from older
versions.

## :tada: Usage

Datalevin is aimed to be a versatile database.

### Use as a Datalog store

In addition to [our API doc](https://cljdoc.org/d/datalevin/datalevin),
Datalevin has almost the same Datalog API as [Datascript](https://github.com/tonsky/datascript), which in turn has
almost the same API as Datomic®, please consult the abundant tutorials, guides
and learning sites available online to learn about the usage of Datomic® flavor
of Datalog.

Here is a simple code example using Datalevin:

```clojure
(require '[datalevin.core :as d])

;; Define an optional schema.
;; Note that pre-defined schema is optional, as Datalevin does schema-on-write.
;; However, attributes requiring special handling need to be defined in schema,
;; e.g. range query, many cardinality, uniqueness, reference type, etc.
;; Similar to Datascript, Datalevin schemas differ from Datomic®:
;; - The schema must be a map of maps, not a vector of maps.
;; - It is not `transact`ed into the db but passed when acquiring connections.
;; - Use `update-schema` to update the schema of an open connection to a DB.
(def schema {:aka  {:db/cardinality :db.cardinality/many}
             ;; :db/valueType is optional, if unspecified, the attribute will be
             ;; treated as EDN blobs, and may not be optimal for range queries
             :name {:db/valueType :db.type/string
                    :db/unique    :db.unique/identity}})

;; Create DB on disk and connect to it, assume write permission to create the dir
(def conn (d/get-conn "/tmp/datalevin/mydb" schema))
;; or if you have a Datalevin server running on myhost with default port 8898
;; (def conn (d/get-conn "dtlv://myname:mypasswd@myhost/mydb" schema))

;; Transact some data
;; `:nation` is not defined in schema, so it will be treated as an EDN blob
(d/transact conn
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
(d/transact conn [[:db/retract 1 :name "Frege"]])

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
## :green_book: Documentation

Please refer to the [API
documentation](https://cljdoc.org/d/datalevin/datalevin) for more details.

## :rocket: Status

Datalevin is extensively tested with property-based testing. It is also used
in production at [Juji](https://juji.io), among other companies.

Running the [benchmark suite adopted from
Datascript](https://github.com/juji-io/datalevin/tree/master/benchmarks/datascript-bench),
which write 100K random datoms in several conditions, and run several queries on
them, on a Ubuntu Linux server with an Intel i7 3.6GHz CPU and a 1TB SSD drive,
here is how it looks.

<p align="center">
<img src="benchmarks/datascript-bench/Read.png" alt="query benchmark" height="300"></img>
<img src="benchmarks/datascript-bench/Write.png" alt="write benchmark" height="300"></img>
</p>


In all benchmarked queries, Datalevin is the fastest among the three tested
systems, as Datalevin has a [cost based query optimizer](doc/query.md) while Datascript and
Datomic do not. Datalevin also has a caching layer for index access. See
[here](https://github.com/juji-io/datalevin/tree/master/benchmarks/datascript-bench)
for a detailed analysis of the results.

We also compared Datalevin and PostgreSQL in handling complex queries, using
[Join Order Benchmark](benchmarks/JOB-bench). On a
MacBook Pro, Apple M3 chip with 12 cores, 30 GB memory and 1TB SSD drive, here's
the average times:

<p align="center">
<img src="benchmarks/JOB-bench/means.png" alt="JOB benchmark averages" height="300"></img>
</p>

Datalevin is about 1.3X faster than PostgreSQL on average in running the complex
queries in this benchmark. The gain is mainly in better query execution time due
to higher quality of generated query plans. For more details, see
[here](benchmarks/JOB-bench).

## :earth_americas: Roadmap

These are the tentative goals that we try to reach as soon as we can. We may
adjust the priorities based on feedback.

* 0.4.0 ~~Native image and native command line tool.~~ [Done 2021/02/27]
* 0.5.0 ~~Native networked server mode with role based access control.~~ [Done 2021/09/06]
* 0.6.0 ~~As a search engine: full-text search across database.~~ [Done 2022/03/10]
* 0.7.0 ~~Explicit transactions, lazy results loading, and results spill to disk when memory is low.~~ [Done 2022/12/15]
* 0.8.0 ~~Long ids; composite tuples; enhanced search engine ingestion speed.~~ [Done 2023/01/19]
* 0.9.0 ~~New Datalog query engine with improved performance.~~ [Done 2024/03/09]
* 1.0.0 New rule evaluation algorithm and incremental view maintenance.
* 1.1.0 Option to store data in compressed form.
* 1.2.0 Extensible de/serialization for arbitrary data.
* 2.0.0 Vector indexing and similarity search.
* 2.1.0 Automatic document indexing.
* 2.2.0 Extended full-text search syntax.
* 3.0.0 JSON API and library/client for popular languages.
* 4.0.0 Transaction log storage and access API.
* 4.1.0 Read-only replicas for server.
* 5.0.0 Distributed mode.


## :arrows_clockwise: Contact

We appreciate and welcome your contributions or suggestions. Please feel free to
file issues or pull requests.

If commercial support is needed for Datalevin, talk to us.

You can talk to us in the `#datalevin` channel on [Clojurians Slack](http://clojurians.net/).

## License

Copyright © 2020-2025 [Juji, Inc.](https://juji.io).

Licensed under Eclipse Public License (see [LICENSE](LICENSE)).
