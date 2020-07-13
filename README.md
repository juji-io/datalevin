<p align="center"><img src="logo.png" alt="datalevin logo" height="140"></img></p>
<h1 align="center">Datalevin</h1> 
<p align="center"> 🧘 Simple durable Datalog database for everyone 💽 </p>

## :hear_no_evil: What and why

> I love Datalog, why hasn't everyone use this already? 

Datalevin is a port of [Datascript](https://github.com/tonsky/datascript) in-memory Datalog database to [Lightning Memory-Mapped Database (LMDB)](https://en.wikipedia.org/wiki/Lightning_Memory-Mapped_Database). 

The rationale is to have a simple and free Datalog query engine running on durable storage.  It is our observation that many developers prefer the flavor of Datalog popularized by [Datomic®](https://www.datomic.com) over any flavor of SQL, once they get to use it. Perhaps it is because Datalog is more declarative and composable than SQL, e.g. the automatic implicit joins seem to be its killer feature.

Datomic® is an enterprise grade software, and its feature set may be an overkill for some use cases. One thing that may confuse casual users is its [temporal features](https://docs.datomic.com/cloud/whatis/data-model.html#time-model). To keep things simple and familiar, Datalevin does not store transaction history, and behaves the same way as most other databases: when data are deleted, they are gone.

Datalevin retains the library property of Datascript, and it is meant to be embedded in applications to manage state. Because data is persistent on disk in Datalevin, application state can survive application restarts, and data size can be larger than memory.  

Datalevin relies on the robust ACID transactional database features of LMDB. Designed for concurrent read intensive workloads, LMDB is used in many projects, e.g. [Cloudflare](https://blog.cloudflare.com/introducing-quicksilver-configuration-distribution-at-internet-scale/) global configuration distribution. LMDB also [performs well](http://www.lmdb.tech/bench/ondisk/) in writing large values (> 2KB). Therefore, it is fine to store documents in Datalevin. 

Independent from Datalog, Datalevin can also be used as an efficient key-value store for [EDN](https://en.wikipedia.org/wiki/Extensible_Data_Notation) data. A number of optimizations are put in place. For instance, it uses a transaction pool to reuse transactions, pre-allocates read/write buffers, and so on. 

## :tada: Usage

## :rocket: Status

Both Datascript and LMDB are very mature and stable libraries. Building on top of them, Datalevin is also extensively tested with [property-based testing](https://github.com/clojure/test.check).

Datalevin is developed to support one of our core products, and we are committed to maintain it for the foreseeable future. 

## :floppy_disk: Differences from Datascript

Datascript is developed by [Nikita Prokopov](https://tonsky.me/) that "is built totally from scratch and is not related by any means to" Datomic®. Although a port, Datalevin differs from Datascript in more ways than the difference in data durability:

* Datalevin is not an immutable database, and there is no "database as a value" feature.  Since history is not kept, transaction ids are not stored. 

* Datoms in a transaction are committed together as a batch, rather than being saved by `with-datom` one at a time. This improves the performance in writing to disk.

* Respects `:db/valueType`. Currently, most [Datomic® value types](https://docs.datomic.com/on-prem/schema.html#value-types) are supported, except bigint, bigdec, uri and tuple. Values with unspecified type are treated as [EDN](https://en.wikipedia.org/wiki/Extensible_Data_Notation) blobs, and are de/serialized with [nippy](https://github.com/ptaoussanis/nippy). 

* Has a value leading index (VAE) for datoms with `:db.type/ref` type attribute; The attribute and value leading index (AVE) is enabled for all datoms, so there is no need to specify `:db/index`. These are the same as Datomic® Cloud.  

* Attributes are stored in indices as integer ids, thus attributes in index access are returned in attribute creation order, not in lexicographic order (i.e. do not expect `:b` to come after `:a`). This is the same as Datomic®.

* Uses 64 bits long integers, and consistently applies `^long` type hints to avoid boxed math performance hit. 

* Has no features that are applicable only for in-memory DBs, such as DB as an immutable data structure, DB serialization, DB pretty print, filtered DB, etc. For now, [LMDB tools](http://www.lmdb.tech/doc/tools.html) can be used to work with the database files.

## :baby: Limitations

* Attribute names have a length limitation: an attribute name cannot be more than 511 bytes long, due to LMDB key size limit.

* Because keys are compared bitwise, for range queries to work as expected on an attribute, its `:db/valueType` should be specified.

* The maximum individual value size is 4GB. In practice, value size is determined by LMDB's ability to find large enough continuous space on disk and Datelevin's ability to pre-allocate off-heap buffers in JVM for them. 

* The total data size of a Datalevin database has the same limit as LMDB's, e.g. 128TB on a modern 64-bit machine that implements 48-bit address spaces.

* There's no network interface as of now, but this may change.

* Currently only supports Clojure on JVM, but adding support for other Clojure-hosting runtime is possible in the future, since bindings for LMDB exist in almost all major languages and available on most platforms.

## :shopping: Alternatives

If you are interested in using the dialect of Datalog pioneered by Datomic®, here are your current options:

* If you need time travel and rich features backed by the authors of Clojure, you should use [Datomic®](https://www.datomic.com).

* If you need an in-memory store, e.g. for single page applications in a browser, [Datascript](https://github.com/tonsky/datascript) is for you.

* If you need features such as bi-temporal graph queries, You may try [Crux](https://github.com/juxt/crux).

* If you don't mind experimental storage backend, you may try [Datahike](https://github.com/replikativ/datahike).

* There was also [Eva](https://github.com/Workiva/eva/), a distributed store, but it is no longer in active development.

* If you need a simple durable store with a battle tested backend, give [Datalevin](https://github.com/juji-io/datalevin) a try.

## License

Copyright © 2020 Juji Inc.

Licensed under Eclipse Public License (see [LICENSE](LICENSE)).
