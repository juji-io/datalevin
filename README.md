<p align="center"><img src="logo.png" alt="datalevin logo" height="120"></img></p>
<h1 align="center">Datalevin</h1> 
<p align="center">Simple durable Datalog database for everyone.</p>

## :hear_no_evil: What and why

Datalevin is a port of [Datascript](https://github.com/tonsky/datascript) in-memory database and Datalog query
engine to work on top of [Lightning Memory-Mapped Database (LMDB)](https://en.wikipedia.org/wiki/Lightning_Memory-Mapped_Database).

The rationale for Datalevin is to provide a simple and free Datalog engine running on durable storage.  It is my observation that many developers prefer the flavor of Datalog populized by [Datomic](https://www.datomic.com) over any flavor of SQL, once they get to use it.  In my opinion, the automatic implict joins in Datalog query is its killer feature.

> I love Datalog, why haven't everyone use this already? 

Datomic is a closed source enterprise software and its feature sets may be an overkill for many use cases. One thing that often confuses novice users is its temporal features. To keep things simple and familiar (yes, this combination exists), Datalevin does not keep transaction history, and behaves the same way as most other databases: when data are deleted, they are gone.

Datalevin retains the library property of Datascript, and it is meant to be embedded in applications to manage state. Because data is persistent on disk in Datalevin, application state can survive application restarts and data size can be larger than memory.  

To fulfill Datalevin's intended use of storing appication state, Datalevin relies on LMDB's robust transactional database design and leverages its high performance for concurrent read intensive workloads. LMDB is a battle tested data store used in [many projects](https://symas.com/lmdb/technical/#projects). For example, LMDB powers [Cloadflare](https://blog.cloudflare.com/introducing-quicksilver-configuration-distribution-at-internet-scale/) global configuration distribution. In addition to good read performance, LMDB performs well in writing values larger than 2KB. Therefore, unlike some alternatives, it is fine to store large values in Datalevin. The maximum individual value size can be 4GB, as long as LMDB can find large enough continous space on disk and JVM can pre-allocate off-heap buffers for them. 

## :tada: Usage


## :floppy_disk: Difference from Datascript

In addition to the diffrence in data durability, Datalevin differs from Datascript in the following ways:

* Does not store transaction ids. Since history is not kept, there is no need to store transanction ids.

* Entity ids are 64 bits long, so as to support a much larger data size.  The total data size of a Datalevin database has the same limit as LMDB's, e.g. 128TB on a modern 64-bit machine that implements 48-bit address spaces.

* Has an additional index that uses values as the primary key (VAE), similar to Datomic.

* Indices respect `:db/valueType`. Currently, most Datomic value types are supported, except bigint, bigdec, uri and tuple. Values with unspecified type are treated as [EDN](https://en.wikipedia.org/wiki/Extensible_Data_Notation) blobs, and are de/serialized with nippy. Because values in Datalevin are compared bitwisely, for range queries to work correctly on an attribute, its `:db/valueType` should be specified.

* Attributes have internal integer ids, and attribute names have a length limitation: an attribute name cannot be more than 511 bytes long, due to LMDB key size limit.

* Handles schema migrations with `swap-attr` function. It only allows safe migration that does not alter existing data (e.g. one to many cardinaity, unique to non-unique, index to non-index and vice vesa), and refuses unsafe schema changes (e.g. many to one cardinality, non-unique to unique, value type changes) that are inconsistent with existing data.

* Datalevin currently only supports Clojure on JVM, but adding support for other Clojure-hosting runtimes is possible in the future, since bindings for LMDB exist in almost all major languages and available on most platforms.

## :shopping: Alternatives

If you are interested in using the dialect of Datalog pioneered by Datomic, here are your current options:

* If you need a simple durable store with a battle tested backend, give [Datalevin](https://github.com/juji-io/datalevin) a try.

* If you need an in-memory store, e.g. for single page applications running in a browser, [Datascript](https://github.com/tonsky/datascript) is for you.

* If you need time travel and rich features backed by the authors of Clojure, you should use [Datomic](https://www.datomic.com).

* If you need features such as bitemporal graph queries, You may try [Crux](https://github.com/juxt/crux).

* If you don't mind experimental storage backend, you may try [Datahike](https://github.com/replikativ/datahike).

* There was also [Eva](https://github.com/Workiva/eva/), a distributed store, but it is no longer in active development.

## License

Copyright © 2020 Juji Inc.

Licensed under Eclipse Public License (see [LICENSE](LICENSE)).
