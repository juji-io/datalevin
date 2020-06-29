# Datalevin

A port of [Datascript](https://github.com/tonsky/datascript) in-memory database and Datalog query
engine to work on top of [Lightning Memory-Mapped Database (LMDB)](https://en.wikipedia.org/wiki/Lightning_Memory-Mapped_Database).

The rationale for Datalevin is to provide a simple and free Datalog engine running on durable storage.  It is my observation that many developers prefer the flavor of Datalog populized by [Datomic](https://datomic.com) over any flavor of SQL, once they get to use it. However, Datomic is an enterprise grade software and its feature sets may be an overkill for many use cases. One thing that often confuses novice users is the temporal feature of Datomic. To keep things simple and familiar (yes, this combination exists), Datalevin does not keep transaction history, and behaves the same way as most other databases: when data are deleted, they are gone.

Datalevin retains the library property of Datascript, and it is meant to be embedded in applications to manage state. Because data is persistent on disk in Datalevin, application state can survive application restarts and data size can be larger than memory.  It is also totally fine to store large values in Datalevin, as LMDB is optimized for reading large values. Effectively, anything can be put in as values. The maximum individual value size is 4GB. In practice, it is limited by LMDB's ability to find large enough continous space on disk for the value and JVM's ability to allocate off-heap buffers for it.

LMDB is a battle tested data backend used in [many projects](https://symas.com/lmdb/technical/#projects). For example, LMDB powers [Cloadflare](https://blog.cloudflare.com/introducing-quicksilver-configuration-distribution-at-internet-scale/) global configuration distribution. Datalevin relies on LMDB's robust transactional database design and leverages its high performance for read intensive workload. As such, Datalevin does not support pluggable storage backend.

## Usage

## Difference from Datascript

In addition to the diffrence in data durability, Datalevin differs from Datascript in the following ways:

* Does not store transaction ids. Since history is not kept, there is no need to store transanction ids.

* Entity ids are 64 bits long, so as to support a much larger data size.  The total data size of a Datalevin database has the same limit as LMDB's, e.g. 128TB on a modern 64-bit machine that implements 48-bit address spaces.

* Has an additional index that uses values as the primary key (VAE), similar to Datomic.

* Indices respects `:db/valueType`. Currently, most Datomic value types are supported, except bigint, bigdec, uri and tuple. Values with unspecified type are treated as EDN blobs, and are de/serialized with nippy. Because values in Datalevin are compared bitwisely, for range queries to work correctly on an attribute, its `:db/valueType` should be specified.

* Attributes have internal integer ids, and attribute names have a length limitation: an attribute name cannot be more than 511 bytes long in binary.

* Handles schema migrations with `swap-attr` function. It only allows safe migration that does not alter existing data (e.g. one to many cardinaity, unique to non-unique, index to non-index and vice vesa), and refuses unsafe schema changes (e.g. many to one cardinality, non-unique to unique, value type changes) that are inconsistent with existing data.

* Datalevin currently only supports Clojure on JVM, but adding support for other Clojure-hosting runtimes is possible in the future, since bindings for LMDB exist in almost all major languages and available on most platforms.

## Alternatives

Assuming that you are interested in using Datalog, here are your options:

* If you need a simple durable store with a battle tested backend, give Datalevin a try.

* If you need a in-memory store, e.g. for single page applications running in a browser, Datascript is for you.

* If you need time travel and rich features backed by the authors of Clojure, you should use Datomic.

* If you need features such as bitemporal graph queries, You may try [Crux](https://github.com/juxt/crux).

* If you don't mind experimental storage backend, you may try [Datahike](https://github.com/replikativ/datahike).

## License

Copyright © 2020 Juji Inc.

Licensed under Eclipse Public License (see [LICENSE](LICENSE)).
