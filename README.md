# Datalevin

A port of [Datascript](https://github.com/tonsky/datascript) in-memory database and Datalog query
engine to work on top of [Lightning Memory-Mapped Database (LMDB)](https://en.wikipedia.org/wiki/Lightning_Memory-Mapped_Database). 

The rationale for Datalevin is to provide a simple and free Datalog engine running on durable storage.  It is my observation that many developers prefer the flavor of Datalog populized by Datomic over any flavor of SQL, once they get to know it. However, Datomic is an enterprise grade software and its feature sets may be an overkill for many use cases. One thing that often confuses casual users is the temporal feature of Datomic, so to keep things simple, Datalevin does not keep transaction history.

Datalevin also retains the library property of Datascript, and it is meant to be embedded in applications to manage state. Because data is persistent on disk in Datalevin, application state can survive application restarts and the allowed data size can be larger than memory.  It is fine to store large values in Datalevin, as LMDB is optimized for reading large values. Effectively, anything can be put in as values. Individual value size is only limited by JVM's ability to allocate off-heap buffers for the value.

Datalevin relies on LMDB's robust transactional database design and leverages its high performance for read intensive workload. As such, it does not support pluggable storage backends.

## Usage

FIXME

## Difference from Datascript

In addition to the diffrence in data durability, Datalevin differs from Datascript in the following ways:

* Does not store transaction ids. Since history is not kept, there is no need to store transanction ids.

* Entity ids are 64 bits long, so as to support a much larger data size.  The total data size of a Datalevin database has the same limit as LMDB's, e.g. 128TB on a modern 64-bit machine that implements 48-bit address spaces.  

* Has an additional index that uses values as the primary key (VAE), similar to Datomic. 

* Respects `:db/valueType`. Currently, most Datomic value types are supported, except bigint, bigdec, uri and tuple. Values with unspecified type are treated as EDN blobs, and are de/serialized with nippy. Because values in Datalevin are compared bitwisely, for numerical values to be sorted correctly, `:db/valueType` must be specified: `:db.type/long`, `:db.type/float` or `:db.type/double`. 

* Attributes have internal integer ids, and attribute names have a length limitation: an attribute name cannot be more than 511 bytes long in binary.

* Datalevin currently only supports Clojure on JVM, but adding support for other Clojure-hosting runtimes is possible in the future, since bindings for LMDB exist in almost all major languages and available on most platforms. 

## License

Copyright © 2020 Juji Inc.

Licensed under Eclipse Public License (see [LICENSE](LICENSE)).
