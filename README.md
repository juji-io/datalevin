# Datalevin

A port of [Datascript](https://github.com/tonsky/datascript) in-memory database and Datalog query
engine to work on top of [Lightning Memory-Mapped Database
(LMDB)](https://en.wikipedia.org/wiki/Lightning_Memory-Mapped_Database). 

Datalevin retains the library property of Datascript, so you can include it in your Clojure code just like any other dependency. Unlike Datomic, Crux or Datehike, Datalevin is not intended to be a standalone database server that one needs to run separately and communicate with over networks.  Datalevin relies on LMDB's lightweight and robust transactional database design and leverages its high performance for read intensive workload. As such, it does not support pluggable storage backends. Datalevin is meant to be embedded in applications to manage state. Because data is persistent on disk in Datalevin, application state can survive application restarts and the allowed data size can be larger than memory. 

## Usage

FIXME

## Difference from Datascript

Besides the diffrence in data durability, Datalevin differs from Datascript in the following ways:

* Internal integer ids are 64 bit long rather than 32 bit like in Datascript, so as to support a much larger dataset.

* Full data is stored only once in Datalevin, for the indices point to the datoms, but are not datoms themselves, whereas Datascript stores full data in indices at least twice. 

* Each datom is associated with an auto-incremented long id, reflecting the order in which datoms are added. Datoms are also stored in such an order, so one can think of this as the log index of Datomic.  

* Has VAET index. So Datalevin has five indices in total, similar to Datomic, whereas Datascript has three.  

* :db/valueType is respected like in Datomic. Because indices in Datalevin are compared bitwisely, for numerical values to be sorted correctly, :db/valueType must be specified, `:db.type/long`, `:db.type/float` or `:db.type/double`. 

* It is also totally acceptable to store large values in Datalevin, as LMDB is optimized for reading large values. Anything can be put in as value. If :db/valueType is unspecified, the value is serialized as Clojure data using nippy. Individual value size is only limited by available memory and JVM's ability to allocate off-heap buffers for them.

* Total data size of the database can be larger than physical memory. It has the same limit as LMDB's, e.g. 128TB on a modern 64-bit machine that implements 48-bit address spaces.  
* Attribute name has a length limit: it cannot has more than 511 bytes in the binary form.

* Datalevin currently only supports Clojure on JVM, but adding support for other Clojure-hosting runtimes is possible in the future, since bindings for LMDB exist in almost all major languages and available on most platforms. 

## License

Copyright © 2020 Juji Inc.

Licensed under Eclipse Public License (see [LICENSE](LICENSE)).
