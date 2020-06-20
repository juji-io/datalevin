# Datalevin

A port of [Datascript](https://github.com/tonsky/datascript) in-memory database and Datalog query
engine to work on top of [Lightning Memory-Mapped Database
(LMDB)](https://en.wikipedia.org/wiki/Lightning_Memory-Mapped_Database). 

Datalevin retains the library property of Datascript, so you can include it in your Clojure code just like any other dependency. Unlike Datomic, Crux or Datehike, Datalevin is not intended be a standalone database server that one needs to run separately and communicate with over networks.  Datalevin relies on LMDB's lightweight and robust transactional database design and leverages its high performance for read intensive workload. As such, it does not support pluggable storage backends. Datalevin is meant to be embedded in applications to manage state. Because data is persistent on disk in Datalevin, application state can survive application restarts and the allowed data size can be larger than memory. 

## Usage

FIXME

## Difference from Datascript

Besides the diffrence in data durability, Datalevin differs from Datascript in the following ways:

* Full data is stored only once, for the indices in Datalevin point to the datoms, but they are not datoms themselves, whereas Datascript stores full data in indices at least twice. 

* Internal integer ids are 64 bit long rather than 32 bit like in Datascript, so as to support a much larger dataset.

* Each datom is associated with an auto-incremented long id, reflecting the order in which datoms are added. Datoms are also stored in such an order, so one can think of this as the log index of Datomic.  

* Has VAET index. So Datalevin has five indices in total, similar to Datomic, whereas Datascript has three.  

* All attributes are put in all indices, therefore `:db/index` and `:db/unique` are unnecessary. Because data in Datalevin is compared bitwise, everything is comparable. So you can put in any data as value and they will be indexed lexicographically. For numbers to be sorted correctly, numeric attribute type needs to be specified, e.g.`:db.type/long`.

* Attribute name has length limitations: it cannot be longer than 400 characters.

* Total data size of the database can be larger than physical memory. It has the same limit as LMDB's, e.g. 128TB on a modern 64-bit machine that implements 48-bit address spaces.  It is also totally acceptable to store large values in Datalevin, as LMDB is optimized for reading large values.  

* Datalevin currently only supports Clojure on JVM, but adding support for other Clojure-hosting runtimes is possible in the future, since bindings for LMDB exist in almost all major languages and available on most platforms. However, Datalevin is unlikely to run in a browser, as it replies on the LMDB binary (written in C) to have local disk access.

## License

Copyright © 2020 Juji Inc.

Licensed under Eclipse Public License (see [LICENSE](LICENSE)).
