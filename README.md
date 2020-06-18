# Datalevin

A port of [Datascript](https://github.com/tonsky/datascript) in-memory database and Datalog query
engine to work on top of [Lightning Memory-Mapped Database
(LMDB)](https://en.wikipedia.org/wiki/Lightning_Memory-Mapped_Database). 

Datalevin retains the library property of Datascript, so you can include it in your Clojure code just like any other dependency. Unlike Datomic, Crux or Datehike, Datalevin is not intent to be a standalone database server that one needs to run separately and communicate with over networks.  Datalevin relies on LMDB's lightweight and robust transactional database design and leverages its high performance for read intensive workload. As such, it does not support pluggable storage backends. Datalevin is meant to be embedded in applications to manage state. Because data is persistent on disk in Datalevin, application state can survive application restarts and the allowed data size can be larger than memory. 

## Usage

FIXME

## Difference from Datascript

Besides the diffrences in data durability, Datalevin differs from Datascript in the following ways:

* Data is stored only once, for the indices in Datalevin actually point to the datoms, whereas Datascript stores full data in indices at least twice. 

* Each datom has an auto increment long id, reflecting the order in which a datom is added. Datoms are stored in such an order, one can think of this as the log index of Datomic.  
* Has VAET index. So Datalevin has five indices, similar to Datomic.  

* All attributes are put in all indices, therefore `:db/index` and `:db/unique` are unnecessary. Because data in Datalevin is compared bitwise, everything is comparable. So you can put in any data as value and they will be indexed lexicographically. For numbers to be sorted correctly, numeric attribute type needs to be specified, e.g.`:db.type/long`.

* Attribute name has length limitations: it cannot be longer than 400 characters.

* It is fine to store large values in Datalevin. Maximal individual datom size can be specified when creating the database, otherwise the default maximual individual datom size is 16KB. The upper limit of the specified datom size is determined by the JVM's ability to allocate at least two off-heap buffers of that size, one for read and one for write. 

* Total data size of the database is the same as LMDB's limit: 128TB on a modern 64-bit machine that implements 48-bit address spaces.

* Datalevin currently only supports Clojure on JVM, but adding support for other Clojure-hosting runtimes is possible, since bindings for LMDB exist in almost all major languages and available on most platforms. However, Datalevin is unlikely to run in a browser, as it replies on the LMDB binary to have disk access.

## License

Copyright © 2020 Juji Inc.

Licensed under Eclipse Public License (see [LICENSE](LICENSE)).
