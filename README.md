# DataLightning

A port of [Datascript](https://github.com/tonsky/datascript) in-memory database and Datalog query
engine to work on top of [Lightning Memory-Mapped Database
(LMDB)](https://en.wikipedia.org/wiki/Lightning_Memory-Mapped_Database). 

Datalightling retains the library property of Datascript, so you can include it in your code just like any dependency. It meant to be embedded in applications to
manage state. However, the data is persistent on disk, so the state survives application restarts and the data size can be larger than memory. 

Unlike Datomic, Crux or Datehike, Datalightning is not an standalone server that one needs to run separately and communicate with over networks. If so desired, you may put an API in front of Datalightning in your application. 

Datalighting leverages LMDB's lightweight and robust design as well as its high performance for read intensive workload. As such, it does not support pluggable storage backends. However, LMDB is widely available on all platforms.

Datalightning currently only supports Clojure on JVM, but adding support for other Clojure-hosting runtimes is possible, since bindings for LMDB exist in almost all major languages. 

## Usage

FIXME

## Difference from DataScript

Besides the diffrences in data durability and supported platforms, Datalightning differs from Datascript in the following ways:

* All attributes are put in the AVET index (i.e. `:db/index` is unnecessary). Because data in Datalightling is compared bitwise, everything is comparable. So you can put in any data as value and they will be indexed lexicographically. For numbers to be sorted correctly, attribute type needs to be specified: `:db.type/long`, `:db.type/double`, etc.

* Schema and other information about the database are stored in the database, available for query.

* Attribute name has length limitation, 468 bytes.

* Maximial value size needs to be declared when creating the database.


## License

Copyright © 2020 Juji Inc.

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

