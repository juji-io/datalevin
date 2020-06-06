# DataLightning

A port of [Datascript](https://github.com/tonsky/datascript) in-memory database and Datalog query
engine to work on top of [Lightning Memory-Mapped Data Manager
(LMDB)](https://en.wikipedia.org/wiki/Lightning_Memory-Mapped_Database). 

Similar to Datascript, Datalightling is a library, so you can include it in your code just like any other Clojure library. It meant to be embedded in applications to
manage state. However, the data is persistent on disk, so the state survives application restarts and the data size can be larger than memory. 

Unlike Datomic, Crux or Datehike, Datalightning is not an standalone server that one needs to run separately and communicate with over networks. If so desired, you may put an API interface in front of Datalightning in your application. 

Datalighting leverages LMDB's lightweight and crash free design as well as its high performance for read intensive workload. As such, it does not support pluggable storage backends. However, LMDB is widely available on all platforms.

Datalightning currently only supports Clojure on JVM, but adding support for other Clojure-hosting runtimes should be easy, since bindings for LMDB exist in almost all major languages. 

## Usage

FIXME


## License

Copyright © 2020 Juji Inc.

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

