# DataLightning

A port of [Datascript](https://github.com/tonsky/datascript) in-memory database and Datalog query
engine on top of [Lightning Memory-Mapped Data Manager
(LMDB)](https://en.wikipedia.org/wiki/Lightning_Memory-Mapped_Database).

Similar to Datascript, Datalightling is a library, meant to be embedded in applications to
manage state. However, the data is persistent on disk, so the state survives
application restarts and the data size can be larger than memory. LMDB's lightweight and crash free design together with its fast read performance
is well suited as the durable storage for Datalightning.

Datalightning currently only supports Clojure, but we plan to add ClojureScript and JS support
in the future as node.js bindings for LMDB exist.

## Usage

FIXME


## License

Copyright © 2020 Juji Inc.

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
