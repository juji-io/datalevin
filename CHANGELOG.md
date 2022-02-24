# Change Log

## WIP
###  Fixed
- Serialized writes for concurrent transactions, #83
### Added
- `defpodfn` macro to define a query function that can be used in babashka pod, #85

## 0.5.29
### Fixed
- Update `max-aid` after schema update (thx @den1k)

## 0.5.28
### Improved
- Updated dependencies, particularly fixed sci version (thx @borkdude)

## 0.5.27
### Fixed
- occasional LMDB crash during multiple threads write or abrupt exit

## 0.5.26
### Improved
- Update graalvm version
### Fixed
- Exception handling in copy

## 0.5.24
### Fixed
- Handle scalar result in remote queries
### Improved
- Server asks client to reconnect if the server is restarted and client
  reconnects automatically when doing next request

## 0.5.23
### Improved
- Bump versions of all dependency

## 0.5.22
### Improved
- More robust handling of abrupt network disconnections
- Automatically maintain the required number of open connections, #68
### Added
- Options to specify the number of connections in the client connection pool
  and to set the time-out for server requests

## 0.4.44
### Fixed
- Backport the dump/load fix from 0.5.20

## 0.5.20
### Fixed
- Dumping/loading Datalog store handles raw bytes correctly

## 0.5.19
### Improved
- Remove client immediately when `disconnect` message is received, clean up
  resources afterwards, so a logically correct number of clients can be obtained
  in the next API call on slow machines.

## 0.5.18
### Fixed
- Occasional server message write corruptions in busy network traffic on Linux.

## 0.5.17
### Added
- JVM uberjar release for download
### Changed
- JVM library is now Java 8 compatible, #69
### Improved
- Auto switch to local transaction preparation if something is wrong with remote
  preparation (e.g. problem with serialization)

## 0.5.16
### Improved
- Do most of transaction data preparation remotely to reduce traffic
### Fixed
- Handle entity serialization, fix #66

## 0.5.15
### Changed
- Allow a single client to have multiple open databases at the same time
- Client does not open db implicitly, user needs to open db explicitly
### Fixed
- New `create-conn` should override the old, fix #65

## 0.5.14
### Added
- `DTLV_LIB_EXTRACT_DIR` environment variable to allow customization of native
  libraries extraction location.
### Improved
- Use clj-easy/graal-build-time, in anticipation of GraalVM 22.

## 0.5.13

### Improved
- Better robust jar layout for `org.clojars.huahaiy/datalevin-native`

## 0.5.10
### Added
- Release artifact `org.clojars.huahaiy/datalevin-native` on clojars, for
  depending on Datalevin while compiling GraalVM native image. User
  no longer needs to manually compile Datalevin C libraries.

## 0.5.9
### Improved
- Only check to refersh db cache at user facing namespaces, so internal db
  calls work with a consistent db view
- Replace unnecessary expensive calls such as `db/-search` or `db/-datoms` with
  cheaper calls to improve remote store access speed.
- documentation

## 0.5.8

### Improved
- More robust build

## 0.5.5

### Improved
- Wrap all LMDB flags as keywords

## 0.5.4
### Fixed
- Don't do AOT in library, to avoid deps error due to exclusion of graal
### Improved
- Expose all LMDB flags in JVM version of kv store

## 0.5.3
### Added
- Transparent networked client/server mode with role based access control. #46
  and #61
- `dtlv exec` takes input from stdin when no argument is given.
### Improved
- When open db, throw exception without proper file permission

## 0.4.40
### Added
- Transactable entity [Thanks @den1k, #48]
- `clear` function to clear Datalog db

## 0.4.35
### Fixed
- Native uses the same version of LMDB as JVM, #58

## 0.4.32
### Improved
- Remove GraalVM and dtlv specific deps from JVM library jar
- Update deps

## 0.4.31
### Improved
- More robust dependency management
### Fixed
- Replacing giant values, this requires Java 11 [#56]

## 0.4.30
### Fixed
- Transaction of multiple instances of bytes [#52, Thanks @den1k]
- More reflection config in dtlv
- Benchmark deps

## 0.4.29
### Fixed
- Correct handling of rule clauses in dtlv
- Documentation clarification that we do not support "db as a value"

## 0.4.28
### Added
- Datafy/nav for entity [Thanks @den1k]
- Some datom convenience functions, e.g. `datom-eav`, `datom-e`, etc.
### Changed
- Talk to Babashka pods client in transit+json

## 0.4.27
### Added
- Exposed more functions to Babashka pod

## 0.4.26
### Added
- Native Datalevin can now work as a Babashka pod

## 0.4.23
### Added
- Compile to native on Windows and handle Windows path correctly
- `close-db` convenience function to close a Datalog db

## 0.4.21
### Changed
- Compile to Java 8 bytecode instead of 11 to have wider compatibility
- Use UTF-8 throughout for character encoding

## 0.4.20
### Improved
- Improve dtlv REPL (doc f) display

## 0.4.19
### Improved
- Provide Datalevin C source as a zip to help compiling native Datalevin dependency

## 0.4.17
### Improved
- Minor improvement on the command line tool

## 0.4.16
### Changed
- Native image now bundles LMDB

## 0.4.13
### Fixed
- Handle list form in query properly in command line shell [#42]

## 0.4.4
### Changed
- Consolidated all user facing functions to `datalevin.core`, so users don't have to understand and require different namespaces in order to use all features.

## 0.4.0
### Changed
- [**Breaking**] Removed AEV index, as it is not used in query. This reduces storage
  and improves write speed.
- [**Breaking**] Change VAE index to VEA, in preparation for new query engine. Now
  all indices have the same order, just rotated, so merge join is more likely.
- [**Breaking**] Change `open-lmdb` and `close-lmdb` to `open-kv` and `close-kv`,
  `lmdb/transact` to `lmdb/transact-kv`, so they are consistent, easier to
  remember, and distinct from functions in `datalevin.core`.

### Added
- GraalVM native image specific LMDB wrapper. This wrapper allocates buffer
  memory in C and uses our own C comparator instead of doing these work in Java,
  so it is faster.
- Native command line shell, `dtlv`

## 0.3.17
### Changed
- Improve Java interop call performance

## 0.3.16
### Changed
- Allow Java interop calls in where clauses, e.g. `[(.getTime ?date) ?timestamp]`, `[(.after ?date1 ?date2)]`, where the date variables are `:db.type/instance`. [#32]

## 0.3.15
### Changed
- Changed default LMDB write behavior to use writable memory map and
  asynchronous msync, significantly improved write speed for small transactions
  (240X improvement for writing one datom at a time).

## 0.3.14
### Fixed
- Read `:db.type/instant` value as `java.util.Date`, not as `long` [#30]

## 0.3.13
### Fixed
- Fixed error when transacting different data types for an untyped attribute [#28, thx @den1k]

## 0.3.12
### Fixed
- proper exception handling in `lmdb/open-lmdb`

## 0.3.11
### Fixed
- Fixed schema update when reloading data from disk

## 0.3.10
### Fixed
- Fixed `core/get-conn` schema update

## 0.3.9
### Changed
- Remove unnecessary locks in read transaction
- Improved error message and documentation for managing LMDB connection
### Added
- `core/get-conn` and `core/with-conn`

## 0.3.8
### Fixed
-  Correctly handle `init-max-eid` for large values as well.

## 0.3.7
### Fixed
- Fixed regression introduced by 0.3.6, where :ignore value was not considered [#25]

## 0.3.6
### Fixed
- Add headers to key-value store keys, so that range queries work in mixed data tables

## 0.3.5
### Changed
- Expose all data types to key-value store API [#24]

## 0.3.4
### Fixed
- thaw error for large values of `:data` type. [#23]
### Changed
- portable temporary directory. [#20, thx @joinr]

## 0.3.3
### Fixed
- Properly initialize max-eid in `core/empty-db`

## 0.3.2
### Changed
- Add value type for `:db/ident` in implicit schema

## 0.3.1
### Changed
- [**Breaking**] Change argument order of `core/create-conn`, `db/empty-db`
  etc., and put `dir` in front, since it is more likely to be specified than
  `schema` in real use, so users don't have to put `nil` for `schema`.

## 0.2.19
### Fixed
- correct `core/update-schema`

## 0.2.18

### Fixed
- correctly handle `false` value as `:data`
- always clear buffer before putting data in

## 0.2.17

### Fixed
- thaw exception when fetching large values

## 0.2.16

### Changed
- clearer error messages for byte buffer overflow

## 0.2.15

### Fixed
- correct schema update

## 0.2.14

### Added
- `core/schema` and `core/update-schema`

## 0.2.13
### Added
- `core/closed?`

## 0.2.12
### Fixed
- `db/entid` allows 0 as eid

## 0.2.11
### Fixed
- fix test

## 0.2.10

### Fixed
- correct results when there are more than 8 clauses
- correct query result size

## 0.2.9
### Changed
- automatically re-order simple where clauses according to the sizes of result sets
- change system dbi names to avoid potential collisions

### Fixed
- miss function keywords in cache keys

## 0.2.8
### Added
- hash-join optimization [submitted PR #362 to Datascript](https://github.com/tonsky/datascript/pull/362)
- caching DB query results, significant query speed improvement

## 0.2.7
### Fixed
- fix invalid reuse of reader locktable slot [#7](https://github.com/juji-io/datalevin/issues/7)
### Changed
- remove MDB_NOTLS flag to gain significant small writes speed

## 0.2.6
### Fixed
- update existing schema instead of creating new ones

## 0.2.5
### Fixed
- Reset transaction after getting entries
- Only use 24 reader slots

## 0.2.4
### Fixed
- avoid locking primitive [#5](https://github.com/juji-io/datalevin/issues/5)
- create all parent directories if necessary

## 0.2.3
### Fixed
- long out of range error during native compile

## 0.2.2
### Changed
- apply [query/join-tuples optimization](https://github.com/tonsky/datascript/pull/203)
- use array get wherenever we can in query, saw significant improvement in some queries.
- use `db/-first` instead of `(first (db/-datom ..))`, `db/-populated?` instead of `(not-empty (db/-datoms ..)`, as they do not realize the results hence faster.
- storage test improvements

## 0.2.1
### Changed
- use only half of the reader slots, so other processes may read

### Added
- add an arity for `bits/read-buffer` and `bits/put-buffer`
- add `lmdb/closed?`, `lmdb/clear-dbi`, and `lmdb/drop-dbi`

## 0.2.0
### Added
- code samples
- API doc
- `core/close`

## 0.1.0
### Added
- Port datascript 0.18.13
