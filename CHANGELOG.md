# Change Log

## WIP
### Improved
- [Test] Fix windows tests.

## 0.8.17 (2023-07-02)
### Added
- [main] Added an `--nippy` option to dump/load database in nippy binary
  format, which handles some data anomalies, e.g. keywords with space in
  them, non-printable data, etc., and produces smaller dump file, #216
### Improved
- [KV] More robust bigdec data type encoding on more platforms
- [All] Create a backup db directory `dtlv-re-index-<unix-timestamp>` inside the
  system temp directory when `re-index`, #213
- [Search] Graceful avoidance of proximity scoring when positions are not indexed

## 0.8.16 (2023-05-10)
### Improved
- Remove Clojure 1.11 features to accommodate older Clojure

## 0.8.15 (2023-05-08)
### Added
- [Search] Consider term proximity in relevance when `:index-position?` search
  engine option is `true`. #203
- [Search] `:proximity-expansion` search option (default `2`) can be used to
  adjust the search quality vs. time trade-off: the bigger the number, the
  higher is the quality, but the longer is the search time.
- [Search] `:proximity-max-dist` search option (default `45`) can be used to
  control the maximal distance between terms that would still be considered as
  belonging to the same span.
- [Search] `create-stemming-token-filter` function to create stemmers, which
  uses Snowball stemming library that supports many languages. #209
- [Search] `create-stop-words-token-filter` function to take a customized stop
  words predicate.
- [KV, Datalog, Search] `re-index` function that dump and load data with new
  settings. Should only be called when no other threads or programs are
  accessing the database. #179
### Fixed
- [KV] More strict type check for transaction data, throw when transacting
  un-thawable data. #208
### Changed
- [Main] Remove `*datalevin-data-readers*` dynamic var, use Clojure's
  `*data-readers*` instead.

## 0.8.14 (2023-04-28)
### Fixed
- [Native] Rollback GraalVM to 22.3.1, as 22.3.2 is missing apple silicon.

## 0.8.13 (2023-04-28)
### Fixed
- [Datalog] Unexpected heap growth due to caching error. #204
- [Datalog] More cases of map size reached errors during transaction. #196
- [Datalog] Existing datoms still appear in `:tx-data` when unchanged. #207

### Improved
- [Datalog] Disable cache during transaction, save memory and avoid disrupting
  concurrent write processes.
- [Native] upgrade GraalVM to 22.3.2
- [Lib] update deps.

## 0.8.12 (2023-04-03)
### Fixed
- [KV] When `open-kv`, don't grow `:mapsize` unnecessarily.
- [Server] automatically reopen DBs for a client that is previously removed from
  the server.

## 0.8.11 (2023-04-02)
### Added
- [Search] `:include-text?` option to store original text. #178.
- [Search] `:texts` and `:texts+offsets` keys to `:display` option of `search`
  function, to return original text in search results.
### Improved
- [Main] more robust `dump` and `load` of Datalog DB on Windows.

## 0.8.10 (2023-04-01)
### Added
- [KV] `:max-readers` option to specify the maximal number of concurrent readers
  allowed for the db file. Default is 126.
- [KV] `max-dbs` option to specify the maximal number of sub-databases (DBI)
  allowed for the db file. Default is 128. It may induce slowness if too big a
  number of DBIs are created, as a linear scan is used to look up a DBI.
### Fixed
- [Datalog] `clear` after db is resized.

## 0.8.9 (2023-03-28)
### Fixed
- [KV] transacting data more than one order of magnitude larger than the
  initial map size in one transaction. #196

## 0.8.8 (2023-03-21)
### Improved
- [Pod] serialize TxReport to regular map. #190
### Fixed
- [Server] migrate old sessions that do not have `:last-active`.

## 0.8.7 (2023-03-21)
### Added
- [Datalog] `datalog-index-cache-limit` function to get/set the limit of Datalog
  index cache. Helpful to disable cache when bulk transacting data. #195
- [Server] `:idle-timeout` option when creating the server, in ms, default is 24
  hours. #122
### Fixed
- [Datalog] error when Clojure collections are used as lookup refs. #194

## 0.8.6 (2023-03-10)
### Fixed
- [Datalog] correctly handle retracting then transacting the same datom in the
  same transaction. #192
- [Datalog] error deleting entities that were previously transacted as part of
  some EDN data. #191.
### Improved
- [Lib] update deps.

## 0.8.5 (2023-02-13)
### Added
- [KV] added tuple data type that accepts a vector of scalar values. This
  supports range queries, i.e. having expected ordering by first element, then
  second element, and so on. This is useful, for example, as path keys for
  indexing content inside documents. When used in keys, the same 511 bytes
  limitation applies.
- [Datalog] added heterogeneous tuple `:db/tupleTypes` and homogeneous tuples
  `:db/tupleType` type. Unlike Datomic, the number of elements in a tuple are
  not limited to 8, as long as they fit inside a 496 bytes buffer. In addition,
  instead of using `nil` to indicate minimal value like in Datomic, one can use
  `:db.value/sysMin` or `:db.value/sysMax` to indicate minimal or maximal
  values, useful for range queries. #167
 - [Main] dynamic var `*datalevin-data-readers*` to support loading custom tag
   literals. (thx @respatialized)
### Fixed
- [Main] dump and load big integers.
### Improved
- [Datalog] avoid unnecessary caching, improve transaction speed up to 25% for
large transactions.
- [Native] upgrade Graalvm to 22.3.1
- [Native] static build on Linux. #185
- [Lib] update deps.

## 0.8.4 (2023-01-20)
### Fixed
- [Datalog] error when large `:db/fulltext` value is added then removed in the same transaction.

## 0.8.2 (2023-01-19)
### Fixed
- [Search] `search-utils/create-ngram-token-filter` now works. #164
- [Datalog] large datom value may throw off full-text indexing. #177

## 0.8.1 (2023-01-19)
### Fixed
- [Datalog] intermittent `:db/fulltext` values transaction error. #177

## 0.8.0 (2023-01-19)
### Changed
- [Search] **Breaking** search index storage format change. Data re-indexing is
  necessary.
### Improved
- [Search] significant indexing speed and space usage improvement: for default
  setting, 5X faster bulk load speed; 2 orders of magnitude faster
  `remove-doc` and 10X disk space reduction; when term positions and offsets are
  indexed: 3X faster bulk load and 40 percent space reduction.
- [Search] added caching for term and document index access, resulting in 5
  percent query speed improvement on average, 35 percent improvement at median.
### Added
- [Search] `:index-position?` option to indicate whether to record term
  positions inside documents, default `false`.
- [Search] `:check-exist?` argument to `add-doc`indicate whether to check the
  existence of the document in the index, default `true`. Set it to `false` when
  importing data to improve ingestion speed.
### Fixed
- [Datalog] increasing indexing time problem for `:db/fulltext` values. #151
- [Search] error when indexing huge documents.
- [KV] spillable results exception in certain cases.
### Removed
- [Search] `doc-refs` function.
- [Search] `search-index-writer` as well as related `write` and
  `commit`functions for client/server, as it makes little sense to bulk load
  documents across network.

## 0.7.12 (2023-01-11)
### Fixed
- [Native] allow native compilation on apple silicon
- [Datalog] db print-method. (thx @den1k)

## 0.7.11 (2023-01-09)
### Fixed
- [Datalog] intermittent concurrent transaction problems

## 0.7.10 (2023-01-09)
### Fixed
- [CI] adjust CI workflow for the latest Graalvm

## 0.7.9 (2023-01-09)
### Improved
- [Datalog] moved entity and transaction ids from 32 bits to 64 bits integers, supporting much larger DB. #144
- [Datalog] wrapped `transact!` inside `with-transaction` to ensure ACID and improved performance
- [Native] updated to the latest Graalvm 22.3.0. #174

## 0.7.8 (2023-01-04)
### Fixed
- [KV] `get-range` regression when results are used in `sequence`. #172
### Improved
- [Datalog] Ported all applicable Datascript improvements since 0.8.13 up to now
  (1.4.0). Notably, added composite tuples feature, new pull implementation,
  many bug fixes and performance improvements. #3, #57, #168
- bump deps

## 0.7.6 (2022-12-16)
### Fixed
- [Server] error when granting permission to a db created by `create-database`
  instead of being created by opening a connection URI
### Improved
- [Datalog] avoid printing all datoms when print a db
- [Doc] clarify that `db-name` is unique on the server. (thx @dvingo)

## 0.7.5 (2022-12-16)
### Improved
- avoid `(random-uuid)`, since not every one is on Clojure 1.11 yet.

## 0.7.4 (2022-12-15)
### Fixed
- typo prevent build on CI

## 0.7.3 (2022-12-15)
### Fixed
- [KV] spill test that prevents tests on MacOS CI from succeeding.

## 0.7.2 (2022-12-15)
### Fixed
- [KV] broken deleteOnExit for temporary files

## 0.7.1 (2022-12-15)
### Improved
- [KV] clean up spill files

## 0.7.0
### Added
- [Platform] embedded library support for Apple Silicon.
- [KV] A new range function `range-seq` that has similar signature as
  `get-range`, but returns a `Seqable`, which lazily reads data items into
  memory in batches (controlled by `:batch-size` option). It should be used
  inside `with-open` for proper cleanup. #108
- [KV] The existent eager range functions, `get-range` and `range-filter`, now
  automatically spill to disk when memory pressure is high. The results, though
  mutable, still implement `IPersistentVector`, so there is no API level
  change. The spill-to-disk behavior is controlled by `spill-opts` option map
  when opening the db, allowing `:spill-threshold` and `:spill-root` options.
### Improved
- [KV] write performance improvement
### Changed
- [KV] Upgrade LMDB to 0.9.29

## 0.6.29
### Added
- [Client] `:client-opts` option map that is passed to the client when opening remote databases.
### Fixed
- [KV] `with-transaction-kv` does not drop prior data when DB is resizing.
- [Datalog] `with-transaction` does not drop prior data when DB is resizing.

## 0.6.28
### Improved
- [Native] Add github action runner image ubuntu-20.04 to avoid using too new a
  glibc version (2.32) that does not exist on most people's machines.

## 0.6.27
### Fixed
- [KV] `with-transaction-kv` does not crash when DB is resizing.

## 0.6.26
### Added
- [KV] `with-transaction-kv` macro to expose explicit transactions for KV
  database. This allows arbitrary code within a transaction to achieve
  atomicity, e.g. to implement compare-and-swap semantics, etc, #110
- [Datalog] `with-transaction` macro, the same as the above for Datalog database
- [KV]`abort-transact-kv` function to rollback writes from within an explicit KV transaction.
- [Datalog] `abort-transact` function, same for Datalog transaction.
- [Pod] Missing `visit` function
### Improved
- [Server] Smaller memory footprint
- bump deps

## 0.6.22
### Added
- [Datalog] `fulltext-datoms` function that return datoms found by full
  text search query, #157
### Fixed
- [Search] Don't throw for blank search query, return `nil` instead, #158
- [Datalog] Correctly handle transacting empty string as a full text value, #159
### Improved
- Datalevin is now usable in deps.edn, #98 (thx @ieugen)

## 0.6.21
### Fixed
- [Datalog] Caching issue introduced in 0.6.20 (thx @cgrand)

## 0.6.20
### Added
- [Pod] `entity` and `touch` function to babashka pod, these return regular
  maps, as the `Entity` type does not exist in a babashka script. #148 (thx
  @ngrunwald)
- [Datalog] `:timeout` option to terminate on deadline for query/pull. #150 (thx
  @cgrand).

## 0.6.19
### Changed
- [Datalog] Entity equality requires DB identity in order to better support
  reactive applications, #146 (thx @den1k)
### Improved
- bump deps

## 0.6.18
### Fixed
- [Search] corner case of search in document collection containing only one term, #143
- [Datalog] entity IDs has smaller than expected range, now they cover full 32 bit integer range, #140
### Added
- [Datalog] Persistent `max-tx`, #142

## 0.6.17
### Added
- [Datalog] `tx-data->simulated-report` to obtain a transaction report without actually persisting the changes. (thx @TheExGenesis)
- [KV] Support `:bigint` and `:bigdec` data types, corresponding to
  `java.math.BigInteger` and `java.math.BigDecimal`, respectively.
- [Datalog] Support `:db.type/bigdec` and `:db.type/bigint`, correspondingly, #138.
### Improved
 - Better documentation so that cljdoc can build successfully. (thx @lread)

## 0.6.16
### Added
- [Datalog] Additional arity to `update-schema` to allow renaming attributes. #131
- [Search] `clear-docs` function to wipe out search index, as it might be faster
  to rebuild search index than updating individual documents sometimes. #132
- `datalevin.constants/*data-serializable-classes*` dynamic var, which can be
  used for `binding` if additional Java classes are to be serialized as part of
  the default `:data` data type. #134
### Improved
- [Datalog] Allow passing option map as `:kv-opts` to underlying KV store when `create-conn`
- bump deps
### Fixed
- [Datalog] `clear` function on server. #133
### Changed
- [Datalog] Changed `:search-engine` option map key to `:search-opts` for consistency [**Breaking**]

## 0.6.15
### Improved
- [Search] Handle empty documents
- [Datalog] Handle safe schema migration, #1, #128
- bump deps
### Added
- [KV] visitor function for `visit` can return a special value `:datalevin/terminate-visit` to stop the visit.

## 0.6.14
### Fixed
- Fixed adding created-at schema item for upgrading Datalog DB from prior 0.6.4 (thx @jdf-id-au)
### Changed
- [**breaking**] Simplified `open-dbi` signature to take an option map instead
### Added
- `:validate-data?` option for `open-dbi`, `create-conn` etc., #121

## 0.6.13
### Fixed
- Schema update regression. #124
### Added
- `:domain` option to `new-search-engine`, so multiple search engines can
  coexist in the same `dir`, each with its own domain, a string. #112

## 0.6.12
### Fixed
- Server failure to update max-eid regression, #123
### Added
- Added an arity to `update-schema` to allow removal of attributes if they are
  not associated with any datoms, #99

## 0.6.11
### Fixed
- Search add-doc error when alter existing docs

## 0.6.10
### Improved
- Persistent server session that survives restarts without affecting clients, #119
- More robust server error handling

## 0.6.9
### Fixed
- Query cache memory leak, #118 (thx @panterarocks49)
- Entity retraction not removing `:db/updated-at` datom, #113
### Added
- `datalevin.search-utils` namespace with some utility functions to customize
  search, #105 (thx @ngrunwald)

## 0.6.8
## Fixed
- Add `visit` KV function to `core` name space
- Handle concurrent `add-doc` for the same doc ref
## Improved
- Bump deps

## 0.6.7
## Fixed
- Handle Datalog float data type, #88
### Improved
- Allow to use all classes in Babashka pods

## 0.6.6
### Improved
- Dump and load Datalog option map
- Dump and load `inter-fn`
- Dump and load regex
- Pass search engine option map to Datalog store

## 0.6.5
### Fixed
- Make configurable analyzer available to client/server

## 0.6.4
### Fixed
- Dot form Java interop regression in query, #103
### Added
- Option to pass an analyzer to search engine, #102
- `:auto-entity-time?` Datalog DB creation option, so entities can optionally have
  `:db/created-at` and `:db/updated-at` values added and maintained
  automatically by the system during transaction, #86
### Improved
- Dependency bump

## 0.6.3
### Added
- `doc-count` function returns the number of documents in the search index
- `doc-refs` function returns a seq of `doc-ref` in the search index
### Improved
- `datalevin.core/copy` function can copy Datalog database directly.

## 0.6.1
### Added
- `doc-indexed?` function
### Improved
- `add-doc` can update existing doc
- `open-kv` function allows LMDB flags, #100

## 0.6.0
### Added
- Built-in full-text search engine, #27
- Key-value database `visit` function to do arbitrary things upon seeing a
  value in a range

### Fixed
- [**breaking**]`:instant` handles dates before 1970 correctly, #94. The storage
  format of `:instant` type has been changed. For existing Datalog DB containing
  `:db.type/instant`, dumping as a Datalog DB using the old version of dtlv, then
  loading the data is required; For existing key-value DB containing `:instant`
  type, specify `:instant-pre-06` instead to read the data back in, then write
  them out as `:instant` to upgrade to the current format.

### Improved
- Improve read performance by adding a cursor pool and switch to a more
  lightweight transaction pool
- Dependency bump

## 0.5.31
### Fixed
- Create pod client side `defpodfn` so it works in non-JVM.
### Added
- `load-edn` for dtlv, useful for e.g. loading schema from a file, #101

## 0.5.30
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
- When open db, throw exception when lacking proper file permission

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
