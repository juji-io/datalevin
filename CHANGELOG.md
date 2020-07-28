# Change Log
All notable changes to this project will be documented in this file. This change log follows the conventions of [keepachangelog.com](http://keepachangelog.com/).

## 0.2.11
## Fixed
- fix test

## 0.2.10

## Fixed
- correct results when there are more than 8 clauses
- correct query result size 

## 0.2.9
## Changed
- automatically re-order simple where clauses according to the sizes of result sets
- change system dbi names to avoid potential collisions

## Fixed
- miss function keywords in cache keys

## 0.2.8
## Added
- hash-join optimization [submitted PR #362 to Datascript](https://github.com/tonsky/datascript/pull/362)
- caching DB query results, significant query speed improvement

## 0.2.7
## Fixed
- fix invalid reuse of reader locktable slot [#7](https://github.com/juji-io/datalevin/issues/7)
## Changed
- remove MDB_NOTLS flag to gain significant small writes speed

## 0.2.6
## Fixed
- update existing schema instead of creating new ones

## 0.2.5
## Fixed
- Reset transaction after getting entries
- Only use 24 reader slots

## 0.2.4
## Fixed
- avoid locking primitive [#5](https://github.com/juji-io/datalevin/issues/5)
- create all parent directories if necessary

## 0.2.3
## Fixed
- long out of range error during native compile

## 0.2.2
## Changed
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
