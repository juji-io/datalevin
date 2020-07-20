# Change Log
All notable changes to this project will be documented in this file. This change log follows the conventions of [keepachangelog.com](http://keepachangelog.com/).

## WIP
## Fixed
- avoid lock primitive
- create all parent directories if necessary

## 0.2.3
## Fixed
- long out of range error during native compile 

## 0.2.2
## Changed
- apply `query/join-tuples` optimization https://github.com/tonsky/datascript/pull/203
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
