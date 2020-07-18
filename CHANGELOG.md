# Change Log
All notable changes to this project will be documented in this file. This change log follows the conventions of [keepachangelog.com](http://keepachangelog.com/).

## WIP
## Changed
- merge join-tuples optimization https://github.com/tonsky/datascript/pull/203

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
