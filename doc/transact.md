# Transactions in Datalevin

Datalevin relies on the transaction mechanism of the underlying key-value store,
LMDB to achieve ACID.

In LMDB, read and write are independent and do not block each other. Read requires a read transaction. Write requires a read-write transaction. These are normally two different transactions.

Writes are serialized, only one thread can write at a time. When multiple threads write concurrently to the same key, whoever writes later wins eventually, because writes are serialized. The first write succeeds, but the value will then be overwritten by the second write.

Reads can be concurrent. Basically each reader thread reads its own view of the
data created at the moment the read transaction starts. When read is concurrent
with write, the newly written values are invisible to the reader, because read
transaction sees a view of the database that is consistent and up to the time
when the read transaction starts, which is before the write transaction commits.

LMDB suggests that:

> Avoid long-lived transactions. Read transactions prevent reuse of pages freed by newer write transactions, thus the database can grow quickly. Write transactions prevent other write transactions, since writes are serialized.

## Explicit Transaction

To obtain features such as compare-and-swap semantics, that is, a group of reads and writes are treated as a single atomic action, Datalevin introduced explicit transaction.

For key-value API, `with-transaction-kv` macro is used for explicit transaction.
`with-transaction` macro is used for Datalog API. Basically, the code in the
body of the macros run inside a single read/write transaction with a single thread. These work the same in all modes of Datalevin: embedded, client/server, or babashka pod.

For example,

Rollback can be done with `abort-transact-kv` and `abort-transact`.

## Bulk Load Data into Datalog Store

### By Transaction

The most straightforward method of transacting one map at a time using
`transact!` works well, as
Datalevin by default uses asynchronous commit, so the overhead of many commits is
not significant.

To avoid this small overhead, one can wrap these `transact!`
calls inside a single transaction by using `with-transaction`, a slight speed up
can be achieved, so this is the recommended approach.

Because Datalevin supports only a single write thread at a time, parallel
transactions actually slow writes down significantly due to the thread switching
overhead.

Transacting a large number of maps in a single `transact!` call, though slightly faster,
tends to blow up the size of the resulting data file, due to the fact that LMDB
 has less chance to reuse discarded pages. Wrapping it in `with-transaction`
 solve the size problem, but can be significantly slower, because when memory map
 resizes, `with-transaction` has to rebuild a large transaction context.


### By `init-db`
