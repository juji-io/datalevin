# Transactions in Datalevin

Datalevin relies on the transaction mechanism of the underlying key-value store,
LMDB to achieve ACID.

In LMDB, read and write are independent and do not block each other. Read
requires a read transaction. Write requires a read-write transaction. These are
normally two different transactions.

Writes are serialized, only one thread can write at a time. When multiple
threads write concurrently to the same key, whoever writes later wins
eventually, because writes are serialized. The first write succeeds, but the
value will then be overwritten by the second write.

Reads can be concurrent. Basically each reader thread reads its own view of the
data created at the moment the read transaction starts. When read is concurrent
with write, the newly written values are invisible to the reader, because read
transaction sees a view of the database that is consistent and up to the time
when the read transaction starts, which is before the write transaction commits.

LMDB suggests that:

> Avoid long-lived transactions. Read transactions prevent reuse of pages freed
> by newer write transactions, thus the database can grow quickly. Write
> transactions prevent other write transactions, since writes are serialized.

## Explicit Transaction

To obtain features such as compare-and-swap semantics, that is, a group of reads
and writes are treated as a single atomic action, Datalevin exposes explicit
transaction.

For key-value API, `with-transaction-kv` macro is used for explicit transaction.
`with-transaction` macro is used for Datalog API. Basically, all the code in the
body of the macros run inside a single read/write transaction with a single
thread. These work the same in all modes of Datalevin: embedded, client/server,
or babashka pod.

Rollback from within the transaction can be done with `abort-transact-kv` and
`abort-transact`.

## Bulk Load Data into Datalog Store

### By Transaction

The most straightforward method of transacting bulk data at a time using
`transact!` works quite well. `transact!` uses `with-transaction` internally.

Because Datalevin supports only a single write thread at a time, parallel
transactions actually slow writes down significantly due to the thread switching
overhead.

However, transacting Datalog data involves a great number of data transformation
and integrity checks, hence it can be slow. When initializing a DB with data, it
may not be necessary to pay the price of this overhead.

### By `init-db`

If it is possible, a much faster way of bulk loading data into an empty DB is to
directly load a list of prepared datoms using `init-db` function. However, it is
the caller's responsibility to ensure these datoms are correct because no check
is performed.

We do not support bulk loading prepared datoms into a DB that is not
empty, because it would be dangerous to bypass data integrity checks.
