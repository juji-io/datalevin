# Write Benchmark (WIP)

The purpose of this benchmark is to plot the write throughput and latency under
various conditions in Datalevin. Hopefully, this gives users some reference data
points to help choosing the right transaction function and the right data batch
size for specific use cases.

We also compare Datalevin's Datalog store with Sqlite, as it is the most popular
embedded relational store.

## Setup

The benchmark is conducted on a 2016 Intel Core i7-6850K CPU @ 3.60GHz with 6
cores, 64GB RAM, 1TB SSD. The OS is Debian 12.9, kernel 6.1.0-28-amd64, running
OpenJDK version "17.0.13" 2024-10-15, and Clojure version is 1.12.0.

To avoid exhausting system resources, the number of asynchronous write requests
in flight is always capped at 1K using a Semaphore.

### Tasks

There are two tasks that are done sequentially.

#### Pure Writes

The first task writes a 8 bytes integer as key and a 36 bytes random UUID string
as value. In Datalevin, this means an entitiy of two attributes, one is a long,
marked as `:db.unique/identity`, and the other a string. In Sqlite, this is a
row of two fields, one is an integer `PRIMARY KEY`, and another a `TEXT`.

The pure write task is to write 1 million such data to an empty DB. The integers
are all even numbers between 1 and 2 millions, so the next task can have 50%
chance of hitting existing data initially.

Write data in batches generrally improve throughput, so we vary the batch sizes
in this task: 1, 10, 100, 1k, and 10k, to test the batching speed up effect.

#### Mixed Read/Write

With 1 million items in DB, we then do 2 million additional operations, with
1 million reads and 1 million writes. Read and write are interleaved. These
reads/writes are individual operations, not batched.

The read/write integers are random number between 1 and 2 millions. So initally
write has a 50% chance of being an addition and 50% chance of being an
overwrite. The chance of being an overwrite increases as more items are
written.

### Measures

For every 10K writes, a set of measures are recorded:

* Time (seconds), time since benchmark run starts.
* Throughput (writes/second), average throughput at the moment.
* Write Latency (milliseconds), average latency of transact function calls.
* Commit Latency (milliseconds), average latency of transaction commits.

The results are written into a CSV file.

### Run

Clojure command line is needed to run the benchmarks.

For example, the command below runs pure write benchmark for `transact-async`
with batch size 10, and save the results in `dl-async-10.csv`:

```bash
time clj -Xwrite :base-dir \"/tmp/dl/\" :batch 10 :f dl-async > dl-10-async.csv
```

This command runs mixed read/write benchmark following the pure write task above:

```bash
time clj -Xmixed :dir \"/tmp/dl/dl-async-10\" :f dl-async > dl-10-async-mixed.csv
```

The command below runs pure write benchmark for Sqlite `INSERT`  with batch size
1, and save the results in `sqlite-1.csv`

```bash
time clj -Xwrite :base-dir \"/tmp/sql/\" :batch 1 :f sql-tx > sqlite-1.csv
```

This command runs the read/write mixed task following the pure write above:

```bash
time clj -Xmixed :dir \"/tmp/sql/sqlite-1\" :f sql-tx > sqlite-1-mixed.csv
```

The total wall clock time, system time and user time are also reported.

## Datalog Transaction

Because Datalog store of Datalevin is intented to be an operational database, we
test the default durable write condition for Datalog in Datalevin.
Correspodingly, we test the default `PRAGMA synchronous=FULL` write condition in
Sqlite.

### Write Conditions

Datalevin has two Datalog transaction functions:

* `transact!`
* `transact-async`

Both are durable by default. In the case of `transact-async`, the returned
future is only realized after the data are flushed to disk.

`transact` is just the blocked version of `transact-async` so it is not tested.
There are two faster `init-db` and `fill-db` functions that directly load
prepared datoms and by-pass the expensive process of turning data into datoms.
These are not tested in this benchmark either, as we are only interested in
transactions of raw data.

### Results

### Remark

## Key Value Transaction

Datalevin wrap LMDB to offer KV store feature. Here we do not compare Datalevin
with other KV stores, as there are plenty of such comparison between LMDB and
others KV stores already.

### Write Conditions

Datalevin supports these transaction functions for key value store:

* `transact-kv`
* `transact-kv-async`

In addition to the default durable write condition, Datalevin supports some
faster, albeit less durable writes, by setting one of these env flags:

* `:nometasync`
* `:nosync`
* `:writemap` + `:mapasync`

We are interested in these non-durable write conditions, because there are many
good use cases for a fast non-durable KV store, such as caching, session
management, temporary data storage, real-time analytics, message queues,
configuration, leaderboards, and so on.

### Results

### Remark
