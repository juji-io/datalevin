# Write Benchmark (WIP)

The purpose of this benchmark is to plot the write throughput and latency under
various conditions in Datalevin. Hopefully, this gives users some reference data
points to help choosing the right transaction function and the right data batch
size for specific use cases.

## Setup

The benchmark is conducted on an Intel Core i7-6850K CPU @ 3.60GHz with 6 cores,
64GB RAM, 1TB SSD. The OS is Debian 12.9, kernel 6.1.0-28-amd64, running OpenJDK
version "17.0.13" 2024-10-15, and Clojure version is 1.12.0.

To avoid exhausting system resources, the number of asynchronous write requests
in flight is always capped at 1K using a Semaphore.

## Measures

For every one million writes, a set of measures are taken:

* Time (seconds), time since benchmark run starts.
* Throughput (writes/second), average throughput at the moment.
* Write Latency (milliseconds), average latency of transact function calls.
* Commit Latency (milliseconds), average latency of transaction commits.

The results are written into a CSV file.

For example, the command below runs benchmark for `transact-kv-async` with batch
size 10, and save the results in `kv-async-10.csv`:

```bash
time clj -Xwrite :base-dir \"/tmp/test/\" :batch 10 :f kv-async > kv-10-async.csv
```

The final wall clock time, system time and user time are also reported.


## Key Value Transaction

### Tasks

#### Random Write

This task writes a 8 bytes even integer between 1 and 200 millions as the key
and a random UUID string as the value. The pure write task is to write **100
millions** such data to an empty DB. The keys are all even numbers, so the next
task can have 50% chance of overwrite existing keys.

#### Mixed Read/Write

With 100 millions items in DB, we then do 20 million additional operations, with
10 million reads and 10 million writes interleaved. The read/write keys are
random number between 1 and 200 millions. So initally write has a 50% chance of
being an addition and 50% chance of being an overwrite. The chance of being an
overwrite increases slightly as more items are written.

### Write Conditions

Datalevin supports these transaction functions for key value store:

* `transact-kv`
* `transact-kv-async`

In addition to the default durable write condition, Datalevin supports some
faster, albeit less durable write flags:

* `:nosync`
* `:nometasync`
* `:writemap` + `:mapasync`

Write data in batches generrally improve throughput, so we vary the batch sizes
as well: 1, 10, 100, 1k, and 10k.

We will show how combinations of these conditions affect throughput and latency.

### Results

## Datalog Transaction

Because a Datalog transaction does a lot more work than just writing key values,
the differences among different env flags in Datalog transactions are not as
pronounced as in KV stores. We only test durable writes for Datalog transaction.

### Write Conditions

Tested the following functions for Datalog DB:

* `transact!`
* `transact-async`

`transact` is just the blocked version of `transact-async` so it is not tested.
There are two faster `init-db` and `fill-db` functions that directly load
prepared datoms and by-pass the expensive process of turning data into datoms.
These are not tested in this benchmark either, as we are only interested in
transactions of raw data.

Similar to KV tasks above, every write transacts an entitiy of two attributes,
one is a long integer, marked as `:db.unique/identity`, and the other an UUID
string. 100 millions such entities are writen first, then 20 million mixed query
and write are conducted. Same measures are also taken.

In addition, we also tested Sqlite with the same tasks in the default durable
transaction mode.
