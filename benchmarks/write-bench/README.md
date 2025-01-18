# Write Benchmark (WIP)

The purpose of this benchmark is to plot the write throughput and latency under
various conditions in Datalevin. Hopefully, this gives users some reference data
points to help choosing the right transaction function and the right data batch
size for specific use cases.

Datalevin supports these transaction functions for Key Value DB:

* `transact-kv-async`
* `transact-kv`

And the following for Datalog DB:

* `transact-async`
* `transact`
* `transact!`

In all transaction functions, we only use Datalevin's default write setting,
i.e. the safest ACID write setting that flushes data to disk when commit. For
now, the faster, albeit less safe write conditions, such as `:mapasync`,
`:nometasync`, `:nosync`, `:writemap`, and so on, are not tested in this
benchmark. For Datalog write, the faster `init-db` and `fill-db` functions that
directly load prepared datoms are not tested in this benchmark either, as we are
only interested in the transaction of raw data.

There are two main tasks, one is pure write in batch write condition, another is
half read half write at the same time.

## Pure Write

This task writes two random UUID strings as the key, and a random UUID string as
the value. So each write has a 72 bytes key and a 36 bytes value.

These data are written in batches, one batch at a time. We vary the batch size
to show the change of write throughput and latency. To avoid exhausting system
resources, the number of asynchronous write requests in flight is capped at 1K
at a time.

Since there's no upper limit on the batch size, we arbitrarily set the maximal
batch size to be 100K. In each benchmark run, we write to an empty DB as fast as
we can for an hour, with batch sizes of 1, 10, 100, 1k, 10k, and 100k,
respectively. For example, the command below runs benchmark for
`transact-kv-async` with batch size 10, and save the results in
`kv-async-10.csv`:

```bash
 timeout 3600s clj -Xwrite :batch 10 :f kv-async > kv-async-10.csv
```

For every 100K writes, the following metrics are collected during the benchmark
run:

* Throughout: the average number of writes per second so far.
* Write Latency: the average latency of the transaction function call, in
  milliseconds.
* Commit Latency: the average latency of receiving the write success
  acknowledgment, in milliseconds. As mentioned, the acknowledged writes are
  guaranteed to be durable.


## Read/Write
