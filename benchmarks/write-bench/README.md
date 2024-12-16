# Write Benchmark

The purpose of this benchmark is to plot the throughput and latency under
various conditions in Datalevin, to give users some reference data points. This
benchmark loosely follows the tasks proposed in [this Rama
benchmark](https://blog.redplanetlabs.com/2024/04/25/better-performance-rama-vs-mongodb-and-cassandra/).
There are two main tasks, one is pure write in batch write condition, another is
read/write at the same time.

## Pure Write

This benchmark writes two UUID strings as the key (imitating Cassandra's
partitioning key and clustering key), and a UUID string as the value. So each
write has a 72 bytes key and a 36 bytes value.

These data are written one batch at a time. Instead of using fixed batch size,
we vary the batch size to show the change of write throughput/latency. Since
there's no upper limit on the batch size, we arbitrarily set the maximal batch
size to be 1 million. In total, we write as fast as we can for 3 hours, with
batch sizes in 1, 10, 100, 1000, 10000, 100000, or 1000000. We also measure
latency of write acknowledgment throughout the run.

### `transact-kv-async`

This is asynchronous write in the KV store. We do not limit the number of calls
in flight. Instead, we write as fast as possible. We report results on committed
and synced to disk writes that are guaranteed to be durable.

### `transact-kv`

### `transact-async`

### `transact`

### `transact!`

## Read/Write
