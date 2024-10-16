# Data Compression in Datalevin (WIP)

In addition to the obvious benefit of reducing storage space, data compression
helps to increase data ingestion and query speed, due to faster key comparison,
better cache locality and reduced workload in general, provided that the
computational overhead of compression and decompression is comparatively low
enough.

## Key Compression

For keys, we use an order preserving compression method, so that range queries
and some predicates can run directly on the compressed data without having to
decompress data first.

A complete and order preserving dictionary based encoding scheme is implemented,
where a fixed length interval and variable length code are used [1]. The fixed
length chosen is two bytes, which captures some first order entropy in data,
while imposes not too much of a memory and computation overhead, i.e. we
consider fixed number of 64K symbols. The variable length code chosen is
Hu-Tucker codes [2], which is optimal. We implement the Hu-Tucker algorithm
using `n` mergeable priority queues to achieve the `O(nlogn)` theoretical bounds
[3]. The symbol frequencies are estimated from sampling a fixed number (64K) of
keys using an optimal reservior sampling algorithm [5].

The resulting optimal binary alphabetic tree of each DBI is represented as an
array of code length (byte) and an array of codes (32 bits integer). These two
arrays are stored in meta data of the DBI in compressed from, and are also kept
in memory, as they are used for encoding raw data. We also keep in memory
pre-computed decoding tables [4] for decoding compressed data 4 bits at a time.
4 bits are chosen as a good balance between memory usage (10 MiB per dictionary)
and decoding performance. These tables are computed from the stored codes during
DBI initialization.

In order to obtain a good compression ratio, the key compression dictionary
should only be created after at least 64K keys have been stored to get good
samples. Therefore keys are initially not compressed when DB is created. User can run `re-index`
function with `:compress?` option enabled to create the dictionary and store the
keys in compressed form when enough keys are seen.
The computation of dictionary and building all necessary encoding and
decoding tables takes less than one second on today's CPU.

### Value Compression

LZ4 is used to compress values, as it has a good balance of speed and
compression ratio. It is enabled when `:compress?` is set to true.

## Wire compression

By default, lz4 compression is used for data sent between client and server. Set
`:compress-message?` option to `false` on the client to disable compression,
e.g. to work with versions of the server prior to `0.9.0`.

## Triple Compression

Literal representation of triples in an index incurs significant redundant
storage. For example, in `:eav` index, there are many repeated values of `e`,
and in `:ave` index, there are many repeated values of `a`. These repetitions of
head elements not just increase the storage size, but also add processing
overhead during query.

Taking advantage of LMDB's dupsort capability, we store the head element value
only once, by treating them as keys. The values are the remaining two elements
of the triple, plus a reference to the full datom if the datom is larger than
the maximal key size allowed by LMDB.

In this scheme, the values are also keys, and they are also compressed using the
same key compression method described above.

## Benchmark

### Compression ratio

### Run time performance

#### Write

#### Query

## References

[1] Zhang, et al. "Order-preserving key compression for in-memory search trees." SIGMOD 2020.

[2] Hu, Te C., and Alan C. Tucker. "Optimal computer search trees and variable-length alphabetical codes." SIAM Journal on Applied Mathematics 21.4 (1971): 514-532.

[3] Davis, Sashka, "Hu-Tucker algorithm for building optimal alphabetic binary
search trees" (1998). Master Thesis. Rochester Institute of Technology.

[4] Bergman, Eyal, and Shmuel T. Klein. "Fast decoding of prefix encoded texts." IEEE Data Compression Conference 2005.

[5] Li, Kim-Hung. "Reservoir-Sampling Algorithms of Time Complexity
O(n(1+log(N/n)))". ACM Transactions on Mathematical Software. 20.4 (1994): 481–493.
