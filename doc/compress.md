# Data Compression in Datalevin

In addition to the obvious benefit of reducing storage space, data compression
helps to increase data ingestion and query speed, due to faster key comparison,
better cache locality and reduce workload in general, provided the computational
overhead of compression and decompression is comparatively low enough.

## Key-value Compression

In Datalevin, compression/decompression process happens automatically in the key-value storage and is transparent to the users.

### Key Compression


For keys, we use an order preserving compression method, so that point queries,
range queries and some predicates can run directly on the compressed data
without having to decompress data first.

A complete and order preserving dictionary based encoding scheme is implemented,
where a fixed length interval and variable length code are used [1]. The fixed
length chosen is two bytes, and the variable length code is Hu-Tucker codes [2],
which is optimal.

When a database is initialized, we first transact the data uncompressed in a
temporary database, where we collect the frequencies of keys. Once a large
enough sample of data (default 100K) is collected, the system computes the
dictionary, and compress the keys of all current and future data using the
dictionary in the permanent database file.

Because the keys are already sorted in the sampling database, a fast linear time
algorithm [3] is used to compute the Hu-Tucker codes, and the resulting optimal
binary alphabetic tree is saved for both encoding and decoding.

### Value Compression

LZ4 is used for compressing values, as it has a good balance of speed and
compression ratio.

## Triple Compression

Literal representation of triples in an index introduces significant redundant
storage. For example, in `:eav` index, there are many repeated values of `e`,
and in `:ave` index, there are many repeated values of `a`. These repetitions of
head elements not just increase the storage size, but also add processing
overhead during query.

Taking advantage of LMDB's dupsort capability, we store the head element value
only once, by treating them as keys. The values are the remaining two elements
of the triple, plus a reference to the full datom if the datom is larger than
the maximal key size allowed by LMDB.

In this scheme, the values are also keys, and they are also compressed using the same key compression method described
above.

## References

[1] Zhang, et al. "Order-preserving key compression for in-memory search trees." SIGMOD 2020.

[2] Hu, Te C., and Alan C. Tucker. "Optimal computer search trees and variable-length alphabetical codes." SIAM Journal on Applied Mathematics 21.4 (1971): 514-532.

[3] Larmore, Lawrence L., and Teresa M. Przytycka. "The optimal alphabetic tree problem revisited." Journal of Algorithms 28.1 (1998): 1-20.
