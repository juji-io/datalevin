# Data Compression in Datalevin

In addition to the obvious benefit of reducing storage space, data compression
helps to increase data ingestion and query speed, due to faster key comparison,
better cache locality and reduced workload in general, provided that the
computational overhead of compression and decompression is comparatively low
enough.

## Key-value Compression

In Datalevin, compression/decompression process happens automatically in the key-value storage and is transparent to the users.

### Key Compression

For keys, we use an order preserving compression method, so that point queries,
range queries and some predicates can run directly on the compressed data
without having to decompress data first.

A complete and order preserving dictionary based encoding scheme is implemented,
where a fixed length interval and variable length code are used [1]. The fixed
length chosen is two bytes, which capture second order patterns in data, and yet
would not impose too much of a memory and computation overhead. The variable
length code is Hu-Tucker codes [2], which is optimal.

When a sub-database is initialized, it is generation zero. We first transact the
data uncompressed in generation zero sub-database, where we collect statistics
of the keys. When every 100k keys are stored, the system checks to see if a new
compression dictionary is needed, if so, compute a new dictionary, compress the
keys of all current data using the new dictionary and store them in generation
one sub-database. Optionally, the same process can happen when copying the
database with `optimize?` set to `true`, in order to improve compression rate
with newer samples.

System decides to compute new dictionary based on the Kullback-Leibler
divergence of the current 2-bytes distribution against the previous distribution
using a Dirichlet prior. We will empirically determine the threshold of number
of bits increment that would significantly impact system performance.

Because the frequencies are integers within a bound, a fast linear time
algorithm [3] [4] is used to compute the Hu-Tucker codes, and the resulting
optimal binary alphabetic tree is saved as tables (as byte arrays) for both
encoding and decoding.

### Value Compression

LZ4 is used for compressing large values, as it has a good balance of speed and compression ratio.

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

In this scheme, the values are also keys, and they are also compressed using the
same key compression method described above.

## References

[1] Zhang, et al. "Order-preserving key compression for in-memory search trees." SIGMOD 2020.

[2] Hu, Te C., and Alan C. Tucker. "Optimal computer search trees and variable-length alphabetical codes." SIAM Journal on Applied Mathematics 21.4 (1971): 514-532.

[3] Larmore, Lawrence L., and Teresa M. Przytycka. "The optimal alphabetic tree problem revisited." Journal of Algorithms 28.1 (1998): 1-20.

[4] Hu, T. C., Lawrence L. Larmore, and J. David Morgenthaler. "Optimal integer alphabetic trees in linear time." European Symposium on Algorithms. 2005.
