# Datalevin Search Engine

## Usage

## Implementation

The search engine is implemented from scratch and does not pull in additional
external dependencies that are not already in Datalevin.

Instead of using a separate storage, the search engine indices are stored in the
same LMDB data file just like other database data. This improves cache locality
and reduces the cost of managing data.

To support approximate full text search, two levels of inverted indices are
used: features -> tokens -> documents. The first level of inverted indices
permits approximate string match within a threshold, e.g. the features could be
trigrams. The second level represents the standard term-document matrix of a
full-text search engine.

The inverted list implementation leverages the `DUPSORT` feature of LMDB, where
multiple values (i.e. the list) of the same key are stored together in sorted
order, allowing very efficient search and update.

Search adopts an efficient approximate dictionary matching algorithm [1]. In
addition to fit the algorithm to the LMDB based inverted indices, we improves
the algorithm by performing one pass index scan only, instead of many.

[1] Okazaki, N. and Tsujii, J., Simple and Efficient Algorithm for Approximate
Dictionary Matching. Proceedings of the 23rd International Conference on
Computational Linguistics (COLING), 2010, pp. 851-859.
