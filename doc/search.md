# Datalevin Search Engine

## Usage

## Implementation

The search engine is implemented from scratch and does not pull in additional
external dependencies that are not already in Datalevin.

Instead of using a separate storage, the search engine indices are stored in the
same LMDB data file just like other database data. This improves cache locality
and reduces the cost of managing data.

To support approximate full text search, two levels of mappings are
used: strings -> tokens -> documents. The first level permits fuzzy
string match within a threshold. The second level represents the standard
term-document matrix stored in an inverted index.

The fuzzy string match uses a variation of SymSpell algorithm [1].

The inverted index implementation leverages the `DUPSORT` feature of LMDB, where
multiple values (i.e. the list) of the same key are stored together in sorted
order, allowing very efficient search and update.

Search in the inverted index uses an algorithm inspired by [2].

[1] SymSpell algorithm https://wolfgarbe.medium.com/1000x-faster-spelling-correction-algorithm-2012-8701fcd87a5f

[2] Okazaki, N. and Tsujii, J., Simple and Efficient Algorithm for Approximate
Dictionary Matching. Proceedings of the 23rd International Conference on
Computational Linguistics (COLING), 2010, pp. 851-859.
