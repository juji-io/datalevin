# Datalevin Search Engine

## Rationale

Traditionally, databases and search engines are separate technology fields.
However, from the point of view of an end user, there is hardly a reason why
these two should be separated. A database is for storing and querying data, so is
a search engine. In a Datalog database, a full-text search function can be seen as
just another function or predicate to be used in a query. For example, The
On-Prem version of Datomic has a `fulltext` function that allows full text
search on a single attribute, which is implemented with Lucene.

Datalevin has a built-in full text search engine that supports more powerful
search across the whole database. The reason why developing a search engine of
our own for Datalevin, instead of using an existing search engine, is the
following:

Standalone search engines, such as Lucene, often have to implement simple
database-like concepts, such as `fields`, in order to capture structural
information of documents. In fact, people often use them as specialized
databases because of that, e.g. Elasticsearch. Datalevin is a versatile Database
with full featured database transaction and query functionalities. Including a
standalone search engine would introduce redundant and unnecessary database-like
features that are less powerful and not as well integrated with the rest of the
database.

Another way to look at this is through the sizes of the dependencies. Popular
search engines, such as Lucene, have huge code base with many features. The
compressed artifact size of Lucene alone is north of 80 MB, whereas the total
size of Datalevin library jar is only 120 KB. Datalevin is meant to be a
simple, fast and versatile database. To this end, we developed our own nicely
integrated full text search engine.

## Usage

## Implementation

As mentioned, the search engine is implemented from scratch and does not pull in
additional external dependencies that are not already in Datalevin. Instead of
using a separate storage, the search engine indices are stored in the same LMDB
data file just like other database data. This improves cache locality and
reduces the cost of managing data.

### Indexing

To support fuzzy full text search, two levels of mappings are encoded in the
search engine indices: strings -> terms -> documents.

The first level of mappings permits approximate string match within a threshold
(default: edit distance <= 2), so the search would be typo tolerant. The fuzzy
string match uses a variation of SymSpell algorithm [1] that pre-computes
possible typos and leverage frequency information of both unigrams and bigrams,
so it is very fast and quite accurate.

The second level of mappings represents the term-document matrix stored in inverted
lists and key-value maps. In addition to the numbers of term occurrences, the
positions of term occurrences in the documents are also indexed to support
proximity query, phrase query and match highlighting.

In more details, the following LMDB sub-databases are created for search supposes:

* `unigrams`: map of term -> term id and collection term frequency
* `bigrams`: map of term-id1, term-id2 -> bigram frequency
* `docs`: map of document id -> document reference and number of unique terms in document
* `rdocs`: map of document reference -> document id
* `term-docs`: inverted list of term id -> document ids
* `positions`: inverted list of document id and term id -> positions and offsets of term

The inverted list implementation leverages the `DUPSORT` feature of LMDB, where
multiple values (i.e. the list) of the same key are stored together in sorted
order, allowing very efficient search, count and update. For example, document
frequency and term frequency can be obtained cheaply and accurately by calling
`list-count` function on `term-docs` and `positions`, respectively, instead of
having to store them separately.

### Searching

Scoring and ranking of documents implement the standard tf-idf and vector space
model [2]. The weighting scheme is `lnu.ltc`, i.e. the document vector has
log-weighted term frequency, no idf, and pivoted unique normalization, while the
query vector uses log-weighted term frequency, idf weighting, and cosine
normalization.

The search algorithm implements an original algorithm with inspiration from [3].
Compared with standard algorithm [2], our algorithm prunes documents that are unlikely
to be relevant due to missing query terms. Not only being more efficient, this pruning
algorithm also addresses an often felt user frustration with the standard
algorithm: a document containing all query terms may be ranked lower than
a document containing only partial query terms. In our algorithm, the documents
containing more complete query terms are considered first. In a top-K situation
with a small K, those documents with very poor query term coverage may not even
participate in the ranking at all.

The details of the pruning algorithm is the following: instead of looping over
all `n` inverted lists of all query terms, we pick the query term with the least edit
distance and the least document frequency (i.e. the most rare term), and use its
posting document ids as the candidates. We loop over this list of
candidate documents, for each document, check if it appears in the inverted
lists of subsequent terms (ordered by document frequency). For each appearance,
we accumulate the matching score using our weighting scheme. During the process,
we prune the candidates who are not going to appear in all `n` inverted lists;
The pruned document ids along with their number of existing appearances are put
into a backup candidates map. If all candidates are exhausted and user still
requests more results, the document ids of the second rarest query term are
added to backup candidates map, which is then promoted to the candidates and are
checked against the remaining terms to ensure the candidates appear in `n-1`
inverted lists. If user keeps asking for more results, the process continues
until there is no inverted list remaining to be checked against.

Essentially, this search algorithm processes documents in the order of the
number of query terms they contains. First those contain all `n` query terms,
then `n-1` terms, then `n-2`, and so on. The query processing workflow is
implemented as Clojure transducers, and the results are wrapped in the
`sequence` function, which performs the calculations incrementally and on
demand.

[1] Garbe, W. SymSpell algorithm https://wolfgarbe.medium.com/1000x-faster-spelling-correction-algorithm-2012-8701fcd87a5f

[2] Manning, C.D., Raghavan, P. and Schütze, H. Introduction to Information
Retrieval, Cambridge University Press. 2008.

[3] Okazaki, N. and Tsujii, J., Simple and Efficient Algorithm for Approximate
Dictionary Matching. Proceedings of the 23rd International Conference on
Computational Linguistics (COLING), 2010, pp. 851-859.
