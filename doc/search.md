# Datalevin Search Engine

## Rationale

Traditionally, databases and search engines are separate technology fields.
However, from the point of view of an end user, there is hardly a reason why
these two should be separated. A database is for storing and querying data, so is
a search engine. Although many databases have some full-text search
capabilities, their performance (in term of relevance and speed) is limited
compared with standalone search engines.

In a Datalog database, a full-text search function can be seen as
just another function or predicate to be used in a query. For example, Datomic
On-Prem has a `fulltext` function that allows full text search on a single
attribute, which uses the Lucene search engine.

Datalevin has a built-in full-text search engine that supports more powerful
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

Another way to look at the issue is through the sizes of the dependencies. Popular
search engines, such as Lucene, have huge code base with many features. The
compressed artifact size of Lucene alone is north of 80 MB, whereas the total
size of Datalevin library jar is only 120 KB.

Finally, with a search engine of our own, we avoid unnecessary write
amplification introduced by storing the source text twice, once in the database,
again in the search engine. Instead, the embedded search engine only needs to
store a reference to the source content that is stored in the database.

## Usage

## Implementation

As mentioned, the search engine is implemented from scratch. Instead of
using a separate storage, the search engine indices are stored in the same LMDB
data file just like other database data. This improves cache locality and
reduces the cost of managing data.

### Indexing

The search engine indices are stored in several inverted lists and key-value maps.
In addition to the numbers of term occurrences, the positions of term
occurrences in the documents are also stored to support proximity query, phrase
query and match highlighting.

In more details, the following LMDB sub-databases are created for search supposes:

* `unigrams`: map of term -> term id
* `docs`: map of document id -> document reference and number of unique terms in document
* `term-docs`: inverted list of term id -> document ids
* `positions`: inverted list of document id and term id -> positions and offsets
  of the term in the document

The inverted list implementation leverages the `DUPSORT` feature of LMDB, where
multiple values (i.e. the list) of the same key are stored together in sorted
order, allowing very efficient search, count and update. For example, document
frequency and term frequency can be obtained cheaply and accurately by calling
`list-count` function on `term-docs` and `positions`, respectively, instead of
having to store them separately.

### Searching

Scoring and ranking of documents implements the standard tf-idf and vector space
model [1]. In order to achieve a good balance between relevance and efficiency,
the weighting scheme chosen is `lnu.ltn`, i.e. the document vector has
log-weighted term frequency, no idf, and pivoted unique normalization, while the
query vector uses log-weighted term frequency, idf weighting, and no
normalization. Pivoted unique normalization is chosen for it takes document
lengths into consideration, and does not needlessly penalize lengthy documents.

Depending on the query, documents selection uses one of two algorithms
that we developed. For cases when the query contains a mixture of very rare terms
and common ones, or when there is only one query term, a candidate pruning
algorithm, called `:prune`, is used; for other cases, the inverted lists of the
query terms are read into bitmaps and documents are found through bitmap
union/intersection, called `:bitmap`.

Compared with standard algorithm [1], our algorithms remove early on those
documents that are unlikely to be relevant due to missing query terms. Not only
is it more efficient, this approach also addresses an often felt user
frustration with the standard algorithm: a document containing all query terms
may be ranked much lower than a document containing only partial query terms. In
our algorithms, the documents containing more query terms are considered first.
When returning top-K result with a small K, those documents with very poor query
term coverage may not even participate in the ranking.

The details of the `:prune` algorithm is the following: instead of
looping over all `n` inverted lists of all query terms, we first pick the query
term with the least edit distance (i.e. with the least typos) and the least
document frequency (i.e. the most rare term), and use its inverted list of
documents as the candidates. This is sufficient, because, for a document to
contain all `n` query terms, it must contains the rarest one among them. More
generally, there is a mathematical property that can be stated as the
following [2]:

```
Let there be a set X with size n and a set Y with any size. For any subset Z in
X of size (n-t+1), if the size of the intersection of X and Y is greater or equal
to t, then Z must intersect with Y.
```
This property allows us to develop search algorithms that use the inverted lists of any
 `n-t+1` terms (we will of course choose the rarest ones) as the candidates, in
 order to find documents that contain `t` terms.

We loop over this list of candidate documents, for each document, check if it
appears in the inverted lists of subsequent query terms. When there is a very
rare term in the query, very few candidate documents need to be examined. When
`n` appearances is found, the document is removed from the candidate list and
added to the result list. Critically, we also remove the candidates that are not
going to appear in all `n` inverted lists; The pruned documents are put into a
backup candidate list to be used later in case more results are requested.

If all candidates are exhausted and user still requests more results, the
documents containing the second rarest query term are added to candidates
list, alone with the backup candidates. They are checked against the
remaining query terms to select the candidates that appear in `n-1` inverted lists.
If user keeps asking for more results, the process continues until candidates
only need to appear in `1` inverted list. Essentially, this search algorithm
processes documents in tiers. First tier are those documents contain all `n`
query terms, then `n-1` terms, then `n-2`, and so on. Document ranking is
performed within a tier, not cross tiers.

The `:bitmap` algorithm follows the same process. However, instead of probing
one pair of document-term at a time, it processes one pair of terms at a time by
intersecting their inverted lists. However, we avoid the combination explosion
problem by leveraging the same mathematical property that enables the `:prune`
algorithm above. Basically, `t` means the number of required overlaps between
query terms and a document. For first tier, set `t=n`, second tier, `t=n-1`, and
so on. For each tier, all we need to do is to union *any* `n-t+1` inverted lists
and then intersect the result with the intersection of the remaining inverted
lists.

This `:bitmap `algorithm is efficient for most cases, except for when
there are very rare terms in the query, then the `:prune` algorithm is
more performant. This condition can be easily checked, and the system can
choose the algorithm smartly, called `:smart` algorithm (default).

The ranking and result preparation are implemented as Clojure transducers, and the
results are wrapped in the `sequence` function, which performs the calculations
incrementally and on demand.

Fuzzy string match is handled by SymSpell algorithm [3], which pre-builds
possible typos and considers frequency information, so it is fast and accurate.

## Benchmark



[1] Manning, C.D., Raghavan, P. and Schütze, H. Introduction to Information
Retrieval, Cambridge University Press. 2008.

[2] Okazaki, N. and Tsujii, J., Simple and Efficient Algorithm for Approximate
Dictionary Matching. Proceedings of the 23rd International Conference on
Computational Linguistics (COLING), 2010, pp. 851-859.

[3] Garbe, W. SymSpell algorithm https://wolfgarbe.medium.com/1000x-faster-spelling-correction-algorithm-2012-8701fcd87a5f
