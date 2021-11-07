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

Standalone search engines, such as [Apache Lucene](https://lucene.apache.org/),
often have to implement simple database-like concepts, such as `fields`, in
order to capture structural information of documents. In fact, people often use
them as specialized databases because of that, e.g. Elasticsearch. Datalevin is
a versatile Database with full featured database transaction and query
functionalities. Including a standalone search engine would introduce redundant
and unnecessary database-like features that are less powerful and not as well
integrated with the rest of the database.

Another way to look at the issue is through the sizes of the dependencies. Popular
search engines, such as Lucene, have huge code base with many features. The
compressed artifact size of Lucene is north of 80 MB, whereas the total
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

The search engine indices are stored in one inverted list and two key-value maps.
In addition to information for each term and each document, the positions of term
occurrences in the documents are also stored to support search, proximity query,
phrase query and match highlighting.

Specifically, the following LMDB sub-databases are created for search supposes:

* `terms`: map of term -> term-info
* `docs`: map of document id -> document reference and document norm
* `positions`: inverted lists of term id and document id -> positions and offsets
  of the term in the document

The inverted list implementation leverages the `DUPSORT` feature of LMDB, where
multiple values (i.e. the list) of the same key are stored together in sorted
order, allowing efficient iteration, count and update. For example, term
frequency is obtained by calling `list-count` on a `positions` list,
instead of having to store them.

Regardless of the method, obtaining term frequency (the number of times a term
appears in a document) is the most expensive yet necessary operation in scoring
documents, as they are indexed by the Cartesian product of terms and documents,
hence too numerous to fit in memory. The main thrust of our search algorithm
development (see blow) was in minimizing its unnecessary calculation.

Other information needed for scoring documents were pre-calculated and loaded
into memory on demand. For example, the norms of all documents were loaded into
memory during search engine initialization, the same as Lucene. Term specific
information are loaded into memory in aggregates at the beginning of query processing,
instead of being accessed piece-meal during the scoring process, unlike Lucene.

Specifically, a `term-info` consists of the following information about a term:

* `term-id`, unique id of the term, assigned auto-incrementally during indexing
* `doc-bitmap`, compressed bitmap of document ids for those documents containing the term
* `block-max-tf`, array of maximum term frequency for a block (default 64) of documents

The reason to store term information as bitmaps and arrays, accessed in
aggregates, instead of storing them in inverted lists, accessed piece-meal, is
because LMDB is very efficient (more efficient than file systems) at
reading/writing large binary blobs. As long as the data structure to be stored
has high (de)serialization speed, it is more efficient to store/retrieve them in
large aggregates than in more granular forms. This does not apply for more
complex data structures with high construction cost, e.g. hash maps. We consider
these general advises in how to best use the so called single-level storage, such as
LMDB. We earned these lessons in our many iterations of speeding up the search.

### Searching

Scoring and ranking of documents implements the standard tf-idf and vector space
model. In order to achieve a good balance between relevance and efficiency,
the weighting scheme chosen is `lnu.ltn` [1], i.e. the document vector has
log-weighted term frequency, no idf, and pivoted unique normalization, while the
query vector uses log-weighted term frequency, idf weighting, and no
normalization. One significant difference from the standard BM25 schema is the document
normalization method. Pivoted unique normalization is chosen for it takes document
lengths into consideration, does not needlessly penalize lengthy documents, and
it is cheaper to calculate.

Depending on the query, documents selection uses one of two algorithms
that we developed. By default, for cases when the query contains a mixture of
very rare terms and common ones, or when there is only one query term, a
candidate document pruning algorithm, called `:prune`, is used; for other cases,
bitmaps union/intersection are used as the initial document selection process,
we call this algorithm `:bitmap`.

Compared with standard approach, e.g. that of Lucene, our algorithms remove
early on those documents that are unlikely to be relevant due to missing query
terms. Not only is it efficient, this approach also addresses an often felt user
frustration with the standard search engines: a document containing all query terms
may be ranked much lower than a document containing only partial query terms. In
our algorithms, the documents containing more query terms are considered first.
When returning top-K result with a small K, those documents with very poor query
term coverage may not even participate in the ranking. For easy of exposition,
we start by describing the `:prune` algorithm.

#### :prune algorithm

The details of the `:prune` algorithm is the following. First, we want to
consider documents containing all `n` query terms. Instead of looping over all
`n` inverted lists of all query terms, we first pick the query term with the
least edit distance (i.e. with the least typos) and the least document frequency
(i.e. the most rare term), and use its inverted list of documents as the
candidates. This is sufficient, because, for a document to contain all `n` query
terms, it must contains the rarest one among them. More generally, there is a
mathematical property that can be stated as the following [5]:

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
the required number of appearances is found, the document is removed from the
candidate list and added to the result list.

Critically, we also remove the candidates that:

* are not going to appear in the required number of inverted lists, based on the
  above mathematical property;

or,

* are not going to make into the top K results, based on an approximate score
  calculation using the block maximum term frequency. This is the main idea [1]
  [2] behind Lucene's search efficiency, but we deploy the idea in a completely
  new way, for we consider only a small number of candidates to begin with,
  instead of considering all but attempting to skip some by sorting and pivoting
  during the scoring process.

The pruned documents are put into a backup candidate list to be used later in
case more results are requested. If all candidates are exhausted and user still
requests more results, the documents containing the second rarest query term are
added to candidates list, alone with the backup candidates. They are checked
against the remaining query terms to select the candidates that appear in `n-1`
inverted lists. If user keeps asking for more results, the process continues
until candidates only need to appear in `1` inverted list.

Essentially, this search algorithm processes documents in tiers. First tier are
those documents containing all `n` query terms, then `n-1` terms, then `n-2`, and
so on. Document ranking is performed within a tier, not cross tiers. As the
number of tiers goes up, the query is gradually turned from an all-AND to
an all-OR query.

#### :bitmap algorithm

The `:bitmap` algorithm follows the same process. However, instead of probing
one pair of document-term at a time, it processes one pair of terms at a time by
intersecting their inverted lists. However, we avoid the combination explosion
problem by leveraging the same mathematical property that enables the `:prune`
algorithm above.

Basically, `t` means the number of required overlaps between
query terms and a document. For the first tier, set `t=n`, second tier, `t=n-1`, and
so on. For each tier, all we need to do is to union *any* `n-t+1` inverted lists
and then intersect the result with the intersection of the remaining inverted
lists.

This `:bitmap `algorithm is efficient for most cases, except for when
there are very rare terms in the query, then the `:prune` algorithm is
more performant. This condition can be easily checked, and the system can
choose the algorithm smartly, called `:smart` algorithm (default).

The result preparation are implemented as Clojure transducers, and the
results are wrapped in the `sequence` function, which performs the calculations
incrementally and on demand.

Fuzzy string match for queries is handled by SymSpell algorithm [3], which pre-builds
possible typos and considers frequency information, so it is fast and accurate.

## Benchmark

References:

[1] Broder, A.Z., Carmel, D., Herscovici, M, Soffer,A., and Zien, J..  Efficient
query evaluation using a two-level retrieval process. In Proceedings of the
twelfth international conference on Information and knowledge management (CIKM
'03). 2003. pp.426–434.

[2] Ding, S. and Suel, T. 2011. Faster top-k document retrieval using block-max
indexes. In Proceedings of the 34th international ACM SIGIR conference on
Research and development in Information Retrieval (SIGIR '11). pp. 993–1002.

[3] Garbe, W. SymSpell algorithm https://wolfgarbe.medium.com/1000x-faster-spelling-correction-algorithm-2012-8701fcd87a5f

[4] Manning, C.D., Raghavan, P. and Schütze, H. Introduction to Information
Retrieval, Cambridge University Press. 2008.

[5] Okazaki, N. and Tsujii, J., Simple and Efficient Algorithm for Approximate
Dictionary Matching. Proceedings of the 23rd International Conference on
Computational Linguistics (COLING '10), 2010, pp. 851-859.
