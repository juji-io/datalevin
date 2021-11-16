# Datalevin Search Engine

Datalevin now includes a built-in full-text search engine.

## Rationale

Traditionally, databases and search engines are separate technology fields.
However, from the point of view of an end user, there is hardly a reason why
these two should be separated. A database is for storing and querying data, so is
a search engine. Although many databases have some full-text search
capabilities, their performance in term of relevance and speed is limited
compared with standalone search engines.

In a Datalog database, a full-text search function can be seen as
just another function or predicate to be used in a query. For example, Datomic
On-Prem has a `fulltext` function that allows full text search on a single
attribute, which is implemented with [Apache Lucene](https://lucene.apache.org/)
search engine.

Datalevin's built-in full-text search engine supports more powerful
search across the whole database. The reason why developing a search engine of
our own for Datalevin, instead of using an existing search engine, is the
following:

Standalone search engines, such as Apache Lucene, often have to implement simple
database-like concepts, such as `fields`, in order to capture structural
information of documents. In fact, people often use them as specialized
databases because of that, e.g. Elasticsearch. Datalevin is a versatile Database
with full featured database transaction and query functionalities. Including a
standalone search engine would introduce redundant and unnecessary database-like
features that are less powerful and not as well integrated with the rest of the
database.

Another way to look at the issue is through the sizes of the dependencies. Popular
search engines, such as Lucene, have huge code base with many features. The
library jar of lucene-core, not including any options or modules, is 3.4 MB (the
whole package download is more than 85 MB), whereas the total size of Datalevin
library jar is 120 KB.

Finally, with a search engine of our own, we avoid unnecessary write
amplification introduced by storing the source text twice, once in the database,
again in the search engine. Instead, the embedded search engine only needs to
store a reference to the source content that is stored in the database.

## Usage

## Implementation

As mentioned, the search engine is implemented from scratch, see [blog](https://yyhh.org/blog/2021/11/t-wand-beat-lucene-in-less-than-600-lines-of-code/). Instead of
using a separate storage, the search engine indices are stored in the same
file along with other database data, i.e. all Datalevin data is stored in a
single [LMDB](http://www.lmdb.tech/doc/) data file. This improves cache locality
and reduces the complexity of managing data. F

### Indexing

The search engine indices are stored in one inverted list and two key-value maps.
In addition to information about each term and each document, the positions of term
occurrences in the documents are also stored to support proximity query, phrase
query and match highlighting.

Specifically, the following LMDB sub-databases are created for search supposes:

* `terms`: map of term -> `term-info`
* `docs`: map of document id -> document reference and document norm
* `positions`: inverted lists of term id and document id -> positions and offsets
  of the term in the document

The inverted list implementation leverages the `DUPSORT` feature of LMDB, where
multiple values (i.e. the list) of the same key are stored together in sorted
order, allowing efficient iteration, count and update.

Most information needed for scoring documents were pre-calculated during
indexing, and loaded into memory on demand. For example, the norms of all
documents were loaded into memory during search engine initialization, the same
as Lucene. Term specific information are loaded into memory in aggregates at the
beginning of query processing.

Specifically, a `term-info` consists of the following information about a term:

* `term-id`, unique id of the term, assigned auto-incrementally during indexing
* `max-weight`, the maximum weight of the term
* `doc-freq-sparse-list`, consists of compressed bitmap of document ids and
  corresponding list of term frequencies

`doc-freq-sparse-list` uses our implementation of a sparse integer list,
constructed with two data structures working together. One structure is the
index, containing document ids, represented by a bitmap; and the other is an
array list of integers, containing term frequencies. This is the primary data
structure for searching, and they are loaded into memory per user query terms.
The list of a term is un/compressed during retrieval/storage as a whole.

### Searching

Scoring and ranking of documents implements the standard `tf-idf` and vector space
model. In order to achieve a good balance between relevance and efficiency,
the weighting scheme chosen is `lnu.ltn` [2], i.e. the document vector has
log-weighted term frequency, no idf, and pivoted unique normalization, while the
query vector uses log-weighted term frequency, idf weighting, and no
normalization. One significant difference from the standard `BM25` schema (used
in Lucne) is the document normalization method. Pivoted unique normalization is
chosen for it takes document lengths into consideration, does not needlessly
penalize lengthy documents, and it is cheaper to calculate.

An original algorithm,  what we call *T-Wand*, is developed and implemented for
searching. *T* stands for "Tiered", *Wand* [1] is a state of art search
algorithm used in many search engines, including Lucene. Our algorithm is an
marriage of the main ideas of *Wand* and some of our owns.

As the name suggests, our algorithm works in tiers. It removes
early on those documents that are unlikely to be relevant due to missing query
terms. Not only is it efficient, this approach also addresses an often felt user
frustration with search engines: a document containing all query terms
may be ranked much lower than a document containing only partial query terms. In
our algorithm, the documents containing more query terms are considered first
and are guaranteed to return earlier than those documents with poorer term coverage.

#### T-Wand algorithm

The details of the *T-Wand* algorithm is the following.

First, we want to consider documents containing all `n` user specified query
terms. Instead of looping over all `n` inverted lists of all query terms, we
first pick the query term with the least edit distance (i.e. with the least
amount of typos) and the least document frequency (i.e. the most rare term), use its
inverted list of documents as the candidates, and forgo all other documents.
This is sufficient, because, for a document to contain all `n` query terms, it
must contains the rarest one among them. More generally, there is a simple
(trivial?) mathematical property that can be stated as the following [3]:

```
Let there be a set X with size n and a set Y with any size. For any subset Z in
X of size (n-t+1), if the size of the intersection of X and Y is greater or equal
to t, then Z must intersect with Y.
```

This property allows us to develop search algorithms that use the inverted lists of any
 `n-t+1` terms alone as the candidates, in order to find documents that contain `t`
 query terms. We will of course choose the rarest ones (i.e. shortest lists) to
 start with. For example, to search for documents with all `n` query terms, it
 is sufficient to search in documents containing the rarest term; to search for
 documents with `n-1` terms, it is sufficient to search in the union of the
 rarest and the second rarest term's documents; so on and so forth. When there
 are very rare terms in the query, very few candidate documents need to be
 examined.

We loop over this list of candidate documents, for each document, check if it
appears in the inverted lists of subsequent query terms.

Critically, during this process, we remove a candidate as soon as the following
two conditions is met:

* this document is not going to appear in the required number of inverted lists,
  based on the aforementioned mathematical property;

or,

* this document is not going to make into the top K results, based on an
  approximate score calculation using the pre-computed maximum weight of the
  terms. This is the main idea behind *Wand* algorithm's search efficiency [1],
  as it allows skipping full scoring of many documents.

If all candidates are exhausted and user still requests more results, the
documents containing the second rarest query term are added to candidates list.
They are checked against the remaining query terms to select the candidates that
appear in `n-1` inverted lists. If user keeps asking for more results, the
process continues until candidates only need to appear in `1` inverted list.

Essentially, this search algorithm processes documents in tiers. First tier are
those documents containing all `n` query terms, then `n-1` terms, then `n-2`, and
so on. Document ranking is performed within a tier, not cross tiers. As the
number of tiers goes up, the query is gradually turned from an all-AND to
an all-OR query.

#### *T-Wand* implementation

The code is written in Clojure. The whole search engine weights less than 600
lines of code. We have not unduly optimized the Clojure code for performance. That
is to say, we have left many search speed optimization opportunities on the
table, by writing idiomatic Clojure for the most part.

As can be seen, the implementation of *T-Wand* relies heavily on intersection
and union of document ids. Our implementation is helped by [Roaring
Bitmaps](https://roaringbitmap.org/), a fast compressed bitmap library used in
many projects. The parallel walking of document ids of different terms required
by *Wand* is achieved by iterating the bitmaps.

As mentioned, our storage of term frequencies is handled by an integer array
list, indexed by a bitmap of document ids. The access of a term frequency is
through the `rank` method of Roaring Bitmaps, which seems to be an innovation,
as far as I can tell. When stored, the integer list is compressed with
[JavaFastPFOR](https://github.com/lemire/JavaFastPFOR), another excellent
library by Prof. Daniel Lemire, the same author of Roaring Bitmaps.

[Eclipse Collections](https://www.eclipse.org/collections/) is used to reduce
memory footprint whenever appropriate.

The query result preparation are implemented as Clojure transducers, and the
results are wrapped in the `sequence` function, which returns results
incrementally and on demand.

Fuzzy string match for terms is handled by [SymSpell algorithm](https://wolfgarbe.medium.com/1000x-faster-spelling-correction-algorithm-2012-8701fcd87a5f), which pre-computes
possible typos and considers frequency information.

## Benchmark

The details of benchmark comparison with Lucene is [here](../search-bench/). The
summary is that we beat Lucene in search speed, but lags in write speed,
as expected.

## References

[1] Broder, A.Z., Carmel, D., Herscovici, M, Soffer,A., and Zien, J..  Efficient
query evaluation using a two-level retrieval process. In Proceedings of the
twelfth international conference on Information and knowledge management (CIKM
'03). 2003. pp.426–434.

[2] Manning, C.D., Raghavan, P. and Schütze, H. Introduction to Information
Retrieval, Cambridge University Press. 2008.

[3] Okazaki, N. and Tsujii, J., Simple and Efficient Algorithm for Approximate
Dictionary Matching. Proceedings of the 23rd International Conference on
Computational Linguistics (COLING '10), 2010, pp. 851-859.
