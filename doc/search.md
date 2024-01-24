# Datalevin Search Engine

Datalevin includes a built-in full-text search engine.

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

The full-text search functionalities are available to use in all supported
Datalevin modes: key-value store, Datalog store, embedded, client/server, or
Babashka pods.

### Standalone search

Datalevin can be used as a standalone search engine. The standalone search API
involves only a few functions: `new-search-engine`, `add-doc`, `remove-doc`, and
`search`.

```Clojure
(require '[datalevin.core :as d])

;; A search engine depends on a key-value store to store the indices.
(def lmdb (d/open-kv "/tmp/search-db"))
(def engine (d/new-search-engine lmdb {:index-position? true
                                       :include-text?   true}))

;; Here are the documents to be indxed, keyed by doc-id
(def docs
  {1 "The quick red fox jumped over the lazy red dogs."
   2 "Mary had a little lamb whose fleece was red as fire."
   3 "Moby Dick is a story of a whale and a man obsessed."})

;; Add the documents into the search index. `add-doc` takes a `doc-ref`, which
;; can be anything that uniquely identify a document, in this case, a doc-id
(d/add-doc engine 1 (docs 1))
(d/add-doc engine 2 (docs 2))
(d/add-doc engine 3 (docs 3))

;; search by default return a list of `doc-ref` ordered by relevance to query
(d/search engine "red")
;;=> (1 2)

;; we can alter the display to show offets of term occurrences as well, useful
;; e.g. to highlight matched terms in documents
(d/search engine "red" {:display :offsets})
;;=> ([1 (["red" [10 39]])] [2 (["red" [40]])])

(sut/search engine "red" {:display :texts+offsets})
;;=> [[1  "The quick red fox jumped over the lazy red dogs." [["red" [10 39]]]]
;;=>  [2  "Mary had a little lamb whose fleece was red as fire." [["red" [40]]]]]

```

### Search in Datalog

Searchable values of the Datalog attributes need to be declared in the
schema, with the `:db/fulltext true` property. The value does not have to be of
 string type, as the indexer will call `str` function on it to convert it to
 string.

A query function `fulltext` is provided to allow full-text search in Datalog
queries. This function takes the db, the query and an optional option map (same
as `search`), and returns a sequence of matching datoms, ordered by relevance to
the query.

```Clojure
(let [db (-> (d/empty-db "/tmp/mydb"
               {:text {:db/valueType :db.type/string
                       :db/fulltext  true}})
             (d/db-with
                 [{:db/id 1,
                   :text  "The quick red fox jumped over the lazy red dogs."}
                  {:db/id 2,
                   :text  "Mary had a little lamb whose fleece was red as fire."}
                  {:db/id 3,
                   :text  "Moby Dick is a story of a whale and a man obsessed."}]))]
    (d/q '[:find ?e ?a ?v
           :in $ ?q
           :where [(fulltext $ ?q) [[?e ?a ?v]]]]
          db
          "red fox"))
;=> #{[1 :text "The quick red fox jumped over the lazy red dogs."]
;     [2 :text "Mary had a little lamb whose fleece was red as fire."]}
```
In the above example, we destructure the returned datoms into three variables,
`?e`, `?a` and `?v`.

As can be seen, by default, the search is across the whole database, not limited
to an individual attribute.

Sometimes, it is useful to group several attributes and search them together, or
an attribute wants to be searched alone, so a concept of search domain is
introduced. A search domain is supported by a search engine of its own, so each
can have its own analyzer and other options. These can be passed in as
`:search-domains` when opening the DB connection.

A fulltext search enabled attribute can then declare the search domains it
belongs to in the schema with `:db.fulltext/domains` vector. In addition, if
`:db.fulltext/autoDomain` is true, an attribute specific domain is created for
the attribute, enabling the same syntax and semantics as that of the Datomic
`fulltext` function. Otherwise, `fulltext` function's option map argument can
take a `:domains` key to specify the domains this search query will be posted to,
and the results are concatenated together.

```clojure
  (let [analyzer (su/create-analyzer
                   {:token-filters [(su/create-stemming-token-filter
                                      "english")]})
        conn     (d/create-conn
                   "/tmp/test-db"
                   {:a/id     {:db/valueType :db.type/long
                               :db/unique    :db.unique/identity}
                    :a/string {:db/valueType        :db.type/string
                               :db/fulltext         true
                               :db.fulltext/domains ["da"]}
                    :b/string {:db/valueType           :db.type/string
                               :db/fulltext            true
                               :db.fulltext/autoDomain true
                               :db.fulltext/domains    ["db"]}}
                   {:search-domains {"da" {:analyzer analyzer}
                                     "db" {}}})
        sa       "The quick brown fox jumps over the lazy dogs"
        sb       "Pack my box with five dozen liquor jugs."
        sc       "How vexingly quick daft zebras jump!"
        sd       "Five dogs jump over my fence."]

    (d/transact! conn [{:a/id 1 :a/string sa :b/string sb}
                       {:a/id 2 :a/string sc :b/string sd}])

    (d/q '[:find [?v ...]
           :in $ ?q
           :where
           [(fulltext $ :b/string ?q) [[?e _ ?v]]]]
         (d/db conn) "jump")
    ;;=> ["Five dogs jump over my fence."]

    (d/q '[:find ([?v ...])
           :in $ ?q
           :where
           [(fulltext $ ?q {:domains ["da" "db"]}) [[?e _ ?v]]]]
         (d/db conn) "jump")
    ;;=> ["The quick brown fox jumps over the lazy dogs"
    ;;=>  "How vexingly quick daft zebras jump!"
    ;;=>  "Five dogs jump over my fence.]"

    (d/close conn)

```

To customize filtering the search results further, a `doc-filter` function can
be supplied in the search option, that takes the `doc-ref` and return true or
false, e.g. `{:doc-filter #(= (:a %) :text)}` will only return datoms that have
attribute `:text`. Or one can choose to put this constraint in the Datalog where
clause, but this will be less efficient.

### Custom Search

The search feature can be customized at indexing time and at query time.

#### Custom analyzer

When creating the search engine, an `:analyzer` option can be used to supply
an analyzer function that takes a document as string, and output a list of `[term
position offset]`. `:query-analyzer` option is for analyzing query.

Some common utility functions for creating analyzers are also provided in
`datalevin.search-utils`  namespace: stemming, stop words, regular expression,
ngrams, prefix, and so on.

#### Search options

See documentation for `search` function to see available options that can be
passed at run time to customize search: domains, top-k, proximity expansion factor, results display, and so on.

## Implementation

As mentioned, the search engine is implemented from scratch, see [blog](https://yyhh.org/blog/2021/11/t-wand-beat-lucene-in-less-than-600-lines-of-code/). Instead of
using a separate storage, the search engine indices are stored in the same
file along with other database data, i.e. all Datalevin data is stored in a
single [LMDB](http://www.lmdb.tech/doc/) data file. This improves cache locality
and reduces the complexity of managing data.

### Indexing

The search engine indices are stored in one inverted list and two key-value maps.
In addition to information about each term and each document, the positions of term
occurrences in the documents are also stored to support match highlighting and
proximity query.

Specifically, the following LMDB sub-databases are created for search supposes:

* `terms`: map of term -> `term-info`.
* `docs`: map of document id -> document reference and document norm.
* `positions`: inverted lists of term id and document id -> positions and offsets
  of the term in the document. This storage is optional, where user set it with
  `:index-position?` option.

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

Critically, during this process, we remove a candidate as soon as one of the
following two conditions is met:

* this document is not going to appear in the required number of inverted lists,
  based on the aforementioned mathematical property. This is the main innovation
  of the T-Wand algorithm.

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

### Term Proximity Scoring

When the user elects to enable term positions indexing, it is beneficial to
utilize the positional information to enable term proximity scoring to enhance
precision of top results. This reflects the intuition that the closer the query
terms are placed together in a document, the more relevant the document
might be. We enable term proximity scoring when `:index-position?` is set to
`true`.

Instead of replacing term frequency by proximity score [5], which is
relatively expensive to calculate, or combining the proximity score with the
tf-idf score [4], which faces the difficult problem of determining the relative
weights of the two scores that may require machine learning, we decide to
perform a two stage procedure: we search by tf-idf based scoring first as usual,
then calculate proximity score only for the top results, and finally produce the
top `k` results according to the proximity score.

For the first tf-idf stage, instead of producing top `k` results only, we produce top
`m * k` results, where `m` is user configurable as `:proximity-expansion`
(default is `2`) search option. This parameter reflects a search quality vs. time
trade-off. The larger is `m`, the better is the search quality, while the search
time would be longer.

A span based proximity scoring algorithm is used to calculate the proximity
contribution of individual terms, and they are then plugged into the Okapi
ranking function to arrive at the final score [5].

## Benchmark

The details of benchmark comparison with Lucene is [here](https://github.com/juji-io/datalevin/tree/master/search-bench). The
summary is that Datalevin search engine beats Lucene in search speed, but lags in write speed, as expected.

## References

[1] Broder, A.Z., Carmel, D., Herscovici, M, Soffer,A., and Zien, J..  Efficient
query evaluation using a two-level retrieval process. In Proceedings of the
twelfth international conference on Information and knowledge management (CIKM
'03). pp.426–434.

[2] Manning, C.D., Raghavan, P. and Schütze, H. Introduction to Information
Retrieval, Cambridge University Press. 2008.

[3] Okazaki, N. and Tsujii, J., Simple and Efficient Algorithm for Approximate
Dictionary Matching. Proceedings of the 23rd International Conference on
Computational Linguistics (COLING '10), 2010, pp. 851-859.

[4] Rasolofo, Y., & Savoy, J.. Term proximity scoring for keyword-based
retrieval systems. In Advances in Information Retrieval: 25th European
Conference on IR Research, (ECIR '03), pp. 207-218.

[5] Song, R., Taylor, M. J., Wen, J. R., Hon, H. W., & Yu, Y. Viewing term
proximity from a different perspective. In Advances in Information Retrieval:
30th European Conference on IR Research, (ECIR '08), pp. 346-357.
