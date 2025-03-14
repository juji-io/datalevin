# Datalevin Vector Indexing and Similarity Search

Datalevin has specialized index for equal-length dense numeric numbers
(vectors), and supports efficient approximate nearest neighbors search
on these vectors based on various similarity metrics.

This functionality is developed on the basis of
[usearch](https://github.com/unum-cloud/usearch) library, which is an
implementation of Hierarchical Navigable Small World (HNSW) graph algorithm [1].
usearch leverages vector instructions in CPUs and is used in several OLAP
stores, such as clickhouse, DuckDB, and so on.

With this feature, Datalevin can be used as a vector database to support
applications such as semantic search, image search, retrieval augmented
generation (RAG), and so on. This feature is currently available on Linux and
MacOS on both x86_64 and arm64 CPUs.

## Configurations

These configurable options can be set when creating the vector index:

* `:dimensions`, the number of dimensions of the vectors. **Required**, no
  default.

* `:metric-type` is the type of similarity metrics. Custom metric may be
  supported in the future.
  - `:euclidean`   [Euclidean
    distance](https://en.wikipedia.org/wiki/Euclidean_distance), length of line
    segment between points. This is the default.
  - `:cosine` [Cosine
    similarity](https://en.wikipedia.org/wiki/Cosine_similarity)i, angle between
    vectors.
  - `:dot-product` [Dot
    product](https://en.wikipedia.org/wiki/Dot_product), sum of element-wise
    products.
  - `:haversine` [Haversine
    formula](https://en.wikipedia.org/wiki/Haversine_formula), spheric
    great-circle distance.
  - `:divergence` [Jensen-Shannon
  divergence](https://en.wikipedia.org/wiki/Jensen%E2%80%93Shannon_divergence),
  symmetrized and smoothed version of Kullback-Leibler divergence of probability
  distributions.
  - `:pearson` [Pearson
    distance](https://en.wikipedia.org/wiki/Distance_correlation), one minus
    normalized Pearson correlation coefficient.
  - `:jaccard` [Jaccard index](https://en.wikipedia.org/wiki/Jaccard_index), set
    intersection over union.
  - `:hamming` [Hamming distance](https://en.wikipedia.org/wiki/Hamming_distance),
    number of positions that are different.
  - `:tanimoto` [Tanimoto
  similarity](https://en.wikipedia.org/wiki/Chemical_similarity) Similarity of
  structures, e.g. chemical fingerprint, molecules structural alignment, etc.
  - `:sorensen` [Sorensen-Dice
    index](https://en.wikipedia.org/wiki/Dice-S%C3%B8rensen_coefficient), F1
    score, combination of precision and recall, or intersection over bitwise union.

* `:quantization` is the scalar type of the vector elements. More types may be
  supported in the future.
  - `:float`, 32 bits float point number, the default.
  - `:double`, 64 bits double float point number
  - `:float16`, 16 bit float, i.e. IEEE 754 floating-point binary16, we expect
    the representation is already converted into a Java Short bit format, e.g.
    using `org.apache.arrow.memory.util.Float16/toFloat16`, or
    `jdk.incubator.vector.Float16/float16ToRawShortBits`
  - `:int8`, 8 bit integer, the representation is the same as byte
  - `:byte`, raw byte

* `:connectivity`, the number of connections per node in the index graph
  structure, i.e. the `M` parameter in the HNSW paper [1]. The default is 16. The
  paper says:
  > A reasonable range of M is 5 to 48. Simulations show that smaller M
  > generally produces better results for lower recalls and/or lower dimensional
  > data, while bigger M is better for high recall and/or high dimensional data.
  > The parameter also defines the memory consumption of the algorithm (which is
  > proportional to M), so it should be selected with care.

* `:expansion-add`, the number of candidates considered when adding a new vector
  to the index, i.e. the `efConstruction` parameter in the paper. It controls
  the index construction speed/quality tradeoff: the larger the number, the
  better is the quality (but with diminished return after certain size) and the
  longer is the indexing time. The default is 128.

* `:expansion-search`, the number of candidates considered during search, i.e.
  the `ef` parameter in the paper. It controls the search speed/quality
  tradeoff, similar to the above. The default is 64.

## Usage

The vector indexing and search functionalities are available to use in all
supported modes: key-value store, Datalog store, embedded, client/server, or
Babashka pods.

### Standalone Vector Indexing and Search

Datalevin can be used as a standalone vector database. The standalone vector API
involves only a few functions: `new-vector-index`, `add-vec`, `remove-vec`, and
`search-vec`.

Each vector is identified with a `vec-ref` that can be any Clojure data (less
than maximal key size of 512 bytes), which should be semantically meaningful for
the application, e.g. a document id, an image id, or a tag.

Multiple vectors can be associated with the same `vec-ref`. For example, you may
have a `vec-ref` `"cat"` for many vectors that are the embedding of different cat
images. Another example: each chunk of a large document (e.g. for RAG) has its
own vector embedding and these vectors are all associated with the same document
id.

```Clojure
(require '[datalevin.core :as d])

;; Vector indexing uses a key-value store to store mapping between vec-ref and
;; vector id
(def lmdb (d/open-kv "/tmp/vector-db"))

;; Create the vector index. The dimensions of the vectors need to be specified.
;; Other options use defaults here.
(def index (d/new-vector-index lmdb {:dimensions 300}))

;; User needs to supply the vectors. Here we load some word2vec vectors from a
;; CSV file, each row contains a word, followed by the elements of the vector,
;; return a map of words to vectors
(def data (->> (d/read-csv (slurp "test/data/word2vec.csv"))
               (drop 1)
               (reduce
                 (fn [m [w & vs]] (assoc m w (mapv Float/parseFloat vs)))
                 {})))

;; Add the vectors to the vector index. `add-vec` takes a `vec-ref`, in this
;; case, a word; and the actual vector, which can be anything castable as a
;; Clojure seq, e,g. Clojure vector, array, etc.
(doseq [[w vs] data] (d/add-vec index w vs))

;; Search by a query vector. return  a list of `:top` `vec-ref` ordered by
;; similarity to the query vector
(d/search-vec index (data "king") {:top 2})
;=> ("king" "queen")
```

### Vector Indexing and Search in Datalog Store

Vectors can be stored in Datalog as attribute values of data type
`:db.type/vec`. Such attribute may have a property `:db.vec/domains`, indicating
which vector search domains the attribute should participate. By default, each
vector attribute is its own domain, with domain name the same as attribute name.

A query function `vec-neighbors` is provided to allow vector search in Datalog
queries. This function takes the DB, the query vector and an optional option map
(same as `search-vec`), and returns a sequence of matching datoms in the form of
`[e a v]` for easy destructuring, ordered by similarity to the query vector.

```Clojure
(let [conn (d/create-conn
                "/tmp/mydb"
                {:id        {:db/valueType :db.type/string
                             :db/unique    :db.unique/identity}
                 :embedding {:db/valueType :db.type/vec}}
                {:vector-opts {:dimensions  300
                               :metric-type :cosine}})]
    ;; use the same `data` defined above
    (d/transact! conn [{:id "cat" :embedding (data "cat")}
                       {:id "rooster" :embedding (data "rooster")}
                       {:id "jaguar" :embedding (data "jaguar")}
                       {:id "animal" :embedding (data "animal")}
                       {:id "physics" :embedding (data "physics")}
                       {:id "chemistry" :embedding (data "chemistry")}
                       {:id "history" :embedding (data "history")}])

    ;; attribute specific search, `?v` has the retrieved vectors.
    (set (d/q '[:find [?i ...]
                :in $ ?q
                :where
                [(vec-neighbors $ :embedding ?q {:top 4}) [[?e ?a ?v]]]
                [?e :id ?i]]
           (d/db conn) (data "cat")))

    ;; domains specific search
    (set (d/q '[:find [?i ...]
                :in $ ?q
                :where
                [(vec-neighbors $ :?q {:top 4 :domains ["embedding"]}) [[?e _ _]]]
                [?e :id ?i]]
           (d/db conn) (data "cat"))))
;;=>  #{"cat" "jaguar" "animal" "rooster"}
```

In the above example, we destructure the results into three variables,
`?e`, `?a` and `?v`.

The search can be specific to an attribute, or specific to a list of domains.


### Search Configurations

#### Search options

`search-vec` and `vec-neighbors` functions support an option map that can be
passed at run time to customize search:

* `:top`  is the number of results desired, default is 10
* `:display` sepcifies how results are displayed, could be one of these:
   - `:refs` only returns `vec-ref`, the default.
   - `:refs+dists` add distances to results.
* `:vec-filter` is a boolean function that takes `vec-ref` and determine if to
  return it.
* `:domains` specifies a list of domains to be searched (see below).

#### Vector search domains

Vectors can be added to different vector search domains, each corresponding to a
vector index of its own. The option map of `new-vector-index` has a `:domain`
that is a string value. If not specified, the default domain is `"datalevin"`.

When starting a  Datalog store, a `:vector-domains` option can be added to the
option map, and its value is a map from domain strings to the option maps of each
vector search domain, which is the same option map as that of `new-vector-index`.

A `:vector-opts` option can be passed to the Datalog store to give default
options in case `:vector-domans` are not given. Note that `:dimensions` option
is required for a vector index.

By default, each attribute with type `:db.type/vec` becomes its own domain
automatically. The domain name follows the following rules:

* for attribute without namespace, the name without `":"` is the domain name,
  e.g. `:vec` has domain name `"vec"`

* for attribute with namespace, in addition, `"/"` needs to be replaced by
`"_"`, e.g. `:name/embedding` has domain name `"name_embedding"`.  This is to
avoid conflict with directory separator `/` on POSIX systems.

Such attribute can also have a `:db.vec/domains` property that
list additional domains this attribute participates in. Users need to make sure
the domains an attribute participate in all have the same vector dimensions.

During search, `:domains` can be added to the option map to specify the
domains to be searched.

## References

[1] Malkov, Yu A and Yashunin, Dmitry A, "Efficient and robust approximate
nearest neighbor search using hierarchical navigable small world graphs", IEEE
transactions on pattern analysis and machine intelligence, 42:4, 2018.
