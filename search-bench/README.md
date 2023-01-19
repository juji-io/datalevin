# Datalevin Search Benchmark

To measure the performance of Datalevin search engine, we stacked it up against
the most prominent full-text search library in the Java world, the venerable
Apache Lucene.

## Test Data

The data source is [Wikipedia database backup dump](https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2), over 20GB XML compressed.

For our purpose, we use
[WikiExtractor](https://github.com/attardi/wikiextractor), a python script, to
extract all articles into a JSON file. Furthermore, we use
[jq](https://stedolan.github.io/jq/) to strip away unneeded
meta data, just leave the article text and the url as the identifier.

```console
wget https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2

wikiextractor -o - --json --no-templates enwiki-latest-pages-articles.xml.bz2 |
jq -s '.[] | select((.text | {url, text}' > wiki.json

```
This may take a few of hours to run, depending on your hardware. It produces a JSON
file containing over 16.8 million articles, totaling 15 GB.

## Test Queries

The test queries are downloaded from [TREC 2009 Million Query
Track](https://trec.nist.gov/data/million.query09.html). These are queries
typed by real users on the Web. We remove unnecessary meta content and leaves
query text only:

```console
wget https://trec.nist.gov/data/million.query/09/09.mq.topics.20001-60000.gz
gzip -d 09.mq.topics.20001-60000.gz
mv 09.mq.topics.20001-60000 queries40k.txt
sed -i -e 's/\([0-9]\+\)\:[0-9]\://g' queries40k.txt
```
This produces a text file containing 40000 queries, totaling 685KB.

## Test Conditions

The same conditions were applied to both Datalevin and Lucene:

* Both run in the default settings.

* A single thread was used to add all documents during indexing.

* Indices were stored on the same disk as input data.

* Punctuation and stop words were removed; the same stop words list was used in
  both Datalevin and Lucene; stemming was not performed, as per the
  default settings of Lucene `ClassicAnalyzer`.

* Query terms were `OR`ed together, per Lucene default. That is, documents matching
  any of the query terms are potential results.

* Top 10 results were returned for each query, only URLs of the articles were returned.

* Concurrent searches were performed by submitting each search query to a thread pool.

* Tests were conducted on an Intel Core i7-6850K CPU @ 3.60GHz with 6 cores (12
  virtual cores), 64GB RAM, 1TB SSD, running Ubuntu 20.04 and OpenJDK 17
  (2021-09-14), with Clojure 1.10.3.

Once the data are prepared, to run the benchmarks, first install
[clojure](https://clojure.org/guides/install_clojure), then run these commands:

```
clj -X:datalevin
clj -X:lucene
```

## Result

### Indexing

Indexing performance is in the following table.

|Measure   | Datalevin | Lucene |
|----|--------|--------|
| Index time (Minutes)  | 36  | 20  |
| Index size (GB)  | 6.8  |  14      |
| Index Speed (GB/Hour)  | 25  |  45      |

Lucene is close to twice as fast as Datalevin at bulk indexing. This is
expected, as Datalevin transacts the indices to the database. One benefit of
storing search index in a transactional database, is that the documents become
immediately searchable, while Lucene indexes documents in very large batches, so
they are not immediately searchable.

Datalevin's index is a single database file of 6.8 GB, while Lucene includes 168
files totaling 14 GB.

If `index-position?` option is turned on, Datalevin took 80 minutes to run, and
produced an index file of 78 GB.

### Searching

We varied the number of threads used for performing concurrent search. The
results are the following:

#### Search Latency (ms)

Here's the average and percentile break-down of search latency (ms) for one
search thread:

|Percentile | Datalevin | Lucene |
|----|--------|--------|
|median | 0.8 | 2.7 |
|75 |2.5 |    5.8           |
|90 |6.8 |  10.6            |
|95 |13.1 |  15.1      |
|99 |52.1 |   29.0           |
|99.9 |184.1 |  60.6            |
|max |619.9 | 148.5 |
|mean | 3.6 |    4.7  |

Search latency are similar across the number of threads, with slightly longer
time when more threads are used (with about 3 ms mean difference between 1 and
12 threads).

Datalevin is 75% faster on average, while more than 3 times faster at median
than Lucene.

Datalevin has much longer query time at long tails though, bringing down the
mean considerably. For queries with huge candidate set, the fixed cost of less optimized
code adds up to overcome the superiority in algorithm. For example, the slowest
query is "s", there's not much room for algorithmic cleverness here, the only
solution is brutal-force iterating the whole 3 million matching documents.


#### Search Throughput (Query per Second)

|Number of threads | Datalevin | Lucene |
|----|--------|--------|
|1 |276.5 | 212.9 |
|2 |554.7 |    414.9           |
|3 |804.4 |  608.8            |
|4 |1057.3 |  805.3      |
|5 |1277.8 |   995.9           |
|6 |1437.6 |  1126.5            |
|7 |1507.6 | 1180.9 |
|8 |1577.9 |    1213.6           |
|9 |1644.3 |  1233.7            |
|10 |1708.5 |  1301.9      |
|11 |1756.0 |   1345.1           |
|12 |1806.7 | 1372.3       |
|forkjoin |1815.6 | 1349.9       |

Datalevin has about 75% higher search throughput, as expected from the means.

For both search engines, search throughput grows linearly with the
number of real CPU cores, so search is a CPU bound task. Past the number of real
cores, adding more threads improves search throughput very little. Using a work
stealing thread pool does improve the throughput a bit than the maximum number of
fixed thread pool.

## Remarks

Lucene query parser seems to be rather picky. We had to manually fix some
queries for the test to run. E.g. Lucene crashed with the query "\"ground beef
recipes\'" for the mismatched quotes. The parser did not support concurrent
query. A new parser has to be created for each query, otherwise the program
would crash if there were more than one thread searching. Basically, some engineering
effort is required for it to be suitable for end user facing applications.

The design of Lucene and Datalevin search engine differs significantly, lending
to different use cases.

Datalevin is designed as an operational database. Datalevin search engine stores
search indices by transacting them into a LMDB database, and the documents are
immediately searchable. Since LMDB has a single writer, having
multiple indexing threads does little to improve the data ingestion speed. On
the other hand, Lucene writes to multiple independent index segments and support great
concurrency while indexing. Therefore, some use cases that Lucene family of
search engines excels in, such as log ingestion, are not suitable for Datalevin.

Lucene has perhaps hundreds of man-years behind its development and is extremely
configurable, while Datalevin search engine is embedded in a newly developed
database with *simplicity* as its motto, so it does not offer any bells and
whistles other than the core search features.

On the other hand, Datalevin search engine already performs very well, and still
has some low-hanging fruits for performance improvement. It will be nicely
integrated with other Datalevin database features to supports our goal of
simplifying data access.
