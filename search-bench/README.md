# Datalevin Search Benchmark

To measure the performance of Datalevin search engine, we stacked it up against
the most prominent full-text search library in the Java word, the venerable
Apache Lucene.

## Test Data

The data source is [Wikipedia database backup dump](https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2), over 18GB XML compressed.

For our purpose, we use
[WikiExtractor](https://github.com/attardi/wikiextractor), a python script, to
extract all articles into a JSON file. Furthermore, we use
[jq](https://stedolan.github.io/jq/) to remove articles containing less than 500
characters (e.g. those only refer to other articles) and strip away unneeded
meta data, just leaving article content as the text and url as the identifier.

```console
wget https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2

wikiextractor -o - --json --no-templates enwiki-latest-pages-articles.xml.bz2 |
jq -s '.[] | select((.text | length) > 500) | {url, text}' > wiki.json

```
This may take a few of hours to run, depending on your hardware. It produces a JSON
file containing over 6.3 million articles, totaling 15GB of text.

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

* A single thread was used to add documents during indexing.

* Indices were stored on the same disk as input data.

* Punctuation and stop words were removed; the same stop words list was used in
  both Datalevin and Lucene; stemming was not performed, as per the
  default settings of Lucene `ClassicAnalyzer`.

* Query terms were `OR`ed together, per Lucene default.

* Top 10 results were returned for each query, only URLs of the articles were returned.

* Concurrent searches were performed by submitting each search query to a thread pool.

* Tests were conducted on an Intel Core i7-6850K CPU @ 3.60GHz with 6 cores (12
  virtual cores), 64GB RAM, 1TB SSD, running Ubuntu 20.04 and OpenJDK 17 (2021-09-14), with Clojure 1.10.3.

Run benchmarks:

```
clj -X:datalevin
clj -X:lucene
```

The benchmarks took a couple of hours to run.

## Result

### Indexing

Indexing performance is in the following table.

|Measure   | Datalevin | Lucene |
|----|--------|--------|
| Index time (Hours)  | 0.99  | 0.32  |
| Index size (GB)  | 13  |  13      |
| Index Speed (GB/Hour)  | 13.1  |  40.6      |

Lucene is 3 times faster than Datalevin at indexing, and the resulting index has the same size.

### Searching

We varied the number of threads used for performing concurrent searching. The
results are the following:

|Number of threads |Measure   | Datalevin | Lucene |
|1 |Search Throughput (QPS)  | | 202 |
| |Search Latency Median (ms)  | |        |
| | &nbsp;&nbsp;&nbsp; 75 percentile  | |        |
| | &nbsp;&nbsp;&nbsp; 90 percentile  | |        |
| | &nbsp;&nbsp;&nbsp; 95 percentile  | |        |
| | &nbsp;&nbsp;&nbsp; 99 percentile  | |        |
|2 |Search Throughput (QPS)  | |        |
| |Search Latency Median (ms)  | |        |
| | &nbsp;&nbsp;&nbsp; 75 percentile  | |        |
| | &nbsp;&nbsp;&nbsp; 90 percentile  | |        |
| | &nbsp;&nbsp;&nbsp; 95 percentile  | |        |
| | &nbsp;&nbsp;&nbsp; 99 percentile  | |        |
|3 |Search Throughput (QPS)  | |        |
| |Search Latency Median (ms)  | |        |
| | &nbsp;&nbsp;&nbsp; 75 percentile  | |        |
| | &nbsp;&nbsp;&nbsp; 90 percentile  | |        |
| | &nbsp;&nbsp;&nbsp; 95 percentile  | |        |
| | &nbsp;&nbsp;&nbsp; 99 percentile  | |        |
|4 |Search Throughput (QPS)  | |        |
| |Search Latency Median (ms)  | |        |
| | &nbsp;&nbsp;&nbsp; 75 percentile  | |        |
| | &nbsp;&nbsp;&nbsp; 90 percentile  | |        |
| | &nbsp;&nbsp;&nbsp; 95 percentile  | |        |
| | &nbsp;&nbsp;&nbsp; 99 percentile  | |        |
|5 |Search Throughput (QPS)  | |        |
| |Search Latency Median (ms)  | |        |
| | &nbsp;&nbsp;&nbsp; 75 percentile  | |        |
| | &nbsp;&nbsp;&nbsp; 90 percentile  | |        |
| | &nbsp;&nbsp;&nbsp; 95 percentile  | |        |
| | &nbsp;&nbsp;&nbsp; 99 percentile  | |        |
|6 |Search Throughput (QPS)  | |        |
| |Search Latency Median (ms)  | |        |
| | &nbsp;&nbsp;&nbsp; 75 percentile  | |        |
| | &nbsp;&nbsp;&nbsp; 90 percentile  | |        |
| | &nbsp;&nbsp;&nbsp; 95 percentile  | |        |
| | &nbsp;&nbsp;&nbsp; 99 percentile  | |        |

Datalevin:

Querying with 0 threads took 221.56 secondsLatency (ms):
mean: 5
median: 2
75 percentile: 4
90 percentile: 10
95 percentile: 20
99 percentile: 79
99.9 percentile: 231
max: 782

Querying with 1 threads took 130.73 secondsLatency (ms):
mean: 6
median: 2
75 percentile: 5
90 percentile: 11
95 percentile: 25
99 percentile: 100
99.9 percentile: 288
max: 998

Querying with 2 threads took 96.72 secondsLatency (ms):
mean: 7
median: 2
75 percentile: 5
90 percentile: 12
95 percentile: 29
99 percentile: 113
99.9 percentile: 322
max: 966

Querying with 3 threads took 77.62 secondsLatency (ms):
mean: 7
median: 2
75 percentile: 5
90 percentile: 13
95 percentile: 30
99 percentile: 125
99.9 percentile: 335
max: 994

Querying with 4 threads took 67.46 secondsLatency (ms):
mean: 8
median: 2
75 percentile: 5
90 percentile: 14
95 percentile: 33
99 percentile: 140
99.9 percentile: 378
max: 1258

Querying with 5 threads took 60.70 secondsLatency (ms):
mean: 9
median: 2
75 percentile: 5
90 percentile: 15
95 percentile: 35
99 percentile: 156
99.9 percentile: 414
max: 1210

Querying with 6 threads took 58.82 secondsLatency (ms):
mean: 10
median: 2
75 percentile: 6
90 percentile: 16
95 percentile: 40
99 percentile: 176
99.9 percentile: 486
max: 1434

Querying with 7 threads took 58.46 secondsLatency (ms):
mean: 11
median: 2
75 percentile: 6
90 percentile: 19
95 percentile: 47
99 percentile: 197
99.9 percentile: 543
max: 2016

Querying with 8 threads took 56.48 secondsLatency (ms):
mean: 12
median: 3
75 percentile: 7
90 percentile: 20
95 percentile: 50
99 percentile: 212
99.9 percentile: 600
max: 2243

Querying with 9 threads took 55.23 secondsLatency (ms):
mean: 13
median: 3
75 percentile: 7
90 percentile: 22
95 percentile: 56
99 percentile: 232
99.9 percentile: 652
max: 2379

Querying with 10 threads took 54.44 secondsLatency (ms):
mean: 14
median: 3
75 percentile: 8
90 percentile: 24
95 percentile: 60
99 percentile: 251
99.9 percentile: 715
max: 2480

Querying with 11 threads took 54.06 secondsLatency (ms):
mean: 16
median: 3
75 percentile: 8
90 percentile: 26
95 percentile: 65
99 percentile: 273
99.9 percentile: 791
max: 2586

hyang@neo:~/workspace/datalevin/search-bench$ clj -X:datalevin

Datalevin:
Work stealing thread pool:

Querying with 0 threads took 47.53 secondsLatency (ms):
mean: 14
median: 3
75 percentile: 8
90 percentile: 24
95 percentile: 57
99 percentile: 230
99.9 percentile: 641
max: 1646

Lucene:
Fixed thread pool:

Querying with 1 threads took 191.59 seconds
Latency (ms):
mean: 4
median: 3
75 percentile: 6
90 percentile: 11
95 percentile: 16
99 percentile: 30
99.9 percentile: 60
max: 144

Querying with 2 threads took 97.10 seconds
Latency (ms):
mean: 4
median: 3
75 percentile: 6
90 percentile: 11
95 percentile: 16
99 percentile: 31
99.9 percentile: 61
max: 114

Querying with 3 threads took 66.15 seconds
Latency (ms):
mean: 4
median: 3
75 percentile: 6
90 percentile: 12
95 percentile: 17
99 percentile: 32
99.9 percentile: 63
max: 120

Querying with 4 threads took 49.76 seconds
Latency (ms):
mean: 4
median: 3
75 percentile: 6
90 percentile: 12
95 percentile: 17
99 percentile: 32
99.9 percentile: 63
max: 121

Querying with 5 threads took 39.95 seconds
Latency (ms):
mean: 4
median: 3
75 percentile: 6
90 percentile: 12
95 percentile: 17
99 percentile: 32
99.9 percentile: 63
max: 121

Querying with 6 threads took 33.92 seconds
Latency (ms):
mean: 5
median: 3
75 percentile: 6
90 percentile: 12
95 percentile: 17
99 percentile: 33
99.9 percentile: 65
max: 128

Querying with 7 threads took 33.18 seconds
Latency (ms):
mean: 5
median: 3
75 percentile: 7
90 percentile: 14
95 percentile: 20
99 percentile: 40
99.9 percentile: 78
max: 165

Querying with 8 threads took 32.46 seconds
Latency (ms):
mean: 6
median: 3
75 percentile: 8
90 percentile: 16
95 percentile: 22
99 percentile: 44
99.9 percentile: 85
max: 190

Querying with 9 threads took 31.81 seconds
Latency (ms):
mean: 7
median: 4
75 percentile: 9
90 percentile: 17
95 percentile: 25
99 percentile: 49
99.9 percentile: 101
max: 260

Querying with 10 threads took 32.66 seconds
Latency (ms):
mean: 8
median: 4
75 percentile: 10
90 percentile: 19
95 percentile: 28
99 percentile: 57
99.9 percentile: 133
max: 307

Querying with 11 threads took 31.28 seconds
Latency (ms):
mean: 8
median: 4
75 percentile: 11
90 percentile: 21
95 percentile: 30
99 percentile: 58
99.9 percentile: 117
max: 253

Querying with 12 threads took 30.69 seconds
Latency (ms):
mean: 9
median: 5
75 percentile: 11
90 percentile: 22
95 percentile: 32
99 percentile: 61
99.9 percentile: 124
max: 266
Work stealing thread pool:

Querying with 0 threads took 30.63 seconds
Latency (ms):
mean: 9
median: 5
75 percentile: 11
90 percentile: 22
95 percentile: 32
99 percentile: 62
99.9 percentile: 123
max: 272

## Remarks

The design of Lucene and Datalevin search engine differs significantly, lending
to different use cases.

Lucene query parser seems to be rather picky. We had to manually fix some
queries for the test to run. E.g. Lucene crashed with the query "\"ground beef
recipes\'" for the mismatched quotes. The parser did not support concurrent
querying. A new parser has to be created for each query, otherwise the program
would crash if there were more than one thread searching.

Datalevin is designed as an operational database, not for analytics. Datalevin
search engine stores search indices by transacting them into a LMDB database. The
documents are immediately searchable once added. Since LMDB has a single writer,
having multiple indexing threads does little to improve the data ingestion
speed. Obviously, Lucene has thousands of man hour behind its development and is
extremely configurable, while Datalevin search engine is embedded in a new database with
"simplicity" as its motto, it is currently just less than 600 lines of Clojure
code, so it does not offer any bells and whistles but the core search features.
On the other hand, Datalevin search engine performs well and is nicely
integrated with other Datalevin database features, so it supports our goal of
simplifying data access.
