# Datalevin Search Benchmark

## Test Data

The data source is [Wikipedia database backup dump](https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2), over 18GB XML compressed.

For our purpose, we use
[WikiExtractor](https://github.com/attardi/wikiextractor), a python script, to
extract all articles into a JSON file. Furthermore, we use
[jq](https://stedolan.github.io/jq/) to remove articles containing less than 500
characters (e.g. those only refer to other articles) and strip away unneeded
meta data, just leaving article content as the text and url as the identifier.
To speed up things a little, about half of articles were sampled (those with odd
number of characters) and used in the subsequent tests.

```console
wget https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2

wikiextractor -o - --json --no-templates enwiki-latest-pages-articles.xml.bz2 |
jq -s '.[] | select((.text | length) > 500) | {url, text}' > wiki.json

cat wiki.json | jq -s '.[] | select(((.text | length) % 2) != 0)' > wiki-odd.json
```
This may take a few hours to run, depending on your hardware. It produces a JSON
file containing about 2 million articles with about 800 million words, totaling 6.8GB.

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

* Indices were stored on disk.

* Punctuation and stop words were removed. The same stop words list was used in
  both Datalevin and Lucene. No stemming was performed.

* Query terms were `AND` together.

* Top 10 results were returned for each query, only URLs of the articles were returned.

* Concurrent searches were performed by submitting each search query to a thread pool.

* Tests were conducted on an Intel Core i7-6850K CPU @ 3.60GHz with 6 cores, 64GB RAM, 1TB SSD, running Ubuntu 20.04 and OpenJDK 17 (2021-09-14), with Clojure 1.10.3.

Run benchmarks:

```
clj -X:datalevin
clj -X:lucene
```

The benchmarks took a few hours to run.

## Result

|   | Datalevin | Lucene |
|----|--------|--------|
| Indexing time (hrs)  | 0.98  | 0.15  |
| Index size (GB)  | 41  |  6.3      |


Indexing took 3558.14 seconds
mean: 70
10 percentile: 0
median: 14
75 percentile: 53
90 percentile: 148
95 percentile: 267
99 percentile: 981
99.9 percentile: 3394
max: 12120

Querying took 193.10 seconds
mean: 57
10 percentile: 0
median: 16
75 percentile: 55
90 percentile: 139
95 percentile: 233
99 percentile: 644
99.9 percentile: 1718
max: 5926

Querying took 181.93 seconds
mean: 54
10 percentile: 0
median: 14
75 percentile: 52
90 percentile: 132
95 percentile: 220
99 percentile: 599
99.9 percentile: 1652
max: 5744

Indexing took 3531.95 seconds
Querying took 155.46 seconds
mean: 46
10 percentile: 0
median: 15
75 percentile: 50
90 percentile: 116
95 percentile: 188
99 percentile: 459
99.9 percentile: 1096
max: 3467

Querying took 138.55 seconds
mean: 41
10 percentile: 0
median: 12
75 percentile: 44
90 percentile: 108
95 percentile: 171
99 percentile: 405
99.9 percentile: 947
max: 3355

Querying took 114.72 seconds
mean: 34
10 percentile: 0
median: 9
75 percentile: 33
90 percentile: 83
95 percentile: 137
99 percentile: 379
99.9 percentile: 1026
max: 3749

Querying took 110.02 seconds
mean: 32
10 percentile: 0
median: 9
75 percentile: 31
90 percentile: 78
95 percentile: 132
99 percentile: 373
99.9 percentile: 1028
max: 3637

Lucene crashed when submitting search tasks to a default `(Executors/newWorkStealingPool)`, which uses a thread pool that has the same number of threads as the number
of virtual cores (12 in our case).

Lucene query parser seems to be rather picky. We had to manually fix some
queries for the test to run. E.g. Lucene crashed with the query "\"ground beef
recipes\'" for the mismatched quotes.

## Remarks

The design of Lucene and Datalevin search engine differs significantly, lending
to different use cases.

Lucene indices data into multiple independent segments, so it supports great
parallelism while indexing data. It also compress the data very well and has
very small index size. However, because search results need to be merged from
multiple segments during query, complicating the code path, results in poor
query concurrency. It seems that significant engineering effort is needed to
make Lucene suitable for fielding a large number end users searching
simultaneously. In addition, the added documents are not immediately searchable.
It is therefore suitable for data analytic work, which it excels, but would be
challenging to support online operations.

Datalevin is designed as an operational database, not for analytics. Datalevin
search engine stores search indices by transacting them into a LMDB database. The
documents are immediately searchable once added. Since LMDB has a single writer,
having multiple indexing threads does little to improve the data ingestion
speed. In addition, data in Datalevin is not currently compressed, so the index
size is significantly larger. Therefore, some Lucene's common use cases would
not be appropriate for the Datalevin search engine, e.g. log ingestion. On the
other hand, Datalevin query performance scales linearly with the number of CPU
cores, and performs on par with Lucene on a single core.

Obviously, Lucene has thousands of man hour behind its development and is extremely
configurable, while Datalevin search engine is embedded in a new database with
"simplicity" as its motto, it is currently just less than 500 lines of Clojure
code, so it does not offer any bells and whistles but the core search features.
On the other hand, Datalevin search engine performs well and is nicely
integrated with other Datalevin database features, so it supports our goal of
simplifying data access.
