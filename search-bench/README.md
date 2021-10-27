# Datalevin Search Benchmark

## Data

The data source is [Wikipedia database backup dump](https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2), over 18GB XML compressed.

For our purpose, we use
[WikiExtractor](https://github.com/attardi/wikiextractor), a python script, to
extract all articles into a JSON file. Furthermore, we use [jq](https://stedolan.github.io/jq/) to remove empty articles and strip away unneeded meta data, just leaving article content as the text and url as the identifier.

```console
wget https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2

wikiextractor -o - --json --no-templates enwiki-latest-pages-articles.xml.bz2 | jq -s '.[] | select(.text != "") | {url, text}' > wiki.json
```
This may take a few hours to run, depending on your hardware. It produces a JSON
file containing 6,351,519 Wikipedia articles with more than 1.6 billion words, totaling 15GB.

## Queries

The test queries are downloaded from [TREC 2009 Million Query
Track](https://trec.nist.gov/data/million.query09.html). We
remove unnecessary meta content and leaves query text only:

```console
wget https://trec.nist.gov/data/million.query/09/09.mq.topics.20001-60000.gz
gzip -d 09.mq.topics.20001-60000.gz
mv 09.mq.topics.20001-60000 queries40k.txt
sed -i -e 's/\([0-9]\+\)\:[0-9]\://g' queries40k.txt
```
This produces a text file containing 40000 queries, totaling 685KB.

## Test Conditions

The same conditions were applied to both Datalevin and Lucene:

* A single thread was used to add documents when indexing, and also for querying.

* Indices were stored on disk.

* Punctuation and stop words were removed. The same stop words list was used in
  both Datalevin and Lucene. No stemming was performed.

* Query terms were `AND` together.

* Top 10 results were returned for each query. The results included highlight information.

* Tests were conducted on Intel Core i7-6850K CPU @ 3.60GHz, 64GB RAM, 1TB SSD,
running Ubuntu 20.04 and OpenJDK 17 (2021-09-14), with Clojure 1.10.3.

Run benchmarks:

```
clj -X:datalevin
clj -X:lucene
```

The benchmarks took a few hours to run.

## Result

|   | Datalevin | Lucene |
|----|--------|--------|
| Indexing time (hrs)  | 2.24  | 0.33  |
| Index size (GB)  | 98  |  13      |





## Remark

Datalevin store search indices in a LMDB database. Since LMDB supports a single
writer at a time, having multiple indexing threads does little to improve
the data ingestion speed. Therefore, some use cases of Lucene would not be
appropriate for Datalevin search engine, e.g. log ingestion.
