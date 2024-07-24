# Benchmarks

The following benchmarks are included:

* [Datascript](datascript-bench) is the benchmark inherited from Datascript,
  that compares Datascript, Datomic and Datalevin on Datalog transaction and
  queries, as well as rule processing using a synthetic data set.
* [Join Order Benchmark](JOB-bench) compare Datalevin and Postgresql on JOB
  benchmark. This is a standard benchmark for SQL databases that consists of
  realistic data set (IMDB) and complex queries, and puts a high demand on the
  quality of query optimizer. Datalevin run Datalog queries translated from
  equivalent SQL.
* [Math Genealogy](math-bench)  compares Datascript, Datomic and Datalevin on
  recursive rule processing using Math Genealogy data set.
* [Search](search-bench) compares Lucene and Datalevin on full-text search
  performance using Wikipedia data set.
