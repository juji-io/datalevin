# Join Order Benchmark (JOB)

[JOB](https://github.com/gregrahn/join-order-benchmark) is a standard SQL
benchmark that stresses database query optimizers, as described in the
influential paper:

Viktor Leis, et al. "How Good Are Query Optimizers, Really?" PVLDB Volume 9, No. 3, 2015 [pdf](http://www.vldb.org/pvldb/vol9/p204-leis.pdf)

This benchmark uses real world data set, and is extremely challenging, compared
with other benchmarks, such as TPC series. We ported this benchmark to Datalog
to see how Datalevin handle complex queries.

## Data Set

The data set is originally from Internet Movie Database
[IMDB](https://developer.imdb.com/non-commercial-datasets/), downloaded in
May 2013. The exported CSV files of the data set can be downloaded from
http://homepages.cwi.nl/~boncz/job/imdb.tgz

Unpack the downloaded `imdb.tgz` to obtain 21 CSV files, totaling 3.7 GiB. Each
CSV file is a table. The data is highly normalized, with many foreign key
references. The biggest table has over 36 million rows, while the smallest
has only 4 rows.

### PostgreSQL

Assume a PostgreSQL server is running.

First import the schema: `psql -f data/schema.sql`

Then copy the CSV data into tables, e.g. in psql:

```
\copy aka_name from aka_name.csv csv escape '\'
\copy aka_title from aka_title.csv csv escape '\'
\copy cast_info from cast_info.csv csv escape '\'
\copy char_name from char_name.csv csv escape '\'
\copy comp_cast_type from comp_cast_type.csv csv escape '\'
\copy company_name from company_name.csv csv escape '\'
\copy company_type from company_type.csv csv escape '\'
\copy complete_cast from complete_cast.csv csv escape '\'
\copy info_type from info_type.csv csv escape '\'
\copy keyword from keyword.csv csv escape '\'
\copy kind_type from kind_type.csv csv escape '\'
\copy link_type from link_type.csv csv escape '\'
\copy movie_companies from movie_companies.csv csv escape '\'
\copy movie_info from movie_info.csv csv escape '\'
\copy movie_info_idx from movie_info_idx.csv csv escape '\'
\copy movie_keyword from movie_keyword.csv csv escape '\'
\copy movie_link from movie_link.csv csv escape '\'
\copy name from name.csv csv escape '\'
\copy person_info from person_info.csv csv escape '\'
\copy role_type from role_type.csv csv escape '\'
\copy title from title.csv csv escape '\'

```

Finally create foreign key indices: `psql -f data/fkindexes.sql`

### Datalevin

We translated the SQL schema to equivalent Datalevin schema, as shown in
`datalevin-bench.core` namespace. The attribute names follow Clojure convention.

The same set of CSV files are transformed into datoms and loaded into
Datalevin by uncommenting and running `(def db ...)`. This loads 277,878,514
datoms into Datalevin.

## Queries

The `queries` directory contains 113 SQL queries for this benchmark. These
queries all involve more than 5 tables and often have 10 or more where clauses,

We manually translated the SQL queries to equivalent Datalevin queries, and
manually verified that PostgreSQL and Datalevin produce exactly the same results
for the same query (note 1).

For example, the query 1b of the benchmark:

```SQL
SELECT MIN(mc.note) AS production_note,
       MIN(t.title) AS movie_title,
       MIN(t.production_year) AS movie_year
FROM company_type AS ct,
     info_type AS it,
     movie_companies AS mc,
     movie_info_idx AS mi_idx,
     title AS t
WHERE ct.kind = 'production companies'
  AND it.info = 'bottom 10 rank'
  AND mc.note NOT LIKE '%(as Metro-Goldwyn-Mayer Pictures)%'
  AND t.production_year BETWEEN 2005 AND 2010
  AND ct.id = mc.company_type_id
  AND t.id = mc.movie_id
  AND t.id = mi_idx.movie_id
  AND mc.movie_id = mi_idx.movie_id
  AND it.id = mi_idx.info_type_id;
```
is translated into the equivalent Datalevin query:

```Clojure
'[:find (min ?mc.note) (min ?t.title) (min ?t.production-year)
  :where
  [?ct :company-type/kind "production companies"]
  [?it :info-type/info "bottom 10 rank"]
  [?mc :movie-companies/note ?mc.note]
  [(not-like ?mc.note "%(as Metro-Goldwyn-Mayer Pictures)%")]
  [?t :title/production-year ?t.production-year]
  [(<= 2005 ?t.production-year 2010)]
  [?mc :movie-companies/company-type ?ct]
  [?mc :movie-companies/movie ?t]
  [?mi :movie-info-idx/movie ?t]
  [?mi :movie-info-idx/info-type ?it]
  [?t :title/title ?t.title]]
```

Most queries in the benchmark are more complex than this example.

## Run Benchmark

These software were tested on a MacBook Pro 16 inch Nov 2023, Apple M3 Pro chip,
6 performance core and 6 efficiency core, 36GB memory, 1TB SSD disk:

* Homebrew PostgreSQL@16
* Datalevin latest in this repository

All software were in default configuration without any tuning.

For PostgreSQL, first run `postgres-time` bash script to warm up. This runs all
113 queries one after another. Then run the script again. The results of the
second run were reported.

The numbers were extracted from PostgreSQL's own `EXPLAIN ANALYZE` results, and
written into a CSV file `postgres_onepass_time.csv`, in order to remove the
impact of client/server communication and other unrelated factors.

```bash
./postgres-time
./postgres-time
```

For Datalevin, both `lein` and `clj` build tools are needed, the former is for
building the main project, and the later for running the benchmark.

Same as the above, we run `clj -Xbench` once to warm up. Then run it again to report the
results. The numbers were extracted from `explain` function results and written
into a CSV file `datalevin_onepass_time.csv`.

```bash
cd ../..
lein run   # this runs essential tests to ensure a good build
cd benchmarks/JOB-bench
clj -Xbench
clj -Xbench
```

We did not run the same query repeatedly and then compute the median or average
for the query, because that would be mainly benchmarking caching behavior of the
databases, as both have various caches. In this test, we are mainly interested
in the behavior of query optimizer.

## Results

We look at the timing results. The total query time can be divided into two
parts: query planning time and plan execution time. Raw data files are the
following:

* [PostgreSQL](postgres_onepass_time.csv)
* [Datalevin](datalevin_onepass_time.csv)

### Wall clock time

Table below shows how long it took for the systems to finish running 113 queries
in this benchmark:

|DB|Wall Clock Time (seconds)|
|---|---|
|PostgreSQL|204|
|Datalevin|156|

Datalevin is about 1.3X faster than PostgreSQL overall in running these complex
queries.

### Planning time

Numbers below are in milliseconds.

|DB|Mean|Min|Median|Max|
|---|---|---|---|---|
|PostgreSQL|9.8 |0.8 |3.6 |44.1 |
|Datalevin|76.6 |5.5 |66.5 |505.8 |

Datalevin spent almost an order of magnitude more time than PostgreSQL on
query planning.

### Execution time

Numbers below are in milliseconds.

|DB|Mean|Min|Median|Max|
|---|---|---|---|---|
|PostgreSQL|1752.6 |3.0 |174.6 |55251.3 |
|Datalevin|1251.5 |0.1 |184.8 |27261.4 |

On average, Datalevin is 1.4X faster than PostgreSQL in plan execution.
The median times are similar, the differences are mainly in
extrema. The best plan in Datalevin can be an order of magnitude faster, while
the worst plan in PostgreSQL can be 2X slower than the worst plan in Datalevin.

## Remarks

For these complex queries, planning time is insignificant compared with the long
execution time. While PostgreSQL is extremely fast in coming up with its plans,
the quality of the plans seems to be worse than that of Datalevin, as it
routinely misses the best plans and occasionally come up with extremely bad
plans that took a long time to run. On the other hand, Datalevin spend more time
in query planning, and it manages to find some very good plans while fares
better when the planning algorithm misses the mark.

PostgreSQL's planning algorithm is based on statistics collected by separate
processes, so it is more expensive to maintain, at the same time, less
effective, due to its strong statistical assumptions that are almost never true
in real data. Datalevin's planning algorithm holds much weaker statistical
assumptions, and is based on counting and sampling at query time, while it is
more expensive to plan, the generated plans are of higher quality, resulting in
better overall query performance in handling complex queries.

Note 1: Manual verification is needed because all the queries return `MIN` results,
  but PostgreSQL version 16's `MIN` function result is not consistent with UTF-8
  encoding, and not even with its own `<` or `LEAST` function results. For
  example, for query 1b, Postgresql 16 `MIN(mc_note)` returns `"(as Grosvenor
  Park)"`, but `"(Set Decoration Rentals)"` should be the correct answer based
  on UTF-8 encoding, as `SELECT LEAST('(as Grosvenor Park)','(Set Decoration
  Rentals)');` returns `"(Set Decoration Rentals)"`. So we removed `MIN()` to
  obtain full results in order to verify that Datalevin produces exactly the
  same results as Postgresql before running `MIN()`.
