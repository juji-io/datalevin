# Join Order Benchmark (JOB)

[JOB](https://github.com/gregrahn/join-order-benchmark) is a standard SQL
benchmark that stresses database query optimizers, as described in the paper:

Viktor Leis, et al. "How Good Are Query Optimizers, Really?" PVLDB Volume 9,
No. 3, 2015 [pdf](http://www.vldb.org/pvldb/vol9/p204-leis.pdf)

## Data Set

The data set is originally from Internet Movie Database
[IMDB](https://developer.imdb.com/non-commercial-datasets/), downloaded in
May 2013. The exported CSV files of the data set can be downloaded from
http://homepages.cwi.nl/~boncz/job/imdb.tgz

Unpack the downloaded `imdb.tgz` to obtain 21 CSV files, totaling 3.7 GiB. Each
CSV file is a table, with the biggest table having over 36 million rows, while
the smallest has only 4 rows.

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

We translated the SQL schema to equivalent Datalevin schema, shown in
`datalevin-bench.core` namespace. The attribute names follow Clojure convention.

The same set of CSV files are transformed into datoms and loaded into
Datalevin by uncommenting and running `(def db ...)`. This loads 277,878,514
dotams into Datalevin.

## Queries

The `queries` directory contains 113 SQL queries for this benchmark. These
queries all involve more than 5 tables and often have 10 or more where clauses.

We manually translated the SQL queries to equivalent Datalevin queries, and
manually verified that PostgreSQL and Datalevin produce exactly the same results
for the same queries (note 1).

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

These software were tested on Intel Core i7-6850K CPU @ 3.60GHz, 64GB RAM, 1 TB
SSD drive, running Ubuntu 22.04:

* PostgreSQL 16.3 (Ubuntu 16.3-1.pgdg22.04+1) on x86_64-pc-linux-gnu, compiled
by gcc (Ubuntu 11.4.0-1ubuntu1~22.04) 11.4.0, 64-bit
* Datalevin latest in this repository

For PostgreSQL, run `postgres-time` script to run all queries. The results are
PostgreSQL's own `EXPLAIN ANALYZE` reports written into a CSV file
`postgres_onepass_time.csv`, in order to remove the impact of client/server
communication and other unrelated factors.

```bash
./postgres-time
```

For Datalevin, both `lein` and `clj` build tools are needed, the former is for
building the main project, and the later for running the benchmark. Same as the
above, the results are `explain` reports written into a CSV file
`datalevin_onepass_time.csv`.

```bash
cd ../..
lein run   # this runs essential tests to ensure a good build
cd benchmarks/JOB-bench
clj -Xbench
```

## Results



Note 1: Manual verification is needed because all the queries return `MIN` results,
  but Postgresql version 16's `MIN` function result is not consistent with UTF-8
  encoding, and not even with its own `<` or `LEAST` function results. For
  example, for query 1b, Postgresql 16 `MIN(mc_note)` returns `"(as Grosvenor
  Park)"`, but `"(Set Decoration Rentals)"` should be the correct answer based
  on UTF-8 encoding, as `SELECT LEAST('(as Grosvenor Park)','(Set Decoration
  Rentals)');` returns `"(Set Decoration Rentals)"`. So we removed `MIN()` to
  obtain full results in order to verify that Datalevin produces exactly the
  same results as Postgresql before running `MIN()`.
