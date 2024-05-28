# Join Order Benchmark (JOB)

[JOB](https://github.com/gregrahn/join-order-benchmark) is a standard SQL
benchmark that stresses database query optimizers, as described in the paper:

Viktor Leis, Andrey Gubichev, Atans Mirchev, Peter Boncz, Alfons Kemper, and
Thomas Neumann. "How Good Are Query Optimizers, Really?" PVLDB Volume 9, No. 3,
2015 [pdf](http://www.vldb.org/pvldb/vol9/p204-leis.pdf)

## Data Set

The data set is originally from
[IMDB](https://developer.imdb.com/non-commercial-datasets/), downloaded in
May 2013.

The exported CSV files of the data set can be downloaded from
http://homepages.cwi.nl/~boncz/job/imdb.tgz

Unpack the downloaded `imdb.tgz` into 21 CSV files.

### Postgresql

Assume a Postgresql server is running.

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

Finally create indices: `psql -f data/fkindexes.sql`


### Datalevin


## Queries

The original benchmark contains 113 SQL queries.
