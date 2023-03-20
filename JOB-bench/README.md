# Join Order Benchmark (JOB)

This is a standard benchmark that uses real world data and stresses database
query optimizers, as described in [1].

The queries can be found [here](https://github.com/gregrahn/join-order-benchmark).

The data set can be downloaded [here](http://homepages.cwi.nl/~boncz/job/imdb.tgz). Un-tar to get the CSV data files. It also contains a `schematext.sql` file.

## Postgresql

To load these data into Postgresql, do the following:

```
sudo -u postgres psql

postgres=# \i /path/schematext.sql
postgres=# \copy aka_name from /path/aka_name.csv csv escape '\'
postgres=# \copy aka_title from /path/aka_title.csv csv escape '\'
...
```
Do this for all the CSV files. Replace `/path` with where the files are.

If you get `Permission denied`, make sure that the whole path is accessible by
`postgres` user, e.g. `chmod 711 /path`.

[1] Viktor Leis, Andrey Gubichev, Atans Mirchev, Peter Boncz, Alfons Kemper, and
Thomas Neumann. "How Good Are Query Optimizers, Really?" PVLDB Volume 9, No. 3,
2015 [pdf](http://www.vldb.org/pvldb/vol9/p204-leis.pdf)
