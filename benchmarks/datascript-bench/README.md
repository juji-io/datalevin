# Datascript Benchmark

This directory contains the original benchmarks from
[Datascript](https://github.com/tonsky/datascript), with code for Datalevin
added, and related software updated to the latest versions.

The data consists of 20K entities of random person information, each entity has
5 attributes, totaling 100K datoms. The benchmarks load and query the data in a
in-memory setting.

## Run benchmarks

To run the benchmarks, you need to have both [leiningen](https://leiningen.org/) and [Clojure CLI](https://clojure.org/guides/deps_and_cli) installed on your system already. Then run these commands in the project root directory.

```
lein javac
cd bench
./bench.clj
```

For more comparisons with other alternatives, you may also consult [this fork](https://github.com/joinr/datalevinbench).

## Results

We ran this benchmark on a 2016 Intel Core i7-6850K CPU @ 3.60GHz with 6 cores, 64GB
RAM, 1TB SSD, running Debian 6.1.128-1 (2025-02-07) x86_64 GNU/Linux and OpenJDK
17, with Clojure 1.12.0.

Datomic peer binary (licensed under Apache 2.0) 1.0.7277 and Datascript 1.7.4
ran in memory only mode, as they require another database for persistence.
Datalevin 0.9.20 does not have a in memory only mode, so it writes to database
files on a RAM disk in `:nosync` mode.

Clojure code for benchmarked tasks are given below in Datalevin syntax.

### Write

Several write conditions are tested.

#### Init

This task loads the pre-prepared 100K `datoms `directly into the databases,
without going through the transaction process. Datomic does not have this
option.

```Clojure
      (d/init-db datoms (u/tmp-dir (str "bench-init" (UUID/randomUUID)))
                 schema {:kv-opts
                 {:flags #{:nordahead :writemap :nosync}}})
```

|DB|Init Latency (ms)|
|---|---|
|Datomic|N/A|
|Datascript|45.8|
|Datalevin|152.7|

Unsurprisingly, Datalevin loads datoms into a DB file slower than Datascript
loads the same data into memory. The difference ratio is over 3X.

#### Add-1

This transacts one datom at a time.

```Clojure
    (reduce
      (fn [db p]
        (-> db
            (d/db-with [[:db/add (:db/id p) :name      (:name p)]])
            (d/db-with [[:db/add (:db/id p) :last-name (:last-name p)]])
            (d/db-with [[:db/add (:db/id p) :sex       (:sex p)]])
            (d/db-with [[:db/add (:db/id p) :age       (:age p)]])
            (d/db-with [[:db/add (:db/id p) :salary    (:salary p)]])))
      (d/empty-db (u/tmp-dir (str "bench-add-1" (UUID/randomUUID)))
                  schema
                  {:kv-opts {:flags #{:nordahead :writemap :nosync}}})
      core/people20k)
```

|DB|Add-1 Latency (ms)|
|---|---|
|Datomic|3601.8|
|Datascript|1353.1|
|Datalevin|2263.6|

Compared with Init, transacting data is an order of magnitude more expensive,
since the transaction logic involves a great many number of reads and checks.

Datascript is the fasted and Datomic is the slowest.

Notice that the difference ratio between Datalevin and Datascript reduces from
3X to less than 2X, which suggests that Datalevin's transaction process is more efficient.
This is likely due to faster reads, as Datalevin and Datascript use the same
high level transaction code, only differ in data access layer.

#### Add-5

This transacts one entity (5 datoms) at a time.

```Clojure
          (reduce (fn [db p] (d/db-with db [p]))
            (d/empty-db (u/tmp-dir (str "bench-add-5" (UUID/randomUUID)))
                        schema
                        {:kv-opts {:flags #{:nordahead :writemap :nosync}}})
            core/people20k)
```

|DB|Add-5 Latency (ms)|
|---|---|
|Datomic|1084.0|
|Datascript|1306.1|
|Datalevin|1520.6|

Datomic does better in this condition, more than halves its transaction
time compared with Add-1 condition, now actually becomes the fastest.

Datascript's improvement over Add-1 is relatively unremarkable.

Datalevin improved over Add-1 a little more.

#### Add-all

This transacts all 100K datoms in one go.

```Clojure
    (d/db-with
      (d/empty-db (u/tmp-dir (str "bench-add-all" (UUID/randomUUID)))
                  schema
                  {:kv-opts {:flags #{:nordahead :writemap :nosync}}})
      core/people20k)
```

|DB|Add-all Latency (ms)|
|---|---|
|Datomic|411.7|
|Datascript|1238.8|
|Datalevin|1298.8|

Datomic again improves greatly.

Datalevin steadily improves its write speed compared with Add-5, now only a
little slower than Datascript.

#### Retract-5

This retracts one entity at a time.

```Clojure
(reduce (fn [db eid] (d/db-with db [[:db.fn/retractEntity eid]])) db eids)
```

|DB|Retract-5 Latency (ms)|
|---|---|
|Datomic|1729.6|
|Datascript|545.6|
|Datalevin|429.5|

Datalevin is the fastest in retracting data, more than 4X faster than Datomic.

Datascript is close to 3X faster than Datomic.

### Read

This benchmark only involves a few simple Datalog queries.

In order to have a fair comparison, all caching is disabled in Datalevin.

#### q1

This is the simplest query: find entity IDs of a bound attribute value,
returning about 4K IDs.

```Clojure
(d/q '[:find ?e
       :where [?e :name "Ivan"]]
      db100k)
```
|DB|Q1 Latency (ms)|
|---|---|
|Datomic|8.1|
|Datascript|0.61|
|Datalevin|0.67|

Datascript is the fastest for this query, with Datalevin slightly slower. Datomic is an order of magnitude slower.

#### q2

This adds an unbound attribute to the results.

```Clojure
    (d/q '[:find ?e ?a
           :where [?e :name "Ivan"]
                  [?e :age ?a]]
      db100k)
```
|DB|Q2 Latency (ms)|
|---|---|
|Datomic|14.7|
|Datascript|2.6|
|Datalevin|0.69|

Datalevin is now over 3X faster than Datascript. The reason is that Datalevin
performs a merge scan on the index instead of a hash join to get the values of
`:age`, so it processed a lot less intermediate results.

Datomic lags further behind Datalevin for this query.

#### q2-switch

This is the same query as q2, just switched the order of the two where clauses.

```Clojure
    (d/q '[:find ?e ?a
           :where [?e :age ?a]
                  [?e :name "Ivan"]]
      db100k)
```
|DB|Q2-switch Latency (ms)|
|---|---|
|Datomic|43.4|
|Datascript|5.8|
|Datalevin|0.66|

In this query, Datalevin has the same speed as q2, and is actually a little
faster. The query optimizer generates identical plans for q2 and q2-switch, so
the performance difference is perhaps due to better JVM warmness as this
benchmark runs latter. Switching the running order confirmed this suspicion.

Now Datascript slows down more than 2X compared with q2. Datomic is even worse,
slows down close to 3X. The reason is that these databases do not have a query
optimizer, so they blindly join one clause at a time. If the first clause
happens to have a huge size, the query slows down significantly.

#### q3

This adds a bound attribute, but its selectivity is low (0.5), so it merely cuts
the number of tuples in half.

```Clojure
    (d/q '[:find ?e ?a
           :where
           [?e :name "Ivan"]
           [?e :age ?a]
           [?e :sex :male]]
         db100k)
```

|DB|Q3 Latency (ms)|
|---|---|
|Datomic|20.2|
|Datascript|3.9|
|Datalevin|1.9|

Datalevin manages to be 2X faster than Datascript and over 10X faster than
Datomic for this query.

#### q4

This adds one more unbound attribute.

```Clojure
    (d/q '[:find ?e ?l ?a
           :where
           [?e :name "Ivan"]
           [?e :last-name ?l]
           [?e :age ?a]
           [?e :sex :male]]
         db100k)
```
|DB|Q4 Latency (ms)|
|---|---|
|Datomic|25.1|
|Datascript|5.8|
|Datalevin|2.3|

Datalevin's s now 2.5X faster than Datascript and over 10X faster than Datomic.

An additional unbound attribute adds relatively small overhead in Datalevin,
while it costs a little more for Datomic and Datascript.

#### qpred1

This is a query with a predicate that limits the values of an attribute to about
half of the value range, returning about 10K tuples.

```Clojure
    (d/q '[:find ?e ?s
           :where [?e :salary ?s]
                  [(> ?s 50000)]]
      db100k)
```
|DB|QPred1 Latency (ms)|
|---|---|
|Datomic|24.7|
|Datascript|7.1|
|Datalevin|2.6|

Datalevin is over 3X faster than Datascript, and about 10X faster than Datomic
for this query. The reason is that the Datalevin optimizer rewrites the
predicate into a range boundary for range scan on `:salary`. This query is
essentially turned into a single range scan in the `:ave` index, so even though
half of all entity IDs are returned, the speed is still fast.

#### qpred2

This is essentially the same query as qpred1, but the lower limit of `:salary`
is passed in as a parameter.

```Clojure
    (d/q '[:find ?e ?s
           :in   $ ?min_s
           :where [?e :salary ?s]
                  [(> ?s ?min_s)]]
      db100k 50000)
```
|DB|QPred2 Latency (ms)|
|---|---|
|Datomic|26.8|
|Datascript|15.9|
|Datalevin|2.7|

Datalevin performs the same as qpred1 for this one. The reason is because the
optimizer plugs the input parameter into the query directly, so it becomes
identical to qpred1.

Datascript performs over 2X worse in this one than in qpred1, the reason is that
Datascript treats each input parameter as an additional relations to be joined,
often performing Cartesian product due to a lack of shared attributes. Datomic
performs this one a little worse than qpred1, for unknown reasons, as the code
is not available.

## Conclusion

Using this benchmark, Datalevin query engine is found to be faster than Datomic
and Datascript with a relatively large margin.

However, the queries in this benchmark are fairly simple. To see Datalevin's
ability to handle complex queries and a much larger data size, see
[JOB Benchmark](../JOB-bench). For Datalevin's durable write performance, see
[write benchmark](../write-bench).
