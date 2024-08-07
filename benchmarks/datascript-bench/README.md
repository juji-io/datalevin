# Datascript Benchmark

This directory contains the original benchmarks from
[Datascript](https://github.com/tonsky/datascript), with code for Datalevin
added, and related software updated to the latest versions.

The data consists of 20K entities of random person information, each entity has
5 attributes, totaling 100K datoms. The benchmarks load and query the data.

## Run benchmarks

To run the benchmarks, you need to have both [leiningen](https://leiningen.org/) and [Clojure CLI](https://clojure.org/guides/deps_and_cli) installed on your system already. Then run these commands in the project root directory.

```
lein javac
cd bench
./bench.clj
```

For more comparisons with other alternatives, you may also consult [this fork](https://github.com/joinr/datalevinbench).

## Results

We ran this benchmark on an Intel Core i7-6850K CPU @ 3.60GHz with 6 cores, 64GB
RAM, 1TB SSD, running Ubuntu 22.04 and OpenJDK 17, with Clojure 1.11.2.

Datomic peer binary (licensed under Apache 2.0) 1.0.7075 and Datascript 1.6.3
ran in memory only mode, as they require another database for persistence.
Datalevin 0.9.0 does not have a memory only mode, so it ran in `:nosync` mode to
better match the condition, i.e. the data were written, but `msync `was not
called to force the flush to disk.

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
                 {:flags #{:nordahead :notls :writemap :nosync}}})
```
Unsurprisingly, Datalevin loads datoms into a DB file on disk slower
than Datascript loads the same data into memory. The difference ratio is about 4X.

|DB|Init Latency (ms)|
|---|---|
|Datomic|N/A|
|Datascript|42.5|
|Datalevin|169.0|

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
                  {:kv-opts {:flags #{:nordahead :notls :writemap :nosync}}})
      core/people20k)
```

Compared with Init, transacting data is an order of magnitude more expensive,
since the transaction logic involves a great many number of reads and checks.

Datalevin and Datomic have similar transaction speed, with the former being a little faster. Datascript is over 2X faster than both.

|DB|Add-1 Latency (ms)|
|---|---|
|Datomic|2601.8|
|Datascript|1027.5|
|Datalevin|2437.9|

Notice that the difference ratio between Datalevin and Datascript reduces from
4X to 2X, which suggests that Datalevin's transaction process is more efficient.
This is likely due to faster reads, as Datalevin and Datascript use the same
high level transaction code, only differ in data access layer.

#### Add-5

This transacts one entity (5 datoms) at a time.

```Clojure
          (reduce (fn [db p] (d/db-with db [p]))
            (d/empty-db (u/tmp-dir (str "bench-add-5" (UUID/randomUUID)))
                        schema
                        {:kv-opts {:flags #{:nordahead :notls :writemap :nosync}}})
            core/people20k)
```

Datalevin reduces its transaction time when transacts 5 datoms at a time, but not
enough to half the time spent.

Datomic does better in this condition, more than halves its transaction
time compared with Add-1 condition.

Datascript is the fastest, but the improvement compared with Add-1 is relatively unremarkable.

|DB|Add-5 Latency (ms)|
|---|---|
|Datomic|1072.8|
|Datascript|824.3|
|Datalevin|1385.6|

#### Add-all

This transacts all 100K datoms in one go.

```Clojure
    (d/db-with
      (d/empty-db (u/tmp-dir (str "bench-add-all" (UUID/randomUUID)))
                  schema
                  {:kv-opts {:flags #{:nordahead :notls :writemap :nosync}}})
      core/people20k)
```

Datalevin steadily improves its write speed compared with Add-5, but the improvement is mild.

Datomic again improves greatly, now actually becomes faster than Datascript.

Datalevin is about 2x slower, but it does write data to disk, even though force
flush is not performed.

|DB|Add-all Latency (ms)|
|---|---|
|Datomic|680.0|
|Datascript|795.5|
|Datalevin|1244.8|

#### Retract-5

This retracts one entity at a time.

```Clojure
(reduce (fn [db eid] (d/db-with db [[:db.fn/retractEntity eid]])) db eids)
```

Datalevin is the fastest in retracting data, more than 5X faster than Datomic.

Datascript is close to 4X faster than Datomic.

|DB|Retract-5 Latency (ms)|
|---|---|
|Datomic|2010.3|
|Datascript|531.5|
|Datalevin|397.1|

### Read

This benchmark only involves a few simple Datalog queries. Benchmarks using complex queries remain a future work.

Because Datalevin has a caching layer, each query benchmark ran in a separate
 database and all having the same data, in order to have fair comparison.

#### q1

This is the simplest query: find entity IDs of a bound attribute value,
returning about 4K IDs.

```Clojure
(d/q '[:find ?e
       :where [?e :name "Ivan"]]
      db100k)
```
Datalevin and Datascript have almost the same speed for this query. Datomic is almost an order of magnitude slower.

|DB|Q1 Latency (ms)|
|---|---|
|Datomic|6.1|
|Datascript|0.66|
|Datalevin|0.67|

#### q2

This adds an unbound attribute to the results.

```Clojure
    (d/q '[:find ?e ?a
           :where [?e :name "Ivan"]
                  [?e :age ?a]]
      db100k)
```
Datalevin is now over 3X faster than Datascript. The reason is that Datalevin
performs a merge scan on the index instead of a hash join to get the values of
`:age`, so it processed a lot less intermediate results.

Datomic is over 10X slower than Datalevin for this query.

|DB|Q2 Latency (ms)|
|---|---|
|Datomic|10.7|
|Datascript|2.9|
|Datalevin|0.76|

#### q2-switch

This is the same query as q2, just switched the order of the two where clauses.

```Clojure
    (d/q '[:find ?e ?a
           :where [?e :age ?a]
                  [?e :name "Ivan"]]
      db100k)
```
In this query, Datalevin has the same speed as q2, and is actually a little
faster. The query optimizer generates identical plans for q2 and q2-switch, so the performance difference is perhaps due to better JVM warmness as this benchmark runs latter. Switching the running order confirmed this suspicion.

|DB|Q2-switch Latency (ms)|
|---|---|
|Datomic|38.0|
|Datascript|6.4|
|Datalevin|0.72|

Now Datascript slows down more than 2X compared with q2. Datomic is even worse,
slows down close to 4X. The reason is that these databases do not have a query
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

Compared with q2, this additional bound attribute slows down Datalevin more than
3X. The reason is that the optimizer rewrites the additional bound attribute `:sex`
into a predicate `[(= ?bound1024 :male)]`, and running a predicate on a value
during merge scan is more expensive than just scanning a value. Since
this bound attribute is not very selective, this is actually not a bad choice.

|DB|Q3 Latency (ms)|
|---|---|
|Datomic|14.6|
|Datascript|4.3|
|Datalevin|2.6|

Datalevin manages to be 1.5X faster than Datascript and over 5X faster than
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
An additional unbound attribute adds relatively small overhead in Datalevin,
while it costs a little more for Datomic and Datascript. Datalevin is now 2X
and 6X faster than Datascript and Datomic, respectively.

|DB|Q4 Latency (ms)|
|---|---|
|Datomic|18.4|
|Datascript|6.5|
|Datalevin|3.2|

#### q5

This is a join on equal values. The selectivity of the joining attribute is
about 0.01, so it is not too bad, but it is still a tough query, because
something close to a Cartesian product of all values is likely to be performed
for such a join.

```Clojure
    (d/q '[:find ?e1 ?l ?a
           :where [?e :name "Ivan"]
                  [?e :age ?a]
                  [?e1 :age ?a]
                  [?e1 :last-name ?l]]
      db100k)
```
Not surprisingly, across the board, the speed for this query is over an order of
magnitude slower compared with other queries.

Datalevin is the fastest, 1.4X faster than Datomic. Datascript fares the
worst in this query, close to be 2.8X slower than Datalevin.

|DB|Q5 Latency (ms)|
|---|---|
|Datomic|178.7|
|Datascript|352.1|
|Datalevin|126.0|

#### qpred1

This is a query with a predicate that limits the values of an attribute to about
half of the value range, returning about 10K tuples.

```Clojure
    (d/q '[:find ?e ?s
           :where [?e :salary ?s]
                  [(> ?s 50000)]]
      db100k)
```
Datalevin is over 3X faster than Datascript, and about 10X faster than Datomic
for this query. The reason is that the Datalevin optimizer rewrites the
predicate into a range boundary for range scan on `:salary`. This query is
essentially turned into a single range scan in the `:ave` index, so even though
half of all entity IDs are returned, the speed is still fast.

|DB|QPred1 Latency (ms)|
|---|---|
|Datomic|22.7|
|Datascript|7.3|
|Datalevin|2.5|

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
Datalevin performs exactly the same as qpred1 for this one. The reason is
because the optimizer plugs the input parameter into the query directly, so it
becomes identical to qpred1.

|DB|QPred2 Latency (ms)|
|---|---|
|Datomic|25.1|
|Datascript|17.6|
|Datalevin|2.5|

Datascript performs over 2X worse in this one than in qpred1, the reason is that
Datascript treats each input parameter as an additional relations to be joined,
often performing Cartesian product due to a lack of shared attributes. Datomic
performs this one a little worse than qpred1, for unknown reasons, as the code
is not available.

## Conclusion

Using this benchmark, Datalevin query engine is found to be faster than Datomic
and Datascript with a relatively large margin.

However, the queries in this benchmark are fairly simple. To see Datalevin's
ability to handle complex queries and a larger data size, please look at [JOB
Benchmark](../JOB-bench).
