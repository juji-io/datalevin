# Datalevin Query Engine

Datalevin has an advanced Datalog query engine that handles complex queries on
large data sets with ease.

## Motivation

One of the main reasons for people to use Datomic flavored Datalog stores is to use
their declarative and composible query language. The simple and elegant query is
backed by a flexible triple store. However, it is a well-know problem that
querying a triple store is much slower than querying RDBMS that stores data in
rows (or columns).

Datalevin solves the problem by developing an innovative query engine based on
the latest research findings and some innovations of our own. We leverage some
unique properties of Datomic-like triple stores to achieve the goal of bringing
triple store query performance to a level competitive with RDBMS.

## Difference from RDF Stores

Although Datomic-like stores are heavily inspired by RDF stores, there are
some differences that impact query engine design.

RDF stores often have a limited number of properties even for huge datasets,
whereas Datomic-like stores may have many more attributes. Fortunately, they are
often specialized. For example, the set of attributes for a class of entities
are often unique. Therefore, leveraging grouping of attributes have greater
benefits.

In Datomic-like stores, the entity vs. entity relationship is explicitly marked
by `:db.type/ref` value type. The cost of resolving entity relationship becomes
lower.

Finally, in Datomic-like stores, the values are stored as they are, instead of as
integer ids, therefore, pushing selection predicates down to index scan methods
brings more benefits.

## New indices: `EnCla` and `Links`

Based on these observations, we introduce two new types of indices. The first is
what we call an `EnCla` index, short for Entity Classes, and we call each entity
class an `encla`. Similar to characteristic sets [9] in RDF research, this
concept captures the defining combination of attributes for a class of entities.

A LMDB map is used to store the `EnCla` index. The key is the unique id of
an encla. The value contains the following information:

* The mapping of the set of attribute ids that defines the encla, to their
  corresponding estimated average cardinality.
* The entity ids belonging to the encla.

`EnCla` index takes up negligible disk space (about 0.002X larger). It is loaded
into memory at system initialization and updated during system run.

In Datomic-like stores, `:db.type/ref` triples provide links between two encla.
Such links are important for simplifying query graph [3] [7]. We store them in a
LMDB dupsort map as an `Links` index:

* Keys are the link definitions: source encla id, target encla id, and the
  reference attribute id.
* Values are the lists of pairs of source entity ids and target entity ids
  belonging to the link.

`Links` index replaces the original `VEA` triple ordering index.

Intuitively, we essentially build virtual tables with foreign keys behind the
scene. This allows us to keep the flexibility of a triple store that enables
simple and elegant query, while reaping the benefits of faster query performance
of a relational store.

Unlike previous research [3] [7] [9], we build these new indices online and keep
them up to date with new data during transactions. As far as we know, Datalevin
is the first production system to develop online algorithms for maintaining
these indices. We pay a small price in transaction processing time (about 20%
slower for large transactions, more for small transactions) and a slightly
larger memory footprint, but gain orders of magnitude query speedup.

## Compressed Triple Storage

Literal representation of triples in an index introduces significant redundant
storage. For example, in `:eav` index, there are many repeated values of `e`,
and in `:ave` index, there are many repeated values of `a`. These repetitions of
head elements not just increase the storage size, but also add processing
overhead during query.

Taking advantage of LMDB's dupsort capability, we store the head element value
only once, by treating them as keys. The values are the remaining two elements
of the triple, plus a reference to the full datom if the datom is larger than
the maximal key size allowed by LMDB.

## Query Optimizations

We built a cost based query optimizer that uses dynamic programming for query
planning [10]. The query engine employs multiple optimization strategies.
Some implement our own new ideas.

### Entity filtering (new)

Instead of relying solely on joins to filter tuples, we leverage the `EnCla`
index to pre-filter entities directly from query patterns. This pre-filter can
significantly reduce the amount of work we have to do.

### Pivot scan

`EnCla` also enables us to use pivot scan [2] that returns multiple attribute
values with a single index scan for star-like attributes.

### Join only effective attributes (new)

Not all attributes of an encla affect the number of results, only those
participate in the joins with other encla or being touched by predicates do. We
only scan these effective attributes in the initial pivot scans. The rest of the
attributes are scanned after all the joins are completed and all predicates are
run. This reduces the chances of materializing attribute values that would not
appear in the final result set. Furthermore, we only consider effective
attributes in planning, which speeds up plan enumeration as well.

### Predicates push-down

As mentioned, we take advantage of the opportunities to push selection
predicates down to index scan in order to minimize unnecessary intermediate
results. This is achieved during query rewrites using magic sets [11].

### Query graph simplification

Since star-like attributes are already handled by pivot scan, the optimizer
works mainly on the simplified graph that consists of stars and the links
between them [3] [7], this significantly reduces the size of optimizer search
space.

### Cumulative average cardinality (new)

During a transaction, we update a cumulative average count for each affected
attribute of the affected encla. This base table cardinality estimation method
is cheap and accurate, leading to better plans.

### Direct count for bounded cardinality (new)

For cardinality estimation with bounded variables, we count them directly,
because range count with bounded values is fast in triple indices.

### Sampling for join and predicate cardinality estimation

For join cardinality estimation, we do sampling at query time [6].
Sampling is cheap in triple indices, because all the attribute are already
unpacked and indexed separately, unlike in RDBMS. We also do sampling for cardinality
estimation with predicates. Sampling is under a dynamic time based budget, with
more time allocated for bigger query graph. After all the budget is spent,
magic numbers are used.

### Left-deep join tree

Our planner generates left-deep join trees, which may not be optimal [8], but work
well for our simplified query graph, since stars are already turned into meta
nodes and mostly chains remain. This also reduces the cost of cardinality
estimation using counting and sampling, which dominates the cost of planning.
Since there is always a base relation in a join, it is much simpler to
accurately estimate using counting and sampling on indices.

### Choose join methods on the fly

Instead of deciding join method during planning, we postpone the decision on
join method until right before executing a join, so join method is chosen using
real cardinality: for small intermediate result size, choose nested loop join;
if the intermediate results are already sorted on join attribute, choose merge
join; hash join as the default.

## Benchmarks

### Datascript

This is included to show the level of improvement of this query engine rewrite,
since several Datalog stores written in Clojure were compared this way.
Using this benchmark, Datalevin before the rewrite was already the fastest, now
we show that this benchmark is too easy, as it involves only one encla, so it
does not even exercise the new query planner.

datascript, datahike, asami, xtdb, datalevin 0.9

### WatDiv

This is a standard benchmark for RDF stores [1].

cozodb, typedb, neo4j, xtdb, jena, rdf4j, datalevin

### JOB

We compare with a RDBMS using JOB benchmark [5], which is based on real world
data and designed to stress the query optimizer.

sqlite, postgresql, datalevin

## Remark

The more granular and redundant storage format of triple stores brings some
challenges to query processing due to its greater demand on storage access, but
it also offer some opportunities to help with query.

We found that the opportunities lie precisely in the "Achilles Heel" of RDBMS
optimizer: cardinality estimation [6]. It is hard to have good cardinality
estimation in RDBMS because the data are stored in rows, so it becomes rather
expensive and complicated trying to unpack them to get attribute value
counts or to sample by rows [4]. On the other hand, it is cheap and
straightforward to count or sample values directly in the already unpacked
indices of triple stores.

## Conclusion

Datalevin query engine stands on the shoulder of a half century of database
research to bring a new hope to triple stores. We have chosen to implement
simple and effective techniques that are consists with our goal of simplifying
data access, and we are also open for the future, e.g. explore learning based
techniques.

## Reference

[1] Aluç, G., Hartig, O., Özsu, M. T. and Daudjee, K. "Diversified Stress
Testing of RDF Data Management Systems". ISWC. 2014.

[2] Brodt, A., Schiller, O. and Mitschang, B. "Efficient resource attribute
retrieval in RDF triple stores." CIKM. 2011.

[3] Gubichev, A., and Neumann, T. "Exploiting the query structure for efficient
join ordering in SPARQL queries." EDBT. Vol. 14. 2014.

[4] Lan, H., Bao, Z. and Peng, Y.. "A survey on advancing the DBMS query
optimizer: cardinality estimation, cost model, and plan enumeration." Data
Science and Engineering, 2021

[5] Leis, V., et al. "How good are query optimizers, really?." VLDB Endowment
2015.

[6] Leis, V., et al. "Cardinality Estimation Done Right: Index-Based Join
Sampling." Cidr. 2017.

[7] Meimaris, M., et al. "Extended characteristic sets: graph indexing for
SPARQL query optimization." ICDE. 2017.

[8] Moerkotte, G., and Neumann, T. "Dynamic programming strikes back."
SIGMOD. 2008.

[9] Neumann, T., and Moerkotte, G. "Characteristic sets: Accurate cardinality
estimation for RDF queries with multiple joins." ICDE. 2011.

[10] Selinger, P. Griffiths, et al. "Access path selection in a relational
database management system." SIGMOD. 1979.

[11] Bancilhon, Francois, et al. "Magic sets and other strange ways to implement
logic programs." Proceedings of the fifth ACM SIGACT-SIGMOD symposium on
Principles of database systems. 1985.
