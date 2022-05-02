# Datalevin Query Engine

## Motivation

One of the main reasons for people to use Datalog stores is their
declarative and composible query language. The simple and elegant language is
often backup by the flexible triple based data storage. However, it is a well
know problem that querying a triple store is slow compared to RDBMS that stores
data in rows.

Datalevin faces the same problem. Currently it mostly inherits Datascript query
engine, which is inefficient, as it does things the following way: all the data
that matches each *individual* triple pattern clause are fetched and turned into
a set of datoms. The sets of datoms matching each clause are then hash-joined
together one set after another. So if a query has _n_ clauses, it would do _n_
index scans and _n-1_ hash joins.

Even with user hand crafted clauses order, this process still performs a lot of
unnecessary data fetching and transformation, joins a lot of unneeded tuples and
materializes a lot of intermediate relations needlessly. Although Datalevin
added a few simple query optimizations, it is far from solving the problems.

To address these problems, we are developing a new query engine. We will leverage
some unique properties of Datomic-like stores to develop a new index, and
the query engine takes advantage of this new index.

## Difference from RDF Stores

Although Datomic-like stores are heavily inspired by RDF stores, there are
some important differences that impacts the query engine design.

In RDF stores, there's no explicit identification of entities, whereas in
Datomic-like stores, the entities are explicitly defined during transactions; in
addition, the entity vs. entity relationship is also explicitly marked by
`:db.type/ref` value type. Consequently, unlike in RDF stores, expensive
algorithms to discover entities and their relationship become unnecessary. We
will exploit these free information for indexing.

On the other hand, RDF stores often have a very limited number of properties
even for huge datasets, whereas Datomic-like stores may have many more attributes as
they are often specialized, e.g. the namespaces of attributes encode
information about entities. Therefore, leveraging grouping of attributes could
have greater benefits in query processing.

In Datomic-like stores, the set of attributes for a class of entities are often unique.
There might be overlapping attributes shared by multiple entity classes, but many
attributes are used by only one class of entities, and these are often prefixed
by namespace unique to that group of entities.

## New index: `EnCla`

Based on these observations, we introduce a new type of indices, what we call
an `EnCla` index, short for Entity Classes. Similar to characteristic sets [8]
in RDF stores or tables in relational DB, this concept captures the defining
combination of attributes for a class of entities.

An `encla` LMDB DBI will be used to store the `EnCla` index. The keys
are the unique integer IDs of each entity class. The value contains two piece of
information:

* `aids`, the set of attribute ids that define the entity class.
* `eids`, the bitmap of the integer ids of the entities belonging to the entity
  class.

`EnCla` index is loaded into memory at system initialization and is kept up to
date with newly transacted data. We pay a small price in transaction slowdown
(about 1.3 times slower) and memory footprint, but gain significant query
speedup. as `EnCla` index allows us to quickly identified relevant entity
classes in the query and find relevant entities associated with them.

## Query Optimizations

The query engine will employs multiple optimizations.

### Leverage entity classes

 Essentially, we leverage the entity classes to pre-filter entities. This pre-filter
 can significantly reduces the amount of work we have to do. For joins, we start
 with clause with least cardinality. "classes" based index scans will also
 participate in this search for least cardinality as if they are normal pattern
 clauses, as they may not be the cheapest.  This addresses the limitation of
 [7], which does not have other indices.

### Pivot scan

For local star-like attributes, the engine will also consider pivot scan [2]
that returns multiple attribute values in a single index scan, because after the
most selective attributes have been joined together to reach a low enough tuple
count, it might be cheaper to obtain remaining attributes with a pivot scan than
match-and-join them.

### Predicates push-down

In RDF stores, all the triple values are normally considered nominal literals,
and hence encoded as integer IDs, whereas in Datomic-like stores, the values
often should be considered ordinal (i.e. as numerical data), hence it is
beneficial to store them as they are and sort them by value. It is important to
push down predicates on values into indexing access methods in order to take
advantage of this kind of storage to minimize unnecessary intermediate results.

### Join methods selection

Traditional planner is based on pair wise joins of two relations, one join at a
time. Different join methods are suitable for different situations: When the
tuples on both sides are sorted on the join attribute,  merge join performs the
best.  It should be used as much as possible when the condition meets. Nested
loop join works well when tuple counts are small.  Hash join can be used when
neither cases are suitable.

Recently, worst-case optimal multi-way join algorithms are proposed [1] and have
been implemented in some commercial DBs [10]. Such join algorithms has also
begun to be tried in RDF cases [4]. In essence, these multi-way algorithms do
nested joins one tuple at a time to avoid materializing data that would not be
in the results. Though optimal for the worst-case, they may not perform better
in practice, e.g. when joins are selective. They are good at dealing with cases
of growing intermediate results [3] for join variables (but not lonely
variables). So the engine collapses multiple pair-wise joins into one multi-way
join when it estimates that the cardinality of intermediate results will be
greater than that of the largest of the participating relations, e.g. as often
the case in those involving value-value joins.

## New Style of Query Evaluation

As a break from the traditional Selinger style query planners [9], where
dynamic programming based query planning is done ahead of query execution, our
query engine works with planning and execution happen in tandem.

The main reason is to be able to obtain more
accurate cost estimation during query execution, rather than relying on purely
speculative ahead of time estimation. In the exploration phase of A* search
algorithm, where we need to determine the next join step, we can leverage
sampling [6] to get better estimation, since the prior joins already produced
results and one side of relation is known.

It is well-known that most cardinality estimation methods under-estimate the
cost [5] due to independence assumption, etc. Fortunately, this under-estimation
of cost plays right into the admissible requirement of A* search algorithm, as
long as we never over-estimate the cost of the remaining joins, we likely obtain
an optimal join path.

Some data statistics is still needed for estimating cost of operations. Some can
be updated during transaction, e.g. effectively collected when we build the
various bitmaps described above, as these bitmaps can easily return the distinct
counts and other statistics. Others are collected at query time, e.g. range scan
count and filter predicate count, since these are relatively cheap to query.
Some optimization can also be used, e.g. when the goal is to find the least
count among several options, counting with a cap can be used to stop counting
after the known minimum is reached.

As to data structure used in joins, instead of returning a list of full datoms,
the query index access functions return a list of bound values of variables
instead. This avoid repetitively deserializing known components of a triple
pattern, and then having to ignore them during joins. Such results are also
consistent with the results of bitmap operations on "classes" and "links"
indices, i.e. they return a list of sorted entity ids. Accordingly, nested
hashmap will be used to collect intermediate results, where each level
corresponds to a variable.

## Reference

[1] Atserias, Albert, Martin Grohe, and Dániel Marx. "Size bounds and query
plans for relational joins." 49th Annual IEEE Symposium on Foundations of
Computer Science. 2008.

[2] Brodt, Andreas, Oliver Schiller, and Bernhard Mitschang. "Efficient resource
attribute retrieval in RDF triple stores." Proceedings of the 20th ACM
international conference on Information and knowledge management (CIKM). 2011.

[3] Freitag, M., Bandle, M., Schmidt, T., Kemper, A., & Neumann, T. (2020). Combining worst-case optimal and traditional binary join processing.

[4] Hogan, Aidan, et al. "A Worst-Case Optimal Join Algorithm for SPARQL." International Semantic Web Conference. Springer, Cham, 2020.

[5] Leis, Viktor, et al. "How good are query optimizers, really?." Proceedings of the VLDB Endowment 9.3 (2015): 204-215.

[6] Leis, Viktor, et al. "Cardinality Estimation Done Right: Index-Based Join Sampling." Cidr. 2017.

[7] Meimaris, Marios, et al. "Extended characteristic sets: graph indexing for
SPARQL query optimization." IEEE 33rd International Conference on Data
Engineering (ICDE). 2017.

[8] Neumann, Thomas, and Guido Moerkotte. "Characteristic sets: Accurate
cardinality estimation for RDF queries with multiple joins." IEEE 27th
International Conference on Data Engineering (ICDE). 2011.

[9] Selinger, P. Griffiths, et al. "Access path selection in a relational database management system." Proceedings of the 1979 ACM SIGMOD international conference on Management of data. 1979.

[10] Veldhuizen, Todd L. "Leapfrog triejoin: A simple, worst-case optimal join algorithm." EDBT/ICDT (2012).
