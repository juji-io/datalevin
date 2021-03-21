# Datalevin Query Engine

## Motivation

Currently, Datalevin mostly inherits Datascript query engine, which is inefficient, as it
does things the following way: all the data that matches each *individual* triple
pattern clause are fetched and turned into a set of datoms. The sets of datoms
matching each clause are then hash-joined together one set after another. So if
a query has _n_ clauses, it would do _n_ index scans and _n-1_ hash joins.

Even with user hand crafted clauses order, this process still performs a lot of
unnecessary data fetching and transformation, joins a lot of unneeded tuples and
materializes a lot of intermediate relations needlessly. Although Datalevin
added a few simple query optimizations, it is far from solving the problems.

To address these problems, we will develop a new query engine that attempts to
evaluate the query with minimal cost. We will also support an `explain` option
for the users to see the query execution steps.

This query engine will leverage some unique properties of Datomic-like stores,
take advantage of some new indexing structures, utilize the state of the art join
algorithms, employ a new query execution style and do concurrent query execution.

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

## New Indices

Based on these observations, we introduce two new type of indices.

### Entity classes

First we introduce a concept of entity class, which refers to the type of
entities. Similar to characteristic sets [8] in RDF stores or tables in
relational DB,  this concept captures the defining combination of attributes for
a class of entities.

In Datomic-like stores, the set of attributes for a class of entities are often unique.
There might be overlapping attributes between entity classes, but many
attributes are used by only one class of entities, and these are often prefixed
by namespace unique to that entity class. We assign auto increment integer
id for each entity class, and represent the mapping from attributes to entity
class with bitmaps.

Specifically, each attribute schema entry has a `:db/classes` key pointing to a
bitmap of entity class ids that include the attribute in their definitions. This
way, we can quickly identify the entity classes relevant to a query or a
transaction through fast bitmap intersections.

An additional "classes" LMDB DBI will be used. The keys will be class ids, and
the values are a map with the following keys: `:attrs` is a bitmap of the
defining attributes of the entity class; `:entities` is a bitmap of the entity
ids in the class. This allows us to quickly find relevant entities once entity classes
are identified in the query.

Unlike previous work in the literature, our definition of entity class is firm
(i.e. one class is defined by one unique set of attributes), but the class
membership is flexible.  As attributes are added to or removed from an entity
during its lifetime in the database, the entity may find itself belonging to
multiple related entity classes at the same time. We accept the slight
overhead of matching more entities than necessary during query, because the cost
of always maintaining accurate entity class membership for all entities is
rather high, for that requires a constant maintenance of a mapping from entity
to its class.  Since the purpose of entity class is only to pre-filter entities,
not to precisely match query constraints, some false positives are acceptable,
as long as there is no false negative. In this sense, entity class works like a
free bloom filter without any hashing.

For common attributes that should not contribute to the defintion of entity
classes, user can mark such attributes under a `:common-attributes` key, as part
of the option map given when openning the DB.

### Class links

We further introduce a notion of entity class link that represents a pair of entity
classes that are connected by triples with `:db.type/ref` type attributes.
Similar to extended characteristic sets [7] or foreign key relation in
relational DB, this concept captures the long range relationship in data.  We
will leverage such declaration and store the resulting graph.

Specifically, a "links" LMDB DBI will be used, the keys are the pair of the
entity class ids of the referring entity class (class of E) and the referred
entity class (class of V), the values are bitmaps of entity ids of
referred entities (V), so that we can look up the link triples quickly in the
"VEA" LMDB DBI, which contains link triples only.

In addition, the schema map has a built-in key `:db/graph`, and its value stores
the adjacency list of the class link graph of the data, i.e. a map of links to
the set of their adjacent links. This graph structure captures the overall structure
of the data. We will use this graph to pre-filter entities for those complex
queries spanning multiple related entity classes.

## Optimizations

The query engine will employs multiple optimizations.

### Leverage links and classes

Our engine will first leverage the entity classes and the links between them
to generate the skeleton of the execution plan.  Essentially, we leverage the
set of attributes and their relationships in the query to:

    a. break up the query into sub-queries that can be independently executed.

    b. pre-filter the entities involved in each sub-query.

This pre-filtering significantly reduces the amount of work we have to do, especially for
complex queries that involves long chains of where clauses.

The engine first considers long range relationships. The reason to avoid
starting with local star-like relationships is because they are often
unselective, due to high correlation between attributes of an entity class.

Entity classes and links form a directed graph, so querying can be considered
matching query graph with the stored data graph.  After the engine identifies
the entity classes as well as their linkage in the query, the query link graph is
then matched to the data link graph, producing a set of link chains.

Each chain can be considered a sub-query, and sub-queries can be processed
by different thread in parallel, so large query can be performed more quickly on
a multi-core system. The results of the sub-queries are then joined together.

For joins within a sub-query, we start with clause with least cardinality.
"classes" and "links" based index scans will also participate in this search for
least cardinality as if they are normal pattern clauses, as they may not be
the cheapest.  This addresses the limitation of [7], which does not have other indices.

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

## A* Search Style Query Evaluation

As a major break from the traditional Selinger style query planners [9], where
dynamic programming based query planning is done ahead of query execution, our
query engine works more like an A* search algorithm, where planning and
execution happen in tandem. The main reason is to be able to obtain more
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
