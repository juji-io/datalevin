# Datalevin Query Engine

Datalevin has an advanced Datalog query engine that handles complex queries
on large data sets with ease.

## Motivation

One of the main reasons for people to use Datomic-like Datalog stores is to use
their declarative and composible query language. The simple and elegant query is
often backed-up by a flexible triple store. However, it is a well-know problem
that querying a triple store is much slower than querying RDBMS that store data
in rows or columns.

Datalevin solves the problem by developing an innovative query engine based on
the latest research findings and some innovations of our own. We leverage some
unique properties of Datomic-like triple stores to achieve the goal of bringing
triple store query performance to a level competitive with RDBMS.

## Difference from RDF Stores

Although Datomic-like stores are heavily inspired by RDF stores, there are
some differences that impact query engine design.

In RDF stores, there's no explicit identification of entities, whereas in
Datomic-like stores, the entities are explicitly defined during transactions; in
addition, the entity vs. entity relationship is also explicitly marked by
`:db.type/ref` value type. Consequently, the cost of resolving
entities and their relationship become much lower.

On the other hand, RDF stores often have a limited number of properties
even for huge datasets, whereas Datomic-like stores may have many more
attributes. Fortunately, they are often specialized. For example, the set of
attributes for a class of entities are often unique. The overlapping
attributes shared by multiple entity classes are rare. Therefore,
leveraging grouping of attributes have greater benefits.

Finally, in Datomic-like stores, the values are stored as they are, instead of as
integer ids like in RDF stores, therefore, pushing predicates down to index scan
methods brings more benefits in Datomic-like stores.

## New indices: `EnCla` and `Links`

Based on these observations, we introduce two new types of indices. The first is
what we call an `EnCla` index, short for Entity Classes. Similar to
characteristic sets [4] in RDF research, this concept captures the defining
combination of attributes for a class of entities.

A LMDB map is used to store the `EnCla` index. The keys
are the IDs of entity classes. The value contains the following information:

* The set of attribute ids that define an encla.
* The entity ids belonging to an encla.

`EnCla` index takes up negligible disk space (about 0.002X larger). It is loaded
into memory at system initialization and updated during system run.

In Datomic-like stores, `:db.type/ref` triples provide links between two entity
classes. Such links are important for simplifying query graph [2] [3]. We store
them in a LMDB dupsort map:

* Keys are link definitions: source encla id, target encla id, and the
  ref attribute id.
* Values are the lists of pairs of source entity ids and target entity ids of a link type.

`Links` index also replaces our `VEA` triple ordering index.

Intuitively, we essentially build virtual tables with foreign keys behind the
scene. This allows us to keep the flexibility of a triple store that enables
simple and elegant query, while reap the benefits of faster query performance of
a relational store.

Unlike previous research [2] [3] [4], we build these new indices online and kept
them up to date with new data during transactions. As far as we know, Datalevin
is the first system to develop online algorithms for these types of indices. We
pay a small price in transaction processing time (about 30% slower) and a
slightly larger memory footprint, but gain orders of magnitude query speedup.

## Query Optimizations

The new query engine will employs multiple optimization strategies. Some are our
innovations.

### Entity filtering (new)

Instead of relying solely on joins to filter tuples, we leverage the `EnCla`
index to pre-filter entities directly from query patterns. This pre-filter can
significantly reduces the amount of work we have.

### Pivot scan

`EnCla` also enables us to use pivot scan [1] that returns multiple attribute
values in a single index scan for star-like attributes.

### Query graph simplification

Since star-like attributes are already handled by entity filtering and pivot
scan, the optimizer works mainly on the simplified graph that consists of stars
and the links between them [2] [3]. This significantly reduces the size of
query search space.

### Search style optimizer (new)

As a break from the traditional Selinger style optimizer [5], where
dynamic programming (DP) query planning is done ahead of query execution, our
query engine does planning and execution at the same time as a shortest path
graph search. This avoids filling the DP table in full.

### Cardinality count (new)

In addition, instead of relying on often inaccurate cardinality estimation, our
new indices afford us to count them accurately and quickly, generating better
execution path.

### Predicates push-down

As mentioned, we take advantage of the opportunities to push predicates on
values down to indexing access methods in order to minimize unnecessary
intermediate results.

## Benchmark

TBD

## Reference

[1] Brodt, A., Schiller, O. and Mitschang, B. "Efficient resource attribute
retrieval in RDF triple stores." CIKM. 2011.

[2] Gubichev, A., and Neumann, T. "Exploiting the query structure for efficient
join ordering in SPARQL queries." EDBT. Vol. 14. 2014.

[3] Meimaris, M., et al. "Extended characteristic sets: graph indexing for
SPARQL query optimization." ICDE. 2017.

[4] Neumann, T., and Moerkotte, G. "Characteristic sets: Accurate cardinality
estimation for RDF queries with multiple joins." ICDE. 2011.

[5] Selinger, P. Griffiths, et al. "Access path selection in a relational
database management system." SIGMOD. 1979.
