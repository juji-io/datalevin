# Datalevin Query Engine

## Motivation

One of the main reasons for people to use Datomic-like Datalog stores is to use
their declarative and composible query language. The simple and elegant query is
often backed-up by a flexible triple store. However, it is a well-know problem
that querying a triple store is slow compared to RDBMS that stores data in rows
or columns.

Datalevin faces the same problem. Currently it mostly inherits Datascript query
engine, which fetches all the data that matches each *individual* triple
pattern and hash-join them one after another.

Even with user hand crafted clauses order, this process performs much
unnecessary data fetching and joins many unneeded tuples. Although Datalevin
added a few simple query optimizations, it is far from solving the problems.

To address these problems, we are developing a new query engine based on the
latest research findings and some of our own ideas. We will leverage
some unique properties of Datomic-like stores to develop new indices.

## Difference from RDF Stores

Although Datomic-like stores are heavily inspired by RDF stores, there are
some differences that impact query engine design.

In RDF stores, there's no explicit identification of entities, whereas in
Datomic-like stores, the entities are explicitly defined during transactions; in
addition, the entity vs. entity relationship is also explicitly marked by
`:db.type/ref` value type. Consequently, the cost of recording
entities and their relationship become cheaper.

On the other hand, RDF stores often have a very limited number of properties
even for huge datasets, whereas Datomic-like stores may have many more
attributes. Fortunately, they are often specialized. For example, the set of
attributes for a class of entities are often unique. There are overlapping
attributes shared by multiple entity classes, but they are rare. Therefore,
leveraging grouping of attributes could have greater benefits.

## New indices: `EnCla` and `Links`

Based on these observations, we introduce two new types of indices. The first is
what we call an `EnCla` index, short for Entity Classes. Similar to
characteristic sets [4] in RDF stores or tables in RDBMS, this concept captures
the defining combination of attributes for a class of entities.

A LMDB map is used to store the `EnCla` index. The keys
are the IDs of each entity class. The value contains the following information:

* The set of attribute ids that define an encla.
* The entity ids belonging to an encla.

`EnCla` index takes up negligible disk space (about 0.002X larger). It is loaded
into memory at system initialization and updated during system run.

In Datomic-like stores, `:db.type/ref` triples provide links between two entity
classes. Such links are important for simplifying query graph [2] [3]. We store
them in a LMDB dupsort map:

* Keys are link definitions: source encla id, target encla id, and the
  ref attribute id.
* Values are the pairs of source entity ids and target entity ids of the link.

`Links` index also replaces `vea` triple ordering index.

Unlike previous research [2] [3] [4], we build these indices online and kept
them up to date with new data during transactions. We pay a small price in
transaction processing time (about 30% slower) and a slightly larger memory
footprint, but gain orders of magnitude query speedup. As far as we know, we are
the first production system to do so.

## Query Optimizations

The new query engine will employs multiple optimization strategies. Some are our
innovation.

### Entity filtering (new)

 We leverage the `EnCla` index to pre-filter entities directly from query
 patterns. This pre-filter can significantly reduces the amount of work we have
 to do.

### Pivot scan

`EnCla` also enables us to use pivot scan [1] that returns multiple attribute
values in a single index scan for star-like attributes.

### Query graph simplification

Since star-like attributes are already handled by entity filtering and pivot
scan, the planner works mainly on the simplified graph that consists of stars and
links between them [2] [3]. This significantly reduces the size of search space.

### Search style optimizer (new)

As a break from the traditional Selinger style optimizer [5], where
dynamic programming (DP) query planning is done ahead of query execution, our
query engine does planning and execution at the same time as a shortest path
graph search. This avoids filling the DP table in full.

### Predicates push-down

In Datomic-like stores, the values are stored as they are, instead of as integer
ids like in RDF stores. It is possible to push predicates on values down into
indexing access methods in order to minimize unnecessary intermediate results.

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