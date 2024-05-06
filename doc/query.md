# Datalevin Query Engine

Datalevin has an innovative query engine that handles complex Datalog queries on
large data sets with ease.

## Motivation

One of the main reasons for people to use Datomic flavored Datalog stores is to
use their declarative and composible query language. In the era of generative
AI, this query language is also found to be an ideal target language for natural
language query translation.

The simple and elegant query language is often backed by a flexible triple
store. However, it is a well-know problem that querying a triple store is much
slower than querying RDBMS that stores data in rows (or columns). [1]

Datalevin solves the problem by developing an advanced query engine based on
the latest research findings and some innovations of our own. We leverage some
unique properties of Datomic-like triple stores to achieve the goal of bringing
triple store query performance to a level competitive with RDBMS.

## Difference from RDF Stores

Although Datomic-like stores are heavily inspired by RDF stores, there are
some differences that impact query engine design. Rather than trying to be
maximally general and open, Datomic-like stores are closer to traditional
databases in its design choices.

RDF stores often have a limited number of properties even for huge datasets,
whereas Datomic-like stores normally have many attributes, and they are
often specialized to a class of entities.  Therefore, filtering by
attribute values can be very efficient.

In Datomic-like stores, entities have explicit integer IDs, which makes the
filtering by entity IDs efficient. The entity vs. entity relationship is also
explicitly marked by `:db.type/ref` value type. The cost of resolving entity
relationship becomes lower.

Conversely, in Datomic-like stores, the data values are stored as they are,
rather than being represented by integer IDs, therefore, pushing selection
predicates down to index scan methods brings more benefits.

Datalevin query engine exploits these design choices to maximize query
performance.

## Nested Triple Storage

Literal representation of triples in an index introduces significant redundant
storage. For example, in `:eav` index, there are many repeated values of `e`,
and in `:ave` index, there are many repeated values of `a` and `v`. These
repetitions of head elements increase not just the storage size, but also
processing overhead during query.

Taking advantage of LMDB's dupsort capability (i.e. a key can be mapped to a
list of values, and this list of values are also sorted, essentially it is a two
level nested B+ trees of B+ trees), we store the head elements only once, by
treating them as keys. The values are the remaining one or two elements of the
triple as a list of values mapped to by a key. This nested triple storage
results in about 20% space reduction, depending on the data.

The main advantage of this list based triple storage is to facilitate counting
of elements, which is the most critical input for query planning. Some list
counts can be immediately read from the index, without performing actual
range scan to count them. For example, in our storage schema, the number of
datoms matching `[?e :an-attr "bound value"]` pattern can be obtained from the
`:ave` index in constant time. Other counts can also be cheaply counted. For
example, cardinality (number of distinct values) of an attribute can be obtained
by counting only the number of keys in an `ave` range. There is no need to
maintain expensive and often inaccurate statistics.

## Query Optimizations

Datalevin query engine employs multiple optimization strategies.

### Predicates push-down

As mentioned above, we take advantage of the opportunities to push selection
predicates down to index scan in order to minimize unnecessary intermediate
results. Currently, two types of predicates are pushed down: 1. inequality
predicates involving one variable and one or two constants are converted to
range boundary in range scan; 2. other predicates involving one attributes are
pushed down to operations that scan the attribute values.

These predicates push-downs are implemented as query rewrites. We also plug the
constant query parameters into the query itself in order to avoid expensive
joins with these bindings turned relations. More query rewrite cases will be
considered in the future.

### Merge scan

For star-like attributes, we utilize an idea similar to pivot scan [2], which
returns multiple attribute values with a single index scan using `:eav` index.
This single scan takes a list of entity IDs, an ordered list of attributes
needed, and corresponding predicates for each attribute, to produce a relation.
This avoids performing joins within the same entity class to obtain the same
relation. The bulk of query execution time is spent on this operation.

The input list of entity IDs may come from a search on `:ave` index that returns
an entity ID list, a set of linking references from a relation produced
in the previous step, or the reverse references from the previous step, and so
on.

### Query graph simplification

Since star-like attributes are already handled by merge scan, the optimizer
works mainly on the simplified graph that consists of stars and the links
between them [3] [7] [9], this significantly reduces the size of optimizer
search space.

### Join methods

Currently, we consider four join methods.

For two sets of where clauses involving two classes of entities respectively,
e.g. `?e` and `?f`, we currently consider the following cases. If there
is a reference attribute in the clauses that connects these two classes of entities
e.g. `[?e :a/ref ?f]`, "forward ref" or "reverse ref" method will be considered.
The forward ref method takes the list of `f?` in an existing relation, then
merge scan values of `?f` entities. Reverse ref method has an extra step, it
starts with `?f` relation and scan `:vae` index to obtain corresponding list of
`?e`, then merge scan values of `?e` entities.

The third case is the value equality case, where `e` and `f` are linked due to
unification of attribute values, then `:ave` index is scanned to find the
target's entity IDs. The above three methods are essentially scanning indices
for a list of entity IDs. Other attribute values then need to be merge scanned
to obtain a full relation.

The fourth method instead does merge scan first, then attempt to join
two relations using hash join afterwards. The choice of probing and building
side of the hash join is determined during execution, instead of being
predetermined by the planner. This avoids the possibility of building the hash
table on the wrong side due to inaccurate cardinality estimation. This property
also makes our join tree more like a zig-zag one, rather than a pure
left-deep join tree.

The choice of these methods is determined by the optimizer based on its cost
estimation.

### Cost based query optimizer

We built a Selinger style cost-based query optimizer that uses dynamic
programming for query planning [10], which is used in almost all RDBMS. Instead
of considering all possible combinations of join orders, the plan enumeration is
based on connected components of the query graph. Each connected component has
its own plan and its own execution sequence. Multiple connected components are
processed concurrently. The resulting relations are joined afterwards, and the 
order of which is based on result size.

### Left-deep join tree

Our planner generates left-deep join trees, which may not be optimal [8], but
work well for our simplified query graph, since stars are already turned into
meta nodes and mostly chains remain. This also reduces the cost of cost
estimation, which dominates the cost of planning. The impact of the loss of
search space is relatively small, compared with the impact of inaccuracy in
cardinality estimation. [5]

We do not consider bushy join trees, as our join methods are mainly based on
scanning indices, so a base relation is needed for each join. Since we
also count in base relations, the cardinality estimation obtained there is quite
accurate, so we want to leverage that accuracy by keeping at least one base
relation in each join.

### Direct counting for cardinality estimation (new)

As mentioned, the main advantage of our system is having more accurate
cardinality estimation. Instead of relying on statistics based estimations
using histograms, we count elements directly, because counts in our list based
triple storage are cheap to obtain. Since the planner is only interested in
smallest count within one entity class, the counting is capped by the current
minimum, so the time spent in counting is minimized. Compared with statistics
based estimation, counting is simple, accurate and always up to date. For those
cases that cannot be counted, we use magic numbers. In the future, we will add
sampling based estimations.

## Limitation

Currently, the query optimizer handles normal where clauses only: triple
patterns and predicates. We will gradually extend the optimizer to consider
more clause types in the future. In addition, only binary relations are
considered at the moment, future work may consider relations on a hypergraph
[8].

## Benchmarks

Right now, only an existing benchmark developed in Datascript is performed. The 
speedup compared with Datascript engine is substantial. The details can be found
[here](https://github.com/juji-io/datalevin/tree/master/datascript-bench).
Queries in this benchmarks are fairly simple, so we plan to port the join order
benchmark (JOB) [5] from SQL in order to properly exercise the query optimizer.

## Remark

The more granular and redundant storage format of triple stores brings some
challenges to query processing due to its greater demand on storage access, but
it also offer some opportunities to help with query processing.

We found that the opportunities lie precisely in the "Achilles Heel" of RDBMS
optimizer: cardinality estimation [6]. It is hard to have good cardinality
estimation in RDBMS because the data are stored in rows, so it becomes rather
expensive and complicated trying to unpack them to get attribute value
counts or to sample by rows [4]. On the other hand, it is cheap and
straightforward to count elements directly in the already unpacked
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
