# Datalevin Query Engine

Datalevin has an advanced Datalog query engine that handles complex queries on
large data sets with ease.

## Motivation

One of the main reasons for people to use Datomic flavored Datalog stores is to
use their declarative and composible query language. In the era of generative
AI, this query language is also found to be an ideal target language for natural
language query translation.

The simple and elegant query language is often backed by a flexible triple
store. However, it is a well-know problem that querying a triple store is much
slower than querying RDBMS that stores data in rows (or columns).

Datalevin solves the problem by developing an innovative query engine based on
the latest research findings and some innovations of our own. We leverage some
unique properties of Datomic-like triple stores to achieve the goal of bringing
triple store query performance to a level competitive with RDBMS.

## Difference from RDF Stores

Although Datomic-like stores are heavily inspired by RDF stores, there are
some differences that impact query engine design. Rather than trying to be
maximally general and open, Datomic-like stores are closer to traditional
databases in its design choices.

RDF stores often have a limited number of properties even for huge datasets,
whereas Datomic-like stores may have many attributes, and they are
often specialized to a class of entities.  Therefore, filtering by
attribute values are often very efficient.

In Datomic-like stores, entities have explicit integer IDs, which makes the
filtering by entity IDs efficient. The entity vs. entity relationship is also
explicitly marked by `:db.type/ref` value type. The cost of resolving entity
relationship becomes lower.

Conversely, in Datomic-like stores, the values are stored as they are, rather than as
integer IDs, therefore, pushing selection predicates down to index scan methods
brings more benefits.

Datalevin search engine exploits these design choices to maximize query performance.


## Compressed Triple Storage

Literal representation of triples in an index introduces significant redundant
storage. For example, in `:eav` index, there are many repeated values of `e`,
and in `:ave` index, there are many repeated values of `a`. These repetitions of
head elements not just increase the storage size, but also add processing
overhead during query.

Taking advantage of LMDB's dupsort capability (i.e. a key can be mapped to a list of
values, and the list of values are also sorted), we store the head element value
only once, by treating them as keys. The values are the remaining two elements
of the triple, plus a reference to the full datom if the datom is larger than
the maximal key size allowed by LMDB.

This list based triple storage also facilitate counting of elements, as the list
count can be immediately read from the index, without performing actual
count, e.g. the number of datoms matching `[?e :attr ?x]` pattern is read directly from the `:ave` index.

## Query Optimizations

We built a cost based query optimizer that uses the dynamic programming for query planning [10]. The query engine employs multiple optimization strategies.

### Predicates push-down

As mentioned above, we take advantage of the opportunities to push selection
predicates down to index scan in order to minimize unnecessary intermediate
results. This is done to predicates involving one attribute only.

### Pivot scan

For star-like attributes, we use pivot scan [2] that returns multiple attribute
values with a single index scan on `:eav` index. This single scan takes an ordered
list of entity IDs, an ordered list of attributes needed, and conditions on
attributes, to produce a relation. This avoids performing joins within the same
entity class to obtain the same relation. The input entity IDs may come from a
search on `:ave` index that returns the smallest result set, or a set of linking
references from a relation produced in the previous steps.

### Query graph simplification

Since star-like attributes are already handled by pivot scan, the optimizer
works mainly on the simplified graph that consists of stars and the links
between them [3] [7], this significantly reduces the size of optimizer search
space.

### Left-deep join tree

Our planner generates left-deep join trees, which may not be optimal [8], but work
well for our simplified query graph, since stars are already turned into meta
nodes and mostly chains remain. This also reduces the cost of cost estimation, which dominates the cost of planning.

### Join methods

For two sets of where clauses involving two classes of entities respectively,
e.g. `?e` and `?f`, we consider three possible methods of joining them. If there
is a reference attribute in the clauses to connect these two classes of entities
e.g. `[?e :a/ref ?f]`, "forward" or "backward" method can be considered, and for
other cases, only hash join is considered. The forward method first sorts the
`e?` relation by value of `?f`, then pivot scan `?f` entities, effectively doing
a merge join with `?f` relation as the inner relation. Backward method has an extra step,
it starts with `?f` relation and scan `:vae` index to merge in corresponding `?e`,
sort the relation by `?e`, then pivot scan `?e` entities. The choice of the
method is determined by cost estimation.

### Direct count for cardinality (new)

Instead of relying on estimation, we count elements directly if possible,
because counts in our list based triple storage is cheap to obtain. For those cases that cannot be counted, we use magic number.

## Remark

The more granular and redundant storage format of triple stores brings some
challenges to query processing due to its greater demand on storage access, but
it also offer some opportunities to help with query.

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

[11] Bancilhon, Francois, et al. "Magic sets and other strange ways to implement
logic programs." Proceedings of the fifth ACM SIGACT-SIGMOD symposium on
Principles of database systems. 1985.

[12] Seshadri, Praveen, et al. "Cost-based optimization for magic: Algebra and
implementation." SIGMOD 1996.
