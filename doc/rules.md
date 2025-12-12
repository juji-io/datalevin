# Datalevin Rules Engine (WIP)

Datalevin has an innovative rule engine that has more expressive powerful than
normal Datalog, and implements an efficient rules evaluation strategy that
leverage the cost based query optimizer. It also includes a novel provenance
based mechanism for incremental maintenance.

## Motivation

The power of Datalog compared to a relational query language lies in the
expressiveness in handling logical rules that can be recursively defined.
However, the logic syntax of traditional Datalog may feel alien and hard to
grasp for some developers. Datomic flavor of Datalog is beneficial, not just in
term of its Clojuristic syntax, but more so in its marrying the traditional
SQL-like structure with traditional Datalog. By using the SQL-like syntax as the
basis and treating traditional Datalog syntax as special rule clauses, users are
gradually introduced to this logic view of Datalog, a win in ergonomics.

The previous implementation of rule clause evaluation algorithm inherited from
Datascript uses a top-down evaluation strategy, which can be less efficient than
a bottom-up strategy [4]. For example, it is prone to run out of memory due to
explosion of tuples from recursive rules. More importantly, this top-down method
cannot take advantage of our cost based query optimizer. To address this
deficiency, we developed a new rules evaluation engine using the latest research
advances in bottom-up Datalog evaluation strategy, with some innovation of our
own.


## Rule Evaluation

The new rule engine uses a bottom-up Datalog evaluation strategy.

### Semi-naive evaluation (SNE)

The rule engine employs semi-naive evaluation [1] [2]. The engine generates
tuples from the rule sets until a fix-point is reached, i.e. when no new tuples
are produced. The evaluation is stratified, where rules run in their strongly
connected components (stratum).

### Seeding tuples (new)

Compared with a standalone SNE engine, Datalevin rule engine is part of the
query engine, so it does not work off a blank slate, but base the work on a warm
start of a set of already produced tuples from outer query clauses. These
seeding tuples are often produced more efficiently than SNE, as they benefit
from indices and the cost based query optimizer. These seeds effectively act as
filters to prevent the generation of unnecessary tuples during SNE.

### Pull-out of non-recursive rule clauses (new)

As an innovation, we identify the clauses that are not involved in recursions,
pull them out and add them to the regular query clauses to allow the cost-based
query optimizer to work on them. That is to say, the rule engine mainly works on
the rules that involved in recursions. This optimization not just simplifies the
rules, but also reduces the number of tuples the rule engine has to generate.
This approach enjoys the benefits of the rule rewrite algorithms such as magic
sets, while avoiding potential slow-down due to the explosion of magic rules
brought by the rewrite.

### Temporel Elimination

To handle recursion with freely mixed aggregation and negation, we mainly follow
the framework proposed in Temporel [3], which is based on a concept of
T-stratification, where a time index is identified and associated with each
stratum. The time index ensures that aggregation/negation happens in one
stratum. It also enables optimizations such as temporal elimination, where only
the results of the last iteration of recursion is needed, so that the recursive
process can be optimized to avoid storing intermediate results.

## Datalog Extensions

We extended the rule syntax to support more powerful applications,
e.g. to implement algorithm for machine learning, statistics, graph analytics,
and so on, enabling in-database data analytics. Examples of such analytics
include gradient descent (hence all types of regressions, SVM, etc.), K-means,
page-rank, and so on.

The primary extension to the rule syntax is to allow aggregation functions, eg.
`sum`, `count`, etc. to appear in the rule head. This extension allows free
mixing of aggregations in recursions, thus increases the expressiveness of
Datalevin rule language significantly.

## References

[1] T. J. Green, S. Huang, B. T. Loo, W. Zhou. Datalog and Recursive Query
Processing. Foundations and Trends in Databases, vol. 5, no. 2, pp. 105–195, 2012.

[2] Maier, David, et al. "Datalog: concepts, history, and outlook." in
Declarative Logic Programming: Theory, Systems, and Applications. 2018. 3-100.

[3] Shaikhha, Amir, et al. "Optimizing Nested Recursive Queries." Proceedings of
the ACM on Management of Data, 2(1). 2024: 1-27.

[4] Ullman, J. D. "Bottom-up beats top-down for datalog." Proceedings of the
eighth ACM SIGACT-SIGMOD-SIGART symposium on Principles of Database
Systems. 1989.
