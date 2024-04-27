# Datalevin Rules Engine

Datalevin has an innovative rule engine that implements an efficient rules
evaluation strategy that leverage the cost based query optimizer. It also
includes a novel provenance based mechanism for incremental maintenance.

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
a bottom-up strategy [4]. More importantly, this implementation cannot take
advantage of our cost based query optimizer. To address this deficiency, we
developed a new rules evaluation engine using the latest research advances in
bottom-up Datalog evaluation strategy, with some innovation of our own.

## Rule Evaluation

Datalevin rule engine implements semi-naive Datalog evaluation algorithm with
stratified negation and aggregation [1] [2]. It also conduct subsumptive demand
transformation[3].

### Pull-out of non-recursive rule clauses (new)

As an innovation of our implementation, we identify the clauses that are not
involved in recursive rule resolution, pull them out and add to the regular
query clauses to allow the query optimizer to work on them. This optimization
not just simplifies the rules, but also reduces the number of datoms the rule
engine has to process. It enjoys the benefits of the well known magic sets
transformation, while avoiding potential slow-down due to un-beneficial transformations.

## References

[1] T. J. Green, S. Huang, B. T. Loo, W. Zhou. Datalog and Recursive Query
Processing. Foundations and Trends in Databases, vol. 5, no. 2, pp. 105–195, 2012.

[2] Maier, David, et al. "Datalog: concepts, history, and outlook." in
Declarative Logic Programming: Theory, Systems, and Applications. 2018. 3-100.

[3] Tekle, K. T., and Y. A. Liu. "More efficient datalog queries: subsumptive
tabling beats magic sets." Proceedings of the 2011 ACM SIGMOD International
Conference on Management of data. 2011.

[4] Ullman, J. D. "Bottom-up beats top-down for datalog." Proceedings of the
eighth ACM SIGACT-SIGMOD-SIGART symposium on Principles of Database
Systems. 1989.
