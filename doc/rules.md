# Datalevin Rules Engine

Datalevin's rule engine implements an efficient Datalog rules evaluation
strategy. It also includes a novel provenance based mechanism for incremental
maintenance.

## Motivation

Datomic flavor of Datalog is interesting, not just in term of its Clojuristic
syntax, but more so in its marrying the traditional SQL-like structure with the
logic based traditional Datalog syntax. The latter may feel alien and hard to
understand for many developers. By using the SQL-like syntax as the basis and
treating traditional Datalog syntax as special rule clauses, users are
gradually introduced to this logic view of Datalog, a win in ergonomics.

The previous implementation of rule clause evaluation inherited from Datascript
uses a top-down evaluation strategy, which is known to be less efficient than
the bottom-up strategy [2]. To address this deficiency, also to leverage the
performance gains of our new query engine, we re-implement the rules evaluation
engine using the latest research advances in bottom-up rule evaluation strategy,
with stratified negation and aggregation [1], with subsumptive demand
transformation [2].



## References

[1] T. J. Green, S. Huang, B. T. Loo, W. Zhou. Datalog and Recursive Query
Processing. Foundations and Trends in Databases, vol. 5, no. 2, pp. 105–195, 2012.

[2] Tekle, K. T., and Y. A. Liu. "More efficient datalog queries: subsumptive tabling beats magic sets." Proceedings of the 2011 ACM SIGMOD International Conference on Management of data. 2011.

[3] Ullman, J. D. "Bottom-up beats top-down for datalog." Proceedings of the eighth ACM SIGACT-SIGMOD-SIGART symposium on Principles of Database Systems. 1989.
