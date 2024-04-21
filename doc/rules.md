# Datalevin Rules Engine

Datalevin's rule engine implements an efficient semi-naive Datalog evaluation
strategy with stratified negation and aggregation [1]. It also includes
a novel provenance based mechanism for incremental maintenance.

## Motivation

Datomic flavor of Datalog is interesting, not just in term of its Clojuristic
syntax, but more so in its marrying the traditional SQL-like structure with the
logic based traditional Datalog syntax. The latter may feel alien and hard to
understand for many developers. By using the SQL-like syntax as the basis and
treating traditional Datalog syntax as a special rule clause, users are
gradually introduced to this logic view of Datalog. However, this is not just a
win in ergonomics, but also helps with leveraging performance gains in query
optimizations.

The previous implementation of rule evaluation inherited from Datascript uses a
top-down evaluation strategy, which can be less efficient than the bottom-up
strategy [2]. We instead implements a bottom-up rule evaluation strategy.

## References

[1] T. J. Green, S. Huang, B. T. Loo, W. Zhou. Datalog and Recursive Query
Processing. Foundations and Trends in Databases, vol. 5, no. 2, pp. 105–195, 2012.

[2] Ullman, J. D. "Bottom-up beats top-down for datalog." Proceedings of the eighth ACM SIGACT-SIGMOD-SIGART symposium on Principles of Database Systems. 1989.
