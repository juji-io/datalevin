# Math Genealogy Benchmark

This directory contains a benchmark of Datalog query performance. This benchmark
was first introduced in a book chapter [1], which compared two Datalog
implementations, XSB [2] and LogicBlox [3], while describing Datomic [4] without
comparing it.

Our purpose of including this benchmark is to test the performance of rule
resolution, as the queries in this benchmark are all based on Datalog rules.

## Data

The data was originally from [Mathematics Genealogy
Project](https://genealogy.math.ndsu.nodak.edu/). We directly use the data set scraped by [this
project](https://github.com/j2kun/math-genealogy-scraper), a `data.json` file  (90MiB, gzipped to 18MiB), scraped on 2019/6/17.

The data set consist of information about 256769 dissertations in mathematics,
256767 student names, and 276635 facts of advisor-student relationship.

## Queries

Four queries are proposed in the benchmark.

### Q1

> Who are the grand-advisors of David Scott Warren?



### Q2

> Which candidates got their degrees from the same university as their advisor?


### Q3

> Which candidates worked in a different area than at least one of their advisors?

### Q4

> Who are the academic ancestors of David Scott Warren?


## Run benchmarks

```
./bench.clj
```

## Results


| System    | Q1 | Q2 | Q3 | Q4
| -------- | ------- | -------- | -------- | -------- |
| Datalevin 0.9.5  | 170 | 1545 | 1280 | 643 |
| Datalevin latest | | | | |

For reference, as described in the book chapter [1], the best results after
manually tweaking queries and adding indices for XSB and LogicBlox, using a slightly smaller data set
(202505 dissertations, 198962 people, and 211107 advising facts), but on a
slower machine (Intel Core i5 2.8 GHz with 8GB RAM) are the following:

| System    | Q1 | Q2 | Q3 | Q4
| -------- | ------- | -------- | -------- | -------- |
| XSB  | 238 | 511 | 325 | 181 |
| LogicBlox | 944 | 2140 | 1740 | 1140 |

LogicBlox is no longer available. A comparison with the latest version of XSB
will be a future work.

## References

[1] Maier, David, et al. "Datalog: concepts, history, and outlook." In
Declarative Logic Programming: Theory, Systems, and Applications. 2018. 3-100.

[2] T. Swift and D. S. Warren. 2011. "XSB: Extending the power of Prolog using
tabling." In Theory and Practice of Logic Programming , vol. 12, no. 1-2, pp.
157–187.

[3] M. Aref, et al. 2015. Design and implementation of the LogicBlox system. In
Proc. of the 2015 ACM SIGMOD International Conference on Management of Data, pp.
1371–1382

[4] J. Anderson, et al. 2016. The Datomic database. In Professional Clojure, pp.
169–215.
