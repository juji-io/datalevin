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
256767 student, and 276635 facts of advisor-student relationship.

## Queries

Four queries are proposed in the benchmark, based on the following rules:

```Clojure
(def rule-author '[[(author ?d ?c)
                    [?d :dissertation/cid ?c]]])

(def rule-adv '[[(adv ?x ?y)
                 [?x :person/advised ?d]
                 (author ?d ?y)]])

(def rule-area '[[(area ?c ?a)
                  [?d :dissertation/cid ?c]
                  [?d :dissertation/area ?a]]])

(def rule-univ '[[(univ ?c ?u)
                  [?d :dissertation/cid ?c]
                  [?d :dissertation/univ ?u]]])

(def rule-anc '[[(anc ?x ?y)
                 (adv ?x ?y)]
                [(anc ?x ?y)
                 (adv ?x ?z)
                 (anc ?z ?y)]])

(def rule-q1 (into rule-author rule-adv))

(def rule-q2 (into rule-q1 rule-univ))

(def rule-q3 (into rule-q1 rule-area))

(def rule-q4 (into rule-q1 rule-anc))

```

### Q1

> Who are the grand-advisors of David Scott Warren?

```Clojure
(d/q '[:find [?n ...]
           :in $ %
           :where
           [?d :person/name "David Scott Warren"]
           (adv ?x ?d)
           (adv ?y ?x)
           [?y :person/name ?n]]
         db rule-q1)
```

### Q2

> Which candidates got their degrees from the same university as their advisor?

```Clojure
(d/q '[:find [?n ...]
           :in $ %
           :where
           (adv ?x ?y)
           (univ ?x ?u)
           (univ ?y ?u)
           [?y :person/name ?n]]
         db rule-q2)
```

### Q3

> Which candidates worked in a different area than at least one of their advisors?

```Clojure
(d/q '[:find [?n ...]
           :in $ %
           :where
           (adv ?x ?y)
           (area ?x ?a1)
           (area ?y ?a2)
           [(!= ?a1 ?a2)]
           [?y :person/name ?n]]
         db rule-q3)
```

### Q4

> Who are the academic ancestors of David Scott Warren?


```Clojure
(d/q '[:find [?n ...]
           :in $ %
           :where
           [?x :person/name "David Scott Warren"]
           (anc ?y ?x)
           [?y :person/name ?n]]
         db rule-q4)
```

## Run benchmarks

```
gzip -d data.json.gz
./bench.clj
```

## Results

Tests were conducted on Ubuntu 22.04 with Intel Core i7 3.6GHz and 64GB RAM,
using OpenJDK 17.0.10, Clojure 1.11.2.

The table below list the query latency results in milliseconds.

| System    | Q1 | Q2 | Q3 | Q4
| -------- | ------- | -------- | -------- | -------- |
| Datomic 1.0.7057   | 3153 | 2926 | 2297 | 112016 |
| Datalevin latest | TBD | TBD | TBD | TBD |
| Datascript 1.6.5  | 302 | 1784 | 1546 | Out of Memory |
| Datalevin 0.9.5  | 186 | 1527 | 1343 | Out of Memory |

Notice that Q4 is particularly challenging. It is a recursive query that
computes progressively larger transitive closures. Datomic took close to 2
minutes to finish. Datascript and Datalevin 0.9.5, sharing the same
implementation, both ran out of memory.

For reference, as described in the book chapter [1], the best results after
manually tweaking queries and adding indices for XSB and LogicBlox, using a
smaller data set (202505 dissertations, 198962 people, and 211107 advising
facts), but on a slower machine (Intel Core i5 2.8 GHz with 8GB RAM) are the
following:

| System    | Q1 | Q2 | Q3 | Q4
| -------- | ------- | -------- | -------- | -------- |
| XSB  | 238 | 511 | 325 | 181 |
| LogicBlox | 944 | 2140 | 1740 | 1140 |


## References

[1] D. Maier, et al. "Datalog: concepts, history, and outlook." In
Declarative Logic Programming: Theory, Systems, and Applications. 2018. 3-100.

[2] T. Swift and D. S. Warren. 2011. "XSB: Extending the power of Prolog using
tabling." In Theory and Practice of Logic Programming , vol. 12, no. 1-2, pp.
157–187.

[3] M. Aref, et al. 2015. Design and implementation of the LogicBlox system. In
Proc. of the 2015 ACM SIGMOD International Conference on Management of Data, pp.
1371–1382

[4] J. Anderson, et al. 2016. The Datomic database. In Professional Clojure, pp.
169–215.
