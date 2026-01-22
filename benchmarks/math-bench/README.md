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

You will need both lein and clj build tools. lein is for building the project
and running tests. clj is for running this benchmark.

```
lein test
gzip -d data.json.gz
./bench.clj
```

## Results

Tests were conducted on a Macbook Pro M3 2023 with 36GB RAM,
using OpenJDK version "21.0.9" 2025-10-21 and Clojure 1.12.3.

The table below lists the query latency results in milliseconds.

| System    | Q1 | Q2 | Q3 | Q4
| -------- | ------- | -------- | -------- | -------- |
| Datomic 1.0.7469   | 1275.1 | 1296.7 | 967.2 | 41192.9 |
| Datascript 1.7.8  | 109.7 | 707.2 | 584.7 | Out of Memory |
| Datalevin latest | 14.4 | 330.9 | 269.6 | 2.9 |

Notice that Q4 is particularly challenging. It has a recursive rule that
computes exponentially larger transitive closures. Datomic took 40 seconds to
finish, whereas Datalevin took less than 40 milliseconds (more than 1000X
faster), and Datascript ran out of memory for this one.

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
