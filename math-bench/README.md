# Math Genealogy Benchmark

This directory contains a benchmark that first appeared in a book chapter [1],
which compared two Datalog implementations, while listing examples of Datomic
without comparing it.

The purpose of this benchmark to test the performance of rule resolution, as the
queries in this benchmark are all based on Datalog rules.

## Data

The data was originally from [Mathematics Genealogy
Project](https://genealogy.math.ndsu.nodak.edu/), scraped by [this
project](https://github.com/j2kun/math-genealogy-scraper). We directly use their
`data.json` result file, scraped on 2019/6/17.

The data consist of 256769 Ph.D. dissertations titles, student
names, advisor names, years, schools, subjects, and other meta information.

## Run benchmarks

## Results

Five queries are proposed in the benchmark.

### Q1 Who are the grand-advisors of David Scott Warren?


### Q2 Which candidates got their degrees from the same university as their advisor?


### Q3 Which candidates worked in a different area than at least one of their advisors?

### Q4 Who are the academic ancestors of David Scott Warren?


### Q5 How many academic ancestors does David Scott Warren have?


[1] Maier, David, et al. "Datalog: concepts, history, and outlook." in
Declarative Logic Programming: Theory, Systems, and Applications. 2018. 3-100.
