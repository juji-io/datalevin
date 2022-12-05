# Datalevin Recipes

## Bulk Load Data

### By Transaction

The most straightforward method of transacting one map at a time using
`transact!` works well, as
Datalevin by default use asynchronous commit, so the overhead of many commits is
not significant.

To avoid this small overhead, one can wrap these `transact!`
calls inside a single transaction by using `with-transaction`, a slight speed up
can be achieved, so this is the recommended approach.

Because Datalevin supports only a single write thread at a time, parallel
transactions actually slow writes down significantly due to the thread switching
overhead.

Transacting a large number of maps in a single `transact!` call, though slightly faster,
tends to blow up the size of the resulting data file, due to the fact that LMDB
 has less chance to reuse discarded pages. Wrapping it in `with-transaction`
 solve the size problem, but can be significantly slower, because when memory map
 resizes, `with-transaction` has to rebuild a large transaction context.
