---
headless: true
---
For indexed views and materialized views that contain [window
functions](/sql/functions/#window-functions) (including aggregate functions used
with an `OVER` clause), when an input record in a partition is
added/removed/changed, Materialize **recomputes the results from scratch** for
that partition (instead of using incremental computation).

The `PARTITION BY` clause of your window function determines your partitions. If
`PARTITION BY` is omitted, all records belong to a single partition (i.e., any
record change results in a recomputation from scratch over the whole input).

To avoid performance issues that may arise as the number of records grows,
consider rewriting your indexed views and materialized views to use idiomatic
Materialize SQL instead of window functions. If your view definitions cannot be
rewritten without the window functions and the performance of window functions
is insufficient for your use case, please [contact our team](/support/).
