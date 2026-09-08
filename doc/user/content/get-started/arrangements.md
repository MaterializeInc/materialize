---
title: "Arrangements"
description: "Understand how Materialize arrangements work."
menu:
  main:
    parent: concepts
    weight: 22
    identifier: 'concepts-arrangements'
aliases:
  - /overview/arrangements/
---

Materialize keeps the state that its dataflows need in memory as
**arrangements**, an indexed representation of a collection. Arrangements are
typically the largest part of a busy cluster's memory, so knowing where they
come from and which of them are shared lets you trade memory for speed
deliberately.

## Materialized views

A view is a query saved under a name; the query runs each time the view is
referenced. A materialized view instead keeps the query's *results* up to date
as its inputs change.

Traditional databases support materialized views only in limited ways: the
view refreshes at set intervals rather than in real time, only a limited subset
of SQL is supported, or every refresh recomputes the view from scratch. These
limitations stem from engines that are optimized for queries that run once and
then wind down, not for maintaining long-running incremental queries.

Materialize is built to maintain long-running incremental queries. It
incrementally updates a much broader class of views than is common in
traditional databases, for example views over multi-way joins with complex
aggregations, and does so in the presence of arbitrary inserts, updates, and
deletes in the inputs while maintaining correctness.

## Dataflows

Materialize can make incremental updates efficiently because it's built on an
incremental data-parallel compute engine, [Differential Dataflow](https://timelydataflow.github.io/differential-dataflow/introduction.html),
which in turn is built on a distributed processing framework called
[Timely Dataflow](https://timelydataflow.github.io/timely-dataflow/).

An index, a materialized view, a `SUBSCRIBE`, and a `SELECT` that cannot be
answered from an existing index each run as a **dataflow**: a graph of
operators that computes the query's result once and then updates it
incrementally as its inputs change. A dataflow reads its inputs from storage,
or from an index's arrangement when the cluster has one.

### Collections

Materialize dataflows act on **collections** of data, [multisets](https://en.wikipedia.org/wiki/Multiset)
that store each event in an update
stream as a triple of `(data, time, diff)`.

Term | Definition
-----|-----------
**data**  |  The record update.
**time**  |  The logical timestamp of the update.
**diff**  |  The change in the number of copies of the record (typically `-1` for deletion, `1` for addition).

## Arrangements

A collection is a stream of updates. To give operators fast access to the
current state of a collection, and to the changes of individual records,
Materialize also keeps collections in an indexed form: an **arrangement**
stores a collection's updates organized by a key, ready to look up every
`(data, time, diff)` entry for that key. The key is chosen by whatever builds
the arrangement: the columns an index is created on, or the columns a join
matches on.

### Where arrangements come from

Arrangements appear in two places:

- An index. `CREATE INDEX` builds an arrangement of the whole indexed
  collection by the index key and keeps it up to date.
- Inside a dataflow, wherever an operator needs random access to its input or
  has to keep state. Every input of a join is arranged by the columns the join
  matches on; an aggregation (`GROUP BY`, `DISTINCT`) keeps its input and its
  results arranged by the group key; `TopK` and `MIN`/`MAX` keep a hierarchy
  of arrangements. The [operator reference](/sql/explain-plan/#reference-plan-operators)
  of `EXPLAIN PLAN` lists which operators arrange, and `EXPLAIN PHYSICAL PLAN`
  shows the arrangements a query will build.

### What is shared

Only an index shares its arrangement: `CREATE INDEX` builds a collection's
arrangement once, and every dataflow on that cluster that can use it imports
it instead of building its own. Every other arrangement is private to the
dataflow that built it. A materialized view writes its output to storage
rather than keeping it arranged, so a query over a materialized view arranges
what it needs itself. A plain view is compiled into each dataflow that uses
it: a view used twice within one dataflow is computed once there, but five
dataflows over one view compute and arrange it five times unless the view is
indexed.

You can find a more detailed analysis of the arrangements built for different
types of queries in our blog post on [Joins in Materialize](https://materialize.com/blog/joins-in-materialize/).

### Arrangement size

An arrangement holds one update per distinct `(data, time)` pair it has seen.
Background compaction advances the times that no reader needs any more and
merges the updates, so a settled arrangement holds one record per distinct row
currently present, and its size is roughly that record count times the bytes
per record. Right after a large change, until the merge finishes, it
transiently holds both the old and the new batches.

The record count can be small even when the number of input rows is large. As
an illustration, consider a histogram of taxi rides grouped by the number of
riders and the fare amount. The number of distinct `(riders, fare)` groups is
much smaller than the number of rides, and the arrangement holds one record
per group.

Memory use peaks while a dataflow [hydrates](/concepts/hydration/) and builds
its arrangements from scratch.

## Analyzing arrangements

Materialize provides tools to analyze arrangements. `EXPLAIN PHYSICAL PLAN`
shows the arrangements a query will build and the indexes it will use, before
you create it. [`EXPLAIN ANALYZE`](/sql/explain-analyze/) attributes the memory
of a running index or materialized view to its operators.
[`mz_introspection.mz_dataflow_arrangement_sizes`](/reference/system-catalog/mz_introspection/#mz_dataflow_arrangement_sizes)
reports the records and bytes of every dataflow's arrangements. See
[Troubleshooting dataflows](/transform-data/dataflow-troubleshooting/) for how
to use them.

## Reducing memory usage

For join ordering, group size hints, and other query-level levers, see
[Optimization](/transform-data/optimization/). The two levers below are about
arrangements themselves.

### Choosing index keys

An index is a full copy of its collection arranged by the index key, so every
index costs memory proportional to the collection's size. A dataflow uses an
index only when its key is exactly the expression the dataflow needs for that
input: the columns it joins on, or the columns a point lookup filters on. Index
the columns your queries join and look up by, and index a view that several
dataflows share so that it is computed and arranged once instead of once per
consumer.

Without explicit key columns, `CREATE INDEX` uses a unique key of the
collection if Materialize knows one, for example a key declared for a source
or the grouping columns of a `GROUP BY`, and otherwise all columns; see
[`CREATE INDEX`](/sql/create-index/#indexed-expressions-vs-stored-columns).

### Type casting

A join condition that needs an implicit cast, for example comparing a 32-bit
and a 64-bit integer, keys the arrangement on the cast expression. An index on
the plain column is then read as a full scan and the dataflow builds a second
arrangement of that input. Index the cast expression
(`CREATE INDEX ON t (a::bigint)`) or align the column types.

## Related pages

* [Indexes](/concepts/indexes/)
* [Optimization](/transform-data/optimization/)
* [Troubleshooting dataflows](/transform-data/dataflow-troubleshooting/)
* [`EXPLAIN PLAN`](/sql/explain-plan/) and [`EXPLAIN ANALYZE`](/sql/explain-analyze/)
* [Joins in Materialize](https://materialize.com/blog/joins-in-materialize/)
* [Differential Dataflow](https://timelydataflow.github.io/differential-dataflow/)
