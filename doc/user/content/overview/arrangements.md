---
title: "Arrangements"
description: "Understand how Materialize arrangements work."
menu:
  main:
    parent: advanced
    weight: 4
---

The mechanisms that maintain materialized views for Materialize dataflows are
called **arrangements**. Understanding arrangements better can help you make
decisions that will reduce memory usage while maintaining performance.

## Materialized views

Before we talk about the arrangements that maintain materialized views, let's
review what materialized views are, how they work in traditional databases, and
how they work in Materialize.

A view is simply a query saved under a name for convenience; the query is
executed each time the view is referenced, without any savings in performance
or speed. But some databases also support something more powerful: materialized
views, which save the *results* of the query for quicker access.

Traditional databases typically only have limited support for materialized views
in two ways: first, the updates to the views generally occur at set intervals,
so views are not updated in real time, and second, only a limited subset of SQL
syntax is supported. In cases where a traditional database *does* support
refreshes for each data update, it tends to be very slow. These limitations
stem from limited support for incremental updates; most databases are not
designed to maintain long-running incremental queries, but instead are
optimized for queries that are executed once and then wound down. This means
that when the data changes, the materialized view must be recomputed from
scratch in all but a few simple cases.

Our mission at Materialize is to manage materialized views better than this.
supports incrementally updating a much broader set of views than is common in
traditional databases (e.g. views over multi-way joins with complex
aggregations), and can do incremental updates in the presence of arbitrary
inserts, updates, and deletes in the input streams while maintaining
correctness.

## Dataflows

Materialize can make incremental updates efficiently because it's built on an
incremental data-parallel compute engine, [Differential Dataflow](https://timelydataflow.github.io/differential-dataflow/introduction.html),
which in turn is built on a distributed processing framework called
[Timely Dataflow](https://timelydataflow.github.io/timely-dataflow/).

When you create a materialized view and issue a query, Materialize creates
a **dataflow**. A dataflow consists of instructions on how to respond to data
input and to changes to that data. Once executed, the dataflow computes the
result of the SQL query, polls the source for updates, and then incrementally
updates the query results when new data arrives.

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

A collection provides a data stream of updates as they happen. To provide fast
access to the changes to individual records, the collection can be represented
in an alternate form, indexed on `data` to present the sequence of changes
(`time, diff`) the collection has undergone. This indexed representation is
called an **arrangement**.

Materialize builds and maintains indexes on both the input and output
collections as well as for many intermediate collections created when
processing a query. Because queries can overlap, Materialize might need to
build the exact same indexes for multiple queries. Instead of performing
redundant work, Materialize builds the index once and maintains it in memory,
sharing the required resources across all queries that use the indexed data.
The index is then effectively a sunk cost, and the cost of each query is
determined only by the new work it introduces.

You can find a more detailed analysis of the arrangements built for different
types of queries in our blog post on [Joins in Materialize](https://materialize.com/joins-in-materialize).

### Arrangement size

The size of an arrangement, or amount of memory it requires, is roughly
proportional to its number of distinct `(data, time)` pairs, which can be small
even if the number of records is large. As an illustration, consider a
histogram of taxi rides grouped by the number of riders and the fare amount.
The number of distinct `(rider, fare)` pairs will be much smaller than the
number of total rides that take place.

The amount of memory that the arrangement requires is then further reduced by
background compaction of historical data.

## Analyzing arrangements

Materialize provides various tools that allow you to analyze arrangements,
although they are post hoc tools best used for debugging, rather than planning
tools to be used before creating indexes or views. See [Diagnosing Using SQL](/ops/troubleshooting/)
and [`EXPLAIN`](/sql/explain/) for more details.

## Reducing memory usage

### Creating indexes manually

When creating an arrangement for a join where the key is not clear, Materialize
attempts to choose a key that will ensure that data is well distributed. If
there is a primary key, that will be used; if there are source fields not
required by the query, they are not included. Often Materialize can pull
primary key info from a Confluent schema.

If Materialize cannot detect a primary key, the default key is the full set of
columns, in order to ensure good data distribution. Creating an unmaterialized
view and then specifying a custom index makes the key smaller.

For more examples of cases where you might want to create an index manually,
see [Joins in Materialize](https://materialize.com/joins-in-materialize/).

### Casting the data type

Currently, Materialize handles implicit casts in a very [memory-intensive way](https://github.com/MaterializeInc/materialize/issues/4171).
Until this issue
is resolved, you can reduce memory usage by building an index on the view with
the type changed for any queries which include implicit casts, for example,
when you combine 32-bit and 64-bit numbers.

## Related topics

* [Joins in Materialize](https://materialize.com/joins-in-materialize/)
* [Diagnosing Using SQL](/ops/troubleshooting/)
* [Deployment](/ops/optimization/)
* [Differential Dataflow](https://timelydataflow.github.io/differential-dataflow/)
