---
title: "Arrangements"
description: "Understand how Materialize arrangements work."
menu:
  main:
    parent: 'overview'
    weight: 4
---

We call the mechanisms that maintain materialized views for Materialize dataflows **arrangements**, and understanding them better can help you make decisions that will reduce memory usage while maintaining performance.

## Materialized views

Before we talk about the arrangements that maintain materialized views, let's review what materialized views are, how they work in traditional databases, and how they work in Materialize.

A view is simply a query saved under a name for convenience; the query is executed each time the view is referenced, without any savings in performance or speed. But some databases also support something more powerful––materialized views, which save the *results* of the query for quicker access.

Traditional databases typically only have limited support for materialized views in two ways: first, the updates to the views generally occur at set intervals, so views are not updated in real time, and second, only a limited subset of SQL syntax is supported. These limitations stem from limited support for incremental updates; most databases are not designed to maintain long-running incremental queries, but instead are optimized for queries that are executed once and then wound down.

This means that when the data changes, the materialized view must be recomputed from scratch in all but a few simple cases. Our raison d’être at Materialize is doing something better than this: Materialize stores materialized views in memory, to make them faster to access, and incrementally updates the views as the data changes, to maintain correctness.

## Dataflows

Materialize can make incremental updates efficiently because it's built on an advanced stream processing dataflow engine, [Differential Dataflow](https://timelydataflow.github.io/differential-dataflow/introduction.html), which in turn is built on a distributed stream processing framework called [Timely Dataflow](https://timelydataflow.github.io/timely-dataflow/).

When you create a materialized view and issue a query, Materialize creates a **dataflow**. A dataflow consists of the following:

- An execution plan for the query
- Independent computational components, or **operators**
- A description of the connections between these components
- Instructions on how to respond to data input, whether the input is static (fed in all at once) or dynamic (new data will continue to arrive after the initial dataflow execution)

Once executed, the dataflow computes and stores the result of the SQL query in memory, polls the source for updates, and then incrementally updates the query results when new data arrives.

The cost of maintaining a dataflow can be very different from the cost of executing a query once. Materialize queries are executed as long-lived stateful dataflows, and impose an ongoing cost of computation and memory. As input data changes, we want to respond quickly and without requiring a full query re-evaluation. This results in an optimization process that prioritizes the re-use of already materialized data.

### Collections

Materialize dataflows act on **collections** of data, [multisets](https://en.wikipedia.org/wiki/Multiset) that store events in an update stream as triples of `(data, time, diff)`.

Term | Definition
-----|-----------
**data**  |  The record update.
**time**  |  The logical timestamp of the update.
**diff**  |  The type of update (`-1` for deletion, `1` for addition or upsert)

Both inputs and outputs are expressed as collections.

## Arrangements

To transform an input into an output, operators
build and maintain indexes on the input and output collections so that they can randomly access collection data in the future. Because queries can overlap, different operators often need to build the exact same indexes for multiple queries. Instead of asking operators to perform redundant work, Materialize simply builds the index once and maintains it in memory, sharing the required resources across all the operators that use the indexed data. The index is then effectively a sunk cost, and the cost of each query is determined only by the new work it introduces.

We call the indexed update stream an **arrangement**.

### Arrangement structure

In the `(data, time, diff)` triples that make up the update stream, `data` is structured as a key-value pair.  Arrangements are indexed on the `data` keys.

<!-- Is this correct? -->
Note that this is not the same as the index created by the `CREATE INDEX` command. `CREATE INDEX` creates an index on a **source** or **table**. An arrangement is an index on the **update stream**. You cannot manually create an arrangement, although you can influence the shape of the arrangements created by Materialize by the way you structure queries and views.

When Materialize creates an arrangement, it attempts to choose a key that will ensure that data is well distributed. If there is a primary key, that will be used; if there are source fields not required by the query, they are not included. Often Materialize can pull primary key info from a Confluent or Kafka schema.

### Arrangement size

The size of an arrangement is related to the size of its accumulated updates, but not directly proportional to them. Instead, an arrangement's size is roughly equivalent to its number of distinct `(data, time)` pairs, which can be small even if the number of records is large. As an illustration, consider a histogram of taxi rides grouped by the number of riders and the fare amount. The number of `(rider, fare)` pairs will be much smaller than the number of total rides that take place.

The size of the arrangement is then further reduced by background [compaction](/ops/deployment/#compaction) of historical data.

### Example arrangements

Let's take a look at some of the arrangements that Materialize creates for different circumstances.

#### `COUNT`

Some of the simplest arrangements are those for Differential Dataflow operators. For example, the `COUNT` operator, which predictably counts items, has two arrangements:

* An arrangement for the input, which indexes the key whose values will be counted
* An arrangement for the output, which maintains the results

`COUNT` reads from the input arrangement to count values, and both reads from the output (to see the previous total) and writes to the output (adding the new updates to the previous total).

![Diagram of arrangements for count](/images/arrangements-count.png "Diagram of arrangements for COUNT")

#### Three-way join

Most exciting place for arrangements is JOINS.

![Diagram of arrangements for a three-way join](/images/arrangements-3-way-join.png "Diagram of arrangements for a three-way join")

Create a materialized view: 3-way join group by fields 1,2,3  - Mz creates arrangements


Trad: if you join on 3 columns, it builds on 1+2+3, but also 1+2, 1+3, and 2+3. Mz doesn’t do this. (Ask Andy about example again)

## Analyzing arrangements

Materialize provides various tools that allow you to analyze arrangements, although they are post hoc tools best used for debugging, rather than planning tools to be used before creating indexes or views. See [Diagnosing Using SQL](/ops/diagnosing-using-sql/) and [Monitoring](/ops/monitoring/) for more details.

## Reducing memory usage

### Creating indexes manually

You can't create arrangements directly, but in some cases you can manually create indexes that will help Materialize create more sharable arrangements -- and therefore reduce memory usage.

For maximal control, you can create an unmaterialized view, then create an index on the view for the keys you know you'll want to search on. This can create a significant memory reduction for cases where Materialize is unable to detect the primary key, for example, if you have a customerID but never actually use it and always search on the join of customer first and last names.

For more examples of cases where you might want to create an index manually, see [Joins in Materialize](https://materialize.com/joins-in-materialize/).

### Casting the data type

Currently, Materialize handles implicit casts in a very [memory-intensive way](https://github.com/MaterializeInc/materialize/issues/4171). Until this issue is resolved, you can reduce memory usage by building an index on the view with the type changed for any queries which include implicit casts, for example, when you combine 32-bit and 64-bit numbers.

## Related topics

* [Joins in Materialize](https://materialize.com/joins-in-materialize/)
* [Diagnosing Using SQL](/ops/diagnosing-using-sql/)
* [Deployment](/ops/deployment/)
* [Differential Dataflow](https://timelydataflow.github.io/differential-dataflow/)
