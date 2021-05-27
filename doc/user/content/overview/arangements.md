---
title: "Arrangements"
description: "Understand how Materialize arrangements work."
menu:
  main:
    parent: 'overview'
    weight: 4
---

We call the mechanisms that maintain materialized views for Materialize dataflows **arrangements**. Understanding them better can help you make decisions that will reduce memory usage while maintaining performance.

## Materialized views

Before we talk about the arrangements that maintain materialized views, let's review what materialized views are, how they work in traditional databases, and how they work in Materialize.

A view is simply a query saved under a name for convenience; the query is executed each time the view is referenced, without any savings in performance or speed. But some databases also support something more powerful––materialized views, which save the *results* of the query for quicker access.

Traditional databases typically only have limited support for materialized views in two ways: first, the updates to the views generally occur at set intervals, so views are not updated in real time, and second, only a limited subset of SQL syntax is supported. These limitations stem from limited support for incremental updates; most databases are not designed to maintain long-running incremental queries, but instead are optimized for queries that are executed once and then wound down. This means that when the data changes, the materialized view must be recomputed from scratch in all but a few simple cases.

Our raison d’être at Materialize is doing something better than this: Materialize stores materialized views in memory, to make them faster to access, and incrementally updates the views as the data changes, to maintain correctness.

## Dataflows

Materialize can make incremental updates efficiently because it's built on an incremental data-parallel compute engine, [Differential Dataflow](https://timelydataflow.github.io/differential-dataflow/introduction.html), which in turn is built on a distributed processing framework called [Timely Dataflow](https://timelydataflow.github.io/timely-dataflow/).

When you create a materialized view and issue a query, Materialize creates a **dataflow**. A dataflow consists of instructions on how to respond to data input. Once executed, the dataflow computes and stores the result of the SQL query in memory, polls the source for updates, and then incrementally updates the query results when new data arrives.

### Collections

Materialize dataflows act on **collections** of data, [multisets](https://en.wikipedia.org/wiki/Multiset) that store each event in an update stream as a triple of `(data, time, diff)`.

Term | Definition
-----|-----------
**data**  |  The record update.
**time**  |  The logical timestamp of the update.
**diff**  |  Number of copies of the record (`-1` for deletion, `1` for addition).

## Arrangements

To transform the source input into the dataflow into the result output, operators (independent computational components)
build and maintain indexes on the input and output collections so that they can quickly access collection data in the future. Because queries can overlap, different operators often need to build the exact same indexes for multiple queries. Instead of asking operators to perform redundant work, Materialize builds the index once and maintains it in memory, sharing the required resources across all the operators that use the indexed data. The index is then effectively a sunk cost, and the cost of each query is determined only by the new work it introduces.

We call the indexed update stream an **arrangement**.

In addition to materialized views, certain internal operators, like `reduce` and `join`, also create arrangements so the operators can respond efficiently to updates.

### Arrangement structure

In the `(data, time, diff)` triples that make up the update stream, `data` is structured as a key-value pair.  Arrangements are indexed on the `data` keys. You cannot manually create an arrangement, although you can influence the shape of the arrangements created by Materialize by the way you structure queries and views.

When creating an arrangement for a join where the user has not specified a key, Materialize attempts to choose a key that will ensure that data is well distributed. If there is a primary key, that will be used; if there are source fields not required by the query, they are not included. Often Materialize can pull primary key info from a Confluent schema.

### Arrangement size

For joins, the size of an arrangement, or amount of memory it requires, is roughly equivalent to its number of distinct `(data, time)` pairs, which can be small even if the number of records is large. As an illustration, consider a histogram of taxi rides grouped by the number of riders and the fare amount. The number of `(rider, fare)` pairs will be much smaller than the number of total rides that take place.

The amount of memory that the arrangement requires is then further reduced by background [compaction](/ops/deployment/#compaction) of historical data.

### Example arrangements

Let's take a look at some of the arrangements that Materialize creates for different circumstances.

#### `COUNT`

Some of the simplest arrangements are those for Differential Dataflow operators. For example, the `COUNT` operator, which Materialize uses for internal logging, has two arrangements:

* An arrangement for the input, which indexes the key whose values will be counted
* An arrangement for the output, which maintains the results

`COUNT` reads from the input arrangement to count values, and both reads from the output (to see the previous total) and writes to the output (adding the new updates to the previous total).

![Diagram of arrangements for count](/images/arrangements-count.png "Diagram of arrangements for COUNT")

#### Three-way join

Let's say that you want to review orders by shipdate. The query itself is:

```sql
SELECT
    l_orderkey,
    o_orderdate,
    o_shippriority,
    sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    customer,
    orders,
    lineitem
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue desc,
    o_orderdate;
```

The query is a three-way join between `customer`, `orders`, and `lineitem`, followed by a reduction. For the purposes of this explanation, we're going to focus on the join and skip the reduction.

When joining the three tables, Materialize builds indexes on `customers` + `orders`  and (`customers` + `orders`) + `lineitem`. It can do this because it separates out the work of the join calculation from the work of indexing information or creating arrangements. Each of the original columns and both of the necessary joins create arrangements that can be reused for other queries.

![Diagram of arrangements for a three-way join](/images/arrangements-3-way-join.png "Diagram of arrangements for a three-way join")

Note that this is a simplification for the sake of an example, focusing on the join and excluding the reduction at the end. In practice, if a join is going into a reduction, Materialize does not produce and maintain an intermediate arrangement as large as the join itself.

You can find a more detailed analysis of the dataflow (and the performance benefits of using Materialize) in our blog post on [Joins in Materialize](https://materialize.com/joins-in-materialize).

## Analyzing arrangements

Materialize provides various tools that allow you to analyze arrangements, although they are post hoc tools best used for debugging, rather than planning tools to be used before creating indexes or views. See [Diagnosing Using SQL](/ops/diagnosing-using-sql/), [Monitoring](/ops/monitoring/), and [`EXPLAIN`](/sql/explain/) for more details.

## Reducing memory usage

### Creating indexes manually

You can't create arrangements directly, but in some cases you can manually create indexes that will help Materialize create more sharable arrangements -- and therefore reduce memory usage.

If Materialize cannot detect a primary key, the default key is the full set of columns, in order to ensure good data distribution. Creating an unmaterialized view and then specifying a custom index makes the key smaller.

For more examples of cases where you might want to create an index manually, see [Joins in Materialize](https://materialize.com/joins-in-materialize/).

### Casting the data type

Currently, Materialize handles implicit casts in a very [memory-intensive way](https://github.com/MaterializeInc/materialize/issues/4171). Until this issue is resolved, you can reduce memory usage by building an index on the view with the type changed for any queries which include implicit casts, for example, when you combine 32-bit and 64-bit numbers.

## Related topics

* [Joins in Materialize](https://materialize.com/joins-in-materialize/)
* [Diagnosing Using SQL](/ops/diagnosing-using-sql/)
* [Deployment](/ops/deployment/)
* [Differential Dataflow](https://timelydataflow.github.io/differential-dataflow/)
