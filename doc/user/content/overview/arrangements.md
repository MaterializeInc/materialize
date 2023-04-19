---
title: "Arrangements"
description: "Understand how Materialize arrangements work."
menu:
  main:
    parent: advanced
    weight: 4
---

Materialize maintains materialized view dataflows with **arrangements**.
Arrangements help reduce memory usage while maintaining performance for your
queries.

## Materialized views

Traditional databases have limited support for materialized views. View
queries usually run on a schedule so cannot update in real time and then only with a small
subset of SQL syntax.

Most databases are not designed to maintain long-running incremental queries.
Database architecture is optimized for one-time query execution and run
materialized view updates as completely new query operations.

## Dataflows

The Materialize engine relies on an incremental data-parallel compute engine
known as Differential Dataflow. Differential dataflow is based on a distributed
processing framework called Timely Dataflow.

When you create a materialized view and issue a query, Materialize creates a
**dataflow**. A dataflow is a set instructions on how to respond to data
input and to changes to that data.The dataflow computes the
result of the SQL query, polls the source for updates, and then incrementally
updates the query results when new data arrives.

### Collections

Materialize dataflows process **multisets**. Multisets are collections of data that
store each event in an update stream as a triple of `(data, time, diff)`. 

Term | Definition -----|----------- **data**  |  The record update.  **time**  |
The logical timestamp of the update.  **diff**  |  The change in the number of
copies of the record (typically `-1` for deletion, `1` for addition).

## Arrangements

Collections stream updates to your data as changes happen. You can create an
index on `data` to provider faster access to changes of individual records.
Indexed collections are called **arrangements**. 

Materialize builds and maintains indexes for collections when processing
queries. Because queries can overlap, Materialize may build the exact
same indexes for multiple queries. Instead of performing redundant work,
Materialize builds the index once, maintains it in memory, and shares the
required resources across all queries that use the indexed data.  

For more information on arrangements built for different
types of queries in our blog post on [Joins in
Materialize](https://materialize.com/joins-in-materialize).

### Arrangement size

The amount of memory an arrangement requires is essentially proportional to the
number of distinct (data, time) pairs. Even in large datasets, the amount of
pairs within can be small.

For example, a histogram of taxi rides grouped by the number of riders and the fare amount.  The number
of distinct `(rider, fare)` pairs will be much smaller than the number of total
rides that take place.

The amount of memory that the arrangement requires is then further reduced by
background compaction of historical data.

> Materialize provides several tools to analyze arrangements. 
These tools are best used for debugging rather than planning indexes or views
See [Diagnosing Using SQL](/ops/troubleshooting/) and [`EXPLAIN`](/sql/explain/) for more details.

## Reducing memory usage

Materialize aims to reduce resource usage and provide query results more
efficiently than traditional databases. Still, you may want to decrease
operational costs for your queries by reducing memory usage in Materialize. 

### Creating indexes manually

One option to reduce memory usage in Materialize is to create an index manually.
If you create an arrangement for a join without an obvious key, Materialize
attempts to choose a key to use. For example, Materialize can infer primary key
information from a Confluent schema.

If Materialize cannot detect a primary key, the default key is the full set of
columns to ensure good data distribution. Creating an unmaterialized
view and then specifying a custom index makes the key smaller.

For more information on when and how to create indexes, see
[Optimization](../../ops/optimization/).  For more in-depth details on joins,
see [Joins in Materialize](https://materialize.com/joins-in-materialize/).

### Type casting

Materialize relies on memory expensive methods to handle implicit casts inserted in join constraints {{% gh 4171 %}}. Until resolved,  you
can reduce memory usage by building an index on the view with the type changed
for any queries that include implicit casts, for example, when you combine
32-bit and 64-bit numbers.

## Related pages

* [Optimization](../../ops/optimization/)
* [Joins in Materialize](https://materialize.com/joins-in-materialize/)
* [Diagnosing Using SQL](/ops/troubleshooting/)
* [Deployment](/ops/optimization/)
* [Differential
Dataflow](https://timelydataflow.github.io/differential-dataflow/)
