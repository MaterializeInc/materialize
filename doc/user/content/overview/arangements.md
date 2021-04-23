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

Traditional databases typically only have limited support for materialized views along two dimensions: first, the updates to the views generally occur at set intervals, so views are not updated in real time, and second, only a limited subset of SQL syntax is supported. These limitations are typically because of these databases’ limited support for incremental updates. Most databases are not designed to maintain long-running incremental queries, but instead are optimized for queries that are executed once and then wound down.

This means that when the data changes, typically the materialized view must be recomputed from scratch in all but a few simple cases. Our raison d’être at Materialize is doing something better than this: Materialize stores materialized views in memory and incrementally updates them as the data changes, maintaining both correctness and speed.

## Dataflows

Materialize can make incremental updates efficiently because it's built on an advanced stream processing dataflow engine, [Differential Dataflow](https://timelydataflow.github.io/differential-dataflow/introduction.html), which in turn is built on a distributed stream processing framework called [Timely Dataflow](https://timelydataflow.github.io/timely-dataflow/).

When you create a materialized view and issue a query, Materialize creates a **dataflow**. A dataflow consists of the following:

- An execution plan for the query
- Independent computational components, or **operators**
- A description of the connections between these components
- Instructions on how to respond to data input, whether the input is static (fed in all at once) or dynamic (new data will continue to arrive after the initial dataflow execution)

Once executed, the dataflow computes and stores the result of the SQL query in memory, polls the source for updates, and then incrementally updates the query results when new data arrives.

The cost of maintaining a dataflow can be very different from the cost of executing a query once. Materialize queries are executed as long-lived stateful dataflow, and impose an ongoing cost of computation and memory. As input data changes, we want to quickly respond and cannot afford a full query re-evaluation. This results in an optimization process that has different priorities than traditional optimizers. Materialize optimizes to enable the re-use of already materialized data, and to do that it relies on sharing arrangements among multiple dataflows.

### Collections

Materialize dataflows act on **collections** of data, [multisets](https://en.wikipedia.org/wiki/Multiset) of updates that store information as triples of `(data, time, diff)`. Both dataflow inputs and dataflow outputs are expressed as collections.

Term | Definition
-----|-----------
**data**  |  The record update.
**time**  |  The logical timestamp of the update.
**diff**  |  The type of update (`-1` for deletion, `1` for addition or upsert)

An example of what updates look like:

```nofmt
// four records are added
(record0, time0, +1)
(record0, time0, +1)
(record1, time0, +1)
(record2, time0, +1)

// one record is "updated"
(record1, time1, -1)
(record2, time1, +1)

// two records are deleted
(record0, time2, -1)
(record2, time2, -1)
```

## Arrangements

To transform an input into an output, operators
typically build and maintain an index of the updates so that they can randomly access them in the future. It's possible that, for different queries, multiple operators would need to build the exact same indexes for their input streams. Instead of asking operators to perform redundant work, Materialize simply builds and maintains that index once, sharing the required resources across all the operators that use the indexed data.

We call the indexed update stream an **arrangement**.

### Arrangement structure

<!-- "Arrangement structure" section is still a work in progress. Do not review." -->
<!-- need example of what data key-value pair looks like -->

`data` is structured as a key-value pair, and arrangements are indexed on the `data` keys. An arrangement records all updates, but an index only maintains the current accumulation of `diff` for each `(data, time)` pair.

<!-- Add example -->

When Materialize creates an arrangement, it attempts to choose a key that will ensure that data is well distributed. If there is a primary key, that will be used; if there are source fields not required by the query, they are not included. <!-- If we’re lucky, we can get Primary key info from schema (Confluent, Kafka). -->

New dataflows can reuse arrangements for materializations, improving overall system performance and reducing resource usage. The cost of each query is determined only by the new work it introduces.

### Arrangement size

The size of an arrangement is related to the size of its accumulated updates, but not directly proportional to them. Instead, an arrangement's size is roughly equivalent to its number of distinct `(data, time)` pairs, which can be small even if the number of records is large. As an illustration, consider a histogram of taxi rides grouped by the number of riders and the fare amount, the number of `(rider, fare)` pairs will be much smaller than the number of total rides that take place.

The size of the arrangement is then further reduced by background [compaction](/ops/deployment/#compaction) of historical data.

<!-- Everything below this line is work in progress. Do not review. -->

### Arrangement examples

A few of examples to demonstrate how arrangements relate to operators, views, and queries:

#### `COUNT`

COUNT operator  will have two arrangements: one by input (arrangement by key)  and then one for the output (the results). COUNT reads from input but doesn’t write to it; both reads and writes to output.

<!-- image -->

#### Three-way join

Most exciting place for arrangements is JOINS.

<!-- image here -->


Create a materialized view: 3-way join group by fields 1,2,3  - Mz creates arrangements


For maximal control:
Do create view (will not build index), then create index on the keys you want
Can be a 2x memory reduction for people
Most likely to benefit from this:
you have input data and we can’t tell what primary key is (you have a customerID but never use it; you always search on join of customer last name-customer first name)
GROUP BY operators: DISTINCT often shows up in correlated subquery (subquery references the outer columns), at some point we need to grab the outer columns. If you were decorrelating it, you would have to create a DISTINCT query
? possibly use scalar indicators ?

Things that traditional indexes do that we don’t do:
Most db indexes are sorted by the value of the thing they’re indexed on. This makes RANGE queries easier. Doesn’t happen in Mz.
Trad: if you join on 3 columns, it builds on 1+2+3, but also 1+2, 1+3, and 2+3. Mz doesn’t do this. (Ask Andy about example again)
In trad dbs: people often have secondary indexes: userID+userSegment, but if you want real customer data, you have to go back to userID table. We don’t do that; we just make a copy of all the data.

#### Delta joins

Delta joins - avoid intermediate arrangements if all collections have arrangements by all of their primary and foreign keys -- uses more arrangements than otherwise but the arrangements are more likely to be shareable with other queries. Requires separate dataflow for each input.

## Analyzing arrangements

### `EXPLAIN PLAN`

`EXPLAIN PLAN` used to debug. Do we have a concept of a table scan/row estimate/the size of the index or the table? EXPLAIN PLAN explains what we’re doing, but doesn’t explain the impact (memory usage) - awkward to use for debugging. <!-- Issue for this -->

### Memory usage

Memory visualizer - /memory - will show you the dataflow graph and the number of records that  are associated with each arrangement (number will be 0 for arrangements are borrowed). If you type CREATE INDEX, we will create an arrangement and leave it in the catalog as a thing future queries will use; CREATE MATERIALIZED * will also create index and leave behind arrangements.

To investigate existing arrangements, query mz_arrangement_sizes logging source. Diagnostic views in mz_catalog connect to this information to operator names and group it by dataflow.

### Sharing

Mz_arrangement_sharing reports the number of times each arrangement is shared. Arrangements identified by worker and operator that created it. only useful for posthoc analysis, doesn’t identify opportunities for sharing.

## Reducing memory usage

You can't create arrangements directly, but you can do some things that will help Materialize create more sharable arrangements -- and therefore reduce memory usage.


Humans can CREATE INDEX/DROP INDEX: Look at joins blog post for examples -- if it’s pre-built, it’s faster. Table may only have a few columns you care about -- each of the indexes will increase memory consumption but reusing them costs basically nothing. Don’t build an index that you’ll only use once -- creating a key.

If Mz can figure out your primary key, it will use that as the index; if you select *, Mz will use all columns. Setting up keys is helpful in reducing memory.

I create a materialized view grouped by userID. I create another materialized view with a different aggregation, may not reuse same arrangement.

customerID (32-bit integer) that you want to combine with a 64-bit integer - just building an index with the type changed, better for you to do it than for us to do it. Changing of types happens a lot -- this is embarrassing. existing bug: https://github.com/MaterializeInc/materialize/issues/4171

Anti-pattern: If you have a query that ends in a GROUP BY, we will have to build an arrangement for you anyway.

JOIN blog post -- storytelling: bad, better, best. Some magical things happen if you have a large set of arrangements: look up blog post

Arrangements house materialized sources and views, but also many internal operators. Ex - differential dataflow join and reduce both require input and output to be arrangements. These are the basis for Mz’s relational operators (the operators in the explain plan for queries). TopK builds a sequence of 16 reduce operators.

## Related topics

* [Joins in Materialize](https://materialize.com/joins-in-materialize/)
* [Diagnosing Using SQL](/ops/diagnosing-using-sql/)
* [Deployment](/ops/deployment/)
