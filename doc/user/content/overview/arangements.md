---
title: "Arrangements"
description: "Understand how Materialize arrangements work."
menu:
  main:
    parent: 'overview'
    weight: 4
---

If you've reviewed [API Components](../api-components/), you know that Materialize provides two types of views:

Type | Use
-----|-----
**Materialized views** | Incrementally updated views whose results are maintained in memory
**Non-materialized views** | Queries saved under a name for reference, like traditional SQL views

Materialized views provide answers to queries amazingly fast, but they can consume a lot of memory. We call the mechanisms that maintain materialized views for Materialize dataflows **arrangements**, and understanding them better can help you make decisions that will reduce memory usage while maintaining performance.

## What is an arrangement?

In the simplest terms, a materialized view is a non-materialized view plus at least one index. Materialize may create the necessary indexes for you, as when you use the [`CREATE MATERIALIZED VIEW`](/sql/create-materialized-view/) or [`CREATE MATERIALIZED SOURCE`](/sql/create-source/) commands, or you may define them yourself with [`CREATE INDEX`](/sql/create-index). The indexes maintain the embedded query's result in memory and are incrementally updated as new data arrives.

When Materialize creates an index to support a materialization, it attempts to choose a key that will ensure that data is well distributed. If there is a primary key, that will be used; if there are source fields not required by the query, they are not included in the index.

Materialize accomplishes incremental updates by creating a set of persistent transformations&emdash;known as  **dataflows**&emdash;that represent the query’s output. Each dataflow is updated by one or more **arrangements**, streams of update triples <!-- Should this be tuples? --> composed of `(data, time, diff)`. `data` is structured as a key-value pair, and arrangements are indexed on the `data` keys. An arrangement records all updates, but an index only maintains the current accumulation of `diff` for each `(data, time)` pair.

New dataflows can reuse arrangements for materializations, improving overall system performance and reducing resource usage.

Any number of dataflows can reuse the same arrangement; the cost of each query is determined only by the new work it introduces.



Materialize makes its best guess.

 Associated memory footprint determined by sizes of arranged collections.

We build them for you. Users can cause arrangements to occur, but the contents are determined by the data or the query.





Background compaction process for historical info reduces memory usage

## Indexes in Materialize vs. traditional indexes

An index in Materialize

An index in Materialize doesn't do everything that an index does in traditional databases

## Arrangements and indexes

Arrangements in Materialize do a lot of the work that indexes do for traditional databases.

Materialize indexes and rdbms indexes

### Arrangement size


Size of an arrangement = roughly the size of accumulated updates
Arrangement costs proportional to the number of distinct (data, time) pairs, which can be small even if the number of records is large, if data can be reduced to a few values. (Ex - histogram of taxi rides by number of occupants + fare amount; fewer distinct pairs than records)

### Example: `COUNT`

COUNT operator  will have two arrangements: one by input (arrangement by key)  and then one for the output (the results). COUNT reads from input but doesn’t write to it; both reads and writes to output. SHOW INDEXES shows sources and views; mz_arrangements? all the sources and views in the system (what is in the memory profiler).

### Example: Three-way join

Most exciting place for arrangements is JOINS. Sales events + customer ids, product ids. I just got a  new record -- what needs to be updated?

<!-- image here -->


Create a materialized view: 3-way join group by fields 1,2,3  - Mz creates arrangements

If we’re lucky, we can get Primary key info from schema (Confluent, Kafka).
For maximal control:
Do create view (will not build index), then create index on the keys you want
Can be a 2x memory reduction for people
Most likely to benefit from this:
you have input data and we can’t tell what primary key is (you have a customerID but never use it; you always search on join of customer last name-customer first name)
GROUP BY operators: DISTINCT often shows up in correlated subquery (subquery references the outer columns), at some point we need to grab the outer columns. If you were decorrelating it, you would have to create a DISTINCT query
? possibly use scalar indicators ?

“The intermediate join is also materialized as needed and the entire set of materializations pertaining to the query is referred to as a collection.” A collection is a group of shared arrangements? We will materialize joins for use without explicit request?

To illustrate, let’s walk through an example of a materialized view implementation.
The diagram below depicts the join of two source inputs and a result set. The result set is materialized in memory and updated continuously in response to changes in the source data. The in-memory materialization is referred to as a shared arrangement, indicated by the orange box in the diagram.

Shared arrangements are like relational database indexes: they provide random access capabilities and can be reused across dataflows.  Shared arrangements are maintained by a background compaction process that folds historical updates, reducing memory usage. The operating system can also page out shared arrangements to swap.

The shared arrangement is sharded across the worker threads and also indexed by keys that depend on the query shape. The intermediate join is also materialized as needed and the entire set of materializations pertaining to the query is referred to as a collection.

Things that traditional indexes do that we don’t do:
Most db indexes are sorted by the value of the thing they’re indexed on. This makes RANGE queries easier. Doesn’t happen in Mz.
Trad: if you join on 3 columns, it builds on 1+2+3, but also 1+2, 1+3, and 2+3. Mz doesn’t do this. (Ask Andy about example again)
In trad dbs: people often have secondary indexes: userID+userSegment, but if you want real customer data, you have to go back to userID table. We don’t do that; we just make a copy of all the data.






Index benefit - speed; drawback - memory.

## Delta joins

Delta joins - avoid intermediate arrangements if all collections have arrangements by all of their primary and foreign keys -- uses more arrangements than otherwise but the arrangements are more likely to be shareable with other queries. Requires separate dataflow for each input.

## Analyzing arrangements

### `EXPLAIN PLAN`

`EXPLAIN PLAN` used to debug. Do we have a concept of a table scan/row estimate/the size of the index or the table? EXPLAIN PLAN explains what we’re doing, but doesn’t explain the impact (memory usage) - awkward to use for debugging

### Memory usage

Memory visualizer - /memory - will show you the dataflow graph and the number of records that  are associated with each arrangement (number will be 0 for arrangements are borrowed). If you type CREATE INDEX, we will create an arrangement and leave it in the catalog as a thing future queries will use; CREATE MATERIALIZED * will also create index and leave behind arrangements.

To investigate existing arrangements, query mz_arrangement_sizes logging source. Diagnostic views in mz_catalog connect to this information to operator names and group it by dataflow.

### Sharing

Mz_arrangement_sharing reports the number of times each arrangement is shared. Arrangements identified by worker and operator that created it.

Mz_arrangements_sharing tells you how many times arrangements are being reused; only useful for posthoc analysis, doesn’t identify opportunities for sharing.

## Recommendations

Humans can CREATE INDEX/DROP INDEX: Look at joins blog post for examples -- if it’s pre-built, it’s faster. Table may only have a few columns you care about -- each of the indexes will increase memory consumption but reusing them costs basically nothing. Don’t build an index that you’ll only use once -- creating a key.

If Mz can figure out your primary key, it will use that as the index; if you select *, Mz will use all columns. Setting up keys is helpful in reducing memory.

I create a materialized view grouped by userID. I create another materialized view with a different aggregation, may not reuse same arrangement.

customerID (32-bit integer) that you want to combine with a 64-bit integer - just building an index with the type changed, better for you to do it than for us to do it. Changing of types happens a lot -- this is embarrassing. existing bug: https://github.com/MaterializeInc/materialize/issues/4171

Anti-pattern: If you have a query that ends in a GROUP BY, we will have to build an arrangement for you anyway.

JOIN blog post -- storytelling: bad, better, best. Some magical things happen if you have a large set of arrangements: look up blog post

Arrangements house materialized sources and views, but also many internal operators. Ex - differential dataflow join and reduce both require input and output to be arrangements. These are the basis for Mz’s relational operators (the operators in the explain plan for queries). TopK builds a sequence of 16 reduce operators.

## Learn more

* [Joins in Materialize](https://materialize.com/joins-in-materialize/)
*
