---
title: "EXPLAIN FILTER PUSHDOWN"
description: "`EXPLAIN FILTER PUSHDOWN` reports filter pushdown statistics for `SELECT` statements and materialized views."
menu:
  main:
    parent: commands
---


{{< public-preview />}}

`EXPLAIN FILTER PUSHDOWN` reports filter pushdown statistics for `SELECT`
statements and materialized views.

## Syntax

{{< diagram "explain-filter-pushdown.svg" >}}

### Explained object

The following objects can be explained with `EXPLAIN FILTER PUSHDOWN`:

 Explained object           | Description
----------------------------|-------------------------------------------------------------------------------
 **select_stmt**            | Display statistics for an ad-hoc [`SELECT` statement](../select).
 **MATERIALIZED VIEW name** | Display statistics for an existing materialized view.

## Details

Materialize's [filter pushdown optimization](../../transform-data/patterns/temporal-filters/#temporal-filter-pushdown)
can be critical for large or append-only collections, since queries with a short
temporal filter many only need to fetch a small number of recent updates
instead of the full history of the data over time. However, it can be hard to
predict how well this optimization will behave for a particular query and
dataset. The standard [`EXPLAIN PLAN`](../../sql/explain-plan/#output-modifiers)
command can give some guidance as to whether this optimization applies to a
query at all... but exactly how much data gets filtered out will depend on both
statistics about the data itself, as well as how the data is chunked into
parts.

`EXPLAIN FILTER PUSHDOWN` looks at the current durable state of the collection,
determines exactly which parts are necessary to answer a `SELECT` query or
rehydrate a materialized view, and reports the total number of parts and bytes
that the query has selected, along with the total number of parts and bytes in
the shard.

## Examples

For the following examples, assume that you have created [an auction house load
generator source](/sql/create-source/load-generator/#creating-an-auction-load-generator)
in your environment.

### Explaining a `SELECT` query

Suppose you're interested in checking the number of recent bids.

```mzsql
SELECT count(*) FROM bids WHERE bid_time + '5 minutes' > mz_now();
```

Over time, the number of bids will grow indefinitely, but the number of recent
bids should stay about the same. If the filter pushdown optimization can make
sure that this query only needs to fetch recent bids from the storage layer
instead of _all_ historical bids, that could have an extreme impact in
performance.

Explaining this query includes a `pushdown=` field under `Source materialize.public.bids`,
which indicates that this filter can be pushed down.

```mzsql
EXPLAIN
SELECT count(*) FROM bids WHERE bid_time + '5 minutes' > mz_now();
```

```nofmt
...

 Source materialize.public.bids
   filter=((timestamp_tz_to_mz_timestamp((#4{bid_time} + 00:05:00)) > mz_now()))
   pushdown=((timestamp_tz_to_mz_timestamp((#4{bid_time} + 00:05:00)) > mz_now()))
```

However, this doesn't suggest how effective filter pushdown will be. Suppose you
have two queries: one which filters to the last minute and one to the last
hour; both can be pushed down, but the second will probably fetch much more
data.

Suppose it's been \~1 hour since you set up the auction house load generator
source, and you'd like to get a sense of how much data your query would need to
fetch.

```mzsql
EXPLAIN FILTER PUSHDOWN FOR
SELECT count(*) FROM bids WHERE bid_time + '5 minutes' > mz_now();
```

```nofmt
         Source          | Total Bytes | Selected Bytes | Total Parts | Selected Parts
-------------------------+-------------+----------------+-------------+----------------
 materialize.public.bids | 146508      | 34621          | 19          | 11
```

It looks like Materialize is fetching about a fifth of the data in terms of
bytes, and about half of the individual parts (this is not unexpected:
Materialize stores older data in larger chunks). If you run this query again,
you'll see the numbers change as more data is ingested and Materialize compacts
it into more efficient representations in the background.

If you instead query for the last hour of data, you can see that since you only
created the auction house source \~1 hour ago, Materialize needs to fetch
almost everything.

```mzsql
EXPLAIN FILTER PUSHDOWN FOR
SELECT count(*) FROM bids WHERE bid_time + '1 hour' > mz_now();
```

```nofmt
         Source          | Total Bytes | Selected Bytes | Total Parts | Selected Parts
-------------------------+-------------+----------------+-------------+----------------
 materialize.public.bids | 162473      | 162473         | 17          | 17
```

## Privileges

The privileges required to execute this statement are:

- `USAGE` privileges on the schemas that all relations in the explainee are contained in.
