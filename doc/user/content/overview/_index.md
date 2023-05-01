---
title: "What is Materialize?"
description: "Learn more about Materialize"
disable_list: true
aliases:
  - /overview/what-is-materialize/
menu:
  main:
    parent: overview
    weight: 5
---

Materialize is a **streaming database** purpose-built for low-latency
applications. You can use it to process data at speeds and scales not possible
in traditional databases, but without the cost, complexity, or development time
of most streaming engines.

If you need to speed up queries that run frequently, or trigger actions as
soon as events happen, Materialize is a good fit. Rather than recalculate
results from scratch, or serve stale cached results, Materialize continually
ingests data and keeps results up-to-date as new data arrives.

## Key features

Materialize combines the accessibility of SQL databases with a streaming engine
that is horizontally scalable, highly available, and strongly consistent.

### Incremental updates

In traditional databases, materialized views help you avoid re-running heavy
queries, typically by caching queries to serve results faster. But you have
to make a compromise between the freshness of the results, the cost of
refreshing the view, and the complexity of the SQL statements you can use.

In Materialize, you don't have to make such compromises. Materialize supports
incrementally updated materialized views that are **always fresh**, even when
using complex SQL statements, like multi-way joins with aggregations. How?
Its engine is built on [Timely](https://github.com/TimelyDataflow/timely-dataflow#timely-dataflow)
and [Differential Dataflow](https://github.com/timelydataflow/differential-dataflow#differential-dataflow)
— data processing frameworks backed by many years of research and optimized for
this exact purpose.

### Standard SQL support

Materialize follows the SQL standard (SQL-92) implementation, so you interact
with it like any relational database: using SQL. You can build complex
analytical workloads using **[any type of join](/sql/join/)**(including
non-windowed joins and joins on arbitrary conditions), but you can also
leverage exciting new SQL patterns enabled by streaming like
[**Change Data Capture (CDC)**](/integrations/#databases),
[**temporal filters**](/sql/patterns/temporal-filters/), and [**subscriptions**](/sql/subscribe/).

### PostgreSQL wire-compatibility

Every database needs a protocol to standardize communication with the outside
world. Materialize uses the [PostgreSQL wire protocol](https://datastation.multiprocess.io/blog/2022-02-08-the-world-of-postgresql-wire-compatibility.html),
which allows it to integrate out-of-the-box with many SQL clients and other
tools in the data ecosystem that support PostgreSQL — like [dbt](/integrations/dbt/).

Don't see the a tool that you’d like to use with Materialize listed under
[Tools and integrations](/integrations/)? Let us know by submitting a
[feature request](https://github.com/MaterializeInc/materialize/issues/new?assignees=&labels=A-integration&template=02-feature.yml)!

### Strong consistency guarantees

By default, Materialize provides the highest level of transaction isolation:
[**strict serializability**](https://jepsen.io/consistency/models/strict-serializable).
This means that it presents as if it were a single process, despite spanning a
large number of threads, processes, and machines. Strict serializability avoids
common pitfalls like eventual consistency and dual writes, which affect the
correctness of your results. You can [adjust the transaction isolation level](/overview/isolation-level/)
depending on your consistency and performance requirements.

## Learn more

- [Key concepts](/overview/key-concepts)
- [Get started with Materialize](/get-started/)
