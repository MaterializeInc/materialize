---
title: "What is Materialize?"
description: "Learn more about Materialize"
disable_list: true
aliases:
  - /overview/what-is-materialize/
menu:
  main:
    parent: get-started
    name: "What is Materialize?"
    weight: 5
---

{{% include-headless "/headless/materialize-intro/intro" %}}

## Materialize offerings

{{% include-headless "/headless/materialize-intro/offerings" %}}

## Key features

Materialize combines the accessibility of SQL databases with a streaming engine
that is horizontally scalable, highly available, and strongly consistent.

### Incremental updates

In traditional databases, materialized views help you avoid re-running heavy
queries, typically by caching queries to serve results faster. But you have
to make a compromise between the freshness of the results, the cost of
refreshing the view, and the complexity of the SQL statements you can use.

In Materialize, you don't have to make such compromises. Materialize supports
incrementally updated view results that are **always fresh** (even when using
complex SQL statements, like multi-way joins with aggregations) for *both*:

- [Indexed views](/concepts/views/#indexes-on-views) and

- [Materialized views](/concepts/views/#materialized-views).

How?
Its engine is built on [Timely](https://github.com/TimelyDataflow/timely-dataflow#timely-dataflow)
and [Differential Dataflow](https://github.com/timelydataflow/differential-dataflow#differential-dataflow)
— data processing frameworks backed by many years of research and optimized for
this exact purpose.

### Standard SQL support

With Materialize, you use SQL to transform your fast-changing data into **live
data products**: the business objects (e.g., a customer, an order, a store) that
your applications, services, dashboards, and AI agents read.

You can express complex transformations using **[any type of
join](/sql/select/join/)** (including non-windowed joins and joins on arbitrary
conditions), as well as SQL patterns
enabled by streaming like [**Change Data Capture (CDC)**](/ingest-data/),
[**temporal filters**](/sql/patterns/temporal-filters/), and
[**subscriptions**](/sql/subscribe/).

{{% include-from-yaml data="materialize_details" name="postgres-compatibility" %}}

### Real-time data ingestion

Materialize provides **native connectors** that allow ingesting data from various external systems:

{{% include-headless "/headless/multilink-box-native-connectors" %}}

For more information, see [Ingest Data](/ingest-data/) and
[Integrations](/integrations/).

### PostgreSQL wire-compatibility

Every database needs a protocol to standardize communication with the outside
world. Materialize uses the [PostgreSQL wire protocol](https://datastation.multiprocess.io/blog/2022-02-08-the-world-of-postgresql-wire-compatibility.html),
which allows it to integrate out-of-the-box with many SQL clients and other
tools in the data ecosystem that support PostgreSQL — like [dbt](/integrations/dbt/).

### Strong consistency guarantees

By default, Materialize provides the highest level of transaction isolation:
**strict serializability**. This means that it presents as if it were a single
process, despite spanning a large number of threads, processes, and machines.
Strict serializability avoids common pitfalls like eventual consistency and dual
writes, which affect the correctness of your results. You can [adjust the
transaction isolation level](/overview/isolation-level/) depending on your
consistency and performance requirements.

## Learn more

- [Key concepts](/concepts/)
- [Get started with Materialize](/get-started/quickstart)
