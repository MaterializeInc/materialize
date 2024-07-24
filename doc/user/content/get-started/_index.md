---
title: "What is Materialize?"
description: "Learn more about Materialize"
disable_list: true
aliases:
  - /overview/what-is-materialize/
  - /overview/
menu:
  main:
    parent: get-started
    name: "What is Materialize?"
    weight: 5
---

Materialize is the Operational Data Warehouse that delivers the speed of
streaming with the ease of a data warehouse. With Materialize, organizations can
operate on real-time data just by using SQL.

If you need to speed up queries that run frequently, or trigger actions as
soon as events happen, Materialize is a good fit. Rather than recalculate
results from scratch, or serve stale cached results, Materialize continually
ingests data and keeps results up-to-date as new data arrives.

{{< callout primary_url="https://materialize.com/register/?utm_campaign=General&utm_source=documentation" primary_text="Get Started">}}

## Try it out! 🚀

1. Sign up for a [free trial account](https://materialize.com/register/?utm_campaign=General&utm_source=documentation).
2. Follow the quickstart guide to learn the basics.
3. Connect your own data sources and start building.

{{</ callout >}}

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

- [Non-materialized views **with an
index**](/concepts/views/#indexes-and-non-materialized-views) and

- [Materialized views](/concepts/views/#materialized-views).

How?
Its engine is built on [Timely](https://github.com/TimelyDataflow/timely-dataflow#timely-dataflow)
and [Differential Dataflow](https://github.com/timelydataflow/differential-dataflow#differential-dataflow)
— data processing frameworks backed by many years of research and optimized for
this exact purpose.

### Standard SQL support

Materialize follows the SQL standard (SQL-92) implementation, so you interact
with it like any relational database: using SQL. You can build complex
analytical workloads using **[any type of join](/sql/join/)** (including
non-windowed joins and joins on arbitrary conditions), but you can also
leverage exciting new SQL patterns enabled by streaming like
[**Change Data Capture (CDC)**](/integrations/#databases),
[**temporal filters**](/sql/patterns/temporal-filters/), and [**subscriptions**](/sql/subscribe/).

### Real-time data ingestion

Materialize provides **native connectors** that allow ingesting data from the various external systems:

{{< multilinkbox >}}
{{< linkbox title="Message Brokers" >}}
- [Kafka](/sql/create-source/kafka)
- [Redpanda](/sql/create-source/kafka)
- [Other message brokers](/integrations/#message-brokers)
{{</ linkbox >}}
{{< linkbox title="Databases (CDC)" >}}
- [PostgreSQL](/sql/create-source/postgres)
- [MySQL](/sql/create-source/mysql)
- [Other databases](/integrations/#other-databases)
{{</ linkbox >}}
{{< linkbox title="Webhooks" >}}
- [Amazon EventBridge](/ingest-data/amazon-eventbridge/)
- [Segment](/ingest-data/segment/)
- [Other webhooks](/sql/create-source/webhook)
{{</ linkbox >}}
{{</ multilinkbox >}}

For more information, see [Ingest Data](/ingest-data/) and
[Integrations](/integrations/).

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

- [Key concepts](/concepts/)
- [Get started with Materialize](/get-started/quickstart)
