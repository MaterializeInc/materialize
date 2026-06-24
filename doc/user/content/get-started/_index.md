---
title: "What is Materialize?"
description: "Materialize is the live data layer for apps and AI agents — build live data products that represent your business, using just SQL."
disable_list: true
aliases:
  - /overview/what-is-materialize/
menu:
  main:
    parent: get-started
    name: "What is Materialize?"
    weight: 5
---

# Materialize: The live data layer for apps and agents

Materialize lets you build live data products to represent your business, using just SQL.

[Try the quickstart →](/get-started/quickstart/)

## How Materialize works

### Real-time ingestion from sources

Materialize connects natively to your operational systems — Postgres, MySQL, Kafka, webhooks — and ingests changes as they happen using Change Data Capture (CDC). Data flows in continuously; there are no batch windows or scheduled pulls.

### Incremental view maintenance

As new data arrives, Materialize updates view results incrementally rather than recalculating from scratch. Complex SQL — multi-way joins, aggregations, temporal filters — stays fresh within about a second. You write SQL once; Materialize handles the maintenance.

### Standard SQL & PGWire support

Materialize speaks standard SQL and the PostgreSQL wire protocol. Any application, dashboard, or BI tool that works with Postgres works with Materialize — no new query language, no new SDK. AI agents query the live context graph through the same SQL surface.

### Consistency guaranteed

By default, Materialize provides strict serializability: every query sees a consistent snapshot that respects real-time write ordering. No eventual consistency, no dual-write races, no stale reads. You can [tune the isolation level](/get-started/isolation-level/) depending on your performance requirements.

## Materialize offerings

{{% include-headless "/headless/materialize-intro/offerings" %}}

{{< callout >}}
## What's new!

- [Faster sink performance with up to 50% lower memory
  usage](/releases/#v26220)

For more information on these and other changes, see the [Release Notes](/releases/).

<<<<<<< Updated upstream
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

Like most databases, you interact with Materialize using **SQL**. You can build
complex analytical
workloads using **[any type of join](/sql/select/join/)** (including
non-windowed joins and joins on arbitrary conditions) as well as leverage new
SQL patterns enabled by streaming like [**Change Data Capture
(CDC)**](/ingest-data/), [**temporal
filters**](/sql/patterns/temporal-filters/), and
[**subscriptions**](/sql/subscribe/).

{{% include-from-yaml data="materialize_details" name="postgres-compatibility" %}}

### Real-time data ingestion

Materialize provides **native connectors** that allow ingesting data from various external systems:

{{< include-md file="shared-content/multilink-box-native-connectors.md" >}}

For more information, see [Ingest Data](/ingest-data/) and
[Integrations](/integrations/).

### PostgreSQL wire-compatibility

Every database needs a protocol to standardize communication with the outside
world. Materialize uses the [PostgreSQL wire protocol](https://datastation.multiprocess.io/blog/2022-02-08-the-world-of-postgresql-wire-compatibility.html),
which allows it to integrate out-of-the-box with many SQL clients and other
tools in the data ecosystem that support PostgreSQL — like [dbt](/integrations/dbt/).

Don't see the a tool that you’d like to use with Materialize listed under
[Tools and integrations](/integrations/)? Let us know by submitting a
[feature request](https://github.com/MaterializeInc/materialize/discussions/new?category=feature-requests&labels=A-integration)!

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
- [Build a live Context Graph](/architecture-patterns/live-context-graph/)
=======
{{</ callout >}}
>>>>>>> Stashed changes
