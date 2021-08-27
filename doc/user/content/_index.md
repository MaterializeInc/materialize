---
title: "Materialize Docs"
htmltitle: "Home"
linktitle: "Docs Home"
menu: "main"
disable_toc: true
disable_list: true
disable_h1: true
weight: 1
---

Materialize is a streaming database for real-time applications. Materialize
accepts input data from a variety of streaming sources (e.g. Kafka) and files
(e.g. CSVs), and lets you query them using SQL.

{{< callout primary_url="/get-started/" primary_text="Get Started">}}
  # Quickstart

  Follow this walkthrough to start creating live views on streaming data with Materialize.
{{</ callout >}}

{{< multilinkbox >}}
{{< linkbox icon="bulb" title="Key Concepts" >}}
- [High-Level Overview]({{< relref "what-is-materialize" >}})
- [API Components: Sources, Views, Indexes, Sinks]({{< relref "/overview/api-components" >}})
- [Materialize Cloud Overview]({{< relref "/cloud/what-is-materialize-cloud" >}})
{{</ linkbox >}}
{{< linkbox icon="touch" title="Demos" >}}
- [Real-Time Analytics Dashboard]({{< relref "/demos/business-intelligence" >}})
- [Streaming SQL on server logs]({{< relref "/demos/log-parsing" >}})
- [A microservice to transform and join two streams of data]({{< relref "/demos/microservice" >}})
{{</ linkbox >}}
{{< linkbox icon="doc" title="Guides" >}}
- [Connect Postgres to Materialize with CDC](/guides/cdc-postgres/)
- [Use Temporal Filters to Index events within a window of time](/guides/temporal-filters/)
- [Interact with Materialize from Node.js](/guides/node-js/)
{{</ linkbox >}}
{{< linkbox icon="book" title="Reference" >}}
- [CREATE SOURCE]({{< relref "/sql/create-source" >}})
- [CREATE MATERIALIZED VIEW]({{< relref "/sql/create-materialized-view" >}})
- [SQL Data Types]({{< relref "/sql/types" >}})
- [SQL Functions]({{< relref "/sql/functions" >}})
{{</ linkbox >}}
{{</ multilinkbox >}}

## What does Materialize do?

Materialize lets you ask questions about your data, and then get low-latency,
correct answers, even as the underlying data changes.

Why not just use your database's built-in functionality to perform these same
computations? Because your database often acts as if it's never been asked that
question before, which means it can take a _long_ time to come up with an
answer, each and every time you pose the query.

Materialize instead keeps the results of the queries and incrementally updates
them as new data comes in. So, rather than recalculating the answer each time
it's asked, Materialize continually updates the answer and gives you the
answer's current state from memory.

Importantly, Materialize supports incrementally updating a much broader set of
views than is common in traditional databases (e.g. views over multi-way joins
with complex aggregations), and can do incremental updates in the presence of
arbitrary inserts, updates, and deletes in the input streams.

## Why should I use Materialize?

If you perform any OLAP queries over relational data and want to reduce the time
it takes to refresh answers to common queries, Materialize can make that happen.

For a sense of scale, it can take queries that most teams would run once-per-day
and instead provide sub-second or single-digit second answers.

## Learn more

- [**Install Materialize**](./install) to try it out.
- [**What is Materialize?**](./overview/what-is-materialize) to learn more about what Materialize does and how it works.
- [**Architecture documentation**](./overview/architecture) for a deeper dive into the `materialized` binary's components and deployment.
