---
title: "Materialize Docs"
htmltitle: "Home"
disable_toc: true
disable_list: true
disable_h1: true
weight: 1
---

Materialize is a streaming database for real-time applications. Materialize
accepts input data from a variety of streaming sources (like Kafka), data stores and databases (like S3 and Postgres), and files
(like CSV and JSON), and lets you query them using SQL.

{{< callout primary_url="https://materialize.com/docs/get-started/" primary_text="Get Started">}}
  # Quickstart

  Follow this walkthrough to start creating live views on streaming data with Materialize.
{{</ callout >}}

{{< multilinkbox >}}
{{< linkbox icon="bulb" title="Key Concepts" >}}
- [Materialize overview](/overview/what-is-materialize)
- [API components](/overview/api-components)
- [Materialize Cloud overview](/cloud/)
{{</ linkbox >}}
{{< linkbox icon="touch" title="Demos" >}}
- [Real-time analytics dashboard](/demos/business-intelligence)
- [Streaming SQL on server logs](/demos/log-parsing)
- [Microservices](/demos/microservice)
{{</ linkbox >}}
{{< linkbox icon="doc" title="Guides" >}}
- [Materialize &amp; Postgres CDC](/guides/cdc-postgres/)
- [Exactly-Once Sinks](/guides/reuse-topic-for-kafka-sink)
- [Materialize &amp; Node.js](/guides/node-js/)
- [Time-windowed computation](/guides/temporal-filters/)
{{</ linkbox >}}
{{< linkbox icon="book" title="Reference" >}}
- [`CREATE SOURCE`](/sql/create-source)
- [`CREATE MATERIALIZED VIEW`](/sql/create-materialized-view)
- [`SQL Data Types`](/sql/types)
- [`SQL Functions`](/sql/functions)
{{</ linkbox >}}
{{</ multilinkbox >}}

## New &amp; updated

- [Release Notes](release-notes/)
- [CDC with MySQL](/guides/cdc-mysql/)
- [Materialize Cloud](/cloud/) - Now in open beta!

## Learn more

- [**Install Materialize**](./install) to try it out or try [Materialize Cloud](/cloud/) for free.
- [**What is Materialize?**](./overview/what-is-materialize) to learn
more about what Materialize does and how it works.
- [**Architecture documentation**](./overview/architecture) for a deeper dive into the `materialized` binary's components and deployment.
