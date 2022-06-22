---
title: "Materialize Docs"
htmltitle: "Home"
disable_toc: true
disable_list: true
disable_h1: true
weight: 1
---

Materialize is a streaming database for real-time applications. Materialize
accepts input data from streaming sources (like Kafka), and databases (like PostgreSQL), and lets you query them using SQL.

{{< callout primary_url="/docs/get-started/" primary_text="Get Started">}}
  # Get started with Materialize

  Follow this walkthrough to start creating live views on streaming data with Materialize.
{{</ callout >}}

{{< multilinkbox >}}
{{< linkbox icon="bulb" title="Key Concepts" >}}
- [Materialize overview](/overview/what-is-materialize/)
- [Key Concepts](/overview/key-concepts/)
- [Materialize Cloud overview](/cloud/)
{{</ linkbox >}}
{{< linkbox icon="doc" title="Guides" >}}
- [Materialize &amp; Postgres CDC](/integrations/cdc-postgres/)
- [dbt &amp; Materialize](/integrations/dbt/)
- [Materialize &amp; Node.js](/integrations/node-js/)
- [Time-windowed computation](/sql/patterns/temporal-filters/)
{{</ linkbox >}}
{{< linkbox icon="book" title="Reference" >}}
- [`CREATE SOURCE`](/sql/create-source/)
- [`CREATE MATERIALIZED VIEW`](/sql/create-materialized-view/)
- [`SQL Data Types`](/sql/types/)
- [`SQL Functions`](/sql/functions/)
{{</ linkbox >}}
{{</ multilinkbox >}}

## New &amp; updated

- [CDC with MySQL](/integrations/cdc-mysql/)
- [Materialize Cloud](/cloud/) - Now in open beta!

## Learn more

- [**Install Materialize**](./install) to try it out or try [Materialize Cloud](/cloud/) for free.
- [**Check out the Materialize blog**](https://www.materialize.com/blog/) for neat things our developers wrote.
- [**What is Materialize?**](./overview/what-is-materialize) to learn
more about what Materialize does and how it works.
- [**Architecture documentation**](./overview/architecture) for a deeper dive into the `materialized` binary's components and deployment.
