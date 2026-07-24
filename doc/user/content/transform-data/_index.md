---
title: "Overview"
description: "Learn how to efficiently transform data using Materialize SQL."
disable_list: true
menu:
  main:
    parent: transform-data
    weight: 5
    identifier: transform-overview
aliases:
  - /self-managed/v25.1/transform-data/troubleshooting/
  - /self-managed/v25.2/transform-data/
---

With Materialize, you use SQL to transform your fast-changing data into **live
data products**: the business objects (e.g., a customer, an order, a store) that
your applications, services, dashboards, and AI agents read.

## From SQL to live data products

You use [views](/concepts/views/) and [materialized
views](/concepts/views/#materialized-views) to define your business objects in
SQL. Materialize keeps the results of **indexed views** and **materialized
views** up to date as it ingests your data.

Structuring your transformations as views gives you:

- **Reuse:** define a query once, then reference it from multiple places.
- **Readability:** save complex logic under a clear, meaningful name.
- **Composability:** break complex logic into stacked view definitions, building
  richer business objects on top of simpler ones.
- **Efficiency:** project only the columns you need, filter out unnecessary
  rows, and convert values to more [compact data types](/sql/types/) where
  possible.

## SQL in Materialize

{{% include-from-yaml data="materialize_details" name="postgres-compatibility" %}}

## Explore this section

{{< multilinkbox >}}
{{< linkbox title="Write idiomatic SQL" >}}
- [Idiomatic Materialize SQL](/transform-data/idiomatic-materialize-sql/)
- [Common query patterns](/transform-data/patterns/)
{{</ linkbox >}}

{{< linkbox title="Optimize and operate" >}}
- [Query optimization](/transform-data/optimization/)
- [Updating materialized views](/transform-data/updating-materialized-views/)
- [Indexes: best practices](/concepts/indexes/#best-practices)
{{</ linkbox >}}

{{< linkbox title="Troubleshoot" >}}
- [Troubleshooting](/transform-data/troubleshooting/)
- [Freshness troubleshooting](/transform-data/freshness-troubleshooting/)
- [Dataflow troubleshooting](/transform-data/dataflow-troubleshooting/)
- [FAQ: Indexes](/transform-data/faq/)
{{</ linkbox >}}
{{</ multilinkbox >}}
