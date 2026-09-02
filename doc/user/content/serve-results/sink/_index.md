---
title: "Sink results"
description: "Sinking results from Materialize to external systems."
disable_list: true
menu:
  main:
    parent: serve-results
    name: "Sink results"
    identifier: 'sink'
    weight: 40
aliases:
  - /self-managed/v25.2/serve-results/sink/
  - /self-managed/v25.2/serve-results/sink/s3/
  - /self-managed/v25.2/serve-results/sink/s3_compatible/
  - /self-managed/v25.2/serve-results/sink/census/
  - /self-managed/v25.2/serve-results/sink/kafka/
  - /self-managed/v25.2/serve-results/sink/snowflake/
  - /self-managed/v25.2/serve-results/sink/sink-troubleshooting/
---

A [sink](/concepts/sinks/) describes the external system you want Materialize to
write data to and details the encoding of that data. You can sink data from a
**materialized** view, a source, or a table.

## Sink methods

To create a sink, you can:

{{< yaml-table data="sink_external_systems" >}}

### Operational guideline

- Avoid putting sinks on the same cluster that hosts sources to allow for
[blue/green deployment](/manage/dbt/blue-green-deployments).

### Troubleshooting

For help, see [Troubleshooting
sinks](/serve-results/sink/sink-troubleshooting/).
