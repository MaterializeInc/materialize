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
---

A [sink](/concepts/sinks/) describes the external system you want Materialize to
write data to and details the encoding of that data.

### Creating a sink

When a user defines a sink over a **materialized** view, source, or table,
Materialize automatically generates the required schema and writes down the
stream of changes to that view or source. In effect, Materialize sinks act as
change data capture (CDC) producers for the given source or view.

During creation, sinks need to load an entire snapshot of the data in memory.

### Operational guideline

- Avoid putting sinks on the same cluster that hosts sources to allow for
[blue/green
deployment](/manage/dbt/development-workflows/#bluegreen-deployments).

### Available guides

The following guides are available for sinking results from Materialize to external systems:

- [Sinking results to Amazon S3](/serve-results/sink/s3/)
- [Sinking results to Census](/serve-results/sink/census/)
- [Sinking results to Kafka/Redpanda](/serve-results/sink/kafka/)
- [Sinking results to Snowflake](/serve-results/sink/snowflake/)

### Troubleshooting

For help, see [Troubleshooting
sinks](/serve-results/sink/sink-troubleshooting/).
