---
title: "Known Limitations"
description: "Materialize's known limitations"
menu: "main"
weight: 100
---

This document applies to Materialize {{< version >}}.

The following links describe features that Materialize does not yet support, but
plans to in future releases. If any of these issues impact you, feel free to let
us know on the linked-to GitHub issue.

For a complete and current overview, check out our [`known-limitations`
tag](https://github.com/MaterializeInc/materialize/issues?q=is%3Aopen+is%3Aissue+label%3Aknown-limitation).

## SQL

### Types

- [Numeric precision is not always equivalent to
  PostgreSQL](https://github.com/MaterializeInc/materialize/issues/1824)
- [Numeric to float conversions are susceptible to floating point
  errors](https://github.com/MaterializeInc/materialize/issues/1102)
- [`date_trunc` for `timestamptz`
  data](https://github.com/MaterializeInc/materialize/issues/1814)
- [Special `date`, `time`, and `timestamp` values from PostgreSQL are not
  supported](https://github.com/MaterializeInc/materialize/issues/1805)
- [Cannot cast from string to time-like
  types](https://github.com/MaterializeInc/materialize/issues/1378)
- [Intervals do not support addition or
  subtraction with other intervals](https://github.com/MaterializeInc/materialize/issues/1682)

### Syntax & Semantics

- [Using a non-existent namespace does not result in an
  error](https://github.com/MaterializeInc/materialize/issues/1684)
- [Column names generated in returned column set are not available to `GROUP
  BY`](https://github.com/MaterializeInc/materialize/issues/1673)

### Performance

- [`ANY` and `ALL` queries generate suboptimal
  plans](https://github.com/MaterializeInc/materialize/issues/744)

## Sources & Sinks

### Kafka

- [JSON-encoded streams are not
  supported](https://github.com/MaterializeInc/materialize/issues/207)
- [Connecting to Kafka brokers with SSL (client) authentication is not
  supported](https://github.com/MaterializeInc/materialize/issues/1785)
- [Connecting Kafka sinks back in as sources is not
  supported](https://github.com/MaterializeInc/materialize/issues/1665)
- [Protobuf data for Kafka sinks is not
  supported](https://github.com/MaterializeInc/materialize/issues/1541)
- [JSON-encoded data for Kafka sinks is not
  supported](https://github.com/MaterializeInc/materialize/issues/1540)
- [Progress tracking for Kafka sinks is not
  supported](https://github.com/MaterializeInc/materialize/issues/1442)

### File sources

- [CSV files with header rows are not
  supported](https://github.com/MaterializeInc/materialize/issues/1982)

### Regex formatting

- [Regex columns cannot be set as
  nullable](https://github.com/MaterializeInc/materialize/issues/1685)
- [Formatting regular expression is not resurface-able through `SHOW CREATE
  SOURCE`](https://github.com/MaterializeInc/materialize/issues/1762)

## Deployment

### Performance

- [Slow queries can impact other, unrelated
  queries](https://github.com/MaterializeInc/materialize/issues/1956)

### Networking

- [Hostnames that resolve to multiple addresses are not
  supported](https://github.com/MaterializeInc/materialize/issues/502)

### Monitoring & Debugging

- [No report exists to show memory usage for
  indexes](https://github.com/MaterializeInc/materialize/issues/1532)
- [`EXPLAIN DATAFLOW` does not include details about `ORDER BY` and
  `LIMIT`](https://github.com/MaterializeInc/materialize/issues/477)
