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

- Numeric precision is not always equivalent to PostgreSQL
  ([#1824](https://github.com/MaterializeInc/materialize/issues/1824))
- Numeric to float conversions are susceptible to floating point errors
  ([#1102](https://github.com/MaterializeInc/materialize/issues/1102))
- `date_trunc` for `timestamptz` data
  ([#1814](https://github.com/MaterializeInc/materialize/issues/1814))
- Special `date`, `time`, and `timestamp` values from PostgreSQL are not
  supported ([#1805](https://github.com/MaterializeInc/materialize/issues/1805))
- Cannot cast from string to time-like types
  ([#1378](https://github.com/MaterializeInc/materialize/issues/1378))

### Syntax & Semantics

- Using a non-existent namespace does not result in an error
  ([#1684](https://github.com/MaterializeInc/materialize/issues/1684))
- Column names generated in returned column set are not available to `GROUP BY`
  ([#1673](https://github.com/MaterializeInc/materialize/issues/1673))

### Performance

- `ANY` and `ALL` queries generate suboptimal plans
  ([#744](https://github.com/MaterializeInc/materialize/issues/744))

## Sources & Sinks

### Kafka

- Kafka sources with more than one partition are not supported
  ([#2169](https://github.com/MaterializeInc/materialize/issues/2169))
- JSON-encoded streams are not supported
  ([#207](https://github.com/MaterializeInc/materialize/issues/207))
- Connecting to Kafka brokers with SSL (client) authentication is not supported
  ([#1785](https://github.com/MaterializeInc/materialize/issues/1785))
- Connecting Kafka sinks back in as sources is not supported
  ([#1665](https://github.com/MaterializeInc/materialize/issues/1665))
- Protobuf data for Kafka sinks is not supported
  ([#1541](https://github.com/MaterializeInc/materialize/issues/1541))
- JSON-encoded data for Kafka sinks is not supported
  ([#1540](https://github.com/MaterializeInc/materialize/issues/1540))
- Progress tracking for Kafka sinks is not supported
  ([#1442](https://github.com/MaterializeInc/materialize/issues/1442))

### Kinesis

- Kinesis sources with more than one shard are not supported
  ([#2222](https://github.com/MaterializeInc/materialize/issues/2222))
- Consistent Kinesis sources are not supported
  ([#2191](https://github.com/MaterializeInc/materialize/issues/2191))
- Enhanced fan-out Kinesis consumers are not supported
  ([#2192](https://github.com/MaterializeInc/materialize/issues/2192))
- Reading from a Kinesis stream as of a sequence number or timestamp is not supported
  ([#2294](https://github.com/MaterializeInc/materialize/issues/2294))
- Kinesis sinks are not supported
  ([#2372](https://github.com/MaterializeInc/materialize/issues/2372))

### File sources

None right now!

### Regex formatting

- Regex columns cannot be set as nullable
  ([#1685](https://github.com/MaterializeInc/materialize/issues/1685))
- Formatting regular expression is not resurface-able through `SHOW CREATE
  SOURCE` ([#1762](https://github.com/MaterializeInc/materialize/issues/1762))

## Deployment

### Performance

- Slow queries can impact other, unrelated queries
  ([#1956](https://github.com/MaterializeInc/materialize/issues/1956))

### Networking

- Hostnames that resolve to multiple addresses are not supported
  ([#502](https://github.com/MaterializeInc/materialize/issues/502))

### Monitoring & Debugging

- No report exists to show memory usage for indexes
  ([#1532](https://github.com/MaterializeInc/materialize/issues/1532))
- `EXPLAIN DATAFLOW` does not include details about `ORDER BY` and `LIMIT`
  ([#477](https://github.com/MaterializeInc/materialize/issues/477))

## Closed known limitations

The following issued used to be known limitations, but we've fixed them in the
specified version.

Fixed in | Known limitation
--------------|-----------------
[v0.2.0] | CSV files with header rows are not supported ([#1982](https://github.com/MaterializeInc/materialize/issues/1982))
[v0.1.3] | Intervals do not support addition or subtraction with other intervals ([#1682](https://github.com/MaterializeInc/materialize/issues/1682))

[v0.2.0]: ../release-notes/#v0.2.0
[v0.1.3]: ../release-notes/#v0.1.3
