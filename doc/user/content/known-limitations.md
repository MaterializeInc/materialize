---
title: "Known Limitations"
description: "Materialize's known limitations"
menu: "main"
weight: 400
---

This document applies to Materialize {{< version >}}.

The following links describe features that Materialize does not yet support, but
plans to in future releases. If any of these issues impact you, feel free to let
us know on the linked-to GitHub issue.

## SQL

### Types

- Numeric precision is not always equivalent to PostgreSQL {{% gh 1824 %}}
- Numeric to float conversions are susceptible to floating point errors {{% gh 1102 %}}

### Performance

- `ANY` and `ALL` queries generate suboptimal plans {{% gh 744 %}}

### Functions

- Materialize does not support [window functions](https://www.postgresql.org/docs/current/tutorial-window.html). In some cases, you may be able to achieve the desired results with [temporal filters](https://materialize.com/temporal-filters/) or the [TOP K by group](/guides/top-k/) idiom instead. {{% gh 213 %}}

### Common table expressions (CTEs)

- CTEs only support `SELECT` queries. {{% gh 4867 %}}
- Materialize inlines the CTE where it's referenced, which could cause
  unexpected performance characteristics for especially complex expressions. {{%
  gh 4867 %}}
- `WITH RECURSIVE` CTEs are not available yet. {{% gh 2516 %}}

## Sources & sinks

### Kafka

- Protobuf data for Kafka sinks is not supported {{% gh 1541 %}}

### Kinesis

- Consistent Kinesis sources are not supported {{% gh 2191 %}}
- Enhanced fan-out Kinesis consumers are not supported {{% gh 2192 %}}
- Reading from a Kinesis stream as of a sequence number or timestamp is not supported {{% gh 2294 %}}
- Kinesis resharding is not supported {{% gh 8776 %}}
- Kinesis sinks are not supported {{% gh 2372 %}}

### File sources

None right now!

### Regex formatting

- Regex columns cannot be set as nullable {{% gh 1685 %}}

## Deployment

### Performance

- Slow queries can impact other, unrelated queries {{% gh 1956 %}}

### Networking

- Hostnames that resolve to multiple addresses are not supported {{% gh 502 %}}

### Monitoring & debugging

- No report exists to show memory usage for indexes {{% gh 1532 %}}

## Closed known limitations

The following issued used to be known limitations, but we've fixed them in the
specified version.

Fixed in | Known limitation
--------------|-----------------
[v0.4.0] | Column names generated in returned column set are not available to `GROUP BY` {{% gh 1673 %}}
[v0.3.0] | JSON-encoded streams are not supported {{% gh 207 %}}
[v0.3.0] | Connecting Kafka sinks back in as sources is not supported {{% gh 1665 %}}
[v0.3.0] | Progress tracking for Kafka sinks is not supported {{% gh 1442 %}}
[v0.3.0] | `date_trunc` for `timestamp with time zone` data {{% gh 1814 %}}
[v0.3.0] | Special `date`, `time`, and `timestamp` values from PostgreSQL are not supported {{% gh 1805 %}}
[v0.3.0] | Cannot cast from string to time-like types {{% gh 1378 %}}
[v0.3.0] | Using a non-existent namespace does not result in an error {{% gh 1684 %}}
[v0.3.0] | Connecting to Kafka brokers with SSL (client) authentication is not supported {{% gh 1785 %}}
[v0.3.0] | Kafka sources with more than one partition are not supported {{% gh 2169 %}}
[v0.3.0] | Formatting regular expression is not resurface-able through `SHOW CREATE SOURCE` {{% gh 1762 %}}
[v0.3.0] |  `EXPLAIN DATAFLOW` does not include details about `ORDER BY` and `LIMIT` {{% gh 477 %}}
[v0.2.1] | Kinesis sources with more than one shard are not supported {{% gh 2222 %}}
[v0.2.0] | CSV files with header rows are not supported {{% gh 1982 %}}
[v0.1.3] | Intervals do not support addition or subtraction with other intervals {{% gh 1682 %}}

[v0.4.0]: /release-notes/#v0.4.0
[v0.3.0]: /release-notes/#v0.3.0
[v0.2.1]: /release-notes/#v0.2.1
[v0.2.0]: /release-notes/#v0.2.0
[v0.1.3]: /release-notes/#v0.1.3
