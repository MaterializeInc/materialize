---
title: "Known Limitations"
description: "Materialize's known limitations"
---

This document applies to Materialize {{< version >}}.

The following links describe features that Materialize does not yet support, but
plans to in future releases. If any of these issues impact you, feel free to let
us know on the linked-to GitHub issue.

## SQL

### Types

- `char list` is not yet supported {{% gh 7613 %}}

### Performance

- `ANY` and `ALL` queries generate suboptimal plans {{% gh 744 %}}

### Functions

- Materialize does not support [window functions](https://www.postgresql.org/docs/current/tutorial-window.html). In some cases, you may be able to achieve the desired results with [temporal filters](/sql/patterns/temporal-filters/) or the [TOP K by group](/sql/patterns/top-k/) idiom instead. {{% gh 213 %}}
- Table functions in scalar positions are only partially supported. We currently
  support no more than one table function in scalar position, which can only be
  situated in the query's outermost projection. {{% gh 1546 %}}

### Common table expressions (CTEs)

- CTEs only support `SELECT` queries. {{% gh 4867 %}}
- `WITH RECURSIVE` CTEs are not available yet. {{% gh 2516 %}}

### Joins

- The combining `JOIN` type must be `INNER` or `LEFT` for `LATERAL` references.
- Nested joins do not support `LATERAL` references.

### `COPY FROM`

- CSV-formatted data does not strictly adhere to Postgres' semantics.
  {{% gh 9074 9075 %}}

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
