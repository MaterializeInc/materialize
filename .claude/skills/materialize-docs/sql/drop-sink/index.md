---
audience: developer
canonical_url: https://materialize.com/docs/sql/drop-sink/
complexity: intermediate
description: '`DROP SINK` removes a sink from Materialize.'
doc_type: reference
keywords:
- DROP SINK
- IF EXISTS
product_area: Sinks
status: stable
title: DROP SINK
---

# DROP SINK

## Purpose
`DROP SINK` removes a sink from Materialize.

If you need to understand the syntax and options for this command, you're in the right place.


`DROP SINK` removes a sink from Materialize.


`DROP SINK` removes a sink from Materialize.

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: kafka-sink-drop --> --> -->

## Syntax

This section covers syntax.

```mzsql
DROP SINK [IF EXISTS] <sink_name>;
```text

Syntax element | Description
---------------|------------
**IF EXISTS** | Optional. If specified, do not return an error if the specified sink does not exist.
`<sink_name>` | The sink you want to drop. You can find available sink names through [`SHOW SINKS`](../show-sinks).

## Examples

This section covers examples.

```mzsql
SHOW SINKS;
```text
```nofmt
my_sink
```text
```mzsql
DROP SINK my_sink;
```text
```nofmt
DROP SINK
```

## Privileges

The privileges required to execute this statement are:

- Ownership of the dropped sink.
- `USAGE` privileges on the containing schema.


## Related pages

- [`SHOW SINKS`](../show-sinks)
- [`CREATE SINK`](../create-sink)
- [`DROP OWNED`](../drop-owned)