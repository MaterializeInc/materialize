---
title: "EXPLAIN ANALYZE"
description: "Reference page for `EXPLAIN ANALYZE`. `EXPLAIN ANALYZE` is used to understand the performance of indexes and materialized views."
menu:
  main:
    parent: commands
---

`EXPLAIN ANALYZE` uses [`mz_introspection`](https://materialize.com/docs/sql/system-catalog/mz_introspection/) sources to report on the performance of indexes and materialized views.

{{< warning >}}
`EXPLAIN` is not part of Materialize's stable interface and is not subject to
our backwards compatibility guarantee. The syntax and output of `EXPLAIN` may
change arbitrarily in future versions of Materialize.
{{< /warning >}}

## Syntax

```mzsql
EXPLAIN ANALYZE
    [ HINTS | [<property> [, <property ...>] [WITH SKEW]]]
    FOR [INDEX ... | MATERIALIZED VIEW ...]
    [ AS SQL ]
;
```

Here `<property>` is either `CPU` or `MEMORY`; you may list them in any order, but each may appear only once.

### Explained object

The following object types can be explained.

Explained object | Description
------|-----
**INDEX name** | Display information for an existing index.
**MATERIALIZED VIEW name** | Display information for an existing materialized view.

### Properties to analyze


Property | Description
------|-----
**`HINTS`** | Annotates the LIR plan with TopK hints.
**`CPU`** | Annotates the LIR plan with information about CPU time consumed.
**`MEMORY`** | Annotates the LIR plan with information about memory consumed.
**`WITH SKEW`** | Adds additional information about average and per-worker consumption (of `CPU` or `MEMORY`).

Note that `CPU` and `MEMORY` can be combined, separated by commas.
When running `EXPLAIN ANALYZE` for `CPU` or `MEMORY`, you can specify `WITH SKEW` to observe average and per worker information, as well.

## Details

Under the hood, `EXPLAIN ANALYZE` runs SQL queries that correlate [`mz_introspection` performance information](https://materialize.com/docs/sql/system-catalog/mz_introspection/) with the LIR operators in [`mz_introspection.mz_lir_mapping`](../../sql/system-catalog/mz_introspection/#mz_lir_mapping).

<!-- TODO add example or two -->

## Privileges

The privileges required to execute this statement are:

- `USAGE` privileges on the schemas that all relations in the explainee are contained in.
