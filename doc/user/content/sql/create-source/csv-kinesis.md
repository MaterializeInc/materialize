---
title: "CREATE SOURCE: CSV over Kinesis"
description: "Learn how to connect Materialize to a CSV-formatted Kinesis stream"
menu:
  main:
    parent: 'create-source'
aliases:
    - /sql/create-source/csv
    - /sql/create-source/csv-source
---

{{% create-source/intro %}}
This document details how to connect Materialize to CSV-formatted Kinesis
streams.

{{< kinesis-alpha >}}

{{% /create-source/intro %}}

## Syntax

{{< diagram "create-source-csv-kinesis.svg" >}}

{{% create-source/syntax-details connector="kinesis" formats="csv" envelopes="append-only" %}}

## Example

### Creating a source from a CSV-formatted Kinesis stream

```sql
CREATE SOURCE csv_kinesis (col_foo, col_bar, col_baz)
FROM KINESIS ARN ... WITH (
    access_key_id = ...,
    secret_access_key = ...
)
FORMAT CSV WITH 3 COLUMNS;
```

This creates a source that...

- Is append-only.
- Has 3 columns (`col_foo`, `col_bar`, `col_baz`). Materialize will not ingest
  any row without 3 columns.

## Related pages

- [`CREATE SOURCE`](../)
- [`CREATE VIEW`](../../create-view)
- [`SELECT`](../../select)
