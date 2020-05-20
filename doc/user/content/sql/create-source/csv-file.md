---
title: "CREATE SOURCE: CSV"
description: "Learn how to connect Materialize to a Protobuf-formatted Kafka topic"
menu:
  main:
    parent: 'create-source'
aliases:
    - /docs/sql/create-source/csv
    - /docs/sql/create-source/csv-source
---

{{% create-source/intro %}}
This document details how to connect Materialize to local CSV files.
{{% /create-source/intro %}}

## Syntax

{{< diagram "create-source-csv.svg" >}}

{{% create-source/syntax-details connector="file" formats="csv" envelopes="append-only" %}}

## Example

### Creating a source from a static CSV

```sql
CREATE SOURCE static_w_header
FROM FILE '[path to .csv]'
FORMAT CSV WITH HEADER;
```

This creates a source that...

- Is append-only.
- Has as many columns as the first row, and uses that row to name the columns.
- Materialize reads once during source creation.

### Creating a source from a dynamic CSV

```sql
CREATE SOURCE dynamic_wo_header (col_foo, col_bar, col_baz)
FROM FILE '[path to .csv]'
WITH (tail = true)
FORMAT CSV WITH 3 COLUMNS;
```

This creates a source that...

- Is append-only.
- Has 3 columns (`col_foo`, `col_bar`, `col_baz`). Materialize will not ingest
  any row without 3 columns.
- Materialize dynamically checks for new entries.

## Related pages

- [`CREATE SOURCE`](../)
- [`CREATE VIEW`](../../create-view)
- [`SELECT`](../../select)
