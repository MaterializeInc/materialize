---
title: "CREATE SOURCE: CSV from local file"
description: "Learn how to connect Materialize to local CSV files"
menu:
  main:
    parent: 'create-source'
aliases:
    - /sql/create-source/csv
    - /sql/create-source/csv-source
---

{{% create-source/intro %}}
This document details how to connect Materialize to local CSV files.
{{% /create-source/intro %}}

## Syntax

{{< diagram "create-source-csv-file.svg" >}}

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
- Materialize reads once during source creation.
- Has as many columns as the first row of the CSV file, and uses that row to name the columns.

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
