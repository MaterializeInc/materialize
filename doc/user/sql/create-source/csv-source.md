---
title: "CREATE SOURCE: CSV"
description: "Learn how to connect Materialize to a Protobuf-formatted Kafka topic"
menu:
  main:
    parent: 'create-source'
aliases:
    - /docs/sql/create-source/csv
---

`CREATE SOURCE` connects Materialize to some data source, and lets you interact
with its data as if it were in a SQL table.

This document details how to connect Materialize to locally available CSV file. For other options, view [`CREATE  SOURCE`](../).

## Conceptual framework

Sources represent connections to resources outside Materialize that it can read
data from. For more information, see [API Components:
Sources](../../../overview/api-components#sources).

## Syntax

{{< diagram "create-source-csv.html" >}}

Field | Use
------|-----
_src&lowbar;name_ | The name for the source, which is used as its table name within SQL.
**FILE** _path_ | The absolute path to the file you want to use as the source.
**WITH (** _option&lowbar;list_ **)** | Options affecting source creation. For more detail, see [`WITH` options](#with-options).
**CSV WITH** _n_ **COLUMNS** | Format the source's data as a CSV with _n_ columns. Any data without _n_ columns is not propagated to the source.
**DELIMITED BY** _char_ | Delimit the CSV by _char_. ASCII comma by default (`','`). This must be an ASCII character; other Unicode code points are not supported.

#### `WITH` options

The following options are valid within the `WITH` clause.

Field | Value | Description
------|-------|------------
`tail` | `bool` | Continually check the file for new content; as new content arrives, process it using other `WITH` options.

## Details


This document assumes you're using a locally available file to load CSV data into Materialize.

### File source details

- `path` values must be the file's absolute path, e.g.
    ```sql
    CREATE SOURCE server_source FROM FILE '/Users/sean/server.log'...
    ```
- All data in file sources are treated as [`text`](../../types/text).

### CSV format details

CSV-formatted sources read lines from a CSV file.

- Every line of the CSV is treated as a row, i.e. there is no concept of
  headers.
- Columns in the source are named `column1`, `column2`, etc.
- Any row with a number of columns other than the specified amount (**CSV WITH**
  _n_ **COLUMNS**) gets ignored, though Materialize will log an error.

### Envelope details

Envelopes determine whether an incoming record inserts new data, updates or
deletes existing data, or both. For more information, see [API Components:
Envelopes](../../../overview/api-components#envelopes).

CSV data can currently only be handled as append-only by Materialize.

## Example

### Creating a source from a dynamic CSV

```sql
CREATE SOURCE test
FROM FILE '[path to .csv]'
WITH (tail = true)
FORMAT CSV WITH 5 COLUMNS;
```

This creates a source that...

- Is append-only.
- Has 5 columns (`column1`...`column5`). Materialize will ignore all data
  received without 5 columns.
- Materialize dynamically checks for new entries.

## Related pages

- [`CREATE SOURCE`](../)
- [`CREATE VIEW`](../../create-view)
- [`SELECT`](../../select)
