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

`CREATE SOURCE` connects Materialize to some data source, and lets you interact
with its data as if it were in a SQL table.

This document details how to connect Materialize to locally available CSV file.
For other options, view [`CREATE  SOURCE`](../).

## Conceptual framework

Sources represent connections to resources outside Materialize that it can read
data from. For more information, see [API Components:
Sources](../../../overview/api-components#sources).

## Syntax

{{< diagram "create-source-csv.html" >}}

Field | Use
------|-----
_src&lowbar;name_ | The name for the source, which is used as its table name within SQL.
_col&lowbar;name_ | Override default column name with the provided [identifier](../../identifiers). If used, a _col&lowbar;name_ must be provided for each column in the created source.
**FILE** _path_ | The absolute path to the file you want to use as the source.
**WITH (** _option&lowbar;list_ **)** | Options affecting source creation. For more detail, see [`WITH` options](#with-options).
**HEADER** | Treat the first line of the CSV file as a header. See [CSV format details](#csv-format-details).
_n_ **COLUMNS** | Format the source's data as a CSV with _n_ columns. See [CSV format details](#csv-format-details).
**DELIMITED BY** _char_ | Delimit the CSV by _char_. ASCII comma by default (`','`). This must be an ASCII character; other Unicode code points are not supported.

#### `WITH` options

The following options are valid within the `WITH` clause.

Field | Value | Description
------|-------|------------
`tail` | `bool` | Continually check the file for new content; as new content arrives, process it using other `WITH` options.

## Details

This document assumes you're using a locally available file to load CSV data
into Materialize.

### File source details

- `path` values must be the file's absolute path, e.g.
    ```sql
    CREATE SOURCE server_source FROM FILE '/Users/sean/server.log'...
    ```
- All data in file sources are treated as [`text`](../../types/text), but can be
  converted to other types using [views](../../create-materialized-view).

### CSV format details

Materialize uses the format method you specify to determine the number of
columns to create in the source, as well as the columns' names.

Method | Outcome
-------|--------
**HEADER** | Materialize reads the first line of the file to determine:<br/><br/>&bull; The number of columns in the file<br/><br/>&bull; The name of each column<br/><br/>The first line of the file is not ingested as data.
_n_ **COLUMNS** | &bull; Materialize treats the file as if it has _n_ columns.<br/><br/>&bull; Columns are named `column1`, `column2`...`columnN`.

- You can override these naming conventions by explicitly naming columns using
  the _col&lowbar;name_ option in `CREATE SOURCE`.
- All rows without the number of columns determined by the format are dropped,
  and Materialize logs an error.

### Envelope details

Envelopes determine whether an incoming record inserts new data, updates or
deletes existing data, or both. For more information, see [API Components:
Envelopes](../../../overview/api-components#envelopes).

CSV data can currently only be handled as append-only by Materialize.

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
