---
title: "FETCH"
description: "`FETCH` retrieves rows from a cursor."
menu:
  main:
    parent: "sql"
---

{{< version-added v0.5.3 >}}

`FETCH` retrieves rows from a query using a cursor previously opened with [`DECLARE`](/sql/declare).

## Syntax

{{< diagram "fetch.svg" >}}

Field | Use
------|-----
_count_ | The number of rows to retrieve. Defaults to `1` if unspecified.
_cursor&lowbar;name_ | The name of an open cursor.

Supported `WITH` option values:

Option name | Value type | Default | Describes
------------|------------|---------|----------
`timeout`   | `interval` | None    | When fetching from a [`TAIL`](/sql/tail) cursor, complete if there are no more rows ready after this timeout. The default will cause `FETCH` to wait for rows to be available. {{< version-added v0.5.5 >}}

## Details

`FETCH` will return at most the specified _count_ of available rows (or all available rows with `ALL`).

For [`TAIL`](/sql/tail) queries, `FETCH` by default will wait for rows to be available before returning.
Specify a timeout of `0s` to return only rows that are immediately available.
