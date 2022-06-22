---
title: "FETCH"
description: "`FETCH` retrieves rows from a cursor."
menu:
  main:
    parent: commands
---

`FETCH` retrieves rows from a query using a cursor previously opened with [`DECLARE`](/sql/declare).

## Syntax

{{< diagram "fetch.svg" >}}

Field | Use
------|-----
_count_ | The number of rows to retrieve. Defaults to `1` if unspecified.
_cursor&lowbar;name_ | The name of an open cursor.

### `WITH` option

The following option is valid within the `WITH` clause.

Option name | Value type | Default | Describes
------------|------------|---------|----------
`timeout`   | `interval` | None    | When fetching from a [`TAIL`](/sql/tail) cursor, complete if there are no more rows ready after this timeout. The default will cause `FETCH` to wait for at least one row to be available.

## Details

`FETCH` will return at most the specified _count_ of available rows. Specifying a _count_ of `ALL` indicates that there is no limit on the number of
rows to be returned.

For [`TAIL`](/sql/tail) queries, `FETCH` by default will wait for rows to be available before returning.
Specifying a _timeout_ of `0s` returns only rows that are immediately available.
