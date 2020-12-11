---
title: "FETCH"
description: "`FETCH` retrieves rows from a cursor."
menu:
  main:
    parent: "sql"
---

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
`TIMEOUT`   | `interval` | `0s`    | When fetching from a [`TAIL`](/sql/tail) cursor, complete if there are no more rows ready after this timeout. The default `0s` will cause `FETCH` to only return rows that have already been produced since the previous `FETCH`.
