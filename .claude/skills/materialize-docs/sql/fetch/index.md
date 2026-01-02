---
audience: developer
canonical_url: https://materialize.com/docs/sql/fetch/
complexity: intermediate
description: '`FETCH` retrieves rows from a cursor.'
doc_type: reference
keywords:
- FETCH
product_area: Indexes
status: stable
title: FETCH
---

# FETCH

## Purpose
`FETCH` retrieves rows from a cursor.

If you need to understand the syntax and options for this command, you're in the right place.


`FETCH` retrieves rows from a cursor.



`FETCH` retrieves rows from a query using a cursor previously opened with [`DECLARE`](/sql/declare).

## Syntax

[See diagram: fetch.svg]

Field | Use
------|-----
_count_ | The number of rows to retrieve. Defaults to `1` if unspecified.
_cursor&lowbar;name_ | The name of an open cursor.

### `WITH` option

The following option is valid within the `WITH` clause.

Option name | Value type | Default | Describes
------------|------------|---------|----------
`timeout`   | `interval` | None    | When fetching from a [`SUBSCRIBE`](/sql/subscribe) cursor, complete if there are no more rows ready after this timeout. The default will cause `FETCH` to wait for at least one row to be available.

## Details

`FETCH` will return at most the specified _count_ of available rows. Specifying a _count_ of `ALL` indicates that there is no limit on the number of
rows to be returned.

For [`SUBSCRIBE`](/sql/subscribe) queries, `FETCH` by default will wait for rows to be available before returning.
Specifying a _timeout_ of `0s` returns only rows that are immediately available.

