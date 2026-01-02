---
audience: developer
canonical_url: https://materialize.com/docs/sql/table/
complexity: intermediate
description: '`TABLE` retrieves all rows from a single table.'
doc_type: reference
keywords:
- TABLE
product_area: Indexes
status: stable
title: TABLE
---

# TABLE

## Purpose
`TABLE` retrieves all rows from a single table.

If you need to understand the syntax and options for this command, you're in the right place.


`TABLE` retrieves all rows from a single table.



The `TABLE` expression retrieves all rows from a single table.

## Syntax

This section covers syntax.

```mzsql
TABLE <table_name>;
```bash

## Details

`TABLE` expressions can be used anywhere that [`SELECT`] expressions are valid.

The expression `TABLE t` is exactly equivalent to the following [`SELECT`]
expression:

```mzsql
SELECT * FROM t;
```bash

## Examples

Using a `TABLE` expression as a standalone statement:

```mzsql
TABLE t;
```text
```nofmt
 a
---
 1
 2
```text

Using a `TABLE` expression in place of a [`SELECT`] expression:

```mzsql
TABLE t ORDER BY a DESC LIMIT 1;
```text
```nofmt
 a
---
 2
```

[`SELECT`]: ../select

