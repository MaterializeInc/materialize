---
audience: developer
canonical_url: https://materialize.com/docs/sql/values/
complexity: intermediate
description: '`VALUES` constructs a relation from value expressions.'
doc_type: reference
keywords:
- VALUES
- INSERT INTO
product_area: Indexes
status: stable
title: VALUES
---

# VALUES

## Purpose
`VALUES` constructs a relation from value expressions.

If you need to understand the syntax and options for this command, you're in the right place.


`VALUES` constructs a relation from value expressions.



`VALUES` constructs a relation from a list of parenthesized value expressions.

## Syntax

This section covers syntax.

```mzsql
VALUES ( expr [, ...] ) [, ( expr [, ...] ) ... ];
```text

Each parenthesis represents a single row. The comma-delimited expressions in
the parenthesis represent the values of the columns in that row.

## Details

`VALUES` expressionscan be used anywhere that [`SELECT`] statements are valid.
They are most commonly used in [`INSERT`] statements, but can also be used
on their own.

## Examples

This section covers examples.

### Use `VALUES` in an `INSERT`

`VALUES` expressions are most commonly used in [`INSERT`] statements. The
following example uses a `VALUES` expression in an [`INSERT`] statement:

```mzsql
INSERT INTO my_table VALUES (1, 2), (3, 4);
```bash

### Use `VALUES` as a standalone expression

`VALUES` expression can be used anywhere that [`SELECT`] statements are valid.

For example:

- As a standalone expression:

  ```mzsql
  VALUES (1, 2, 3), (4, 5, 6);
  ```text

  The above expression returns the following results:

  ```nofmt
  column1 | column2 | column3
  --------+---------+---------
        1 |       2 |       3
        4 |       5 |       6
  ```text

- With an `ORDER BY` and `LIMIT`:

  ```mzsql
  VALUES (1), (2), (3) ORDER BY column1 DESC LIMIT 2;
  ```text

  The above expression returns the following results:

  ```nofmt
  column1
  --------
        3
        2
  ```

[`INSERT`]: ../insert
[`SELECT`]: ../select

