---
title: "VALUES"
description: "`VALUES` constructs a relation from value expressions."
menu:
  main:
    parent: commands
---

`VALUES` constructs a relation from a list of parenthesized value expressions.

## Syntax

```mzsql
VALUES ( expr [, ...] ) [, ( expr [, ...] ) ... ]
```

Each parenthesis represents a single row. The comma-delimited expressions in
the parenthesis represent the values of the columns in that row.

## Details

`VALUES` expressionscan be used anywhere that [`SELECT`] statements are valid.
They are most commonly used in [`INSERT`] statements, but can also be used
on their own.

## Examples

### Use `VALUES` in an `INSERT`

`VALUES` expressions are most commonly used in [`INSERT`] statements. The
following example uses a `VALUES` expression in an [`INSERT`] statement:

```mzsql
INSERT INTO my_table VALUES (1, 2), (3, 4);
```

### Use `VALUES` as a standalone expression

`VALUES` expression can be used anywhere that [`SELECT`] statements are valid.

For example:

- As a standalone expression:

  ```mzsql
  VALUES (1, 2, 3), (4, 5, 6);
  ```

  The above expression returns the following results:

  ```nofmt
  column1 | column2 | column3
  --------+---------+---------
        1 |       2 |       3
        4 |       5 |       6
  ```

- With an `ORDER BY` and `LIMIT`:

  ```mzsql
  VALUES (1), (2), (3) ORDER BY column1 DESC LIMIT 2;
  ```

  The above expression returns the following results:

  ```nofmt
  column1
  --------
        3
        2
  ```

[`INSERT`]: ../insert
[`SELECT`]: ../select
