---
title: "VALUES"
description: "`VALUES` constructs a relation from value expressions."
menu:
  main:
    parent: commands
---

The `VALUES` expression constructs a relation from value expressions.

## Syntax

{{< diagram "values-expr.svg" >}}

Field  | Use
-------|-----
_expr_ | The value of a single column of a single row.

## Details

`VALUES` expressions can be used anywhere that [`SELECT`] expressions are valid.
They are most commonly used in [`INSERT`] statements, but they can also stand
alone.

## Examples

Using a `VALUES` expression as a standalone statement:

```sql
VALUES (1, 2, 3), (4, 5, 6);
```
```nofmt
 column1 | column2 | column3
---------+---------+---------
       1 |       2 |       3
       4 |       5 |       6
```

Using a `VALUES` expression in place of a `SELECT` expression:

```sql
VALUES (1), (2), (3) ORDER BY column1 DESC LIMIT 2;
```
```nofmt
 column1
---------
       3
       2
```

Using a `VALUES` expression in an [`INSERT`] statement:

```sql
INSERT INTO t VALUES (1, 2), (3, 4);
```

[`INSERT`]: ../insert
[`SELECT`]: ../select
