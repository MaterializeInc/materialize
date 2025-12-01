---
title: "TABLE"
description: "`TABLE` retrieves all rows from a single table."
menu:
  main:
    parent: commands
---

The `TABLE` expression retrieves all rows from a single table.

## Syntax

```mzsql
TABLE <table_name>;
```

## Details

`TABLE` expressions can be used anywhere that [`SELECT`] expressions are valid.

The expression `TABLE t` is exactly equivalent to the following [`SELECT`]
expression:

```mzsql
SELECT * FROM t;
```

## Examples

Using a `TABLE` expression as a standalone statement:

```mzsql
TABLE t;
```
```nofmt
 a
---
 1
 2
```

Using a `TABLE` expression in place of a [`SELECT`] expression:

```mzsql
TABLE t ORDER BY a DESC LIMIT 1;
```
```nofmt
 a
---
 2
```

[`SELECT`]: ../select
