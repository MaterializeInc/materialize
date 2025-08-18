---
title: "Table functions"
description: "Functions that return multiple rows"
menu:
  main:
    parent: 'sql-functions'
---

## Overview

[Table functions]((/sql/functions/#table-functions)) return multiple rows from one input row. They are typically used in the `FROM` clause, where their arguments are allowed to refer to columns of earlier tables in the `FROM` clause.

For example, the `unnest` table function expands a list into a collection of rows, where each row will contain one item of the list. For example, if we have the following input data:
```mzsql
CREATE TABLE t(l int list);
INSERT INTO t VALUES (LIST[5, 7, 8]), (LIST[3, 3]);
```
then
```mzsql
SELECT l
FROM t;
```
gives us one list per row:
```
    l
---------
 {3,3}
 {5,7,8}
(2 rows)
```
but
```mzsql
SELECT l, e
FROM
  t,
  unnest(l) AS e;
```
gives us one list item per row:
```
    l    | e
---------+---
 {3,3}   | 3
 {3,3}   | 3
 {5,7,8} | 5
 {5,7,8} | 7
 {5,7,8} | 8
(5 rows)
```
(The `l` column, which contains as many copies of each input list as the list length, is included in the above `SELECT`'s output columns for demonstration purposes only; in real queries, you would typically omit the original list from the `SELECT` to avoid blowing up the data size.)

## `WITH ORDINALITY`

You can also add `WITH ORDINALITY` after a table function call in a `FROM`
clause. This causes an `ordinality` column to appear, which is a 1-based
numbering of the output rows of each call of the table function.

For example, `unnest(...) WITH ORDINALITY` includes the `ordinality` column containing the 1-based numbering of the unnested items:
```
SELECT l, e, ordinality
FROM
  t,
  unnest(l) WITH ORDINALITY AS e;

    l    | e | ordinality
---------+---+------------
 {3,3}   | 3 |          1
 {3,3}   | 3 |          2
 {5,7,8} | 5 |          1
 {5,7,8} | 7 |          2
 {5,7,8} | 8 |          3
(5 rows)
```

## Table- and column aliases

You can use table- and column aliases to name both the result column(s) of a table function as well as the ordinality column, if present. For example:
```mzsql
SELECT l, u.e, u.o
FROM
  t,
  unnest(l) WITH ORDINALITY AS u(e, o);
```

You can also name less columns in the column alias list than the number of columns in the output of the table function (plus `WITH ORDINALITY`, if present), in which case the extra columns retain their original names.

## `ROWS FROM`

Normally, if you use multiple table functions together, the output will be their cross product. For example:
```mzsql
SELECT *
FROM
  generate_series(1, 2) AS g1,
  generate_series(6, 7) AS g2;
```
```

 g1 | g2
----+----
  1 |  6
  1 |  7
  2 |  6
  2 |  7
(4 rows)
```

You can get a different behavior by using the `ROWS FROM` clause, which zips the outputs of multiple table functions rather than taking their cross product. In other words, the first output rows of all the table functions are combined into a single row, the second output rows of all the table functions are combined into a second row, and so on. For example:
```mzsql
SELECT *
FROM
  ROWS FROM (
    generate_series(1, 2),
    generate_series(6, 7)
  ) AS t(g1, g2);
```
```

 g1 | g2
----+----
  1 |  6
  2 |  7
(2 rows)
```

If not all the table functions in a `ROWS FROM` clause produce the same number of rows, nulls are used for padding:
```mzsql
SELECT *
FROM
  ROWS FROM (
    generate_series(1, 3),
    generate_series(6, 7)
  ) AS t(g1, g2);
```
```

 g1 | g2
----+----
  1 |  6
  2 |  7
  3 |
(3 rows)
```

You can't use `WITH ORDINALITY` on the individual table functions in a `ROWS FROM` clause, but you can use `WITH ORDINALITY` on the entire `ROWS FROM` clause. For example:
```mzsql
SELECT *
FROM
  ROWS FROM (
    generate_series(5, 6),
    generate_series(8, 9)
  ) WITH ORDINALITY AS t(g1, g2, o);
```
```

 g1 | g2 | o
----+----+---
  5 |  8 | 1
  6 |  9 | 2
(2 rows)
```

For `ROWS FROM` clauses, you can use table- and column aliases only on the entire `ROWS FROM` clause, not on the individual table functions.

## Table functions in the `SELECT` clause

You can call table functions in the `SELECT` clause. These will be executed as if they were at the end of the `FROM` clause, but their output columns will be at the appropriate position specified by their positions in the `SELECT` clause.

However, table functions in a `SELECT` clause have a number of restrictions (similar to Postgres):
- If there are multiple table functions in the `SELECT` clause, they are executed as if in an implicit `ROWS FROM` clause.
- `WITH ORDINALITY` and (explicit) `ROWS FROM` are not allowed.
- You can give a table function call a column alias, but not a table alias.
- If there are multiple output columns of a table function (e.g., `regexp_extract` has an output column per capture group), these will be combined into a single column, with a record type.

## Tabletized scalar functions

You can also call ordinary scalar functions in the `FROM` clause as if they were table functions. In that case, their output will be considered a table with a single row and column.

## See also

See a list of table functions in the [function reference]((/sql/functions/#table-functions)).
