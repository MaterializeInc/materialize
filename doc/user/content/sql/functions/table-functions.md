---
title: "Table functions"
description: "Functions that return multiple rows"
menu:
  main:
    parent: 'sql-functions'
---

## Overview

[Table functions](/sql/functions/#table-functions) return multiple rows from one
input row. They are typically used in the `FROM` clause, where their arguments
are allowed to refer to columns of earlier tables in the `FROM` clause.

For example, consider the following table whose rows consist of lists of
integers:

```mzsql
CREATE TABLE quizzes(scores int list);
INSERT INTO quizzes VALUES (LIST[5, 7, 8]), (LIST[3, 3]);
```

Query the `scores` column from the table:

```mzsql
SELECT scores
FROM quizzes;
```

The query returns two rows, where each row is a list:

```
 scores
---------
 {3,3}
 {5,7,8}
(2 rows)
```

Now, apply the [`unnest`](/sql/functions/#unnest) table function to expand the
`scores` list into a collection of rows, where each row contains one list item:

```mzsql
SELECT scores, score
FROM
  quizzes,
  unnest(scores) AS score; -- In Materialize, shorthand for AS t(score)
```

The query returns 5 rows, one row for each list item:

```
 scores  | score
---------+-------
 {3,3}   |     3
 {3,3}   |     3
 {5,7,8} |     5
 {5,7,8} |     7
 {5,7,8} |     8
(5 rows)
```

{{< tip >}}

For illustrative purposes, the original `scores` column is included in the
results (i.e., query projection). In practice, you generally would omit
including the original list to minimize the return data size.

{{</ tip >}}

## `WITH ORDINALITY`

When a table function is used in the `FROM` clause, you can add `WITH
ORDINALITY` after the table function call. `WITH ORDINALITY` adds a column that
includes the **1**-based numbering for each output row, restarting at **1** for
each input row.

The following example uses `unnest(...) WITH ORDINALITY` to include the `ordinality` column containing the **1**-based numbering of the unnested items:
```mzsql
SELECT scores, score, ordinality
FROM
  quizzes,
  unnest(scores) WITH ORDINALITY AS t(score,ordinality);
```

The results includes the `ordinality` column:
```
 scores  | score | ordinality
---------+-------+------------
 {3,3}   |     3 |          1
 {3,3}   |     3 |          2
 {5,7,8} |     5 |          1
 {5,7,8} |     7 |          2
 {5,7,8} |     8 |          3
(5 rows)
```

## Table- and column aliases

You can use table- and column aliases to name both the result column(s) of a table function as well as the ordinality column, if present. For example:
```mzsql
SELECT scores, t.score, t.listidx
FROM
  quizzes,
  unnest(scores) WITH ORDINALITY AS t(score,listidx);
```

You can also name fewer columns in the column alias list than the number of
columns in the output of the table function (plus `WITH ORDINALITY`, if
present), in which case the extra columns retain their original names.


## `ROWS FROM`

When you select from multiple relations without specifying a relationship, you
get a cross join. This is also the case when you select from multiple table
functions in `FROM` without specifying a relationship.

For example, consider the following query that selects from two table functions
without a relationship:

```mzsql
SELECT *
FROM
  generate_series(1, 2) AS g1,
  generate_series(6, 7) AS g2;
```

The query returns every combination of rows from both:

```

 g1 | g2
----+----
  1 |  6
  1 |  7
  2 |  6
  2 |  7
(4 rows)
```

Using `ROWS FROM` clause with the multiple table functions, you can zip the
outputs of the table functions (i.e., combine the n-th output row from each
table function into a single row) instead of the cross product.
That is, combine first output rows of all the table functions into the first row, the second output rows of all the table functions are combined into
a second row, and so on.

For example, modify the previous query to use `ROWS FROM` with the table
functions:

```mzsql
SELECT *
FROM
  ROWS FROM (
    generate_series(1, 2),
    generate_series(6, 7)
  ) AS t(g1, g2);
```

Instead of the cross product, the results are the "zipped" rows:

```
 g1 | g2
----+----
  1 |  6
  2 |  7
(2 rows)
```

If the table functions in a `ROWS FROM` clause produce a different number of
rows, nulls are used for padding:
```mzsql
SELECT *
FROM
  ROWS FROM (
    generate_series(1, 3),  -- 3 rows
    generate_series(6, 7)   -- 2 rows
  ) AS t(g1, g2);
```

The row with the `g1` value of 3 has a null `g2` value (note that if using psql,
psql prints null as an empty string):

```
| g1 | g2   |
| -- | ---- |
| 3  | null |
| 1  | 6    |
| 2  | 7    |
(3 rows)
```

For `ROWS FROM` clauses:
- you can use `WITH ORDINALITY` on the entire `ROWS FROM` clause, not on the
individual table functions within the `ROWS FROM` clause.
- you can use table- and column aliases only on the entire `ROWS FROM` clause,
not on the individual table functions within `ROWS FROM` clause.

For example:

```mzsql
SELECT *
FROM
  ROWS FROM (
    generate_series(5, 6),
    generate_series(8, 9)
  ) WITH ORDINALITY AS t(g1, g2, o);
```

The results contain the ordinality value in the `o` column:

```

 g1 | g2 | o
----+----+---
  5 |  8 | 1
  6 |  9 | 2
(2 rows)
```


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

See a list of table functions in the [function reference](/sql/functions/#table-functions).
