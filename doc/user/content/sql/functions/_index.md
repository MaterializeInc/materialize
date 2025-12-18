---
title: "SQL functions & operators"
description: "Learn more about the SQL functions and operators supported in Materialize"
menu:
  main:
    identifier: sql-functions
    parent: reference
    weight: 100
disable_list: true
---

This page details Materialize's supported SQL [functions](#functions) and [operators](#operators).

## Functions

### Unmaterializable functions

Several functions in Materialize are **unmaterializable** because their output
depends upon state besides their input parameters, like the value of a session
parameter or the timestamp of the current transaction. You cannot create an
[index](/sql/create-index) or materialized view that depends on an
unmaterializable function, but you can use them in non-materialized views and
one-off [`SELECT`](/sql/select) statements.

Unmaterializable functions are marked as such in the table below.

### Side-effecting functions

Several functions in Materialize are **side-effecting** because their evaluation
changes system state. For example, the `pg_cancel_backend` function allows
canceling a query running on another connection.

Materialize offers only limited support for these functions. They may be called
only at the top level of a `SELECT` statement, like so:

```mzsql
SELECT side_effecting_function(arg, ...);
```

You cannot manipulate or alias the function call expression, call multiple
side-effecting functions in the same `SELECT` statement, nor add any additional
clauses to the `SELECT` statement (e.g., `FROM`, `WHERE`).

Side-effecting functions are marked as such in the table below.

{{% fnlist %}}

## Operators

### Generic operators

Operator | Computes
---------|---------
`val::type` | Cast of `val` as `type` ([docs](cast))

### Boolean operators

Operator | Computes
---------|---------
`AND` | Boolean "and"
`OR` | Boolean "or"
`=` | Equality
`<>` | Inequality
`!=` | Inequality
`<` | Less than
`>` | Greater than
`<=` | Less than or equal to
`>=` | Greater than or equal to
`a BETWEEN x AND y` | `a >= x AND a <= y`
`a NOT BETWEEN x AND y` | `a < x OR a > y`
`a IS NULL` | `a = NULL`
`a ISNULL` | `a = NULL`
`a IS NOT NULL` | `a != NULL`
`a IS TRUE` | `a` is true, requiring `a` to be a boolean
`a IS NOT TRUE` | `a` is not true, requiring `a` to be a boolean
`a IS FALSE` | `a` is false, requiring `a` to be a boolean
`a IS NOT FALSE` | `a` is not false, requiring `a` to be a boolean
`a IS UNKNOWN` | `a = NULL`, requiring `a` to be a boolean
`a IS NOT UNKNOWN` | `a != NULL`, requiring `a` to be a boolean
`a LIKE match_expr [ ESCAPE escape_char ]` | `a` matches `match_expr`, using [SQL LIKE matching](https://www.postgresql.org/docs/13/functions-matching.html#FUNCTIONS-LIKE)
`a ILIKE match_expr [ ESCAPE escape_char ]` | `a` matches `match_expr`, using case-insensitive [SQL LIKE matching](https://www.postgresql.org/docs/13/functions-matching.html#FUNCTIONS-LIKE)

### Numbers operators

Operator | Computes
---------|---------
`+` | Addition
`-` | Subtraction
`*` | Multiplication
`/` | Division
`%` | Modulo
`&` | Bitwise AND
<code>&vert;</code> | Bitwise OR
`#` | Bitwise XOR
`~` | Bitwise NOT
`<<`| Bitwise left shift
`>>`| Bitwise right shift

### String operators

Operator | Computes
---------|---------
<code>&vert;&vert;</code> | Concatenation
`~~` | Matches LIKE pattern case sensitively, see [SQL LIKE matching](https://www.postgresql.org/docs/13/functions-matching.html#FUNCTIONS-LIKE)
`~~*` | Matches LIKE pattern case insensitively (ILIKE), see [SQL LIKE matching](https://www.postgresql.org/docs/13/functions-matching.html#FUNCTIONS-LIKE)
`!~~` | Matches NOT LIKE pattern (case sensitive), see [SQL LIKE matching](https://www.postgresql.org/docs/13/functions-matching.html#FUNCTIONS-LIKE)
`!~~*` | Matches NOT ILIKE pattern (case insensitive), see [SQL LIKE matching](https://www.postgresql.org/docs/13/functions-matching.html#FUNCTIONS-LIKE)
`~` | Matches regular expression, case sensitive
`~*` | Matches regular expression, case insensitive
`!~` | Matches regular expression case sensitively, and inverts the match
`!~*` | Match regular expression case insensitively, and inverts the match

The regular expression syntax supported by Materialize is documented by the
[Rust `regex` crate](https://docs.rs/regex/*/#syntax).
The maximum length of a regular expression is 1 MiB in its raw form, and 10 MiB
after compiling it.

{{< warning >}}
Materialize regular expressions are similar to, but not identical to, PostgreSQL
regular expressions.
{{< /warning >}}

### Time-like operators

Operation | Computes
----------|------------
[`date`](../types/date) `+` [`interval`](../types/interval) | [`timestamp`](../types/timestamp)
[`date`](../types/date) `-` [`interval`](../types/interval) | [`timestamp`](../types/timestamp)
[`date`](../types/date) `+` [`time`](../types/time) | [`timestamp`](../types/timestamp)
[`date`](../types/date) `-` [`date`](../types/date) | [`integer`](../types/integer)
[`timestamp`](../types/timestamp) `+` [`interval`](../types/interval) | [`timestamp`](../types/timestamp)
[`timestamp`](../types/timestamp) `-` [`interval`](../types/interval) | [`timestamp`](../types/timestamp)
[`timestamp`](../types/timestamp) `-` [`timestamp`](../types/timestamp) | [`interval`](../types/interval)
[`time`](../types/time) `+` [`interval`](../types/interval) | `time`
[`time`](../types/time) `-` [`interval`](../types/interval) | `time`
[`time`](../types/time) `-` [`time`](../types/time) | [`interval`](../types/interval)

### JSON operators

{{% json-operators %}}

### Map operators

{{% map-operators %}}

### List operators

List operators are [polymorphic](../types/list/#polymorphism).

{{% list-operators %}}
