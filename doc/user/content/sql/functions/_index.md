---
title: "Functions + Operators"
description: "Find all of the great SQL functions you know and love..."
menu:
  main:
    identifier: sql-functions
    parent: sql
    weight: 20
disable_list: true
disable_toc: true
---

This page details Materialize's supported SQL [functions](#functions) and [operators](#operators).

## Functions

### Unmaterializable functions

Several functions in Materialize are **unmaterializable** because their output
depends upon state besides their input parameters, like the value of a session
parameter or the timestamp of the current transaction. You cannot create an
[index](/sql/create-index) or materialized view that depends on an
unmaterializable function, but you can use them in unmaterialized views and
one-off [`SELECT`](/sql/select) statements.

Unmaterializable functions are marked as such in the table below.

{{< fnlist >}}

## Operators

### Generic

Operator | Computes
---------|---------
`val::type` | Cast of `val` as `type` ([docs](cast))

### Boolean

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

### Numbers

Operator | Computes
---------|---------
`+` | Addition
`-` | Subtraction
`*` | Multiplication
`/` | Division
`%` | Modulo
`&` | Bitwise AND
`|` | Bitwise OR
`#` | Bitwise XOR
`~` | Bitwise NOT
`<<`| Bitwise left shift
`>>`| Bitwise right shift

### String

Operator | Computes
---------|---------
<code>&vert;&vert;</code> | Concatenation
`~` | Matches regular expression, case sensitive
`~*` | Matches regular expression, case insensitive
`!~` | Does not match regular expression, case insensitive
`!~*` | Does not match regular expression, case insensitive

The regular expression syntax supported by Materialize is documented by the
[Rust `regex` crate](https://docs.rs/regex/*/#syntax).

{{< warning >}}
Materialize regular expressions are similar to, but not identical to, PostgreSQL
regular expressions.
{{< /warning >}}

### Time-like

Operation | Computes
----------|------------
[`date`](../types/date) `+` [`interval`](../types/interval) | [`timestamp`](../types/timestamp)
[`date`](../types/date) `-` [`interval`](../types/interval) | [`timestamp`](../types/timestamp)
[`date`](../types/date) `+` [`time`](../types/time) | [`timestamp`](../types/timestamp)
[`date`](../types/date) `-` [`date`](../types/date) | [`interval`](../types/interval)
[`timestamp`](../types/timestamp) `+` [`interval`](../types/interval) | [`timestamp`](../types/timestamp)
[`timestamp`](../types/timestamp) `-` [`interval`](../types/interval) | [`timestamp`](../types/timestamp)
[`timestamp`](../types/timestamp) `-` [`timestamp`](../types/timestamp) | [`interval`](../types/interval)
[`time`](../types/time) `+` [`interval`](../types/interval) | `time`
[`time`](../types/time) `-` [`interval`](../types/interval) | `time`
[`time`](../types/time) `-` [`time`](../types/time) | [`interval`](../types/interval)

### JSON

{{% json-operators %}}

### Map

{{% map-operators %}}

### List

List operators are [polymorphic](../types/list/#polymorphism).

{{% list-operators %}}
