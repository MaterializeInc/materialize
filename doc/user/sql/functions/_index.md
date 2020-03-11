---
title: "Functions + Operators"
description: "Find all of the great SQL functions you know and love..."
disable_list: true
disable_toc: true
weight: 1
---

This page details Materialize's supported SQL [functions](#functions) and [operators](#operators).

## Functions

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
`a IS NOT NULL` | `a != NULL`
`a LIKE match_expr` | `a` matches `match_expr`, using [SQL LIKE matching](https://www.w3schools.com/sql/sql_like.asp)

### Numbers

Operator | Computes
---------|---------
`+` | Addition
`-` | Subtraction
`*` | Multiplication
`/` | Division
`%` | Modulo

### String

Operator | Computes
---------|---------
<code>&vert;&vert;</code> | Concatenation

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
