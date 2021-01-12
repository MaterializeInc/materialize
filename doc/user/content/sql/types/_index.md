---
title: "SQL Data Types"
description: "Learn more about the SQL data types you love...."
menu:
  main:
    identifier: sql-types
    name: Data Types
    parent: sql
    weight: 1
disable_list: true
---

Type | Aliases | Use | Size (bytes) | Syntax
-----|---------|-----|--------------|--------
[`boolean`](boolean) | `bool` | State of `TRUE` or `FALSE` | 1 | `TRUE`, `FALSE`
[`bigint`](integer) | `int8` | Large signed integer | 8 | `123`
[`date`](date) | | Date without a specified time | 4 | `DATE '2007-02-01'`
[`double precision`](float) | `float`, `float8`, `double` | Double precision floating-point number | 8 | `1.23`
[`integer`](integer) | `int`, `int4` | Signed integer | 4 | `123`
[`interval`](interval) | | Duration of time | 32 | `INTERVAL '1-2 3 4:5:6.7'`
[`jsonb`](jsonb) | `json` | JSON | Variable | `'{"1":2,"3":4}'::jsonb`
[`map`](map) | | Map with [`text`](text) keys and a uniform value type | Variable | `'{a: 1, b: 2}'::map[text=>int]`
[`list`](list) | | Multidimensional list | Variable | `LIST[[1,2],[3]]`
[`record`](record) | | Tuple with arbitrary contents | Variable | `ROW($expr, ...)`
[`numeric`](numeric) | `decimal` | Signed exact number with user-defined precision and scale | 16 | `1.23`
[`oid`](oid) | | PostgreSQL object identifier | 4 | `123`
[`real`](float) | `float4` | Single precision floating-point number | 4 | `1.23`
[`text`](text) | `string` | Unicode string | Variable | `'foo'`
[`time`](time) | | Time without date | 4 | `TIME '01:23:45'`
[`timestamp`](timestamp) | | Date and time | 8 | `TIMESTAMP '2007-02-01 15:04:05'`
[`timestamp with time zone`](timestamp) | `timestamp with time zone` | Date and time with timezone | 8 | `TIMESTAMPTZ '2007-02-01 15:04:05+06'`
[Arrays](array) (`[]`) | | Multidimensional array | Variable | `ARRAY[...]`
