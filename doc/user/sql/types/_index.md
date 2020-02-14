---
title: "SQL Data Types"
description: "Learn more about the SQL data types you love...."
disable_list: true
disable_toc: true
---

Type | Aliases | Use | Size (bytes) | Syntax
-----|---------|-----|--------------|--------
[`bigint`](integer) | `int8` | Large signed integer | 8 | `123`
[`boolean`](boolean) | `bool` | State of `TRUE` or `FALSE` | 1 | `TRUE`, `FALSE`
[`date`](date) | | Date without a specified time | 4 | `DATE '2007-02-01'`
[`numeric`](numeric) | `decimal` | Signed exact number with user-defined precision and scale | 16 | `1.23`
[`double precision`](float) | `float`, `float8` | Double precision floating-point number | 8 | `1.23`
[`real`](float) | `float4` | Single precision floating-point number | 4 | `1.23`
[`integer`](integer) | `int4`, `int` | Signed integer | 4 | `123`
[`interval`](interval) | | Duration of time | 32 | `INTERVAL '1-2 3 4:5:6.7'`
[`jsonb`](jsonb) | `json` | JSON | Variable | `'{"1":2,"3":4}'::jsonb`
[`text`](text) | `string` | Unicode string | Variable | `'foo'`
[`time`](time) | | Time without date | 4 | `TIME '01:23:45'`
[`timestamp`](timestamp) | | Date and time | 8 | `TIMESTAMP '2007-02-01 15:04:05'`
[`timestamp with time zone`](timestamp) | `timestamptz` | Date and time with timezone | 8 | `TIMESTAMPTZ '2007-02-01 15:04:05+06'`

The names listed in the "Type" column represent the names specified in the SQL
standard. For compatibility with other SQL database systems, Materialize often
uses one of the aliases listed in the "Aliases" column to refer to the type
internally and in error messages.
