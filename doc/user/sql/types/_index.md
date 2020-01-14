---
title: "SQL Data Types"
description: "Learn more about the SQL data types you love...."
disable_list: true
disable_toc: true
---

Type | Use | Size (bytes) | Syntax
-----|-------------|----------------------|--------
[`boolean`](boolean) | State of `TRUE` or `FALSE` | 1 | `TRUE`, `FALSE`
[`date`](date) | Date without a specified time | 4 | `DATE '2007-02-01'`
[`decimal`](decimal) | Signed exact number with user-defined precision and scale | 16 | `1.23`
[`float`](float) | Signed variable-precision, inexact number | 8 | `1.23`
[`int`](int) | Signed integer | 8 | `123`
[`interval`](interval) | Duration of time | 32 | `INTERVAL '1-2 3 4:5:6.7'`
[`jsonb`](jsonb) | JSON | Variable | `'{"1":2,"3":4}'::JSONB`
[`string`](string) | Unicode string | Variable | `'foo'`
[`timestamp`](timestamp) | Date and time | 8 | `TIMESTAMP '2007-02-01 15:04:05'`
[`timestamptz`](timestamp) | Date and time with timezone | 8 | `TIMESTAMPTZ '2007-02-01 15:04:05+06'`
