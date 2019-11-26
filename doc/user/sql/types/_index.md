---
title: "SQL Data Types"
description: "Learn more about the SQL data types you love...."
disable_list: true
disable_toc: true
---

Type | Use | Storage Size (bytes) | Syntax
-----|-------------|----------------------|--------
[`BOOLEAN`](boolean) | State of `TRUE` or `FALSE` | 1 | `TRUE`, `FALSE`
[`DATE`](date) | Date without a specified time | 4 | `DATE '2007-02-01'`
[`DECIMAL`](decimal) | Signed exact number with user-defined precision and scale | 16 | `1.23`
[`FLOAT`](float) | Signed variable-precision, inexact number | 8 | `1.23`
[`INT`](int) | Signed integer | 8 | `123`
[`STRING`](string) | Unicode string | Variable | `'foo'`
[`TIMESTAMP`](timestamp) | Date and time | 8 | `TIMESTAMP '2007-02-01 15:04:05'`
[`TIMESTAMPTZ`](timestamp) | Date and time with timezone | 8 | `TIMESTAMPTZ '2007-02-01 15:04:05+06'`
