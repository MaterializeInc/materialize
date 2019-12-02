---
title: "FLOAT Data Type"
description: "Expresses a signed variable-precision, inexact number"
menu:
  main:
    parent: 'sql-types'
---

`FLOAT` data expresses a variable-precision, inexact number.

Detail | Info
-------|------
**Size** | 8 bytes
**Min value** | 2.2250738585072014 × 10<sup>-308</sup>
**Max value** | 1.7976931348623158 × 10<sup>308</sup>

## Syntax

{{< diagram "type-float.html" >}}

## Details

- Materialize assumes untyped numeric literals containing decimal points are `DECIMAL`; to use `FLOAT`, you must explicitly cast them as we've done below.

## Examples

```sql
SELECT 1.23::FLOAT AS float_v;
```
```shell
 float_v
---------
    1.23
```
