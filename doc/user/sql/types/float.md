---
title: "Floating-Point Data Types"
description: "Express signed, inexact numbers"
menu:
  main:
    parent: 'sql-types'
---

`real` and `double precision` data express a variable-precision, inexact number
with a floating decimal point.

Type               | Aliases           | Size    | Range
-------------------|-------------------|---------|------
`real`             | `float4`          | 4 bytes | Approx. 1E-37 to 1E+37 with 6 decimal digits of precision
`double precision` | `float`, `float8` | 8 bytes | Approx. 1E-307 to 1E+307 with 15 decimal digits of precision

## Syntax

{{< diagram "type-float.html" >}}

## Details

- Materialize assumes untyped numeric literals containing decimal points are [`decimal`](../decimal); to use `float`, you must explicitly cast them as we've done below.

## Details

### Valid casts

In addition to the casts listed below, `real` and `double precision` values
can be cast to and from one another.

#### From `real` or `double precision`

You can [cast](../../functions/cast) `real` or `double precision` to:

- [`int`](../int)
- [`decimal`](../float)
- [`string`](../string)

#### To `real` or `double precision`

You can [cast](../../functions/cast) the following types to `real` or `double
precision`:

- [`int`](../int)
- [`decimal`](../float)

## Examples

```sql
SELECT 1.23::real AS real_v;
```
```nofmt
 real_v
---------
    1.23
```
