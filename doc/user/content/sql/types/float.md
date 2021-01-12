---
title: "Floating-Point Data Types"
description: "Express signed, inexact numbers"
menu:
  main:
    parent: 'sql-types'
aliases:
    - /sql/types/real
    - /sql/types/double
    - /sql/types/double-precision
    - /sql/types/float64
    - /sql/types/float4
    - /sql/types/float8
---

## `float4` info

Detail | Info
-------|------
**Size** | 4 bytes
**Aliases** | `real`
**OID** | 700
**Range** | Approx. 1E-37 to 1E+37 with 6 decimal digits of precision

## `float8` info

Detail | Info
-------|------
**Size** | 8 bytes
**Aliases** | `float`, `double`, `double precision`
**Addressability** | `pg_catalog.float8`
**OID** | 701
**Range** | Approx. 1E-307 to 1E+307 with 15 decimal digits of precision

## Syntax

{{< diagram "type-float.svg" >}}

## Details

### Literals

Materialize assumes untyped numeric literals containing decimal points are
[`decimal`](../decimal); to use `float`, you must explicitly cast them as we've
done below.

### Special values

Floating-point numbers have three special values, as specified in IEEE 754:

Value       | Aliases                    | Represents
------------|----------------------------|-----------
`NaN`       |                            | Not a number
`Infinity`  | `Inf`, `+Infinity`, `+Inf` | Positive infinity
`-Infinity` | `-Inf`                     | Negative infinity

To input these special values, write them as a string and cast that string to
the desired floating-point type. For example:

```sql
SELECT 'NaN'::real AS nan
```
```nofmt
 nan
-----
 NaN
```

The strings are recognized case insensitively.

### Valid casts

In addition to the casts listed below, `float4` and `float8` values can be cast
to and from one another.

#### From `float4` or `float8`

You can [cast](../../functions/cast) `float4` or `float8` to:

- [`int`](../int)
- [`numeric`](../numeric)
- [`text`](../text)

#### To float4` or `float8`

You can [cast](../../functions/cast) the following types to `float4` or
`float8`:

- [`int`](../int)
- [`numeric`](../numeric)
- [`text`](../text)

## Examples

```sql
SELECT 1.23::real AS real_v;
```
```nofmt
 real_v
---------
    1.23
```
