---
title: "Floating-point types"
description: "Express signed, inexact numbers"
menu:
  main:
    parent: 'sql-types'
aliases:
    - /sql/types/double
    - /sql/types/double-precision
    - /sql/types/float4
    - /sql/types/float8
    - /sql/types/real
---

## `real` info

Detail | Info
-------|------
**Size** | 4 bytes
**Aliases** | `float4`
**Catalog name** | `pg_catalog.float4`
**OID** | 700
**Range** | Approx. 1E-37 to 1E+37 with 6 decimal digits of precision

## `double precision` info

Detail | Info
-------|------
**Size** | 8 bytes
**Aliases** | `float`,`float8`, `double`
**Catalog name** | `pg_catalog.float8`
**OID** | 701
**Range** | Approx. 1E-307 to 1E+307 with 15 decimal digits of precision

## Syntax

{{< diagram "type-float.svg" >}}

## Details

### Literals

Materialize assumes untyped numeric literals containing decimal points are
[`numeric`](../numeric); to use `float`, you must explicitly cast them as we've
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

In addition to the casts listed below, `real` and `double precision` values can be cast
to and from one another. The cast from `real` to `double precision` is implicit and the cast from `double precision` to `real` is by assignment.

#### From `real`

You can [cast](../../functions/cast) `real` or `double precision` to:

- [`int`](../int) (by assignment)
- [`numeric`](../numeric) (by assignment)
- [`text`](../text) (by assignment)

#### To `real`

You can [cast](../../functions/cast) to `real` or `double precision` from the following types:

- [`int`](../int) (implicitly)
- [`numeric`](../numeric) (implicitly)
- [`text`](../text) (explicitly)

## Examples

```sql
SELECT 1.23::real AS real_v;
```
```nofmt
 real_v
---------
    1.23
```
