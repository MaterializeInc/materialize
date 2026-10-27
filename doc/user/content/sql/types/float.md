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

{{% include-syntax file="examples/sql_types/float" example="syntax" %}}

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

```mzsql
SELECT 'NaN'::real AS nan
```
```nofmt
 nan
-----
 NaN
```

The strings are recognized case insensitively.

### Aggregate precision

To support incremental updates, including retractions, Materialize accumulates
`sum` over `real` and `double precision` values in a fixed-point
representation. Aggregates computed from `sum` inherit its behavior: `avg`,
`stddev`, `stddev_pop`, `stddev_samp`, `variance`, `var_pop`, and `var_samp`.
Aggregates that do not sum their inputs, such as `min` and `max`, return exact
input values.

When these aggregates run in a dataflow, for example in an index, a
materialized view, a subscription, or a `SELECT` query that reads from a source
or table, each input value is truncated toward zero to a multiple of
2<sup>-24</sup> (approximately 6E-8) before it is added:

- Values with a magnitude smaller than 2<sup>-24</sup> contribute `0`.
- Values with fractional parts that are not multiples of 2<sup>-24</sup> lose
  precision, for example `0.1` contributes `0.09999996423721313`.
- The result is incorrect if the magnitude of the final sum reaches
  2<sup>103</sup> (approximately 1E+31).

Queries that Materialize evaluates entirely during planning, such as
aggregates over constant `VALUES` lists, use floating-point addition and are
not subject to this truncation.

If your application requires exact sums of fractional values, use
[`numeric`](../numeric), which is not subject to this truncation.

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

```mzsql
SELECT 1.23::real AS real_v;
```
```nofmt
 real_v
---------
    1.23
```
