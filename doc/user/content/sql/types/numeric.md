---
title: "numeric type"
description: "Expresses an exact number with user-defined precision and scale"
menu:
  main:
    parent: 'sql-types'
aliases:
    - /sql/types/decimal
---

`numeric` data expresses an exact number with user-defined precision and scale.

Detail | Info
-------|------
**Size** | ~32 bytes
**Aliases** | `dec`, `decimal`
**Catalog name** | `pg_catalog.numeric`
**OID** | 1700
**Precision** | 39
**Scale** | [0, 39]

## Syntax

### Numeric values

{{< diagram "type-numeric-val.svg" >}}

Field | Use
------|-----------
**E**_exp_ | Multiply the number preceding **E** by 10<sup>exp</sup>

### Numeric definitions

{{< diagram "type-numeric-dec.svg" >}}

Field | Use
------|-----------
_precision_ | **Ignored**: All `numeric` values in Materialize have a precision of 39.
_scale_ | The total number of fractional decimal digits to track, e.g. `.321` has a scale of 3. _scale_ cannot exceed the maximum precision.

## Details

### Input

Materialize assumes untyped numeric literals are `numeric` if they:
- Contain decimal points or e-notation.
- Exceed `bigint`'s maximum or minimum values.

Materialize does not accept any numeric literals that exceed 39 digits of precision.

### Output

Materialize trims all trailing zeroes off of `numeric` values, irrespective of
their specified scale. This behavior lets us perform byte-level equality when
comparing rows of data, where we would otherwise need to decode rows' columns'
values for comparisons.

### Precision

All `numeric` values have a precision of 39, which cannot be changed. For ease
of use, Materialize accepts input of any value <= 39, but ignores the specified
value.

Note that the leading zeroes in values between -1 and 1 (i.e. `(-1, 1)`) are not
counted digits of precision.

For details on exceeding the `numeric` type's maximum precision, see
[Rounding](#rounding).

### Scale

By default, `numeric` values do not have a specified scale, so values can have
anywhere between 0 and 39 digits after the decimal point. For example:

```sql
CREATE TABLE unscaled (c NUMERIC);
INSERT INTO unscaled VALUES
  (987654321098765432109876543210987654321),
  (9876543210987654321.09876543210987654321),
  (.987654321098765432109876543210987654321);

SELECT c FROM unscaled;

                     c
-------------------------------------------
   987654321098765432109876543210987654321
 0.987654321098765432109876543210987654321
  9876543210987654321.09876543210987654321
```

However, if you specify a scale on a `numeric` value, values will be rescaled
appropriately. If the resulting value exceeds the maximum precision for
`numeric` types, you'll receive an error.

```sql
CREATE TABLE scaled (c NUMERIC(39, 20));

INSERT INTO scaled VALUES
  (987654321098765432109876543210987654321);
```
```
ERROR:  numeric field overflow
```
```sql
INSERT INTO scaled VALUES
  (9876543210987654321.09876543210987654321),
  (.987654321098765432109876543210987654321);

SELECT c FROM scaled;

                    c
------------------------------------------
                   0.98765432109876543211
 9876543210987654321.09876543210987654321
```

### Rounding

`numeric` operations will always round off fractional values to limit their
values to 39 digits of precision.

```sql
SELECT 2 * 9876543210987654321.09876543210987654321 AS rounded;

                 rounded
------------------------------------------
 19753086421975308642.1975308642197530864
```

However, if a value exceeds is >= 1E39 or <= -1E39, it generates an
overflow, i.e. it will not be rounded.

### Overflow

Operations generating values >= 1E39 or <= -1E39 are considered overflown.

### Underflow

Operations generating values within the range `(-1E-40, 1E-40)` are considered
underflown.

### Aggregations (`sum`)

**tl;dr** If you use `sum` on `numeric` values, retrieving values of `Infinity`
or `-Infinity` from the operation signals you have overflown the `numeric` type.
At this point, you should start retracting values or risk causing Materialize to
panic.

Materialize's dataflow engine (Differential) requires operations to retain their
commutativity and associativity––a simple way to think about this invariant is
that if you add a series of values, and then subtract the same values in some
random order, you must deterministically reach zero.

While other types in Materialize achieve this by simply allowing overflows (i.e.
going from the maximum 64-bit signed integer to the minimum 64-bit signed
integer), the `numeric` type does not support an equivalent operation.

To provide the invariants that Differential requires, `numeric` values are
instead aggregated into an even larger type behind the scenes––one that supports
twice as many digits of precision. This way, if your aggregation exceeds the
`numeric` type's bounds, we can continue to track the magnitude of excess.

To signal that the `numeric` value is in this state, the operation begins
returning `Infinity` or `-Infinity`. At this point, you must either subtract or
retract values to return representable `numeric` values.

However, because the underlying aggregated type is not of infinite precision, it
too can overflow. In this case, Materialize will instead panic and crash. To
avoid crashing, always immediately retract or subtract values when encountering
infinite results from `sum`.

### Valid casts

#### From `numeric`

You can [cast](../../functions/cast) `numeric` to:

- [`int`/`bigint`](../int) (by assignment)
- [`real`/`double precision`](../float) (implicitly)
- [`text`](../text) (by assignment)

#### To `numeric`

You can [cast](../../functions/cast) from the following types to `numeric`:

- [`int`/`bigint`](../int) (implicitly)
- [`real`/`double precision`](../float) (by assignment)
- [`text`](../text) (explicitly)

## Examples

```sql
SELECT 1.23::numeric AS num_v;
```
```nofmt
 num_v
-------
  1.23
```
<hr/>

```sql
SELECT 1.23::numeric(38,3) AS num_38_3_v;
```
```nofmt
 num_38_3_v
------------
      1.230
```

<hr/>

```sql
SELECT 1.23e4 AS num_w_exp;
```
```nofmt
 num_w_exp
-----------
     12300
```
