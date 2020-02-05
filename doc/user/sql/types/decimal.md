---
title: "decimal Data Type"
description: "Expresses an exact number with user-defined precision and scale"
aliases:
    - /docs/sql/types/numeric
menu:
  main:
    parent: 'sql-types'
---

`decimal` data expresses an exact number with user-defined precision and scale.

Detail | Info
-------|------
**Size** | 16 bytes
**Max precision** | 38
**Max scale** | 38
**Default** | 38 precision, 0 scale

## Syntax

### Decimal values

{{< diagram "type-decimal-val.html" >}}

Field | Use
------|-----------
**E**_exp_ | Multiply the number preceeding **E** by 10<sup>exp</sup>

### Decimal definitions

{{< diagram "type-decimal-def.html" >}}

Field | Use
------|-----------
_precision_ | The total number of decimal values to track, e.g., `100` has a precision of 3. However, all `decimal` values in Materialize have a precision of 38.
_scale_ | The total number of fractional decimal values to track, e.g. `.321` has a scale of 3. _scale_ cannot exceed the maximum precision.

## Details

- Materialize assumes untyped numeric literals containing decimal points or e-notation are `decimal`.
- By default, Materialize uses 38 precision and 0 scale, as per the SQL standard.
- Materialize allows you to set the scale to any value in the set `(0, 38)`; however, the precision cannot be changed from 38.

### Valid casts

#### From `decimal`

You can [cast](../../functions/cast) `decimal` to:

- [`int`](../int)
- [`float`](../float)
- [`string`](../string)

#### To `decimal`

You can [cast](../../functions/cast) the following types to `decimal`:

- [`int`](../int)
- [`float`](../float)

## Examples

```sql
SELECT 1.23::decimal AS dec_v;
```
```nofmt
 dec_v
-------
     1
```
<hr/>

```sql
SELECT 1.23::decimal(38,3) AS dec_38_3_v;
```
```nofmt
 dec_38_3_v
------------
      1.230
```

<hr/>

```sql
SELECT 1.23e4 AS dec_w_exp;
```
```nofmt
 dec_w_exp
-----------
     12300
```
