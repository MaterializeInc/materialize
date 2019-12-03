---
title: "float Data Type"
description: "Expresses a signed variable-precision, inexact number"
menu:
  main:
    parent: 'sql-types'
---

`float` data expresses a variable-precision, inexact number.

Detail | Info
-------|------
**Size** | 8 bytes
**Min value** | 2.2250738585072014 × 10<sup>-308</sup>
**Max value** | 1.7976931348623158 × 10<sup>308</sup>

## Syntax

{{< diagram "type-float.html" >}}

## Details

- Materialize assumes untyped numeric literals containing decimal points are [`decimal`](../decimal); to use `float`, you must explicitly cast them as we've done below.

## Details

### Valid casts

#### From `float`

You can [cast](../../functions/cast) `float` to:

- [`int`](../int)
- [`decimal`](../float)
- [`string`](../string)

#### To `float`

You can [cast](../../functions/cast) the following types to `float`:

- [`int`](../int)
- [`decimal`](../float)

## Examples

```sql
SELECT 1.23::float AS float_v;
```
```nofmt
 float_v
---------
    1.23
```
