---
title: "CAST Function and Operator"
description: "Returns the value converted to the specified type"
menu:
  main:
    parent: 'sql-functions'
---

The `cast` function and operator return a value converted to the specified type.

## Signatures

{{< diagram "func-cast.html" >}}

{{< diagram "op-cast.html" >}}

Parameter | Type | Description
----------|------|------------
_val_ | [Any](../../types) | The value you want to convert.
_type_ | [Typename](../../types) | The return value's type.

### Return value

`cast` returns the value with the type specified by the _type_ parameter.

## Details

### Valid casts

Source type | Return type
------------|------------
`int` | `float`
`int` | `decimal`
`int` | `string`
`float`| `int`
`float`| `decimal`<sup>1</sup>
`float`| `string`
`decimal` | `int`
`decimal` | `float`
`decimal` | `string`
`date` | `timestamp`
`date` | `timestamptz`
`date` | `string`

<sup>1</sup> Casting a `float` to a `decimal` can yield an imprecise result due to the floating point arithmetic involved in the conversion.

## Examples

```sql
SELECT CAST (CAST (100.21 AS decimal(10, 2)) AS float) AS dec_to_float;
```
```nofmt
 dec_to_float
--------------
       100.21
```
<hr/>

```sql
SELECT 100.21::decimal(10, 2)::float AS dec_to_float;
```
```nofmt
 dec_to_float
--------------
       100.21
```
