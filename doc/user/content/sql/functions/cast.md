---
title: "CAST Function and Operator"
description: "Returns the value converted to the specified type"
menu:
  main:
    parent: 'sql-functions'
---

The `cast` function and operator return a value converted to the specified type.

## Signatures

{{< diagram "func-cast.svg" >}}

{{< diagram "op-cast.svg" >}}

Parameter | Type | Description
----------|------|------------
_val_ | [Any](../../types) | The value you want to convert.
_type_ | [Typename](../../types) | The return value's type.

The following special syntax is permitted if _val_ is a string literal:

{{< diagram "lit-cast.svg" >}}

### Return value

`cast` returns the value with the type specified by the _type_ parameter.

## Details

### Valid casts

Casts may be:

* **Implicit** -- Values are automatically converted (for example, when you add `int4` to `int8`, the `int4` value is automatically converted to `int8`)
* **Explicit** -- You must invoke `CAST` deliberately
* **Assignment** -- Value of one type are converted automatically when inserted into a column of a different type

Casts that occur implicitly or by assignment can also be explicitly invoked.

Source type | Return type | Cast type(s)
------------|-------------|----------
`bool`  |  `int` | explicit
`bool`  | `text`  | assignment
`bytea`  | `text` | assignment
`int` | `boolean` | explicit
`int` | `oid` | implicit
`int`  | `int64`  |  implicit
`int` | `float` | implicit
`int` | `decimal` | implicit
`int` | `text` | assignment
`int64`  |   |
`float`| `int` | assignment
`float`| `decimal`<sup>1</sup> | assignment
`float`| `text` | assignment
`decimal` | `int` | assignment
`decimal` | `float` | implicit
`decimal` | `text` | assignment
`decimal`  | `decimal`   |
`date` | `timestamp` | implicit
`date` | `timestamptz` | implicit
`date` | `text` | assignment
`list` | `text` | assingment
`list` | `list` | implicit
`time` | `interval` | implicit
`time` | `text` | assignment
`timestamp`  | `date` | assignment
`timestamp`  | `text` | assignment
`timestamp`  | `timestamptz` | implicit
`timestamptz`  | `date` | assignment
`timestamptz`  | `text` | assignment
`timestamptz`  | `timestamp` | implicit
`interval` | `time` | assignment
`interval` | `text` | assignment
`text` | `bool` | explicit
`text` | `int` | explicit
`text` | `oid` | explicit
`text` | `float` | explicit
`text` | `decimal` | explicit
`text` | `numeric` | ?same as decimal?
`text` | `date` | explicit
`text` | `time` | explicit
`text` | `timestamp` | explicit
`text` | `timestamptz` | explicit
`text` | `interval` | explicit
`text` | `uuid` |
`text` | `jsonb` |
`text`  |  `bytea` | explicit
`text` | `list` | explicit
`text` | `map` | explicit
`record` | `text` | assignment
`array`  |  `text` |  assignment
`map`  |  `text` |  assignment
`jsonb`  | `bool`  |  explicit
`jsonb`  | `int32`  |  explicit
`jsonb`  | `int64`  |  explicit
`jsonb`  | `float32`  |  explicit
`jsonb`  | `float64`  |  explicit
`jsonb`  | `decimal`  |  explicit
`jsonb`  | `string`  |  assignment
`uuid`  | `text`  |  assignment

<sup>1</sup> Casting a `float` to a `decimal` can yield an imprecise result due to the floating point arithmetic involved in the conversion.

## Examples

```sql
SELECT INT '4';
```
```nofmt
 ?column?
----------
         4
```

<hr>

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
