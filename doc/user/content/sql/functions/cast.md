---
title: "CAST Function and Operator"
description: "Returns the value converted to the specified type"
menu:
  main:
    parent: 'sql-functions'
---

The `cast` function and operator return a value converted to the specified [type](../../types/).

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
* **Assignment** -- Values of one type are converted automatically when inserted into a column of a different type
* **Explicit** -- You must invoke `CAST` deliberately

Casts allowed in less strict contexts are also allowed in stricter contexts: that is, implicit casts can also occur explicitly or by assignment, and casts by assignment can also be explicitly invoked.

Source type | Return type | Cast type(s)
------------|-------------|----------
`array`  |  `text` |  Assignment
`bigint`  |  `bool` |  Explicit
`bigint`  |  `decimal` |  Implicit
`bigint`  | `int`  |  Assignment
`bigint`  | `float`  | Implicit
`bigint`  | `real`  | Implicit
`bigint`  | `text`  | Assignment
`bool`  |  `int` | Explicit
`bool`  | `text`  | Assignment
`bytea`  | `text` | Assignment
`date` | `text` | Assignment
`date` | `timestamp` | Implicit
`date` | `timestamptz` | Implicit
`decimal`  | `bigint`  | Assignment
`decimal` | `float` | Implicit
`decimal` | `int` | Assignment
`decimal`  | `real`  |  Implicit
`decimal` | `text` | Assignment
`float`| `bigint` | Assignment
`float`| `decimal`<sup>1</sup> | Assignment
`float`| `int` | Assignment
`float`| `real` | Assignment
`float`| `text` | Assignment
`int`  | `bigint`  |  Implicit
`int` | `boolean` | Explicit
`int` | `decimal` | Implicit
`int` | `float` | Implicit
`int` | `oid` | Implicit
`int`  | `real`  |  Implicit
`int` | `text` | Assignment
`interval` | `text` | Assignment
`interval` | `time` | Assignment
`jsonb`  | `bigint`  |  Explicit
`jsonb`  | `bool`  |  Explicit
`jsonb`  | `decimal`  |  Explicit
`jsonb`  | `float`  |  Explicit
`jsonb`  | `int`  |  Explicit
`jsonb`  | `real`  |  Explicit
`jsonb`  | `text`  |  Assignment
`list` | `list` | Implicit
`list` | `text` | Assignment
`map`  |  `text` |  Assignment
`oid`  |  `int` |  Assignment
`oid`  |  `text` | Explicit
`real`  |  `bigint` |  Assignment
`real`  |  `decimal` |  Assignment
`real`  |  `float` | Implicit
`real`  |  `int` |  Assignment
`real`  |  `text` | Assignment
`record` | `text` | Assignment
`text` | `bigint` | Explicit
`text` | `bool` | Explicit
`text`  | `bytea` | Explicit
`text` | `date` | Explicit
`text` | `decimal` | Explicit
`text` | `float` | Explicit
`text` | `int` | Explicit
`text` | `interval` | Explicit
`text` | `jsonb` | Explicit
`text` | `list` | Explicit
`text` | `map` | Explicit
`text` | `oid` | Explicit
`text`  | `real`   |  Explicit
`text` | `time` | Explicit
`text` | `timestamp` | Explicit
`text` | `timestamptz` | Explicit
`text` | `uuid` | Explicit
`time` | `interval` | Implicit
`time` | `text` | Assignment
`timestamp`  | `date` | Assignment
`timestamp`  | `text` | Assignment
`timestamp`  | `timestamptz` | Implicit
`timestamptz`  | `date` | Assignment
`timestamptz`  | `text` | Assignment
`timestamptz`  | `timestamp` | Assignment
`uuid`  | `text`  |  Assignment

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

## Related topics
* [Data Types](../../types/)
