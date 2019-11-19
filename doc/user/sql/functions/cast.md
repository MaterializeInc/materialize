---
title: "CAST Function and Operator"
description: "Returns the value converted to the specified type"
menu:
  main:
    parent: 'sql-functions'
---

The `cast` function and operator return a value converted to the specified type.

## Parameters

{{< diagram "func-cast.html" >}}

{{< diagram "op-cast.html" >}}

Parameter | Type | Description
----------|------|------------
_val_ | Any | The value you want to convert.
_type_ | Typename | The return value's type.

## Return value

`cast` returns the value with the type specified by the _type_ parameter.

## Details

### Valid casts

Source type | Return type
------------|------------
Int | Int
Int | Float
Int | Decimal
Float | Float
Float | Int
Decimal | Decimal
Decimal | Int
Decimal | Float
Date | Timestamp
Date | TimestampTZ

## Examples

```sql
SELECT CAST(CAST(100.21 AS DECIMAL(10,2)) AS FLOAT) AS dec_to_float;
```
```bash
 dec_to_float
--------------
       100.21
```
<hr/>

```sql
SELECT 100.21::DECIMAL(10,2)::FLOAT AS dec_to_float;
```
```bash
 dec_to_float
--------------
       100.21
```
