---
title: "DECIMAL Data Type"
description: "Expresses an exact number with user-defined precision and scale"
menu:
  main:
    parent: 'sql-types'
---

`DECIMAL` data expresses an exact number with user-defined precision and scale.

Detail | Info
-------|------
**Size** | 16 bytes
**Max precision** | 38
**Max scale** | 38
**Default** | 38 precision, 0 scale

## Syntax

### Decimal values

{{< diagram "type-decimal-val.html" >}}

### Decimal definitions

{{< diagram "type-decimal-def.html" >}}

Field | Definition
------|-----------
_percision_ | The total number of decimal values to track, e.g., `100` has a precision of 3. However, all `DECIMAL` values in Materialize have a precision of 38.
_scale_ | The total number of fractional decimal values to track, e.g. `.321` has a scale of 3.

## Details

- By default, Materialize uses 38 precision and 0 scale, as per the SQL standard.
- Materialize allows you to set the scale to any value in the set `(0, 38)`; however, the precision cannot be changed from 38.
- Materialize assumes untyped numeric literals containing decimal points are `DECIMAL`.

## Examples

```sql
SELECT 1.23::DECIMAL AS dec_v;
```
```shell
 dec_v
-------
     1
```
<hr/>

```sql
SELECT 1.23::DECIMAL(38,3) AS dec_38_3_v;
```
```shell
 dec_38_3_v
------------
      1.230
```
