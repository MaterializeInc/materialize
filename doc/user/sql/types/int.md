---
title: "int Data Type"
description: "Expresses a signed integer"
menu:
  main:
    parent: 'sql-types'
---

`int` data expresses a signed integer.

Detail | Info
-------|------
**Size** | 8 bytes
**Min value** | -9223372036854775808
**Max value** | 9223372036854775807

## Details

### Valid casts

#### From `int`

You can [cast](../../functions/cast) `int` to:

- [`decimal`](../decimal)
- [`float`](../float)
- [`string`](../string)

#### To `int`

You can [cast](../../functions/cast) the following types to `int`:

- [`decimal`](../decimal)
- [`float`](../float)

## Examples

```sql
SELECT 123::int AS int_v;
```
```nofmt
 int_v
-------
   123
```

<hr/>

```sql
SELECT 1.23::int AS int_v;
```
```nofmt
 int_v
-------
     1
```
