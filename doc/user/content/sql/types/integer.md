---
title: "Integer Data Types"
description: "Express signed integers"
menu:
  main:
    parent: 'sql-types'
aliases:
  - /sql/types/bigint
  - /sql/types/int
  - /sql/types/int4
  - /sql/types/int8
---

## `int4` info

Detail | Info
-------|------
**Size** | 4 bytes
**Aliases** | `int`, `integer`
**OID** | 23
**Range** | [-2,147,483,648, 2,147,483,647]

## `int8` info

Detail | Info
-------|------
**Size** | 8 bytes
**Aliases** | `bigint`
**OID** | 20
**Range** | [-9,223,372,036,854,775,808, 9,223,372,036,854,775,807]

## Details

### Valid casts

In addition to the casts listed below, all integer types can be cast to and from
all other integer types.

#### From `int4` or `int8`

You can [cast](../../functions/cast) `int4` or `int8` to:

- [`bool`](../bool)
- [`numeric`](../numeric)
- [`float4`/`float8`](../float)
- [`text`](../text)

#### To `int4` or `int8`

You can [cast](../../functions/cast) the following types to `int4` or `int8`:

- [`numeric`](../numeric)
- [`float4`/`float8`](../float)

## Examples

```sql
SELECT 123::integer AS int_v;
```
```nofmt
 int_v
-------
   123
```

<hr/>

```sql
SELECT 1.23::integer AS int_v;
```
```nofmt
 int_v
-------
     1
```
