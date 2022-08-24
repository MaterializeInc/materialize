---
title: "Integer types"
description: "Express signed integers"
menu:
  main:
    parent: 'sql-types'
aliases:
  - /sql/types/bigint
  - /sql/types/int
  - /sql/types/int2
  - /sql/types/int4
  - /sql/types/int8
  - /sql/types/smallint
---

## `smallint` info

Detail | Info
-------|------
**Size** | 2 bytes
**Aliases** | `int2`
**Catalog name** | `pg_catalog.int2`
**OID** | 23
**Range** | [-32,768, 32,767]

## `integer` info

Detail | Info
-------|------
**Size** | 4 bytes
**Aliases** | `int`, `int4`
**Catalog name** | `pg_catalog.int4`
**OID** | 23
**Range** | [-2,147,483,648, 2,147,483,647]

## `bigint` info

Detail | Info
-------|------
**Size** | 8 bytes
**Aliases** | `int8`
**Catalog name** | `pg_catalog.int8`
**OID** | 20
**Range** | [-9,223,372,036,854,775,808, 9,223,372,036,854,775,807]

## Details

### Valid casts

For details about casting, including contexts, see [Functions:
Cast](../../functions/cast).

#### Between integer types

From | To | Required context
-----|----|--------
`smallint` | `integer` | Implicit
`smallint` | `bigint` | Implicit
`integer` | `smallint` | Assignment
`integer` | `bigint` | Implicit
`bigint` | `smallint` | Assignment
`bigint` | `integer` | Assignment

#### From integer types

You can cast integer types to:

To | Required context
---|--------
[`boolean`](../boolean) (`integer` only) | Explicit
[`numeric`](../numeric) | Implicit
[`oid`](../oid) | Implicit
[`real`/`double precision`](../float) | Implicit
[`text`](../text) | Assignment
[`uint2`/`uint4`/`uint8`](../uint) | Depends on specific cast

#### To `integer` or `bigint`

You can cast the following types to integer types:

From | Required context
---|--------
[`boolean`](../boolean) (`integer` only) | Explicit
[`jsonb`](../jsonb) | Explicit
[`oid`](../oid) (`integer` and `bigint` only) | Assignment
[`numeric`](../numeric) | Assignment
[`real`/`double precision`](../float) | Assignment
[`text`](../text) | Explicit
[`uint2`/`uint4`/`uint8`](../uint) | Depends on specific cast

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
