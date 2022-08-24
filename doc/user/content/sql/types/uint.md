---
title: "Unsigned Integer types"
description: "Express unsigned integers"
menu:
  main:
    parent: 'sql-types'
aliases:
  - /sql/types/uint2
  - /sql/types/uint4
  - /sql/types/uint8
---

## `uint2` info

Detail | Info
-------|------
**Size** | 2 bytes
**Catalog name** | `mz_catalog.uint2`
**OID** | 16,460
**Range** | [0, 65,535]

## `uint4` info

Detail | Info
-------|------
**Size** | 4 bytes
**Catalog name** | `mz_catalog.uint4`
**OID** | 16,462
**Range** | [0, 4,294,967,295]

## `uint8` info

Detail | Info
-------|------
**Size** | 8 bytes
**Catalog name** | `mz_catalog.uint8`
**OID** | 14,464
**Range** | [0, 18,446,744,073,709,551,615]

## Details

### Valid casts

For details about casting, including contexts, see [Functions:
Cast](../../functions/cast).

#### Between unsigned integer types

From    | To      | Required context
--------|---------|--------
`uint2` | `uint4` | Implicit
`uint2` | `uint8` | Implicit
`uint4` | `uint2` | Assignment
`uint4` | `uint8` | Implicit
`uint8` | `uint2` | Assignment
`uint8` | `uint4` | Assignment

#### From unsigned integer types

You can cast unsigned integer types to:

To | Required context
---|--------
[`numeric`](../numeric) | Implicit
[`real`/`double precision`](../float) | Implicit
[`text`](../text) | Assignment
[`smallint`/`integer`/`bigint`](../integer) | Depends on specific cast

#### To `uint4` or `uint8`

You can cast the following types to unsigned integer types:

From | Required context
---|--------
[`boolean`](../boolean) (`integer` only) | Explicit
[`jsonb`](../jsonb) | Explicit
[`oid`](../oid) (`integer` and `bigint` only) | Assignment
[`numeric`](../numeric) | Assignment
[`real`/`double precision`](../float) | Assignment
[`text`](../text) | Explicit
[`smallint`/`integer`/`bigint`](../integer) | Depends on specific cast

## Examples

```sql
SELECT 123::uint4 AS int_v;
```
```nofmt
 int_v
-------
   123
```

<hr/>

```sql
SELECT 1.23::uint4 AS int_v;
```
```nofmt
 int_v
-------
     1
```
