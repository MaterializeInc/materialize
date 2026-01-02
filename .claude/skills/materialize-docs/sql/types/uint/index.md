---
audience: developer
canonical_url: https://materialize.com/docs/sql/types/uint/
complexity: intermediate
description: Express unsigned integers
doc_type: reference
keywords:
- Unsigned Integer types
- Catalog name
- OID
- Size
- Range
- SELECT 123
- SELECT 1
product_area: Indexes
status: stable
title: Unsigned Integer types
---

# Unsigned Integer types

## Purpose
Express unsigned integers

If you need to understand the syntax and options for this command, you're in the right place.


Express unsigned integers



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

This section covers details.

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

This section covers examples.

```mzsql
SELECT 123::uint4 AS int_v;
```text
```nofmt
 int_v
-------
   123
```text

<hr/>

```mzsql
SELECT 1.23::uint4 AS int_v;
```text
```nofmt
 int_v
-------
     1
```

