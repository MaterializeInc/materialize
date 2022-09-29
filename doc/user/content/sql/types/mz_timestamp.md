---
title: "mz_timestamp type"
description: "Expresses an internal timestamp"
menu:
  main:
    parent: 'sql-types'
---

`mz_timestamp` data expresses an internal timestamp.

## `mz_timestamp` info

Detail | Info
-------|------
**Size** | 8 bytes
**Catalog name** | `mz_catalog.mz_timestamp`
**OID** | 16552
**Min value** | 0
**Max value** | 18446744073709551615

## Details

- This type is produced by `mz_now()`.
- In general this is an opaque type, designed to ease the use of `mz_now()` by making various timestamp types castable to it.

### Valid casts

For details about casting, including contexts, see [Functions:
Cast](../../functions/cast).

Integer, numeric, and text casts must be in the form of milliseconds since the Unix epoch.

From | To | Required context
-----|----|--------
`mz_timestamp` | `text` | Assignment
`text` | `mz_timestamp` | Assignment
`uint4` | `mz_timestamp` | Implicit
`uint8` | `mz_timestamp` | Implicit
`int4` | `mz_timestamp` | Implicit
`int8` | `mz_timestamp` | Implicit
`numeric` | `mz_timestamp` | Implicit
`timestamp` | `mz_timestamp` | Implicit
`timestamptz` | `mz_timestamp` | Implicit

### Valid operations

There are no supported operations or functions on `mz_timestamp` types.
