---
title: "MzTimestamp type"
description: "Expresses an internal timestamp"
menu:
  main:
    parent: 'sql-types'
---

`mztimestamp` data expresses an internal timestamp.

## `mztimestamp` info

Detail | Info
-------|------
**Quick Syntax** | `MZTIMESTAMP 946684800000`
**Size** | 8 bytes
**Catalog name** | `pg_catalog.mztimestamp`
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
`mztimestamp` | `text` | Assignment
`text` | `mztimestamp` | Assignment
`uint4` | `mztimestamp` | Implicit
`uint8` | `mztimestamp` | Implicit
`int4` | `mztimestamp` | Implicit
`int8` | `mztimestamp` | Implicit
`numeric` | `mztimestamp` | Implicit
`timestamp` | `mztimestamp` | Implicit
`timestamptz` | `mztimestamp` | Implicit

### Valid operations

There are no supported operations or functions on `mztimestamp` types.
