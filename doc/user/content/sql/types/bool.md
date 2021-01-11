---
title: "bool Data Type"
description: "Expresses TRUE or FALSE"
menu:
  main:
    parent: 'sql-types'
aliases:
    - sql/types/boolean
---

`bool` data expresses a binary value of either `TRUE` or `FALSE`.

Detail | Info
-------|------
**Quick Syntax** | `TRUE` or `FALSE`
**Size** | 1 byte
**Aliases** | `boolean`
**Catalog name** | `pg_catalog.bool`
**OID** | 16

## Syntax

{{< diagram "type-bool.svg" >}}

## Details

### Valid casts

#### From `bool`

You can [cast](../../functions/cast) `bool` to:

- [`text`](../text)
- [`int`](../int)

#### To `bool`

You can [cast](../../functions/cast) the following types to `bool`:

- [`int`](../int)
- [`text`](../text)

## Examples

```sql
SELECT TRUE AS t_val;
```
```nofmt
 t_val
-------
 t
```

```sql
SELECT FALSE AS f_val;
 f_val
-------
 f
```
