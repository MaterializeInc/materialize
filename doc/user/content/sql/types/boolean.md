---
title: "boolean type"
description: "Expresses TRUE or FALSE"
menu:
  main:
    parent: 'sql-types'
aliases:
    - sql/types/bool
---

`boolean` data expresses a binary value of either `TRUE` or `FALSE`.

Detail | Info
-------|------
**Quick Syntax** | `TRUE` or `FALSE`
**Size** | 1 byte
**Aliases** | `bool`
**Catalog name** | `pg_catalog.bool`
**OID** | 16

## Syntax

{{< diagram "type-bool.svg" >}}

## Details

### Valid casts

#### From `boolean`

You can [cast](../../functions/cast) from `boolean` to:

- [`int`](../int) (explicitly)
- [`text`](../text) (by assignment)

#### To `boolean`

You can [cast](../../functions/cast) the following types to `boolean`:

- [`int`](../int) (explicitly)
- [`jsonb`](../jsonb) (explicitly)
- [`text`](../text) (explicitly)

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
