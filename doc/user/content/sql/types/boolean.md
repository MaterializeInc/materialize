---
title: "boolean Data Type"
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

## Syntax

{{< diagram "type-bool.svg" >}}

## Details

### Valid casts

#### From `boolean`

You can [cast](../../functions/cast) `boolean` to:

- [`text`](../text)

#### To `boolean`

You can [cast](../../functions/cast) the following types to `boolean`:

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
<hr/>
```sql
SELECT FALSE AS f_val;
```
```nofmt
 f_val
-------
 f
```
