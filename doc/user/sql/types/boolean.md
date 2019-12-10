---
title: "boolean Data Type"
description: "Expresses TRUE or FALSE"
menu:
  main:
    parent: 'sql-types'
---

`boolean` data expresses a binary value of either `TRUE` or `FALSE`.

Detail | Info
-------|------
**Quick Syntax** | `TRUE` or `FALSE`
**Size** | 1 byte

## Syntax

{{< diagram "type-bool.html" >}}

## Details

### Valid casts

#### From `boolean`

You cannot cast `boolean` to any other type.

#### To `boolean`

You can cast [`int`](../int) to `boolean`.

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
