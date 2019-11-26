---
title: "BOOLEAN Data Type"
description: "Expresses TRUE or FALSE"
menu:
  main:
    parent: 'sql-types'
---

`BOOLEAN` data expresses a binary value of either `TRUE` or `FALSE`.

Detail | Info
-------|------
**Quick Syntax** | `TRUE` or `FALSE`
**Size** | 1 byte

## Syntax

{{< diagram "type-bool.html" >}}

## Examples

```sql
SELECT TRUE AS t_val;
```
```shell
 t_val
-------
 t
```
<hr/>
```sql
SELECT FALSE AS f_val;
```
```shell
 f_val
-------
 f
```
