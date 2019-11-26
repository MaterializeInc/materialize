---
title: "STRING Data Type"
description: "Expresses a Unicode string"
menu:
  main:
    parent: 'sql-types'
---

`STRING` data expresses a Unicode string.

Detail | Info
-------|------
**Size** | Variable

## Syntax

{{< diagram "type-string.html" >}}

## Examples

```sql
SELECT 'hello' AS str_val;
```
```shell
 str_val
---------
 hello
```
