---
title: "INT Data Type"
description: "Expresses a signed integer"
menu:
  main:
    parent: 'sql-types'
---

`INT` data expresses a signed integer.

Detail | Info
-------|------
**Size** | 8 bytes
**Min value** | -9223372036854775808
**Max value** | 9223372036854775807

## Examples

```sql
SELECT 123::INT AS int_v;
```
```shell
 int_v
-------
   123
```

<hr/>

```sql
SELECT 1.23::INT AS int_v;
```
```shell
 int_v
-------
     1
```
