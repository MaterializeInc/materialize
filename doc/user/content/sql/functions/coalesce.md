---
title: "COALESCE function"
description: "Returns the first non-NULL element provided."
menu:
  main:
    parent: 'sql-functions'
---

`COALESCE` returns the first non-`NULL` element provided.

## Signatures

Parameter | Type | Description
----------|------|------------
val | [Any](../../types) | The values you want to check.

### Return value

All elements of the parameters for `coalesce` must be of the same type; `coalesce` returns that type, or _NULL_.

## Examples

```sql
SELECT coalesce(NULL, 3, 2, 1) AS coalesce_res;
```
```nofmt
 res
-----
   3
```
