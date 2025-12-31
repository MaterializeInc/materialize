---
audience: developer
canonical_url: https://materialize.com/docs/sql/functions/coalesce/
complexity: intermediate
description: Returns the first non-NULL element provided.
doc_type: reference
keywords:
- COALESCE function
- SELECT COALESCE
product_area: Indexes
status: stable
title: COALESCE function
---

# COALESCE function

## Purpose
Returns the first non-NULL element provided.

If you need to understand the syntax and options for this command, you're in the right place.


Returns the first non-NULL element provided.



`COALESCE` returns the first non-`NULL` element provided.

## Signatures

Parameter | Type | Description
----------|------|------------
val | [Any](../../types) | The values you want to check.

### Return value

All elements of the parameters for `coalesce` must be of the same type; `coalesce` returns that type, or _NULL_.

## Examples

This section covers examples.

```mzsql
SELECT coalesce(NULL, 3, 2, 1) AS coalesce_res;
```text
```nofmt
 res
-----
   3
```

