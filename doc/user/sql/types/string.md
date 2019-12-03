---
title: "string Data Type"
description: "Expresses a Unicode string"
aliases:
    - /docs/sql/types/text
    - /docs/sql/types/varchar
    - /docs/sql/types/char
menu:
  main:
    parent: 'sql-types'
---

`string` data expresses a Unicode string. This is equivalent to `text` or `varchar` in RDBMSes.

Detail | Info
-------|------
**Quick Syntax** | `'foo'`
**Size** | Variable

## Syntax

{{< diagram "type-string.html" >}}

## Details

### Valid casts

#### From `string`

You cannot cast `string` to any other type.

#### To `string`

You can cast [all types](../) to `string`.

## Examples

```sql
SELECT 'hello' AS str_val;
```
```nofmt
 str_val
---------
 hello
```
