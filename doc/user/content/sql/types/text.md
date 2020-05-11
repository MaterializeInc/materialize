---
title: "text Data Type"
description: "Expresses a Unicode string"
aliases:
    - /docs/sql/types/string
    - /docs/sql/types/varchar
menu:
  main:
    parent: 'sql-types'
---

`text` data expresses a Unicode string. This is equivalent to `string` or
`varchar` in other RDBMSes.

Detail | Info
-------|------
**Quick Syntax** | `'foo'`
**Size** | Variable

## Syntax

{{< diagram "type-text.svg" >}}

## Details

### Valid casts

#### From `text`

You can [cast](../../functions/cast) `text` to [all types](../). Casts from
text will error if the string is not valid input for the destination type.

#### To `text`

You can [cast](../../functions/cast) [all types](../) to `text`.

## Examples

```sql
SELECT 'hello' AS text_val;
```
```nofmt
 text_val
---------
 hello
```
