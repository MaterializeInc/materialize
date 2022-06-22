---
title: "text type"
description: "Expresses a Unicode string"
aliases:
    - /sql/types/string
    - /sql/types/varchar
menu:
  main:
    parent: 'sql-types'
---

`text` data expresses a Unicode string. This is equivalent to `string` or
`varchar` in other RDBMSes.

Detail | Info
-------|------
**Quick Syntax** | `'foo'`
**Aliases** | `string`
**Size** | Variable
**Catalog name** | `pg_catalog.text`
**OID** | 25

## Syntax

### Standard

{{< diagram "type-text.svg" >}}

To escape a single quote character (`'`) in a standard string literal, write two
adjacent single quotes:

```sql
SELECT 'single''quote' AS output
```
```nofmt
   output
------------
single'quote
```

All other characters are taken literally.

### Escape

A string literal that is preceded by an `e` or `E` is an "escape" string
literal:

{{< diagram "type-escape-text.svg" >}}

Escape string literals follow the same rules as standard string literals, except
that backslash character (`\`) starts an escape sequence. The following escape
sequences are recognized:

Escape sequence | Meaning
----------------|--------
`\b`  | Backspace
`\f`  | Form feed
`\n`  | Newline
`\r`  | Carriage return
`\t`  | Tab
`\uXXXX`, `\UXXXXXXXX`  | Unicode codepoint, where `X` is a hexadecimal digit

Any other character following a backslash is taken literally, so `\\` specifies
a literal backslash, and `\'` is an alternate means of escaping the single quote
character.

Unlike in PostgreSQL, there are no escapes that produce arbitrary byte values,
in order to ensure that escape string literals are always valid UTF-8.

## Details

### Valid casts

#### From `text`

You can [cast](../../functions/cast) `text` to [all types](../) except [`record`](../../types/record/). All casts are explicit. Casts from text
will error if the string is not valid input for the destination type.

#### To `text`

You can [cast](../../functions/cast) [all types](../) to `text`. All casts are by assignment.

## Examples

```sql
SELECT 'hello' AS text_val;
```
```nofmt
 text_val
---------
 hello
```

<hr>

```sql
SELECT E'behold\nescape strings\U0001F632' AS escape_val;
```
```nofmt
   escape_val
-----------------
 behold         +
 escape strings😲
```
