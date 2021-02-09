---
title: "bytea Data Type"
description: "Expresses a Unicode string"
aliases:
    - /sql/types/string
    - /sql/types/varchar
menu:
  main:
    parent: 'sql-types'
---

The `bytea` data type allows the storage of [binary strings](https://www.postgresql.org/docs/9.0/datatype-binary.html) or what is typically thought of as "raw bytes". Materialize supports both the typical formats for input and output: PostgreSQL's escape format and the hex format.

## Hex format

The "hex" format encodes binary data as 2 hexadecimal digits per byte, most significant nibble first. The entire string is preceded by the sequence `\x` (to distinguish it from the escape format). The hexadecimal digits can be either upper or lower case, and whitespace is permitted between digit pairs (but not within a digit pair nor in the starting `\x` sequence). The hex format is compatible with a wide range of external applications and protocols, and it tends to be faster to convert than the escape format, so its use is preferred.

## PostgreSQL escape format

Detail | Info
-------|------
**Quick Syntax** | ``
**Aliases** |  ``
**Size** | Variable
**Catalog name** | ``
**OID** |

## Syntax

### Standard

{{< diagram "type-bytea.svg" >}}

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

You can [cast](../../functions/cast) `text` to [all types](../). Casts from text
will error if the string is not valid input for the destination type.

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
