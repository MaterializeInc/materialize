---
audience: developer
canonical_url: https://materialize.com/docs/sql/types/text/
complexity: intermediate
description: Expresses a Unicode string
doc_type: reference
keywords:
- Quick Syntax
- Catalog name
- OID
- Aliases
- Size
- text type
- SELECT E
product_area: Indexes
status: stable
title: text type
---

# text type

## Purpose
Expresses a Unicode string

If you need to understand the syntax and options for this command, you're in the right place.


Expresses a Unicode string



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

This section covers syntax.

### Standard

[See diagram: type-text.svg]

To escape a single quote character (`'`) in a standard string literal, write two
adjacent single quotes:

```mzsql
SELECT 'single''quote' AS output
```text
```nofmt
   output
------------
single'quote
```text

All other characters are taken literally.

### Escape

A string literal that is preceded by an `e` or `E` is an "escape" string
literal:

[See diagram: type-escape-text.svg]

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

This section covers details.

### Valid casts

#### From `text`

You can [cast](../../functions/cast) `text` to [all types](../) except [`record`](../../types/record/). All casts are explicit. Casts from text
will error if the string is not valid input for the destination type.

#### To `text`

You can [cast](../../functions/cast) [all types](../) to `text`. All casts are by assignment.

## Examples

This section covers examples.

```mzsql
SELECT 'hello' AS text_val;
```text
```nofmt
 text_val
---------
 hello
```text

<hr>

```mzsql
SELECT E'behold\nescape strings\U0001F632' AS escape_val;
```text
```nofmt
   escape_val
-----------------
 behold         +
 escape strings😲
```

