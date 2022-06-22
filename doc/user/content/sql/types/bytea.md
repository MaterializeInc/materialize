---
title: "bytea type"
description: "Expresses a Unicode string"
aliases:
    - /sql/types/string
    - /sql/types/varchar
menu:
  main:
    parent: 'sql-types'
---

The `bytea` data type allows the storage of [binary strings](https://www.postgresql.org/docs/9.0/datatype-binary.html) or what is typically thought of as "raw bytes". Materialize supports both the typical formats for input and output: the hex format and the historical PostgreSQL escape format. The hex format is preferred.

Hex format strings are preceded by `\x` and escape format strings are preceded by `\`.

For more information about `bytea`, see the [PostgreSQL binary data type documentation](https://www.postgresql.org/docs/13/datatype-binary.html#id-1.5.7.12.9).

Detail | Info
-------|------
**Quick Syntax** | `'\xDEADBEEF'` (hex),  `'\000'` (escape)
**Size** | 1 or 4 bytes plus the actual binary string
**Catalog name** | `pg_catalog.bytea`
**OID** | 17

## Syntax

### Hex format

{{< diagram "type-bytea-hex.svg" >}}

In some cases, the initial backslash may need to be escaped by doubling it (`\\`). For more information, see the PostgreSQL documentation on [string constants](https://www.postgresql.org/docs/13/sql-syntax-lexical.html#SQL-SYNTAX-STRINGS).

### Escape format

{{< diagram "type-bytea-esc.svg" >}}

In the escape format, octet values can be escaped by converting them into their three-digit octal values and preceding them with backslashes; the backslash itself can be escaped as a double backslash. While any octet value *can* be escaped, the values in the table below *must* be escaped.

Decimal octet value | Description | Escaped input representation | Example | Hex representation
------------|--------|----|-----------|----
0  | zero octet | `'\000'` | `'\000'::bytea` | `\x00`
39  | single quote |`''''` or `'\047'` | `''''::bytea` | `\x27`
92  | backslash | `'\\' or '\134'` | `'\\'::bytea` | `\x5c`
0 to 31 and 127 to 255  | "non-printable" octets | `'\xxx'` (octal value) | `'\001'::bytea` | `\x01`

## Details

### Valid casts

#### From `bytea`

You can [cast](../../functions/cast) `bytea` to [text](../text) by assignment. Casts from `bytea`
will error if the string is not valid input for the destination type.

#### To `bytea`

You can explicitly [cast](../../functions/cast) [text](../text) to `bytea`.

## Examples

```sql
SELECT '\xDEADBEEF'::bytea AS bytea_val;
```
```nofmt
 bytea_val
---------
 \xdeadbeef
```

<hr>

```sql
SELECT '\000'::bytea AS bytea_val;
```
```nofmt
   text_val
-----------------
 \x00
```
