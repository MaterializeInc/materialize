---
title: "Identifiers"
description: "SQL identifiers are names of columns and database objects such as sources and views."
menu:
  main:
    parent: reference
    name: 'SQL identifiers'
    weight: 125
---

In Materialize, identifiers are names used to refer to columns and database
objects like sources, views, and indexes.

## Naming restrictions

Materialize has the following naming restrictions for identifiers:

| Position            | Allowed Characters                                                                 |
|---------------------|------------------------------------------------------------------------------------|
| **First character** | ASCII letters (`a`-`z`, `A`-`Z`), underscore (`_`), or any non-ASCII character     |
| **Remaining**       | ASCII letters (`a`-`z`, `A`-`Z`), digits (`0`-`9`), underscores (`_`), dollar sign (`$`), or any non-ASCII character |

To override these restrictions, you can enclose the identifier in double quotes;
e.g., `"123_source"` or `"fun_source_@"`. When enclosed in double-quotes, all
characters in the identifier are interpreted literally with the exception double
quotes. To include double quotes within a double-quoted identifier, you can
escape them by writing two adjacent double-quotes, as in `"includes""quote"`.

{{< note >}}
The identifiers `"."` and `".."` are not allowed.
{{</ note >}}

## Case sensitivity

Materialize performs case folding (the caseless comparison of text) for identifiers, which means that identifiers are effectively case-insensitive (`foo` is the same as `FOO` is the same as `fOo`). This can cause issues when column names come from data sources which do support case-sensitive names, such as Avro-formatted sources.

To avoid conflicts, double-quote all field names (`"field_name"`) when working with case-sensitive sources.

## Keyword collision

Materialize is very permissive with accepting SQL keywords as identifiers (e.g.
`offset`, `user`). If Materialize cannot use a keyword as an
identifier in a particular location, it throws a syntax error. You can wrap the
identifier in double quotes to force Materialize to interpret the word as an
identifier instead of a keyword.

For example, `SELECT offset` is invalid, because it looks like a mistyping of
`SELECT OFFSET <n>`. You can wrap the identifier in double quotes, as in
`SELECT "offset"`, to resolve the error.

We recommend that you avoid using keywords as identifiers whenever possible, as
the syntax errors that result are not always obvious.

The current keywords are listed below.

{{< kwlist >}}
