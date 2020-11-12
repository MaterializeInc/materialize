---
title: "Identifiers"
description: "SQL identifiers are names of columns and database objects such as sources and views."
weight: 11
menu:
  main:
    parent: 'sql'
---

In Materialize, identifiers are used to refer to columns and database objects
like sources, views, and indexes.

## Naming restrictions

- The first character of an identifer must be an ASCII letter
  (`a`-`z` and `A`-`Z`), an underscore (`_`), or any non-ASCII character.

- The remaining characters of an identifier must be ASCII letters
  (`a`-`z` and `A`-`Z`), ASCII digits (`0`-`9`), underscores (`_`),
  dollar signs (`$`), or any non-ASCII characters.

You can circumvent any of the above rules by double-quoting the the identifier,
e.g. `"123_source"` or `"fun_source_@"`. All characters inside a quoted
identifier are taken literally, except that double-quotes must be escaped by
writing two adjacent double-quotes, as in `"includes""quote"`.

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

The keywords known to the latest [unstable build](/versions/#unstable-builds)
of Materialize are listed below. Note that new keywords may be added in any
release.

{{< kwlist >}}
