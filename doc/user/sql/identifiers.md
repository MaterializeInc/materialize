---
title: "Identifiers"
description: "SQL identifiers are names of elements such as sources and views."
weight: 11
menu:
  main:
    parent: 'sql'
---

In Materialize, identifiers are used to refer to SQL elements like sources, views, and indexes.

## Naming restrictions

Identifiers must:

- Begin with an ASCII letter (i.e. `[a-zA-Z]` in terms of regex)
- Only contain ASCII characters

You can circumvent any of the above rules by double-quoting the the identifier, e.g. `"123_source"` or `"fun_source_😀"`. When using double-quoted identifiers, the quotation marks are part of the identifier and must be used whenever referring to the element.

## Keyword collision

Materialize is very permissive with letting users typical SQL keywords as identifiers (e.g. `name`, `user`). If Materialize cannot use a keyword as an identifier in a particular location, it throws a syntax error.
