---
title: "Identifiers"
description: "SQL identifiers are names of elements such as sources and views."
weight: 11
menu:
  main:
    parent: 'sql'
---

In Materialize, identifiers are used to refer to elements of your SQL nodes like sources, views, and indexes.

## Naming restrictions

Identifiers must begin with an ASCII letter (i.e. `[a-zA-Z]` in terms of regex) and only contain ASCII characters.

However, you can circumvent any naming restriction by placing the string in double-quotes, e.g. `"123_source"`, `"fun_source_😀"`. When using double-quoted identifiers, the quotation marks are part of the identifer and must be used whenever referring to the element.

## Keyword collision

Materialize is very permissive with letting users typical SQL keywords as identifiers (e.g. `name`, `user`). If Materialize cannot use a keyword as an identifer in a particular location, it throws a syntax error.
