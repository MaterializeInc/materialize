---
audience: developer
canonical_url: https://materialize.com/docs/sql/show-databases/
complexity: intermediate
description: '`SHOW DATABASES` returns a list of all databases in Materialize.'
doc_type: reference
keywords:
- LIKE
- WHERE
- SHOW DATABASES
product_area: Indexes
status: stable
title: SHOW DATABASES
---

# SHOW DATABASES

## Purpose
`SHOW DATABASES` returns a list of all databases in Materialize.

If you need to understand the syntax and options for this command, you're in the right place.


`SHOW DATABASES` returns a list of all databases in Materialize.



`SHOW DATABASES` returns a list of all databases in Materialize.

## Syntax

This section covers syntax.

```sql
SHOW DATABASES
[LIKE <pattern> | WHERE <condition(s)>]
;
```text

Syntax element                | Description
------------------------------|------------
**LIKE** \<pattern\>          | If specified, only show databases that match the pattern.
**WHERE** <condition(s)>      | If specified, only show databases that match the condition(s).

## Details

This section covers details.

### Output format

`SHOW DATABASES`'s output is a table with one column, `name`.

## Examples

This section covers examples.

```mzsql
CREATE DATABASE my_db;
```text
```mzsql
SHOW DATABASES;
```text
```nofmt
materialize
my_db
```

