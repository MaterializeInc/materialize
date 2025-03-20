---
title: "SHOW DATABASES"
description: "`SHOW DATABASES` returns a list of all databases in Materialize."
menu:
  main:
    parent: commands
---

`SHOW DATABASES` returns a list of all databases in Materialize.

## Syntax

```sql
SHOW DATABASES
[LIKE <pattern> | WHERE <condition(s)>]
```

Option                        | Description
------------------------------|------------
**LIKE** \<pattern\>          | If specified, only show databases that match the pattern.
**WHERE** <condition(s)>      | If specified, only show databases that match the condition(s).

## Details

### Output format

`SHOW DATABASES`'s output is a table with one column, `name`.

## Examples

```mzsql
CREATE DATABASE my_db;
```
```mzsql
SHOW DATABASES;
```
```nofmt
materialize
my_db
```
