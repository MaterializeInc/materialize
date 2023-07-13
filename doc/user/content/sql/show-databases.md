---
title: "SHOW DATABASES"
description: "`SHOW DATABASES` returns a list of all databases in Materialize."
menu:
  main:
    parent: commands
---

`SHOW DATABASES` returns a list of all databases in Materialize.

## Syntax

{{< diagram "show-databases.svg" >}}

## Details

### Output format

`SHOW DATABASES`'s output is a table with one column, `name`.

## Examples

```sql
CREATE DATABASE my_db;
```
```sql
SHOW DATABASES;
```
```nofmt
materialize
my_db
```
