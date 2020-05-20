---
title: "SHOW DATABASES"
description: "`SHOW DATABASES` returns a list of all databases available to your Materialize instances."
menu:
  main:
    parent: 'sql'
---

`SHOW DATABASES` returns a list of all databases available to your Materialize
instances.

## Syntax

{{< diagram "show-databases.svg" >}}

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
