---
title: "SHOW DATABASES"
description: "`SHOW DATABASES` returns a list of all databases available to your Materialize instances."
menu:
  main:
    parent: commands
---

`SHOW DATABASES` returns a list of all databases available to your Materialize
instances.

## Syntax

{{< diagram "show-databases.svg" >}}

## Details

### Output format

`SHOW DATABASES`'s output is a table with one column, `name`.

{{< version-changed v0.5.0 >}}
The output column is renamed from `DATABASES` to `name`.
{{< /version-changed >}}

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
