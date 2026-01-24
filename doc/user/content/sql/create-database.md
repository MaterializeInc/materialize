---
title: "CREATE DATABASE"
description: "`CREATE DATABASE` creates a new database."
menu:
  main:
    parent: 'commands'
---

Use `CREATE DATABASE` to create a new database.

## Syntax

{{% include-syntax file="examples/create_database" example="syntax" %}}

## Details

Databases can contain schemas. By default, each database has a schema called
`public`. For more information about databases, see
[Namespaces](/sql/namespaces).

## Examples

```mzsql
CREATE DATABASE IF NOT EXISTS my_db;
```
```mzsql
SHOW DATABASES;
```
```nofmt
materialize
my_db
```

## Privileges

The privileges required to execute this statement are:

{{% include-headless "/headless/sql-command-privileges/create-database" %}}

## Related pages

- [`DROP DATABASE`](../drop-database)
- [`SHOW DATABASES`](../show-databases)
