---
title: "CREATE SCHEMA"
description: "`CREATE SCHEMA` creates a new schema."
menu:
  main:
    parent: 'commands'
---

`CREATE SCHEMA` creates a new schema.

## Syntax

{{% include-syntax file="examples/create_schema" example="syntax" %}}

## Details

By default, each database has a schema called `public`.

For more information, see [Namespaces](../namespaces).

## Examples

```mzsql
CREATE SCHEMA my_db.my_schema;
```
```mzsql
SHOW SCHEMAS FROM my_db;
```
```nofmt
public
my_schema
```

## Privileges

The privileges required to execute this statement are:

{{% include-headless "/headless/sql-command-privileges/create-schema" %}}

## Related pages

- [`DROP DATABASE`](../drop-database)
- [`SHOW DATABASES`](../show-databases)
