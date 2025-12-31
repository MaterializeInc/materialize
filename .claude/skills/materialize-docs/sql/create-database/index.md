---
audience: developer
canonical_url: https://materialize.com/docs/sql/create-database/
complexity: intermediate
description: '`CREATE DATABASE` creates a new database.'
doc_type: reference
keywords:
- CREATE DATABASE
- IF NOT EXISTS
- SHOW DATABASES
product_area: Indexes
status: stable
title: CREATE DATABASE
---

# CREATE DATABASE

## Purpose
`CREATE DATABASE` creates a new database.

If you need to understand the syntax and options for this command, you're in the right place.


`CREATE DATABASE` creates a new database.



`CREATE DATABASE` creates a new database.

## Conceptual framework

Materialize mimics SQL standard's namespace hierarchy, which is:

- Databases (highest level)
- Schemas
- Tables, views, sources
- Columns (lowest level)

Each layer in the hierarchy can contain elements directly beneath it. In this
instance, databases can contain schemas.

For more information, see [Namespaces](../namespaces).

## Syntax

[See diagram: create-database.svg]

Field | Use
------|-----
**IF NOT EXISTS** | If specified, _do not_ generate an error if a database of the same name already exists. <br/><br/>If _not_ specified, throw an error if a database of the same name already exists. _(Default)_
_database&lowbar;name_ | A name for the database.

## Details

For details about databases, see [Namespaces: Database
details](../namespaces/#database-details).

## Examples

This section covers examples.

```mzsql
CREATE DATABASE IF NOT EXISTS my_db;
```text
```mzsql
SHOW DATABASES;
```text
```nofmt
materialize
my_db
```

## Privileges

The privileges required to execute this statement are:

- `CREATEDB` privileges on the system.


## Related pages

- [`DROP DATABASE`](../drop-database)
- [`SHOW DATABASES`](../show-databases)

