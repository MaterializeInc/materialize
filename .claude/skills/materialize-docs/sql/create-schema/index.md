---
audience: developer
canonical_url: https://materialize.com/docs/sql/create-schema/
complexity: intermediate
description: '`CREATE SCHEMA` creates a new schema.'
doc_type: reference
keywords:
- CREATE SCHEMA
- IF NOT EXISTS
- SHOW SCHEMAS
product_area: Indexes
status: stable
title: CREATE SCHEMA
---

# CREATE SCHEMA

## Purpose
`CREATE SCHEMA` creates a new schema.

If you need to understand the syntax and options for this command, you're in the right place.


`CREATE SCHEMA` creates a new schema.



`CREATE SCHEMA` creates a new schema.

## Conceptual framework

Materialize mimics SQL standard's namespace hierarchy, which is:

- Databases (highest level)
- Schemas
- Tables, views, sources
- Columns (lowest level)

Each layer in the hierarchy can contain elements directly beneath it. In this
instance, schemas can contain tables, views, and sources.

For more information, see [Namespaces](../namespaces).

## Syntax

[See diagram: create-schema.svg]

Field | Use
------|-----
**IF NOT EXISTS** | If specified, _do not_ generate an error if a schema of the same name already exists. <br/><br/>If _not_ specified, throw an error if a schema of the same name already exists. _(Default)_
_schema&lowbar;name_ | A name for the schema. <br/><br/>You can specify the database for the schema with a preceding `database_name.schema_name`, e.g. `my_db.my_schema`, otherwise the schema is created in the current database.

## Examples

This section covers examples.

```mzsql
CREATE SCHEMA my_db.my_schema;
```text
```mzsql
SHOW SCHEMAS FROM my_db;
```text
```nofmt
public
my_schema
```

## Privileges

The privileges required to execute this statement are:

- `CREATE` privileges on the containing database.


## Related pages

- [`DROP DATABASE`](../drop-database)
- [`SHOW DATABASES`](../show-databases)

