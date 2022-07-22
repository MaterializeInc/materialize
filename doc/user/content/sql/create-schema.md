---
title: "CREATE SCHEMA"
description: "`CREATE SCHEMA` creates a new schema."
menu:
  main:
    parent: 'commands'
---

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

{{< diagram "create-schema.svg" >}}

Field | Use
------|-----
**IF NOT EXISTS** | If specified, _do not_ generate an error if a schema of the same name already exists. <br/><br/>If _not_ specified, throw an error if a schema of the same name already exists. _(Default)_
_schema&lowbar;name_ | A name for the schema. <br/><br/>You can specify the database for the schema with a preceding `database_name.schema_name`, e.g. `my_db.my_schema`, otherwise the schema is created in the current database.

## Examples

```sql
CREATE SCHEMA my_db.my_schema;
```
```sql
SHOW SCHEMAS FROM my_db;
```
```nofmt
public
my_schema
```

## Related pages

- [DROP DATABASE](../drop-database)
- [SHOW DATABASES](../show-databases)
