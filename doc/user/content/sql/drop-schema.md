---
title: "DROP SCHEMA"
description: "`DROP SCHEMA` removes a schema from Materialize."
menu:
  main:
    parent: commands
---

`DROP SCHEMA` removes a schema from Materialize.

## Syntax

{{< diagram "drop-schema.svg" >}}

Field | Use
------|-----
**IF EXISTS** | Do not return an error if the named schema does not exist.
_schema&lowbar;name_ | The schema you want to remove. For available schemas, see [`SHOW SCHEMAS`](../show-schemas).
**CASCADE** | Remove the schema and its dependent objects.
**RESTRICT** | Do not remove this schema if it contains any sources or views. _(Default)_

## Details

Before you can drop a schema, you must [drop all sources](../drop-source) and
[views](../drop-view) it contains, or use the **CASCADE** option.

## Example

### Remove a schema with no dependent objects
```sql
SHOW SOURCES FROM my_schema;
```
```nofmt
my_file_source
```
```sql
DROP SCHEMA my_schema;
```

### Remove a schema with dependent objects
```sql
SHOW SOURCES FROM my_schema;
```
```nofmt
my_file_source
```
```sql
DROP SCHEMA my_schema CASCADE;
```

### Remove a schema only if it has no dependent objects

You can use either of the following commands:

- ```sql
  DROP SCHEMA my_schema;
  ```
- ```sql
  DROP SCHEMA my_schema RESTRICT;
  ```

### Do not issue an error if attempting to remove a nonexistent schema

```sql
DROP SCHEMA IF EXISTS my_schema;
```

## Privileges

{{< private-preview />}}

The privileges required to execute this statement are:

- Ownership of the dropped schema.
- `USAGE` privileges on the containing database.

## Related pages

- [`SHOW SCHEMAS`](../show-schemas)
- [`CREATE SCHEMA`](../create-schema)
- [DROP OWNED](../drop-owned)
