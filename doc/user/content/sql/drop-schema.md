---
title: "DROP SCHEMA"
description: "`DROP SCHEMA` removes a schema from Materialize."
menu:
  main:
    parent: commands
---

`DROP SCHEMA` removes a schema from Materialize.

## Syntax

```mzsql
DROP SCHEMA [IF EXISTS] <schema_name> [CASCADE|RESTRICT];
```

Syntax element | Description
---------------|------------
**IF EXISTS** | Optional. If specified, do not return an error if the named schema does not exist.
`<schema_name>` | The schema you want to remove. For available schemas, see [`SHOW SCHEMAS`](../show-schemas).
**CASCADE** | Remove the schema and its dependent objects.
**RESTRICT** | Do not remove this schema if it contains any sources or views. _(Default)_

## Details

Before you can drop a schema, you must [drop all sources](../drop-source) and
[views](../drop-view) it contains, or use the **CASCADE** option.

## Example

### Remove a schema with no dependent objects
```mzsql
SHOW SOURCES FROM my_schema;
```
```nofmt
my_file_source
```
```mzsql
DROP SCHEMA my_schema;
```

### Remove a schema with dependent objects
```mzsql
SHOW SOURCES FROM my_schema;
```
```nofmt
my_file_source
```
```mzsql
DROP SCHEMA my_schema CASCADE;
```

### Remove a schema only if it has no dependent objects

You can use either of the following commands:

- ```mzsql
  DROP SCHEMA my_schema;
  ```
- ```mzsql
  DROP SCHEMA my_schema RESTRICT;
  ```

### Do not issue an error if attempting to remove a nonexistent schema

```mzsql
DROP SCHEMA IF EXISTS my_schema;
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/drop-schema.md" >}}

## Related pages

- [`SHOW SCHEMAS`](../show-schemas)
- [`CREATE SCHEMA`](../create-schema)
- [`DROP OWNED`](../drop-owned)
