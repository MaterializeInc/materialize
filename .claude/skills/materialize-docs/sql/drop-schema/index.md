---
audience: developer
canonical_url: https://materialize.com/docs/sql/drop-schema/
complexity: intermediate
description: '`DROP SCHEMA` removes a schema from Materialize.'
doc_type: reference
keywords:
- DROP SCHEMA
- IF EXISTS
- SHOW SCHEMAS
- RESTRICT
- CASCADE
product_area: Indexes
status: stable
title: DROP SCHEMA
---

# DROP SCHEMA

## Purpose
`DROP SCHEMA` removes a schema from Materialize.

If you need to understand the syntax and options for this command, you're in the right place.


`DROP SCHEMA` removes a schema from Materialize.



`DROP SCHEMA` removes a schema from Materialize.

## Syntax

This section covers syntax.

```mzsql
DROP SCHEMA [IF EXISTS] <schema_name> [CASCADE|RESTRICT];
```text

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

This section covers example.

### Remove a schema with no dependent objects
```mzsql
SHOW SOURCES FROM my_schema;
```text
```nofmt
my_file_source
```text
```mzsql
DROP SCHEMA my_schema;
```bash

### Remove a schema with dependent objects
```mzsql
SHOW SOURCES FROM my_schema;
```text
```nofmt
my_file_source
```text
```mzsql
DROP SCHEMA my_schema CASCADE;
```bash

### Remove a schema only if it has no dependent objects

You can use either of the following commands:

- ```mzsql
  DROP SCHEMA my_schema;
  ```text
- ```mzsql
  DROP SCHEMA my_schema RESTRICT;
  ```bash

### Do not issue an error if attempting to remove a nonexistent schema

```mzsql
DROP SCHEMA IF EXISTS my_schema;
```

## Privileges

The privileges required to execute this statement are:

- Ownership of the dropped schema.
- `USAGE` privileges on the containing database.


## Related pages

- [`SHOW SCHEMAS`](../show-schemas)
- [`CREATE SCHEMA`](../create-schema)
- [`DROP OWNED`](../drop-owned)

