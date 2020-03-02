---
title: "DROP SCHEMA"
description: "`DROP SCHEMA` removes a schema from your Materialize instances."
menu:
  main:
    parent: 'sql'
---

`DROP SCHEMA` removes a schema from your Materialize instances.

## Syntax

{{< diagram "drop-schema.html" >}}

Field | Use
------|-----
**IF EXISTS** | Do not return an error if the specified schema does not exist.
_schema&lowbar;name_ | The schema you want to drop. For available schemas, see [`SHOW SCHEMAS`](../show-schemas).
**RESTRICT** | Do not drop this schema if it contains any sources or views. _(Default)_
**CASCADE** | Drop all views and sources within this schema.

## Details

Before you can drop a schema, you must [drop all sources](../drop-source) and
[views](../drop-view) it contains, or use the **CASCADE** option.

## Example

```sql
SHOW SOURCES FROM my_schema
```
```nofmt
my_file_source
```
```sql
DROP SCHEMA my_schema
```
```nofmt
schema 'my_schema' cannot be dropped without CASCADE while it contains objects
```
```sql
DROP SCHEMA my_schema CASCADE
```

## Related pages

- [`SHOW SCHEMAS`](../show-schemas)
- [`CREATE SCHEMA`](../create-schema)
