---
title: "SHOW OBJECTS"
description: "`SHOW OBJECTS` returns a list of all objects available to your Materialize instances."
menu:
  main:
    parent: commands
aliases:
    - /sql/show-object
---

`SHOW OBJECTS` returns a list of all objects available to your Materialize instances in a given schema.
Objects include tables, sources, sinks, views, indexes, secrets and connections.

## Syntax

{{< diagram "show-objects.svg" >}}

Field | Use
------|-----
_schema&lowbar;name_ | The schema to show objects from. Defaults to `public` in the current database. For available schemas, see [`SHOW SCHEMAS`](../show-schemas).

## Details

### Output format

`SHOW OBJECTS` will output a table with two columns, `name`and `type`.

## Examples

```sql
SHOW SCHEMAS;
```
```nofmt
  name
--------
 public
```
```sql
SHOW OBJECTS FROM public;
```
```nofmt
  name          | type
----------------+-------
my_table        | table
my_source       | source
my_view         | view
my_other_source | source
```
```sql
SHOW OBJECTS;
```
```nofmt
  name    | type
----------+-------
my_table  | table
my_source | source
my_view   | view
```

## Related pages

- [`SHOW TABLES`](../show-tables)
- [`SHOW SOURCES`](../show-sources)
- [`SHOW SINKS`](../show-sinks)
- [`SHOW VIEWS`](../show-views)
- [`SHOW INDEXES`](../show-indexes)
- [`SHOW SECRETS`](../show-secrets)
- [`SHOW CONNECTIONS`](../show-connections)
