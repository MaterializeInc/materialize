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
Objects include tables, sources, views, and indexes.

## Syntax

{{< diagram "show-objects.svg" >}}

Field | Use
------|-----
_schema&lowbar;name_ | The schema to show objects from. Defaults to `public` in the current database. For available schemas, see [`SHOW SCHEMAS`](../show-schemas).

## Details

### Output format

`SHOW OBJECTS` will output a table with one column, `name`.

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
  name
----------------
my_table
my_source
my_view
my_other_source
```
```sql
SHOW OBJECTS;
```
```nofmt
  name
----------
my_table
my_source
my_view
```

## Related pages

- [`SHOW TABLES`](../show-tables)
- [`SHOW SOURCES`](../show-sources)
- [`SHOW VIEWS`](../show-views)
- [`SHOW INDEXES`](../show-indexes)
