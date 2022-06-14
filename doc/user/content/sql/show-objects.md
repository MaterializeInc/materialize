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

`SHOW OBJECTS`'s output is a table with one column, `name`. `SHOW FULL OBJECTS` will output a table with
two columns, `name` and `type`. `type` indicates whether the object was created by the `system` or a `user`.

## Examples

```sql
SHOW SCHEMAS;
```
```nofmt
public
```
```sql
SHOW OBJECTS FROM public;
```
```nofmt
my_table
my_source
my_view
my_other_source
```
```sql
SHOW OBJECTS;
```
```nofmt
my_table
my_source
my_view
```

```sql
SHOW FULL OBJECTS;
```
```nofmt
my_table
my_source
my_view
my_other_source
```

```sql
SHOW EXTENDED FULL OBJECTS;
```
```nofmt
my_table        user
my_source       user
my_view         user
my_other_source user
builtin_view    system
```

## Related pages

- [`SHOW TABLES`](../show-tables)
- [`SHOW SOURCES`](../show-sources)
- [`SHOW VIEWS`](../show-views)
- [`SHOW INDEXES`](../show-indexes)
