---
title: "SHOW MATERIALIZED VIEWS"
description: "`SHOW MATERIALIZED VIEWS` returns a list of materialized views in your Materialize instances."
menu:
  main:
    parent: commands
---

`SHOW MATERIALIZED VIEWS` returns a list of materialized views in your Materialize instances.

## Syntax

{{< diagram "show-materialized-views.svg" >}}

Field | Use
------|-----
_schema&lowbar;name_ | The schema to show materialized views from. Defaults to `public` in the current database. For available schemas, see [`SHOW SCHEMAS`](../show-schemas).
_cluster&lowbar;name_ | The cluster to show materialized views from. If omitted, materialized views from all clusters are shown.
**FULL** | Return details about your materialized views.

## Details

### Output format for `SHOW FULL MATERIALIZED VIEWS`

`SHOW FULL MATERIALIZED VIEWS`'s output is a table, with this structure:

```nofmt
 cluster | name  | type
---------+-------+------
 ...     | ...   | ...
```

Field | Meaning
------|--------
**cluster** | The name of the [cluster](/overview/key-concepts/#clusters) maintaining the materialized view.
**name** | The name of the materialized view.
**type** | Whether the materialized view was created by the `user` or the `system`.

## Examples

### Default behavior

```sql
SHOW MATERIALIZED VIEWS;
```
```nofmt
         name
----------------------
 my_materialized_view
```

### Show details about views

```sql
SHOW FULL MATERIALIZED VIEWS;
```
```nofmt
 cluster |         name         | type
---------+----------------------+------
 default | my_materialized_view | user
```

## Related pages

- [`SHOW CREATE MATERIALIZED VIEW`](../show-create-materialized-view)
- [`CREATE MATERIALIZED VIEW`](../create-materialized-view)
