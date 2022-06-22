---
title: "SHOW VIEWS"
description: "`SHOW VIEWS` returns a list of views in your Materialize instances."
menu:
  main:
    parent: commands
---

`SHOW VIEWS` returns a list of views in your Materialize instances.

## Syntax

{{< diagram "show-views.svg" >}}

Field | Use
------|-----
_schema&lowbar;name_ | The schema to show views from. Defaults to `public` in the current database. For available schemas, see [`SHOW SCHEMAS`](../show-schemas).
**MATERIALIZED** | Only return materialized views, i.e. those with [indexes](../create-index). Without specifying this option, this command returns all views, including non-materialized views.
**FULL** | Return details about your views.

## Details

### Output format for `SHOW FULL VIEWS`

`SHOW FULL VIEWS`'s output is a table, with this structure:

```nofmt
 name  | type | materialized | volatility
-------+------+--------------+-----------
 ...   | ...  | ...          | ...
```

Field | Meaning
------|--------
**name** | The name of the view.
**type** | Whether the view was created by the `user` or the `system`.
**materialized** | Does the view have an in-memory index? For more details, see [`CREATE INDEX`](../create-index).
**volatility** | Whether the view is [volatile](/overview/volatility). Either `volatile`, `nonvolatile`, or `unknown`.

## Examples

### Default behavior

```sql
SHOW VIEWS;
```
```nofmt
         name
-------------------------
 my_nonmaterialized_view
 my_materialized_view
```

### Only show materialized views

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
SHOW FULL VIEWS
```
```nofmt
          name           | type | materialized
-------------------------+------+--------------
 my_nonmaterialized_view | user | f
 my_materialized_view    | user | t
```

## Related pages

- [`SHOW CREATE VIEW`](../show-create-view)
- [`SHOW INDEX`](../show-index)
- [`CREATE VIEW`](../create-view)
