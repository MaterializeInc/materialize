---
title: "SHOW VIEWS"
description: "`SHOW VIEWS` returns a list of views in your Materialize instances."
menu:
  main:
    parent: 'sql'
---

`SHOW VIEWS` returns a list of views in your Materialize instances.

## Syntax

{{< diagram "show-views.svg" >}}

Field | Use
------|-----
_schema&lowbar;name_ | The schema to show sources from. Defaults to `public` in the current database. For available schemas, see [`SHOW SCHEMAS`](../show-schemas).
**MATERIALIZED** | Only return materialized views, i.e. those with [indexes](../create-index). Without specifying this option, this command returns all views, including non-materialized views.
**FULL** | Return details about your views.

## Details

### Output format for `SHOW FULL VIEW`

`SHOW FULL VIEW`'s output is a table, with this structure:

```nofmt
 VIEWS | TYPE | MATERIALIZED
-------+------+--------------
 ...   | ...  | ...
```

Field | Meaning
------|--------
**VIEWS** | The name of the view
**TYPE** | Whether the view was created by the user or the system
**MATERIALIZED** | Does the view have an in-memory index? For more details, see [`CREATE INDEX`](../create-index)

## Examples

### Default behavior

```sql
SHOW VIEWS;
```
```nofmt
         VIEWS
-------------------------
 my_nonmaterialized_view
 my_materialized_view
```

### Only show materialized views

```sql
SHOW MATERIALIZED VIEWS;
```
```nofmt
        VIEWS
----------------------
 my_materialized_view
```

### Show details about views

```sql
SHOW FULL VIEWS
```
```nofmt
          VIEWS          | TYPE | MATERIALIZED
-------------------------+------+--------------
 my_nonmaterialized_view | USER | f
 my_materialized_view    | USER | t
```

```sql
CREATE VIEW my_nonmaterialized_view AS
    SELECT col2, col3 FROM my_materialized_view;
```

## Related pages

- [`SHOW CREATE VIEW`](../show-create-view)
- [`SHOW INDEX`](../show-index)
- [`CREATE VIEW`](../create-view)
