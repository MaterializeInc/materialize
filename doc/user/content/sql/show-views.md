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
**FULL** | Return details about your views.

## Details

### Output format for `SHOW FULL VIEWS`

`SHOW FULL VIEWS`'s output is a table, with this structure:

```nofmt
 name  | type
-------+------
 ...   | ...
```

Field | Meaning
------|--------
**name** | The name of the view.
**type** | Whether the view was created by the `user` or the `system`.

## Examples

### Default behavior

```sql
SHOW VIEWS;
```
```nofmt
  name
---------
 my_view
```

### Show details about views

```sql
SHOW FULL VIEWS;
```
```nofmt
  name   | type
---------+------
 my_view | user
```

## Related pages

- [`SHOW CREATE VIEW`](../show-create-view)
- [`CREATE VIEW`](../create-view)
