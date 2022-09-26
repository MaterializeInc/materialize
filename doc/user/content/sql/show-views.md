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

## Details

### Output format for `SHOW VIEWS`

`SHOW VIEWS`'s output is a table, with this structure:

```nofmt
 name
-------
 ...
```

Field | Meaning
------|--------
**name** | The name of the view.

## Examples

```sql
SHOW VIEWS;
```
```nofmt
  name
---------
 my_view
```

## Related pages

- [`SHOW CREATE VIEW`](../show-create-view)
- [`CREATE VIEW`](../create-view)
