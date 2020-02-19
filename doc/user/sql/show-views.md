---
title: "SHOW VIEWS"
description: "`SHOW VIEWS` returns a list of views in your Materialize instances."
menu:
  main:
    parent: 'sql'
---

`SHOW VIEWS` returns a list of views in your Materialize instances.

## Syntax

{{< diagram "show-views.html" >}}

Field | Use
------|-----
**MATERIALIZED** | Only return materialized views, i.e. those with [indexes](../create-index). Without specifying this option, this command returns all views, including non-materialized views.
**FULL** | Return details about your views.

## Details

### Output format for `SHOW FULL VIEWS`

`SHOW FULL VIEWS`'s output is a table, with this structure:

```nofmt
 VIEWS | TYPE | QUERYABLE | MATERIALIZED
-------+------+-----------+--------------
 ...   | ...  | ...       | ...
```

Field | Meaning
------|--------
**VIEWS** | The name of the view
**TYPE** | Whether the view was created by the user or the system
**QUERYABLE** | Can the view process [`SELECT`](../select) statements? You can always `SELECT FROM...` materialized views, but non-materialized views [have a specific set of requirements](../create-view/#selecting-from-non-materialized-views).
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
          VIEWS          | TYPE | QUERYABLE | MATERIALIZED
-------------------------+------+-----------+--------------
 my_nonmaterialized_view | USER | t         | f
 my_materialized_view    | USER | t         | t
```

In this case `my_nonmaterialized_view` would only be queryable if it depended on `my_materialized_view`, e.g.

```sql
CREATE VIEW my_nonmaterialized_view AS
    SELECT col2, col3 FROM my_materialized_view;
```

## Related pages

- [`SHOW CREATE VIEW`](../show-create-view)
- [`SHOW INDEX`](../show-index)
- [`CREATE VIEW`](../create-view)
