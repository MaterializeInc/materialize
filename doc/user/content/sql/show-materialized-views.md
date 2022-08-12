---
title: "SHOW MATERIALIZED VIEWS"
description: "`SHOW MATERIALIZED VIEWS` returns a list of materialized views being maintained in Materialize."
menu:
  main:
    parent: commands
---

`SHOW MATERIALIZED VIEWS` returns a list of materialized views being maintained
in Materialize.

## Syntax

{{< diagram "show-materialized-views.svg" >}}

Field | Use
------|-----
_schema&lowbar;name_ | The schema to show materialized views from. Defaults to `public` in the current database. For available schemas, see [`SHOW SCHEMAS`](../show-schemas).
_cluster&lowbar;name_ | The cluster to show materialized views from. If omitted, materialized views from all clusters are shown.
**FULL** | Returns all materialized views, including their associated cluster and type.

## Examples

```sql
SHOW MATERIALIZED VIEWS;
```

```nofmt
     name
--------------
 winning_bids
```

```sql
SHOW FULL MATERIALIZED VIEWS;
```

```nofmt
 cluster |     name     | type
---------+--------------+------
 default | winning_bids | user
```

```sql
SHOW MATERIALIZED VIEWS LIKE '%bid%';
```

```nofmt
     name
--------------
 winning_bids
```

## Related pages

- [`SHOW CREATE MATERIALIZED VIEW`](../show-create-materialized-view)
- [`CREATE MATERIALIZED VIEW`](../create-materialized-view)
