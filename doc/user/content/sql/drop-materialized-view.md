---
title: "DROP MATERIALIZED VIEW"
description: "`DROP MATERIALIZED VIEW` removes a materialized view from Materialize."
menu:
  main:
    parent: commands
---

`DROP MATERIALIZED VIEW` removes a materialized view from Materialize. If there
are other views depending on the materialized view, you must explicitly drop
them first, or use the `CASCADE` option.

## Syntax

{{< diagram "drop-materialized-view.svg" >}}

Field | Use
------|-----
**IF EXISTS** | Do not return an error if the named materialized view does not exist.
_view&lowbar;name_ | The materialized view you want to drop. For available materialized views, see [`SHOW MATERIALIZED VIEWS`](../show-materialized-views).
**RESTRICT** | Do not drop this materialized view if any other views depend on it. _(Default)_
**CASCADE** | Drop all views that depend on this materialized view.

## Examples

### Dropping a materialized view with no dependencies

```sql
DROP MATERIALIZED VIEW winning_bids;
```
```nofmt
DROP MATERIALIZED VIEW
```

### Dropping a materialized view with dependencies

```sql
DROP MATERIALIZED VIEW winning_bids;
```

```nofmt
ERROR:  cannot drop materialize.public.winning_bids: still depended
upon by catalog item 'materialize.public.wb_custom_art'
```

## Related pages

- [`CREATE MATERIALIZED VIEW`](../create-materialized-view)
- [`SHOW MATERIALIZED VIEWS`](../show-materialized-views)
- [`SHOW CREATE MATERIALIZED VIEW`](../show-create-materialized-view)
