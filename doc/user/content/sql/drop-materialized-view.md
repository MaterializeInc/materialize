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

```mzsql
DROP MATERIALIZED VIEW [IF EXISTS] <view_name> [RESTRICT|CASCADE];
```

Syntax element | Description
---------------|------------
**IF EXISTS** | Optional. If specified, do not return an error if the named materialized view does not exist.
`<view_name>` | The materialized view you want to drop. For available materialized views, see [`SHOW MATERIALIZED VIEWS`](../show-materialized-views).
**RESTRICT** | Optional. Do not drop this materialized view if any other views depend on it. _(Default)_
**CASCADE** | Optional. If specified, drop the materialized view and all views that depend on this materialized view.

## Examples

### Dropping a materialized view with no dependencies

```mzsql
DROP MATERIALIZED VIEW winning_bids;
```
```nofmt
DROP MATERIALIZED VIEW
```

### Dropping a materialized view with dependencies

```mzsql
DROP MATERIALIZED VIEW winning_bids;
```

```nofmt
ERROR:  cannot drop materialize.public.winning_bids: still depended
upon by catalog item 'materialize.public.wb_custom_art'
```

## Privileges

The privileges required to execute this statement are:

{{< include-md
file="shared-content/sql-command-privileges/drop-materialized-view.md" >}}

## Related pages

- [`CREATE MATERIALIZED VIEW`](../create-materialized-view)
- [`SHOW MATERIALIZED VIEWS`](../show-materialized-views)
- [`SHOW CREATE MATERIALIZED VIEW`](../show-create-materialized-view)
- [`DROP OWNED`](../drop-owned)
