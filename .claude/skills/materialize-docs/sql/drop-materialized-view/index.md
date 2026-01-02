---
audience: developer
canonical_url: https://materialize.com/docs/sql/drop-materialized-view/
complexity: intermediate
description: '`DROP MATERIALIZED VIEW` removes a materialized view from Materialize.'
doc_type: reference
keywords:
- DROP MATERIALIZED VIEW
- DROP THEM
- IF EXISTS
- RESTRICT
- CASCADE
- DROP MATERIALIZED
product_area: Views
status: stable
title: DROP MATERIALIZED VIEW
---

# DROP MATERIALIZED VIEW

## Purpose
`DROP MATERIALIZED VIEW` removes a materialized view from Materialize.

If you need to understand the syntax and options for this command, you're in the right place.


`DROP MATERIALIZED VIEW` removes a materialized view from Materialize.



`DROP MATERIALIZED VIEW` removes a materialized view from Materialize. If there
are other views depending on the materialized view, you must explicitly drop
them first, or use the `CASCADE` option.

## Syntax

This section covers syntax.

```mzsql
DROP MATERIALIZED VIEW [IF EXISTS] <view_name> [RESTRICT|CASCADE];
```text

Syntax element | Description
---------------|------------
**IF EXISTS** | Optional. If specified, do not return an error if the named materialized view does not exist.
`<view_name>` | The materialized view you want to drop. For available materialized views, see [`SHOW MATERIALIZED VIEWS`](../show-materialized-views).
**RESTRICT** | Optional. Do not drop this materialized view if any other views depend on it. _(Default)_
**CASCADE** | Optional. If specified, drop the materialized view and all views that depend on this materialized view.

## Examples

This section covers examples.

### Dropping a materialized view with no dependencies

```mzsql
DROP MATERIALIZED VIEW winning_bids;
```text
```nofmt
DROP MATERIALIZED VIEW
```bash

### Dropping a materialized view with dependencies

```mzsql
DROP MATERIALIZED VIEW winning_bids;
```text

```nofmt
ERROR:  cannot drop materialize.public.winning_bids: still depended
upon by catalog item 'materialize.public.wb_custom_art'
```

## Privileges

The privileges required to execute this statement are:

- Ownership of the dropped materialized view.
- `USAGE` privileges on the containing schema.


## Related pages

- [`CREATE MATERIALIZED VIEW`](../create-materialized-view)
- [`SHOW MATERIALIZED VIEWS`](../show-materialized-views)
- [`SHOW CREATE MATERIALIZED VIEW`](../show-create-materialized-view)
- [`DROP OWNED`](../drop-owned)

